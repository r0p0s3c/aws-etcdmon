#!/usr/bin/env python
import time
import sys
import json
import argparse
import urlparse
import etcd
import os
import random
from datetime import datetime
import dbus
import logging
import boto3
from enum import Enum

# set up logger and args as globals so everyone can use it
# we do things this way so modules/libraries we use don't log at our level, whatever it is
logger = logging.getLogger(sys.argv[0])
args = None

# constrain msg type for msgs in etcd queue
class EtcdMsgType(Enum):
    LEADERMUTEX = 1
    PEERMSG = 2

class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)

def as_enum(d):
    if "__enum__" in d:
        name, member = d["__enum__"].split(".")
        return getattr(globals()[name], member)
    else:
        return d



def _getsystemd():
    sysbus = dbus.SystemBus()
    systemd1 = sysbus.get_object('org.freedesktop.systemd1', '/org/freedesktop/systemd1')
    manager = dbus.Interface(systemd1, 'org.freedesktop.systemd1.Manager')
    return manager

# check if etcd2 unit is active but only recently started
def checketcdstarting(startsecs):
    po = dbus.SystemBus().get_object('org.freedesktop.systemd1',_getsystemd().GetUnit('etcd2.service'))
    state = str(po.Get('org.freedesktop.systemd1.Unit','ActiveState',dbus_interface='org.freedesktop.DBus.Properties'))
    starttime = float(po.Get('org.freedesktop.systemd1.Unit','ActiveEnterTimestamp',dbus_interface='org.freedesktop.DBus.Properties'))

    logger.debug("etcd2 state=%s starttime=%f"%(state,starttime))

    if (state == 'active' or state == 'activating') and (starttime+(startsecs*1000000) >= (time.time()*1000000)):
        return True
    return False

def writeetcdconf(path, initclusterstate, cluster):
    if initclusterstate == 2:
        initclusterstate = 'existing'
    elif initclusterstate == 1:
        initclusterstate='new'
    else:
        raise RuntimeError('writeetcdconf called with initclusterstate==%s'%initclusterstate)

    unitstr = '[Service]\nEnvironment=ETCD_NAME=%s\nEnvironment=ETCD_INITIAL_CLUSTER_STATE=%s\nEnvironment=ETCD_INITIAL_CLUSTER=%s\n'%(os.environ['COREOS_EC2_INSTANCE_ID'], initclusterstate,','.join(["%s=%s"%(id, uri) for id, uri in cluster.iteritems()]))

    with open(path,'w') as conffile:
        logger.debug('writing etcd initial config to %s'%path)
        conffile.write(unitstr)

    logger.debug('reloading systemd')
    _getsystemd().Reload()

def restartetcd():
    logger.debug('restarting etcd2')
    restartunit('etcd2.service')

def restartunit(unit):
    try:
        _getsystemd().RestartUnit(unit, 'replace')
    except dbus.exceptions.DBusException as e:
        logger.warning('Exception restarting unit: %s'%e)

def getasmsg(queue, msgfilterfunc=lambda body: 'LifecycleTransition' in body):
    msgs = getmsgs(queue)
    msg = None

    if msgs:
        msg = msgs[0]
        body = json.loads(msg.body)
        if 'Event' in body and body['Event']=="autoscaling:TEST_NOTIFICATION":
            # delete test msg and call ourselves to get the next one
            logger.debug('got a test msg')
            msg.delete()
            return getasmsg(queue, msgfilterfunc)
        elif msgfilterfunc(body):
            # handle instance start/delete
            logger.debug('got a matching AS msg')
        else:
            logger.debug('did not get a matching AS msg')
            msg = None

    return msg

# attempt to get leadership mutex; check etcd membership if it is passed
def getleadermutex(queue,etcdconn=None):
    msgs = getetcdqmsg(queue,EtcdMsgType.LEADERMUTEX,10)
    leaderid = None

    if etcdconn:
        leaderid = etcdconn.leader['name']

    # we always put a new msg if we are leader to keep them fresh
    if (not msgs and not etcdconn) or (leaderid == os.environ['COREOS_EC2_INSTANCE_ID']):
        for msg in msgs:
            m = json.loads(msg.body,object_hook=as_enum)
            if m['leaderid']!=os.environ['COREOS_EC2_INSTANCE_ID']:
                logger.debug('deleting old leadermutex msg (old leaderid=%s)'%m['leaderid'])
                msg.delete()
        logger.debug('enqueuing new leadermutex msg')
        putmsg(queue,json.dumps({'msgtype':EtcdMsgType.LEADERMUTEX,'leaderid':os.environ['COREOS_EC2_INSTANCE_ID']},cls=EnumEncoder),args.msggroupid)
        return True

    return False

# get a message of msgtype msgtype from etcd q
def getetcdqmsg(queue,msgtype=EtcdMsgType.PEERMSG,maxmsgs=1):
    msgs = getmsgs(queue,maxmsgs=10)
    matchingmsgs = []

    for msg in msgs:
        m = json.loads(msg.body,object_hook=as_enum)
        if m['msgtype'] == msgtype:
            logger.debug('got a matching etcd queue msg')
            matchingmsgs+=[msg]

        if len(matchingmsgs)==maxmsgs:
            break

    if not matchingmsgs:
        logger.debug('did not get matching etcd queue msg')
    return matchingmsgs

# get message from etcd queue and check we are in it
# iff return that, otherwise None
def getetcdpeersmsg(queue):
    msgs = getetcdqmsg(queue,EtcdMsgType.PEERMSG)
    peers = None

    if msgs:
        msg = msgs[0]
        logger.debug('got possible etcd peer msg')
        plist = json.loads(msg.body)['peers']

        # check if our machineid is in cluster, otherwise not for us
        if os.environ['COREOS_EC2_INSTANCE_ID'] in plist:
            peers = plist
            msg.delete()
        else:
            logger.debug('etdc peer msg doesn\'t contain our machineid, not for us')
    return peers


def getmsgs(queue,maxmsgs=1):
    msgs = queue.receive_messages(MaxNumberOfMessages=maxmsgs)
    
    if msgs:
        logger.debug('%d msgs received from queue %s'%(len(msgs),queue))
    else:
        logger.debug('did not get msg from queue %s'%queue)

    return msgs

# expects msgbody to be string
def putmsg(queue, msgbody, groupid):
    logger.debug('putting msg (groupid: %s) %s -> %s'%(groupid, msgbody, queue))

    if groupid:
        queue.send_message(MessageBody=msgbody,MessageGroupId=groupid)
    else:
        queue.send_message(MessageBody=msgbody)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('region', help="region")
    parser.add_argument('asqueueurl', help="URL of autoscaler queue")
    parser.add_argument('etcdqueueurl', help="URL of etcd queue")
    parser.add_argument('msggroupid', help="Token to use for message grouping")
    parser.add_argument('-d', '--debug', action="store_true", help="enable debug logging")
    parser.add_argument('-i', '--unitwaittime', type=int, default=30, help="number of \
            seconds to wait for unit startup/restart/reload")
    parser.add_argument('-w', '--waittime', type=int, default=15, help="number of seconds \
            to wait between checking for change in leadership or queue messages")
    parser.add_argument('-r', '--waitrand', type=float, default=0.1, help="randomisation to use with wait times")
    parser.add_argument('-f', '--etcdconfpath', default="/etc/systemd/system/etcd2.service.d/10-init.conf", help="directory to write etcd systemd unit override/conf to")
    parser.add_argument('-s', '--etcdstatedir', default="/var/lib/etcd2/member", help="etcd2 state dir to check")
    parser.add_argument('-u', '--restartunitfile', default="/restartunit.list", help="file listing units to restart on etcd2 membership change")
    global args
    args = parser.parse_args()


    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

# create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

# create formatter
    formatter = logging.Formatter('[%(funcName)s:%(lineno)s] %(message)s')

# add formatter to ch
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    if args.debug:
        logger.debug('logging at level DEBUG')
        logger.debug('REGION=%s ASQUEUEURL=%s ETCDQUEUEURL=%s'%(args.region,args.asqueueurl,args.etcdqueueurl))
    else:
        logger.info('logging at level INFO')

# initclusterstate: 0 == no init required, 1 == new, 2 == existing
    initclusterstate = 0
    memberids = None
    cluster = dict()
    asqueue = boto3.resource('sqs', region_name=args.region).Queue(args.asqueueurl)
    etcdqueue = boto3.resource('sqs', region_name=args.region).Queue(args.etcdqueueurl)

# check if etcd2 is up
    while checketcdstarting(args.unitwaittime):
        logger.debug('etcd2 still starting, waiting')
        time.sleep(args.unitwaittime)

    etcdclient = etcd.Client(host=os.environ['COREOS_EC2_IPV4_LOCAL'], port=2379)

# try to connect to etcdclient
#   if etcd2 is not initialised
#       if we can get mutex
#           initialise etcd2 with us as sole cluster member
#       else
#           get etcd peer list
#           initialise etcd2
#   restart etcd2

    try:
        etcdclient.leader
    except etcd.EtcdException:
        # could not connect to etcd, do init thing
        logger.debug('initial connect to etcd failed, likely unconfigured')

        if not getleadermutex(etcdqueue):
            logger.debug('did not get leadership, getting peer list')
            peers = getetcdpeersmsg(etcdqueue)

            # wait until we have peers
            while not peers:
                logger.debug('waiting for list of peers')
                time.sleep(max(1,1+random.uniform(-1*args.waitrand,args.waitrand)))
                peers = getetcdpeersmsg(etcdqueue)

            logger.debug('got list of peers (%s), initialising'%peers.keys())
            writeetcdconf(args.etcdconfpath, 2 , peers)
        else:
            logger.debug('initialising etcd config as single member/leader')
            writeetcdconf(args.etcdconfpath, 1 , {os.environ['COREOS_EC2_INSTANCE_ID']:"http://%s:2380"%os.environ['COREOS_EC2_IPV4_LOCAL']})
        # start etcd
        restartetcd()

#   while true:
#       if we are leader:
#           get mutex
#           XXX deal with if we can't get mutex?
#           get AS msg
#           if AS msg is for us
#              ignore
#           else
#              send etcd peerlist msg
# 

    while True:
        logger.debug('in monitoring loop')
        try:
            if etcdclient.leader['name'] == os.environ['COREOS_EC2_INSTANCE_ID']:
                if not getleadermutex(etcdqueue,etcdclient):
                    logger.error('we are leader but could not get mutex, bouncing etcd2 to try to lose leadership')
                    restartetcd()
                else:
                    logger.debug('we are leader, processing messages')
                    msg = getasmsg(asqueue)

                    if msg:
                        body = json.loads(msg.body)
                        logger.debug('msg received: %s',body)

                        if body['LifecycleTransition'] == 'autoscaling:EC2_INSTANCE_TERMINATING':
                            # delete from cluster after checking
                            memberid = [k for k, v in etcdclient.members.iteritems() if v['name']==body['EC2InstanceId']]
                            if memberid:
                                memberid = memberid[0]
                                logger.info('deleting member %s'%memberid)
                                try:
                                    etcdclient.deletemember(memberid)
                                    msg.delete()
                                except etcd.EtcdException as e:
                                    logger.warning('error deleting member %s (%s): %s'%(memberid, body['EC2InstanceId'], e))
                            else:
                                logger.info('ignoring delete of non-member with machineid %s'%body['EC2InstanceId'])
                        elif body['LifecycleTransition'] == 'autoscaling:EC2_INSTANCE_LAUNCHING':
                            # sendmsg with peerURLs to queue for new member to configure themselves
                            logger.info('launch of %s detected'%body['EC2InstanceId'])

                            # check if it's a msg for us
                            if body['EC2InstanceId'] == os.environ['COREOS_EC2_INSTANCE_ID']:
                                logger.info('got an AS msg for ourselves, ignoring')
                                msg.delete()
                            else:
                                bsmembers = [k for k,v in etcdclient.members.iteritems() if v['name']=='' or not v['clientURLs']]

                                if bsmembers:
                                    # there is one member that is still bootstrapping/joining, do not add more
                                    # otherwise we will have quorum issues
                                    # note message visibility means the AS messages are not visible in the queue
                                    # as we are reading them, but we will eventually process them and add all members
                                    logger.warning('member %s is still bootstrapping/joining, delaying add of %s'%(bsmembers[0],body['EC2InstanceId']))
                                    # reset the AS message time to now so other waiting potential members
                                    # do not assume there is no leader
                                    body['Time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                                    putmsg(asqueue,json.dumps(body),None)
                                    logger.debug('re-adding AS message for %s with time reset to now'%body['EC2InstanceId'])
                                else:
                                    # get new instance's ip from aws
                                    ip = boto3.resource('ec2', region_name=args.region).Instance(body['EC2InstanceId']).private_ip_address

                                    if ip:
                                        # check if ip already member
                                        if ip not in [urlparse.urlparse(v['peerURLs'][0]).hostname for k, v in etcdclient.members.iteritems()]:
                                            logger.info('sending peerlist to etcd queue')
                                            putmsg(etcdqueue, json.dumps({'msgtype':EtcdMsgType.PEERMSG,'peers':dict({k:v['peerURLs'][0] for k, v in etcdclient.members.iteritems()}.items() + {body['EC2InstanceId']:"http://%s:2380"%ip}.items())},cls=EnumEncoder),args.msggroupid)
                                            logger.info('adding new member %s to cluster'%ip)
                                            # add to cluster
                                            etcdclient.addmember("http://%s:2380"%ip)
                                        else:
                                            logger.info('%s already in member list, not adding'%ip)
                                    else:
                                        logger.info('could not get ip for instance %s, ignoring'%body['EC2InstanceId'])

                                msg.delete()
                        else:
                            logger.warning('unknown AS msg received')
            else:
                logger.debug('not leader')

            # see if there has been a cluster membership change
            newmemberids = sorted([v['name'] for k,v in etcdclient.members.iteritems() if v['name']])
            if memberids != newmemberids:
                logger.info('cluster membership change, restarting units')

                if memberids:
                    logger.debug('(old)memberids = %s'%(','.join(memberids)))
                logger.debug('newmemberids = %s'%(','.join(newmemberids)))
                # restart units
                with open(args.restartunitfile,'r') as f:
                    for unit in f:
                        unit = unit.rstrip()
                        logger.debug('restarting unit %s'%unit)
                        restartunit(unit)
                        time.sleep(args.unitwaittime)
                memberids = newmemberids

        except etcd.EtcdException:
            logger.debug('could not connect to etcd, sleeping')
            # sleep for a bit
        sleeptime = max(1,args.waittime+(args.waittime*random.uniform(-1*args.waitrand,args.waitrand)))
        logger.debug('sleeping for %f seconds'%sleeptime)
        time.sleep(sleeptime)


if __name__ == "__main__":
    main()

# vim: ts=4:sw=4
