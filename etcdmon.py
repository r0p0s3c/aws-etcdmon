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

parser = argparse.ArgumentParser()
parser.add_argument('region', help="region")
parser.add_argument('asqueueurl', help="URL of autoscaler queue")
parser.add_argument('etcdqueueurl', help="URL of etcd queue")
parser.add_argument('-d', '--debug', action="store_true", help="enable debug logging")
parser.add_argument('-n', '--dryrun', action="store_true", help="read-only operation: \
        do not delete/modify anything")
parser.add_argument('-i', '--initwaittime', type=int, default=120, help="number of \
        seconds to wait on startup before assuming no leader exists")
parser.add_argument('-w', '--waittime', type=int, default=15, help="number of seconds \
        to wait between checking for change in leadership or queue messages")
parser.add_argument('-r', '--waitrand', type=float, default=0.1, help="randomisation to use with wait times")
parser.add_argument('-f', '--etcdconfpath', default="/etc/systemd/system/etcd2.service.d/10-init.conf", help="directory to write etcd systemd unit override/conf to")
parser.add_argument('-s', '--etcdstatedir', default="/var/lib/etcd2/member", help="etcd2 state dir to check")
args = parser.parse_args()

# we do things this way so modules/libraries we use don't log at our level, whatever it is
logger = logging.getLogger(sys.argv[0])

if args.debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(message)s')

# add formatter to ch
ch.setFormatter(formatter)

logger.addHandler(ch)

if args.debug:
    logger.debug('logging at level DEBUG')
    logger.debug('REGION=%s ASQUEUEURL=%s ETCDQUEUEURL=%s'%(args.region,args.asqueueurl,args.etcdqueueurl))
else:
    logger.info('logging at level INFO')


def _getsystemd():
    sysbus = dbus.SystemBus()
    systemd1 = sysbus.get_object('org.freedesktop.systemd1', '/org/freedesktop/systemd1')
    manager = dbus.Interface(systemd1, 'org.freedesktop.systemd1.Manager')
    return manager

def writeetcdconf(path, initclusterstate, cluster):
    if initclusterstate:
        initclusterstate = 'existing'
    else:
        initclusterstate='new'

    unitstr = '[Service]\nEnvironment=ETCD_NAME=%s\nEnvironment=ETCD_INITIAL_CLUSTER_STATE=%s\nEnvironment=ETCD_INITIAL_CLUSTER=%s\n'%(os.environ['COREOS_EC2_INSTANCE_ID'], initclusterstate,','.join(["%s=%s"%(id, uri) for id, uri in cluster.iteritems()]))

    if not args.dryrun:
        with open(path,'w') as conffile:
            logger.debug('writing etcd initial config to %s'%path)
            conffile.write(unitstr)
    else:
        logger.debug('would\'ve written %s to %s'%(unitstr, path))

    if not args.dryrun:
        logger.debug('reloading systemd')
        _getsystemd().Reload()
    else:
        logger.debug('would\'ve reloaded systemd')

def restartetcd():
    if args.dryrun:
        logger.debug('would\'ve restarted etcd2')
    else:
        logger.debug('restarting etcd2')
        _getsystemd().RestartUnit('etcd2.service', 'replace')

def getasmsg(queue, msgfilterfunc=lambda body: 'LifecycleTransition' in body):

    msg = getmsg(queue)

    if msg:
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

# get peers message from etcd queue
# returns the peers msg unaltered
def getetcdpeers(queue):
    msg = getmsg(queue)
    peers = None

    if msg:
        logger.debug('got an etcd peer msg')
        peers = json.loads(msg.body)

        if args.dryrun:
            logger.debug('would\'ve deleted etcd peer msg')
        else:
            msg.delete()
    return peers


def getmsg(queue):
    msg = queue.receive_messages(MaxNumberOfMessages=1)
    
    if msg:
        logger.debug('got a msg from queue %s'%queue)
        msg = msg[0]
    else:
        logger.debug('did not get msg from queue %s'%queue)

    return msg

def putmsg(queue, msgbody):
    logger.debug('putting msg %s -> %s'%(msgbody, queue))
    queue.send_message(MessageBody=msgbody)

# initclusterstate: 0 == new, 1 == existing
initclusterstate = 1
cluster = dict()
asqueue = boto3.resource('sqs', region_name=args.region).Queue(args.asqueueurl)
etcdqueue = boto3.resource('sqs', region_name=args.region).Queue(args.etcdqueueurl)

# check if etcd2 is up
etcdclient = etcd.Client(host=os.environ['COREOS_EC2_IPV4_LOCAL'], port=2379)

# logic is to loop while checking:
# - if etcdq has a message, assume we are not leader and configure etcd with given peers
# - if as q has a message, check msg time to allow chance for potentially existing leader
#   to clear it
# keep looping until one or the other happens

while True:
    # always try to connect to etcd in case it was restarting
    try:
        leader = etcdclient.leader
    except etcd.EtcdException:
        # could not connect to etcd, do init thing
        logger.debug('could not connect to etcd, initialising config')

        # check if there is an etcdqueue msg, if so we are non-leader
        cluster = getetcdpeers(etcdqueue)
        if cluster:
            initclusterstate = 1
            break
        else:
            # check if there is a launch msg for us, if so make sure it's old enough before
            # assuming we are leader
            msg = getasmsg(asqueue, msgfilterfunc=lambda body: 'LifecycleTransition' in body and body['LifecycleTransition']=='autoscaling:EC2_INSTANCE_LAUNCHING' and body['EC2InstanceId'] == os.environ['COREOS_EC2_INSTANCE_ID'])

            if msg:
                logger.debug('found lifecycle message in as queue')
                body = json.loads(msg.body)
                # check if event time is too old
                if (datetime.now() - datetime.strptime(body['Time'], '%Y-%m-%dT%H:%M:%S.%fZ')).total_seconds() > args.initwaittime:
                    logger.info('going to be leader')
                    # assume we need to be leader
                    if args.dryrun:
                        logger.debug('would\'ve deleted as msg')
                    else:
                        logger.debug('deleted as msg')
                        msg.delete()

                    initclusterstate = 0
                    break
                else:
                    # there was a message in the queue, but it was too recent
                    # sleep to allow leader to clear queue
                    logger.debug('lifecycle message too recent, sleeping')
                    time.sleep(args.initwaittime+(args.initwaittime*random.uniform(-1*args.waitrand,args.waitrand)))

# init cluster var in case it is None
if not cluster:
    cluster = dict()

cluster[os.environ['COREOS_EC2_INSTANCE_ID']]="http://%s:2380"%os.environ['COREOS_EC2_IPV4_LOCAL']
logger.info('cluster peers are %s'%','.join([v for k, v in cluster.iteritems()]))
# write etcd systemd config
writeetcdconf(args.etcdconfpath, initclusterstate, cluster)
# start etcd
restartetcd()
# connect to etcd
while True:
    try:
        etcdclient.leader
        logger.debug('etcd up, proceeding')
        break
    except etcd.EtcdException:
        logger.debug('could not connect to etcd, sleeping')
        time.sleep(1)


# start main monitoring loop
while True:
    logger.debug('in monitoring loop')
    leaderid = etcdclient.leader['name']

    if leaderid == os.environ['COREOS_EC2_INSTANCE_ID']:
        # we are leader, check and deal with queue messages
        logger.debug('we are leader, processing messages')
        # leaders loop more frequently to beat non-leader members at everything
        waittime = args.waittime/4
        msg = getasmsg(asqueue)

        if msg:
            body = json.loads(msg.body)

            if body['LifecycleTransition'] == 'autoscaling:EC2_INSTANCE_TERMINATING':
                # delete from cluster after checking
                memberid = [k for k, v in etcdclient.members.iteritems() if v['name']==body['EC2InstanceId']]
                
                if memberid:
                    # due to list comprehension
                    memberid = memberid[0]
                    if args.dryrun:
                        logger.info('would\'ve deleted member %s'%memberid)
                    else:
                        logger.info('deleting member %s'%memberid)
                        try:
                            etcdclient.deletemember(memberid)
                        except etcd.EtcdException as e:
                            logger.warning('error deleting member %s (%s): %s'%(memberid, body['EC2InstanceId'], e))
                else:
                    logger.info('ignoring delete of non-member with machineid %s'%body['EC2InstanceId'])
                msg.delete()
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
                        # waittime needs to be even shorter to catch the new member quickly
                        waittime = waittime/4
                        # reset the AS message time to now so other waiting potential members
                        # do not assume there is no leader
                        body['Time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        msg.delete()
                        putmsg(asqueue,body)
                        logger.debug('re-adding AS message for %s with time reset to now'%body['EC2InstanceId'])
                    else:
                        # get new instance's ip from aws
                        ip = boto3.resource('ec2', region_name=args.region).Instance(body['EC2InstanceId']).private_ip_address

                        if ip:
                            # check if ip already member
                            if ip not in [urlparse.urlparse(v['peerURLs'][0]).hostname for k, v in etcdclient.members.iteritems()]:
                                logger.info('sending peerlist to etcd queue')
                                putmsg(etcdqueue, json.dumps({k:v['peerURLs'][0] for k, v in etcdclient.members.iteritems()}))

                                if args.dryrun:
                                    logger.info('would\'ve added new member %s to cluster'%ip)
                                else:
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
        waittime = args.waittime
        logger.debug('not leader')

    # sleep for a bit
    sleeptime = waittime+(waittime*random.uniform(-1*args.waitrand,args.waitrand))
    logger.debug('sleeping for %f seconds'%sleeptime)
    time.sleep(sleeptime)
