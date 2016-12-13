#!/usr/bin/env python
import json
import argparse
import etcd
import os
from datetime import datetime
import dbus
import logging
import time
import boto3
import sys
import urlparse

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
parser.add_argument('-f', '--etcdconfpath', default="/etc/systemd/system/etcd2.service.d/10-init.conf", help="directory to write etcd systemd unit override/conf to")
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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

logger.addHandler(ch)

def _getsystemd():
    sysbus = dbus.SystemBus()
    systemd1 = sysbus.get_object('org.freedesktop.systemd1', '/org/freedesktop/systemd1')
    manager = dbus.Interface(systemd1, 'org.freedesktop.systemd1.Manager')
    return manager

def writeetcdconf(path, initclusterstate, cluster):
    if initclusterstate:
        initclusterstate='existing'
    else:
        initclusterstate='new'

    unitstr = '[Service]\nEnvironment=ETCD_NAME=%s\nEnvironment=ETCD_INITIAL_CLUSTER_STATE=%s\nEnvironment=ETCD_INITIAL_CLUSTER=%s\n'%(os.environ['COREOS_EC2_INSTANCE_ID'],initclusterstate,','.join(["%s=%s"%(id,uri) for id,uri in cluster.iteritems()]))

    if not args.dryrun:
        with open(path,'w') as conffile:
            logger.debug('writing etcd initial config to %s'%path)
            conffile.write(unitstr)
    else:
        logger.debug('would\'ve written %s to %s'%(unitstr,path))

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
            return getasmsg(queue,msgfilterfunc)
        elif msgfilterfunc(body):
            # handle instance start/delete
            logger.debug('got a matching AS msg')
        else:
            logger.debug('did not get a matching AS msg')

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
    logger.debug('putting msg %s -> %s'%(msgbody,queue))
    queue.send_message(MessageBody=msgbody)

# initclusterstate: 0 == new, 1 == existing
initclusterstate=1
cluster = {}
asqueue = boto3.resource('sqs',region_name=args.region).Queue(args.asqueueurl)
etcdqueue = boto3.resource('sqs',region_name=args.region).Queue(args.etcdqueueurl)

# check if etcd2 is up
etcdclient = etcd.Client(host=os.environ['COREOS_EC2_IPV4_LOCAL'],port=2379)
try:
    leader = etcdclient.leader
except etcd.EtcdException:
    # could not connect to etcd, do init thing
    logger.debug('could not connect to etcd, initialising config')

    while True:
        # check if there is a launch msg for us, if so make sure it's old enough before
        # assuming we are leader
        msg = getasmsg(asqueue,msgfilterfunc=lambda body: 'LifecycleTransition' in body and body['LifecycleTransition']=='autoscaling:EC2_INSTANCE_LAUNCHING' and body['EC2InstanceId'] == os.environ['COREOS_EC2_INSTANCE_ID'])

        if msg:
            logger.debug('found lifecycle message in as queue')
            body = json.loads(msg.body)
            # check if event time is too old
            if (datetime.now() - datetime.strptime(body['Time'],'%Y-%m-%dT%H:%M:%S.%fZ')).total_seconds() > args.initwaittime:
                logger.info('going to be leader')
                # assume we need to be leader
                if args.dryrun:
                    logger.debug('would\'ve deleted as msg')
                else:
                    logger.debug('deleted as msg')
                    msg.delete()

                initclusterstate=0
                cluster = {os.environ['COREOS_EC2_INSTANCE_ID']:"http://%s:2380"%os.environ['COREOS_EC2_IPV4_LOCAL']}
                break
            else:
                # there was a message in the queue, but it was too recent
                # sleep to allow leader to clear queue
                logger.debug('lifecycle message too recent, sleeping')
                time.sleep(args.initwaittime)
        else:
            # no messages in queue, presume we are not going to be leader
            logger.info('no lifecycle message in queue, assuming non-leader')
            initclusterstate=1
            break

    if initclusterstate == 1:
        # check etcdqueue and wait for peer information from leader
        while not cluster:
            cluster = getetcdpeers(etcdqueue)
            if not cluster:
                logger.debug('did not get peers, sleeping to try again')
                time.sleep(args.initwaittime)
        cluster[os.environ['COREOS_EC2_INSTANCE_ID']]="http://%s:2380"%os.environ['COREOS_EC2_IPV4_LOCAL']
        logger.info('cluster peers are %s'%','.join([v for k,v in cluster.iteritems()]))
    # write etcd systemd config
    writeetcdconf(args.etcdconfpath,initclusterstate,cluster)
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
        logger.debug('we are leader, processing messages')
        # we are leader, check and deal with queue messages
        msg = getasmsg(asqueue)

        if msg:
            body = json.loads(msg.body)

            if body['LifecycleTransition'] == 'autoscaling:EC2_INSTANCE_TERMINATING':
                # delete from cluster
                if args.dryrun:
                    logger.info('would\'ve deleted member %s'%body['EC2InstanceId'])
                else:
                    logger.info('deleting member %s'%body['EC2InstanceId'])
                    try:
		    	etcdclient.deletemember(body['EC2InstanceId'])
		    except etcd.EtcdException as e:
			logger.warning('error deleting memberi %s'%body['EC2InstanceId'])
                msg.delete()
            elif body['LifecycleTransition'] == 'autoscaling:EC2_INSTANCE_LAUNCHING':
                # sendmsg with peerURLs to queue for new member to configure themselves
                logger.info('launch of %s detected'%body['EC2InstanceId'])

                # check if it's a msg for us
                if body['EC2InstanceId'] == os.environ['COREOS_EC2_INSTANCE_ID']:
                    logger.info('got an AS msg for ourselves, ignoring')
                else:
                    # get new instance's ip from aws
                    ip = boto3.resource('ec2',region_name=args.region).Instance(body['EC2InstanceId']).private_ip_address

                    if ip:
                        # check if ip already member
                        if ip not in [urlparse.urlparse(v['peerURLs'][0]).hostname for k,v in etcdclient.members.iteritems()]:
                            logger.info('sending peerlist to etcd queue')
                            putmsg(etcdqueue,json.dumps({k:v['peerURLs'][0] for k,v in etcdclient.members.iteritems()}))

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
        logger.debug('not leader')

    # sleep for a bit
    logger.debug('sleeping for a bit')
    time.sleep(args.waittime)
