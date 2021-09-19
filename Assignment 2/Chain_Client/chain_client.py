# simple gRPC client for SJSU CS 249 chain replication assignment

import click
import kazoo.client
from colorama import (Fore,Style)
import time
import grpc

import chain_pb2
import chain_pb2_grpc

zk = None
base_path = None

# uses the base_path to create a canonicalized path from a znode name
def zk_full_name(name):
    return f"{base_path}/{name}"

# this will get either a head or tail stub depending on the truth
# value of ishead
def get_head_tail(ishead):
    childs = zk.get_children(base_path)
    # children don't come back in order
    childs = sorted([c for c in childs if c.startswith('replica-')])
    if childs:
        data = zk.get(zk_full_name(childs[0 if ishead else -1])) 
        # the get returns binary data and stat, we just care about the data
        # use decode() to make it a string
        hostport = data[0].decode().split("\n")[0]
        channel = grpc.insecure_channel(hostport)
        return chain_pb2_grpc.HeadChainReplicaStub(channel) if ishead else chain_pb2_grpc.TailChainReplicaStub(channel)
    return None


@click.group()
@click.argument('zkhostport')
@click.argument('control_path')
def chain_client(zkhostport, control_path):
    global zk, base_path
    zk = kazoo.client.KazooClient(hosts=zkhostport)
    zk.start()
    base_path = control_path

@chain_client.command()
@click.argument('key')
def get(key):
    '''get the value of KEY'''
    tail = get_head_tail(ishead=False)
    reply = tail.get(chain_pb2.GetRequest(key=key))
    print(f"rc={reply.rc}: {key} is {reply.value}")


@chain_client.command()
@click.argument('key')
@click.argument('increment', type=click.INT)
def inc(key, increment):
    '''increment the value of KEY'''
    head = get_head_tail(ishead=True)
    reply = head.increment(chain_pb2.IncRequest(key=key, incValue=increment))
    print(f"rc={reply.rc}: {key} incremented by {increment}")


@chain_client.command()
def watch_chain():
    '''watch the chain for replica changes'''
    # nice! look at this magical annotation that kazoo has!
    @zk.ChildrenWatch(base_path)
    def watch_children(childs):
        # filter only the replica- znodes and sort them (get_children doesn't return in order)
        childs = sorted([c for c in childs if c.startswith('replica-')])
        if childs:
            data = [zk.get(zk_full_name(child))[0].decode().replace('\n', '-')[7:30] for child in childs]
            click.echo(f"{Fore.RED}=>{Style.RESET_ALL}".join(data))
        else:
            click.echo(f"{Fore.RED}no chain{Style.RESET_ALL}")

    # just loop forever. the above function will get invoked whenever the children change
    while True:
        time.sleep(10)

if __name__ == '__main__':
    chain_client()
