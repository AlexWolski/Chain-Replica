# simple gRPC client for SJSU CS 249 chain replication assignment

import click
import kazoo.client
from colorama import (Fore,Style)
import threading
import time
import grpc

import chain_pb2
import chain_pb2_grpc

import chain_debug_pb2
import chain_debug_pb2_grpc

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
    '''get the value of KEY'''
    head = get_head_tail(ishead=True)
    reply = head.increment(chain_pb2.IncRequest(key=key, incValue=increment))
    print(f"rc={reply.rc}: {key} incremented by {increment}")


@chain_client.command()
@click.option("--once/--continuous", default=False,
              help="once causes the chain to just be printed once and then exit")
@click.option("--znode/--data", default=False,
              help="show the znode name in the chain or list the host/port and name info")
def watch_chain(once, znode):
    '''watch the chain for replica changes'''
    # nice! look at this magical annotation that kazoo has!
    @zk.ChildrenWatch(base_path)
    def watch_children(childs):
        # filter only the replica- znodes and sort them (get_children doesn't return in order)
        childs = sorted([c for c in childs if c.startswith('replica-')])
        if childs:
            data = [child if znode else zk.get(zk_full_name(child))[0].decode().replace('\n', '-')[7:35] for child in childs]
            click.echo(f"{Fore.RED}=>{Style.RESET_ALL}".join(data))
        else:
            click.echo(f"{Fore.RED}no chain{Style.RESET_ALL}")
        if once:
            exit(0)

    # just loop forever. the above function will get invoked whenever the children change
    while True:
        time.sleep(10)

@chain_client.command()
@click.argument("znode")
@click.option("--log-limit", default=10, show_default=True,
              help="the limit of log entries to show from each replica")
@click.option("--show-data/--no-show-data", default=True, show_default=True,
              help="show the state of the replica")
def debug(znode, log_limit, show_data):
    '''list the debug information for the replica corresponding to the znode.
       only the number is needed for the znode name. replica-0000 will be auto prepended.
       if "all" is passed. every znode in the chain will be checked
    '''
    if znode == "all":
        znodes = [child for child in zk.get_children(base_path) if child.startswith("replica-")]
        znodes.sort()
    elif not znode.startswith("replica-"):
        znodes=[f"replica-{int(znode):0>10d}"]
    else:
        znodes = [znode]

    for znode in znodes:
        try:
            click.echo("--------------------------------")
            click.echo(f"{Style.BRIGHT}{znode}{Style.RESET_ALL}:")
            # if znode doesn't start with replica- assume that only the number portion is being passed
            full_name = zk_full_name(znode)
            data = zk.get(full_name)[0].decode()
            click.echo(data)
            # first line of the data is the grpc host and port
            hostport = data.split('\n')[0]
            channel = grpc.insecure_channel(hostport)
            try:
                reply = chain_debug_pb2_grpc.ChainDebugStub(channel).debug(chain_debug_pb2.ChainDebugRequest())
            except grpc._channel._InactiveRpcError as e:
                click.echo(f"{Fore.RED}{hostport} does not implement the ChainDebug service. {e}|{e.details()}{Style.RESET_ALL}")
                exit(0)
            if show_data:
                click.echo("state:")
                for key,value in reply.state.items():
                    click.echo(f"  {key}=>{value}")
                click.echo(f"xid={reply.xid}")
                for s in reply.sent:
                    click.echo(f"  {s}")
            for log in reply.logs[:log_limit]:
                click.echo(log)
        except kazoo.exceptions.NoNodeError:
            click.echo(f"{Fore.RED}{full_name} does not exist{Style.RESET_ALL}")

@chain_client.command()
@click.argument("key")
@click.argument("blast_count", default=3)
def test_chain(key, blast_count):
    '''test a chain by:\n
       \b
       1) get the specified key
       2) start BLAST_COUNT threads to increment the key
       3) get the key again
       4) wait for the increments to complete
       5) get the key again
    '''
    tail = get_head_tail(ishead=False)
    reply = tail.get(chain_pb2.GetRequest(key=key))
    print(f"rc={reply.rc}: {key} is {reply.value}")
    if reply.rc:
        return

    def inc_thread(increment):
        head = get_head_tail(ishead=True)
        reply = head.increment(chain_pb2.IncRequest(key=key, incValue=increment))
        print(f"increment {key} by {increment} rc={reply.rc}")

    threads = []
    for i in range(blast_count):
        t = threading.Thread(target=inc_thread, args=(i+3,))
        t.start()
        threads.append(t)

    print(f"started {blast_count} inc threads")

    reply = tail.get(chain_pb2.GetRequest(key=key))
    print(f"rc={reply.rc}: {key} is {reply.value}")
    if reply.rc:
        return

    for t in threads:
        t.join()

    print("threads are finished")

    reply = tail.get(chain_pb2.GetRequest(key=key))
    print(f"rc={reply.rc}: {key} is {reply.value}")


if __name__ == '__main__':
    chain_client()
