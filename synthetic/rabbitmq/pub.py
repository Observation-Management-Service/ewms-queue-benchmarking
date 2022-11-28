import argparse
import asyncio
from functools import partial
import logging
from multiprocessing import Process
import random
import string
import time
from uuid import uuid4

from mqclient import Queue
from rest_tools.client import RestClient


async def pub(work_queue: Queue, msg_size: int = 100, batch_size: int = 100) -> None:
    """Send messages to queue"""
    async with work_queue.open_pub() as p:
        for _ in range(batch_size):
            data = ''.join(random.choices(string.ascii_letters, k=msg_size))
            uid = uuid4().hex
            now = time.time()
            await p.send({'uuid': uid, 'time': now, 'data': data})
            logging.warning(f'pub {uid} with size {msg_size}')


def pub_wrapper(workq, *args, **kwargs):
    asyncio.run(pub(workq(), *args, **kwargs))


class MyRestClient:
    def __init__(self, address: str, token: str, queue_name: str):
        self.queue_name = queue_name
        self.uid = uuid4().hex
        self.delay = 0.
        self._rc = RestClient(address, token)
        self._rc.request_seq('POST', f'/benchmarks/{queue_name}/pubs', {'id': self.uid, 'delay': self.delay})

    async def send(self, data):
        data['delay'] = self.delay
        ret = await self._rc.request('PUT', f'/benchmarks/{self.queue_name}/pubs/{self.uid}', data)
        if ret.get('quit'):
            raise StopIteration()
        self.delay = ret.get('delay', self.delay)
        if self.delay > 0:
            await asyncio.sleep(self.delay)


async def main():
    parser = argparse.ArgumentParser(description='Publisher')
    parser.add_argument('--parallel', type=int, default=1, help='run pubs/workers in parallel, <N> per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--batch-size', type=int, default=100, help='batch size for messages')
    parser.add_argument('--server-address', help='monitoring server address')
    parser.add_argument('--server-access-token', help='monitoring server access token')
    parser.add_argument('--num-msgs', type=int, default=0, help='number of messages to publish (default: infinite)')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('address', default='localhost', help='queue address')
    parser.add_argument('queue_name', default='queue', help='queue name')
    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args.loglevel.upper(), format=logformat)

    workq = partial(Queue, 'rabbitmq', address=args.address, name=args.queue_name)
    rest_client = None
    if args.server_address:
        rest_client = MyRestClient(args.server_address, args.server_access_token, args.queue_name)

    try:
        total_msgs = 0
        msgs = 0
        total_duration = 0
        while args.num_msgs == 0 or total_msgs < args.num_msgs:
            start = time.time()
            if args.parallel > 1:
                processes = [Process(target=pub_wrapper, args=(workq, args.msg_size, args.batch_size)) for _ in range(args.parallel)]
                for p in processes:
                    p.start()
                for p in processes:
                    p.join()
                msgs = args.batch_size*args.parallel
            else:
                await pub(workq(), args.msg_size, args.batch_size)
                msgs = args.batch_size
            total_msgs += msgs
            duration = time.time()-start
            total_duration += duration
            throughput = msgs/duration
            total_throughput = total_msgs/total_duration
            logging.info('num messages: %d', total_msgs)
            if rest_client:
                await rest_client.send({'messages': msgs, 'total_messages': total_msgs, 'throughput': throughput, 'total_throughput': total_throughput})
    except StopIteration:
        logging.info('QUIT received')
    logging.info('done publishing, exiting')

if __name__ == '__main__':
    asyncio.run(main())
