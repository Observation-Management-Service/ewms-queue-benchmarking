import argparse
import asyncio
from functools import partial
import logging
from multiprocessing import Process
from multiprocessing import Queue as mpQueue
import time
from uuid import uuid4

from mqclient import Queue
from rest_tools.client import RestClient


async def worker(work_queue: Queue, delay: float, batch_size: float) -> None:
    """Demo example worker."""
    msgs_received = 0
    queue_time = 0.
    latency = 0.
    async with work_queue.open_sub() as stream:
        lt_start = time.time()
        async for data in stream:
            uid = data['uuid']
            now = time.time()
            qt = now - data['time']
            lat = now - lt_start
            msg_size = len(data['data'])
            logging.warning(f'recv {uid} with size {msg_size}, qt {qt}, and latency {lat}')
            await asyncio.sleep(delay)
            logging.warning(f'complete {uid} with size {msg_size}')
            msgs_received += 1
            queue_time += qt
            latency += lat
            if msgs_received >= batch_size:
                break
            lt_start = time.time()
    return {
        'messages': msgs_received,
        'queue_time': queue_time,
        'latency': latency,
    }


def worker_wrapper(workq, msg_return, *args, **kwargs):
    ret = asyncio.run(worker(workq(), *args, **kwargs))
    msg_return.put(ret)


class MyRestClient:
    def __init__(self, address: str, token: str, queue_name: str, delay: int):
        self.queue_name = queue_name
        self.uid = uuid4().hex
        self.delay = delay
        self._rc = RestClient(address, token)
        self._rc.request_seq('POST', f'/benchmarks/{queue_name}/workers', {'id': self.uid, 'delay': self.delay})

    async def send(self, data):
        data['delay'] = self.delay
        ret = await self._rc.request('PUT', f'/benchmarks/{self.queue_name}/workers/{self.uid}', data)
        if ret.get('quit'):
            raise StopIteration()
        self.delay = ret.get('delay', self.delay)
        if self.delay > 0:
            await asyncio.sleep(self.delay)


async def main():
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--parallel', type=int, default=1, help='run workers in parallel, <N> per slot')
    parser.add_argument('--batch-size', type=int, default=100, help='batch size for messages')
    parser.add_argument('--delay', type=float, default=.1, help='sleep time for each message processed (to simulate work)')
    parser.add_argument('--server-address', help='monitoring server address')
    parser.add_argument('--server-access-token', help='monitoring server access token')
    parser.add_argument('--num-msgs', type=int, default=0, help='number of messages to publish (default: infinite)')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('address', default='localhost', help='queue address')
    parser.add_argument('queue_name', default='queue', help='queue name')
    parser.add_argument(
        '--prefetch', type=int, default=10, help='queue prefetch'
    )
    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args.loglevel.upper(), format=logformat)

    if args.num_msgs and args.num_msgs % args.batch_size != 0:
        raise RuntimeError('num msgs must be a multiple of batch size')

    workq = partial(Queue, 'rabbitmq', address=args.address, name=args.queue_name)
    rest_client = None
    if args.server_address:
        rest_client = MyRestClient(args.server_address, args.server_access_token, args.queue_name, args.delay)

    total_msgs = 0
    total_queue_time = 0.
    total_latency = 0.
    total_duration = 0.
    try:
        while args.num_msgs == 0 or total_msgs < args.num_msgs:
            msgs = 0
            queue_time = 0.
            latency = 0.
            start = time.time()
            if args.parallel > 1:
                ret = mpQueue()
                processes = [Process(target=worker_wrapper, args=(workq, ret, args.delay, args.batch_size)) for _ in range(args.parallel)]
                for p in processes:
                    p.start()
                for p in processes:
                    p.join()
                while not ret.empty():
                    ret2 = ret.get_nowait()
                    msgs += ret2['messages']
                    queue_time += ret2['queue_time']
                    latency += ret2['latency']
            else:
                ret = await worker(workq(), args.delay, args.batch_size)
                msgs = ret['messages']
                queue_time = ret['queue_time']
                latency = ret['latency']
            total_msgs += msgs
            total_queue_time += queue_time
            total_latency += latency
            duration = time.time()-start
            total_duration += duration
            throughput = msgs/duration
            total_throughput = total_msgs/total_duration
            logging.info('num messages: %d', total_msgs)
            if rest_client:
                await rest_client.send({
                    "messages": msgs, "total_messages": total_msgs,
                    "queue_time": queue_time/msgs if msgs else 0.,
                    "total_queue_time": total_queue_time/total_msgs if total_msgs else 0.,
                    "latency": latency/msgs if msgs else 0.,
                    "total_latency": total_latency/total_msgs if total_msgs else 0.,
                    'throughput': throughput, 'total_throughput': total_throughput,
                })
    except StopIteration:
        logging.info('QUIT received')

    logging.info('done working, exiting')


if __name__ == '__main__':
    asyncio.run(main())
