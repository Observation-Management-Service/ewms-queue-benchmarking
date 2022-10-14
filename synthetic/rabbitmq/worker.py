import argparse
import asyncio
from functools import partial
import logging
from multiprocessing import Process
from multiprocessing import Queue as mpQueue
import random
import string
import time
from uuid import uuid4

from mqclient import Queue
from htcondor.htchirp import HTChirp


async def worker(work_queue: Queue, delay: float, batch_size: float) -> None:
    """Demo example worker."""
    msgs_received = 0
    async with work_queue.open_sub() as stream:
        async for data in stream:
            uid = data['uuid']
            msg_size = len(data['data'])
            logging.warning(f'recv {uid} with size {msg_size}')
            await asyncio.sleep(delay)
            logging.warning(f'complete {uid} with size {msg_size}')
            msgs_received += 1
            if msgs_received >= batch_size:
                break
    return msgs_received

def worker_wrapper(workq, msg_return, *args, **kwargs):
    ret = asyncio.run(worker(workq(), *args, **kwargs))
    msg_return.put(ret)

def chirp_msgs(msgs: int):
    with HTChirp() as chirp:
        chirp.set_job_attr('MSGS', str(msgs))

async def main():
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--parallel', type=int, default=1, help='run workers in parallel, <N> per slot')
    parser.add_argument('--batch-size', type=int, default=100, help='batch size for messages')
    parser.add_argument('--delay', type=float, default=.1, help='sleep time for each message processed (to simulate work)')
    parser.add_argument('--condor-chirp', action='store_true', help='use HTCondor chirp to report msgs and get delay')
    parser.add_argument('--num-msgs', type=int, default=0, help='number of messages to publish (default: infinite)')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('address', default='localhost', help='queue address')
    parser.add_argument('queue_name', default='queue', help='queue name')
    parser.add_argument(
        '--prefetch', type=int, default=10, help='queue prefetch'
    )
    args = parser.parse_args()

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args.loglevel.upper(), format=logformat)

    if args.num_msgs and args.num_msgs % args.batch_size != 0:
        raise RuntimeError('num msgs must be a multiple of batch size')

    workq = partial(Queue, 'rabbitmq', address=args.address, name=args.queue_name)

    msgs = 0
    while args.num_msgs == 0 or msgs < args.num_msgs:
        if args.parallel > 1:
            ret = mpQueue()
            processes = [Process(target=worker_wrapper, args=(workq, ret, args.delay, args.batch_size)) for _ in range(args.parallel)]
            for p in processes:
                p.start()
            for p in processes:
                p.join()
            while not ret.empty():
                msgs += ret.get_nowait()
        else:
            ret = await worker(workq(), args.delay, args.batch_size)
            msgs += ret
        logging.info('num messages: %d', msgs)
        if args.condor_chirp:
            chirp_msgs(msgs)

    logging.info('done working, exiting')


if __name__ == '__main__':
    asyncio.run(main())