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
from htcondor.htchirp import HTChirp

async def server(work_queue: Queue, msg_size: int = 100, batch_size: int = 100) -> None:
    """Send messages to queue"""
    async with work_queue.open_pub() as p:
        for _ in range(batch_size):
            data = ''.join(random.choices(string.ascii_letters, k=msg_size))
            uid = uuid4().hex
            await p.send({'uuid': uid, 'data': data})
            logging.warning(f'pub {uid} with size {msg_size}')

def server_wrapper(workq, *args, **kwargs):
    asyncio.run(server(workq(), *args, **kwargs))

def delay(num_total, chirp=False):
    msgs = 0 
    while num_total == 0 or msgs < num_total:
        if not chirp:
            yield
            time.sleep(1)
        else:
            with HTChirp() as chirp:
                msgs = chirp.get_job_attr('MSGS')
                m = yield
                msgs += m
                chirp.set_job_attr('MSGS', msgs)
                if chirp.get_job_attr('QUIT'):
                    return
                time.sleep(chirp.get_job_attr('DELAY'))

async def main():
    parser = argparse.ArgumentParser(description='Publisher')
    parser.add_argument('--parallel', type=int, default=1, help='run pubs/workers in parallel, <N> per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--batch-size', type=int, default=100, help='batch size for messages')
    parser.add_argument('--condor-chirp', action='store_true', help='use HTCondor chirp to report msgs and get delay')
    parser.add_argument('--num-msgs', type=int, default=0, help='number of messages to publish (default: infinite)')
    parser.add_argument('address', default='localhost', help='queue address')
    parser.add_argument('queue_name', default='queue', help='queue name')
    args = parser.parse_args()

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logformat)

    workq = partial(Queue, 'rabbitmq', address=args.address, name=args.queue_name)

    delay_gen = delay(args.num_msgs, args.condor_chirp)
    for _ in delay_gen:
        if args.parallel > 1:
            processes = [Process(target=server_wrapper, args=(workq, args.msg_size, args.batch_size)) for _ in range(args.parallel)]
            for p in processes:
                p.start()
            for p in processes:
                p.join()
            delay_gen.send(args.batch_size * args.parallel)
        else:
            asyncio.run(server(workq(), args.msg_size, args.batch_size))
            delay_gen.send(args.batch_size)

if __name__ == '__main__':
    asyncio.run(main())