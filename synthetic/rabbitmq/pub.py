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
from wipac_dev_tools import strtobool

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

def chirp_msgs(msgs: int):
    with HTChirp() as chirp:
        chirp.set_job_attr('MSGS', str(msgs))
        if strtobool(chirp.get_job_attr('QUIT')):
            raise StopIteration()
        time.sleep(float(chirp.get_job_attr('DELAY')))

async def main():
    parser = argparse.ArgumentParser(description='Publisher')
    parser.add_argument('--parallel', type=int, default=1, help='run pubs/workers in parallel, <N> per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--batch-size', type=int, default=100, help='batch size for messages')
    parser.add_argument('--condor-chirp', action='store_true', help='use HTCondor chirp to report msgs and get delay')
    parser.add_argument('--num-msgs', type=int, default=0, help='number of messages to publish (default: infinite)')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('address', default='localhost', help='queue address')
    parser.add_argument('queue_name', default='queue', help='queue name')
    args = parser.parse_args()

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args.loglevel.upper(), format=logformat)

    workq = partial(Queue, 'rabbitmq', address=args.address, name=args.queue_name)

    try:
        msgs = 0
        while args.num_msgs == 0 or msgs < args.num_msgs:
            if args.parallel > 1:
                processes = [Process(target=server_wrapper, args=(workq, args.msg_size, args.batch_size)) for _ in range(args.parallel)]
                for p in processes:
                    p.start()
                for p in processes:
                    p.join()
                msgs += args.batch_size*args.parallel
            else:
                await server(workq(), args.msg_size, args.batch_size)
                msgs += args.batch_size
            logging.info('num messages: %d', msgs)
            if args.condor_chirp:
                chirp_msgs(msgs)
    except StopIteration:
        logging.info('condor QUIT received')
    logging.info('done publishing, exiting')

if __name__ == '__main__':
    asyncio.run(main())