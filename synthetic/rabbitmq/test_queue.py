import argparse
import asyncio
import logging
import subprocess
from functools import partial
from multiprocessing import Process

from mqclient import Queue


async def server(work_queue: Queue, result_queue: Queue) -> None:
    """Demo example server."""

    async with work_queue.open_pub() as p:
        for i in range(10):
            await p.send({'id': i, 'cmd': f'echo "{i}"'})

    results = {}
    result_queue.timeout = 60
    async with result_queue.open_sub() as stream:
        async for data in stream:
            assert isinstance(data, dict)
            results[data['id']] = data['out']
            if len(results) >= 10:
                break

    logging.info('results: %r', results)
    assert len(results) == 10
    for i in results:
        assert results[i].strip() == str(i)



async def worker(recv_queue: Queue, send_queue: Queue) -> None:
    """Demo example worker."""
    async with recv_queue.open_sub() as stream, send_queue.open_pub() as p:
        async for data in stream:
            cmd = data['cmd']
            out = subprocess.check_output(cmd, shell=True)
            data['out'] = out.decode('utf-8')
            await p.send(data)

def worker_wrapper(workq, resultq):
    asyncio.run(worker(workq(), resultq()))

async def main():
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('-a', '--address', default='localhost', help='queue address')
    parser.add_argument('--work-queue', default='queue1', help='work queue')
    parser.add_argument('--result-queue', default='queue2', help='result queue')
    parser.add_argument(
        '--prefetch', type=int, default=10, help='result queue prefetch'
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    workq = partial(Queue, address=args.address, name=args.work_queue)
    resultq = partial(Queue, address=args.address, name=args.result_queue, prefetch=args.prefetch)
    
    workq2 = Queue('rabbitmq', address=args.address, name=args.work_queue)
    resultq2 = Queue(
        'rabbitmq', address=args.address, name=args.result_queue, prefetch=args.prefetch
    )

    p = Process(target=worker_wrapper, args=(workq, resultq))
    p.start()
    await server(workq2, resultq2)
    p.terminate()


if __name__ == '__main__':
    asyncio.run(main())