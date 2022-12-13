import argparse
from glob import glob
import logging
import os
from pathlib import Path
import time

import htcondor
from rest_tools.client import ClientCredentialsAuth, RestClient


logger = logging.getLogger('submitter')


def get_schedd():
    coll_query = htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd)
    for schedd_ad in coll_query:
        schedd = htcondor.Schedd(schedd_ad)
        break
    else:
        raise RuntimeError('no schedd found!')
    return schedd


def create_jobs(queue_address, queue_name, server=None, access_token=None, pubs=1, workers=1, parallel=True, msgs_per_pub=1000, msg_size=100, delay=0, scratch=Path('/tmp'), venv=None, **kwargs):
    scratch.mkdir(parents=True, exist_ok=True)

    access = f'--server-access-token {access_token}' if access_token else ''

    log_base = scratch / queue_name
    log_pub = f'{log_base}.pub'
    log_worker = f'{log_base}.worker'

    env_script = scratch / 'env.sh'
    with env_script.open('w') as f:
        print('#!/bin/sh', file=f)
        if venv:
            print(f'. {venv}/bin/activate', file=f)
        print('exec "$@"', file=f)
    env_script.chmod(0o700)

    schedd = get_schedd()

    pub_job_count = pubs
    pub_jobs = schedd.submit(htcondor.Submit({
        'executable': str(env_script),
        'output': f'{log_pub}.$(ProcId).out',
        'error': f'{log_pub}.$(ProcId).err',
        'log': f'{log_base}.log',
        'initialdir': os.getcwd(),
        'transfer_input_files': 'pub.py',
        'transfer_output_files': '',
        'request_cpus': '1',
        'request_memory': '1GB',
        'arguments': f'python pub.py --server-address {server} {access} --num-msgs {msgs_per_pub} --msg-size {msg_size} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=pub_job_count)

    worker_job_count = workers
    worker_jobs = schedd.submit(htcondor.Submit({
        'executable': str(env_script),
        'output': f'{log_worker}.$(ProcId).out',
        'error': f'{log_worker}.$(ProcId).err',
        'log': f'{log_base}.log',
        'initialdir': os.getcwd(),
        'transfer_input_files': 'worker.py',
        'transfer_output_files': '',
        'request_cpus': '1',
        'request_memory': '1GB',
        'priority': '1',
        'arguments': f'python worker.py --server-address {server} {access} --delay {delay} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=worker_job_count)

    return {
        'queue_name': queue_name,
        'pub_jobs': pub_jobs,
        'pub_job_count': pub_job_count,
        'worker_jobs': worker_jobs,
        'worker_job_count': worker_job_count,
        'log': f'{log_base}.log',
    }


def monitor_jobs(jobs, total_messages=100, time_limit=-1, client=None):
    queue_name = jobs['queue_name']
    pub_cluster = jobs['pub_jobs'].cluster()
    worker_cluster = jobs['worker_jobs'].cluster()

    sent = 0
    recv = 0

    start = time.time()
    try:
        while recv < total_messages and (time_limit == -1 or time.time()-start < time_limit):
            try:
                # get status update
                ret = client.request_seq('GET', f'/benchmarks/{queue_name}')
                sent = ret['pub-messages']
                recv = ret['worker-messages']
                logger.info('sent: %d, recv: %d', sent, recv)

                time.sleep(1)
            except KeyboardInterrupt:
                logger.warning('shutting down')
                raise

            except Exception:
                logger.info('job/server error', exc_info=True)

    finally:
        logger.warning(f'removing jobs {pub_cluster} {worker_cluster}')
        schedd = get_schedd()
        schedd.act(htcondor.JobAction.Remove, [f'{pub_cluster}', f'{worker_cluster}'], reason='cleanup')
        time.sleep(1)

    if time_limit != -1 and time.time()-start >= time_limit:
        if logger.isEnabledFor(logging.DEBUG):
            for name in sorted(glob(jobs['log'].rsplit('.',1)[0]+'*')):
                logger.debug('filename %s\n%s', name, open(name).read())
        raise Exception('hit time limit')


def decimal1(s):
    try:
        return int(s)
    except ValueError:
        return f'{s:.1f}'


def mkpath(s):
    try:
        p = Path(s)
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        raise ValueError('invalid path')
    return p


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue-address', help='queue address')
    parser.add_argument('--server', help='benchmark server address')
    parser.add_argument('--auth-url', help='OpenID url')
    parser.add_argument('--auth-client-id', help='OpenID client id')
    parser.add_argument('--auth-client-secret', help='OpenID client secret')
    parser.add_argument('--pubs', type=int, default=1, help='# of publishers')
    parser.add_argument('--workers', type=int, default=1, help='# of workers')
    parser.add_argument('--msgs-per-pub', type=int, default=10000, help='# of messages each pub should send')
    parser.add_argument('--parallel', action='store_true', help='run pubs/workers in parallel, 10x per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--delay', type=decimal1, default=0, help='delay in seconds')
    parser.add_argument('--scratch', type=mkpath, default='/scratch/dschultz/queue-benchmarks', help='scratch location')
    parser.add_argument('--venv', help='(optional) venv location for jobs')
    parser.add_argument('--time-limit', type=int, default=-1, help='(optional) time limit before killing jobs')
    parser.add_argument('--override', action='store_true', help='override previous benchmark')
    parser.add_argument('--loglevel', default='info', help='log level')
    args = vars(parser.parse_args())

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args['loglevel'].upper(), format=logformat)

    total = args['msgs_per_pub'] * args['pubs']
    logging.info(f'goal: {total} messages')

    pubs = args['pubs']
    workers = args['workers']
    if args['parallel']:
        pubs *= 10
        workers *= 10
    msg_size = args['msg_size']
    delay = args['delay']
    queue_name = f'rabbitmq-p{pubs}-w{workers}-m{msg_size}-d{delay}'
    logging.info(f'Creating benchmark {queue_name}')
    args['queue_name'] = queue_name

    if args['auth_url']:
        client = ClientCredentialsAuth(
            args['server'],
            args['auth_url'],
            args['auth_client_id'],
            args['auth_client_secret'],
        )
        args['access_token'] = client.access_token
    else:
        logging.warning('Running without auth!')
        client = RestClient(args['server'])

    if args['override']:
        client.request_seq('DELETE', f'/benchmarks/{queue_name}')
    client.request_seq('POST', '/benchmarks', {
        'name': queue_name,
        'pubs': pubs,
        'workers': workers,
        'messages-per-pub': args['msgs_per_pub'],
        'message-size': msg_size,
        'delay': delay,
        'expected-messages': total,
    })

    job_info = create_jobs(**args)
    monitor_jobs(job_info, total, args['time_limit'], client=client)


if __name__ == '__main__':
    main()
