import argparse
from collections import defaultdict
import logging
import os
from pathlib import Path
import time

import classad
import htcondor


logger = logging.getLogger('submitter')


def get_schedd():
    coll_query = htcondor.Collector('localhost').locateAll(htcondor.DaemonTypes.Schedd)
    for schedd_ad in coll_query:
        schedd = htcondor.Schedd(schedd_ad)
        break
    else:
        raise RuntimeError('no schedd found!')
    return schedd


def create_jobs(queue_address, pubs=1, workers=1, parallel=True, msgs_per_pub=1000, msg_size=100, delay=0, scratch=Path('/tmp'), venv=None, **kwargs):
    scratch.mkdir(parents=True, exist_ok=True)

    queue_name = f'{pubs}p{workers}w{msg_size}b{delay}s'

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

    pub_job_count = max(1, pubs//10) if parallel else pubs
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
        '+WantIOProxy': 'true', # enable chirp
        '+QUIT': 'false',
        '+MSGS': '0',
        '+DELAY': f'{delay+1}',
        'arguments': f'python pub.py --condor-chirp --num-msgs {msgs_per_pub} --msg-size {msg_size} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=pub_job_count)

    worker_job_count = max(1, workers//10) if parallel else workers
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
        '+WantIOProxy': 'true', # enable chirp
        '+MSGS': '0',
        'arguments': f'python worker.py --condor-chirp --delay {delay} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=worker_job_count)

    return {
        'pub_jobs': pub_jobs,
        'pub_job_count': pub_job_count,
        'worker_jobs': worker_jobs,
        'worker_job_count': worker_job_count,
        'log': f'{log_base}.log',
    }


def monitor_jobs(jobs, total_messages=100):
    total_jobs = jobs['pub_job_count'] + jobs['worker_job_count']
    complete_jobs = 0

    pub_cluster = jobs['pub_jobs'].cluster()
    log_base = jobs['log'].rsplit('.',1)[0]
    pub_delay = 0
    pub_last_update = time.time()
    pub_messages = defaultdict(int)
    worker_messages = defaultdict(int)

    quitting = False
    exit_status = True

    schedd = get_schedd()
    jel = htcondor.JobEventLog(jobs['log'])

    try:
        while complete_jobs < total_jobs:
            try:
                for event in jel.events(None):
                    print(event)

                    try:
                        if event.type in { htcondor.JobEventType.JOB_TERMINATED,
                                           htcondor.JobEventType.JOB_ABORTED,
                                           htcondor.JobEventType.JOB_HELD,
                                           htcondor.JobEventType.CLUSTER_REMOVE }:
                            complete_jobs += 1
                            if event.type != htcondor.JobEventType.JOB_TERMINATED or event['ReturnValue'] != 0:
                                exit_status = False
                                if event.cluster == pub_cluster:
                                    t = 'pub'
                                else:
                                    t = 'worker'
                                logger.warning(f'{t} job failed')
                                errfile = Path(f'{log_base}.{t}.{event.proc}.err')
                                if errfile.exists():
                                    with errfile.open() as f:
                                        for line in f:
                                            logger.info('stderr: %s', line.rstrip())
                                outfile = Path(f'{log_base}.{t}.{event.proc}.out')
                                if outfile.exists():
                                    with outfile.open() as f:
                                        for line in f:
                                            logger.info('stdout: %s', line.rstrip())
                            if complete_jobs >= total_jobs:
                                logger.info('successfully shut down')
                                break

                        if event.type == htcondor.JobEventType.ATTRIBUTE_UPDATE:
                            if event.cluster == pub_cluster:
                                pub_messages[event.proc] = int(event['MSGS'])
                            else:
                                worker_messages[event.proc] = int(event['MSGS'])
                            sent = sum(pub_messages)
                            recv = sum(worker_messages)
                            logging.info('sent', sent, '| recv', recv)

                            if recv >= total_messages:
                                logging.info('reached message limit, shutting down')
                                schedd.edit(f'{pub_cluster}', 'QUIT', 'true')

                            if pub_last_update + 10 < time.time(): # rate limit updates
                                pub_last_update = time.time()
                                # update the pub DELAY
                                if recv - sent > jobs['worker_job_count'] * 100:
                                    pub_delay = pub_delay * 2 + 1
                                    schedd.edit(f'{pub_cluster}', 'DELAY', f'{pub_delay}')
                                elif recv - sent < jobs['worker_job_count'] * 5:
                                    pub_delay -= 1
                                    schedd.edit(f'{pub_cluster}', 'DELAY', f'{pub_delay}')
                    except Exception:
                        logger.info('event processing error', exc_info=True)

            except KeyboardInterrupt:
                if quitting:
                    logger.warning('force quit')
                    raise
                logger.warning('shutting down')
                schedd.edit(f'{pub_cluster}', 'QUIT', 'true')
                quitting = True

            except Exception as e:
                logger.info('job event log error', exc_info=True)
                try:
                    jel.close()
                    jel = htcondor.JobEventLog(jobs['log'])
                except Exception:
                    logger.warning('cannot reopen event log', exc_info=True)
                    raise e

    finally:
        logger.warning('removing jobs')
        schedd.act(htcondor.JobAction.Remove, f'{pub_cluster}')
        schedd.act(htcondor.JobAction.Remove, f'{jobs["worker_jobs"].cluster()}')

    if not exit_status:
        raise Exception('some jobs failed')


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
    parser.add_argument('--address', dest='queue_address', type=str, help='queue address')
    parser.add_argument('--pubs', type=int, default=1, help='# of publishers')
    parser.add_argument('--workers', type=int, default=1, help='# of workers')
    parser.add_argument('--msgs-per-pub', type=int, default=10000, help='# of messages each pub should send')
    parser.add_argument('--parallel', action='store_true', help='run pubs/workers in parallel, 10x per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--delay', type=decimal1, default=0, help='delay in seconds')
    parser.add_argument('--scratch', type=mkpath, default='/scratch/dschultz/queue-benchmarks', help='scratch location')
    parser.add_argument('--venv', default=None, help='(optional) venv location for jobs')
    parser.add_argument('--loglevel', default='info', help='log level')
    args = vars(parser.parse_args())

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args['loglevel'].upper(), format=logformat)

    total = args['msgs_per_pub'] * args['pubs']
    logging.info(f'goal: {total} messages')

    job_info = create_jobs(**args)
    monitor_jobs(job_info, total)


if __name__ == '__main__':
    main()