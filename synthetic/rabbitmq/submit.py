import argparse
import logging
from pathlib import Path
import time

import classad
import htcondor


logger = logging.getLogger('submitter')


def create_jobs(queue_address, pubs=1, workers=1, parallel=True, msg_size=100, delay=0, scratch=Path('/tmp'), **kwargs):
    scratch.mkdir(parents=True, exist_ok=True)

    queue_name = f'{pubs}p{workers}w{msg_size}b{delay}s'

    log_base = scratch / queue_name
    log_pub = f'{log_base}.pub'
    log_worker = f'{log_base}.worker'

    coll_query = htcondor.Collector('localhost').locateAll(htcondor.DaemonTypes.Schedd)
    for schedd_ad in coll_query:
        schedd = htcondor.Schedd(schedd_ad)
        break
    else:
        raise RuntimeError('no schedd found!')

    pub_job_count = max(1, pubs//10) if parallel else pubs
    pub_jobs = schedd.submit(htcondor.Submit({
        'executable': 'env.sh',
        'output': f'{log_pub}.$(ProcId).out',
        'error': f'{log_pub}.$(ProcId).err',
        'log': f'{log_base}.log',
        'request_cpus': '1',
        'request_memory': '1GB',
        '+WantIOProxy': 'true', # enable chirp
        '+QUIT': 'false',
        '+MSGS': '0',
        '+DELAY': f'{delay+1}',
        'arguments': f'python pub.py --msg-size {msg_size} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=pub_job_count)

    worker_job_count = max(1, workers//10) if parallel else workers
    worker_jobs = schedd.submit(htcondor.Submit({
        'executable': 'env.sh',
        'output': f'{log_worker}.$(ProcId).out',
        'error': f'{log_worker}.$(ProcId).err',
        'log': f'{log_base}.log',
        'request_cpus': '1',
        'request_memory': '1GB',
        '+WantIOProxy': 'true', # enable chirp
        '+MSGS': '0',
        'arguments': f'python worker.py --delay {delay} --parallel {10 if parallel else 1} {queue_address} {queue_name}',
    }), count=worker_job_count)

    return {
        'pub_jobs': pub_jobs,
        'pub_job_count': pub_job_count,
        'worker_jobs': worker_jobs,
        'worker_job_count': worker_job_count,
        'log': f'{log_base}.log',
    }

def monitor_jobs(jobs):
    total_jobs = jobs['pub_job_count'] + jobs['worker_job_count']
    complete_jobs = 0

    pub_cluster = jobs['pub_jobs'].cluster()
    pub_delay = 0
    pub_last_update = time.time()
    pub_messages = defaultdict(int)
    worker_messages = defaultdict(int)

    quitting = False

    schedd = htcondor.Schedd()
    jel = htcondor.JobEventLog(jobs['log'])

    try:
        while complete_jobs < total_jobs:
            try:
                for event in jel.events():
                    print(event)

                    try:
                        if event.type in { htcondor.JobEventType.JOB_TERMINATED,
                                           htcondor.JobEventType.JOB_ABORTED,
                                           htcondor.JobEventType.JOB_HELD,
                                           htcondor.JobEventType.CLUSTER_REMOVE }:
                            complete_jobs += 1
                            if complete_jobs >= total_jobs:
                                logger.info('successfully shut down')
                                break

                        if event.type == htcondor.JobEventType.JOB_AD_INFORMATION:
                            if event.cluster == pub_cluster:
                                pub_messages[event.proc] = event['MSGS']
                            else:
                                worker_messages[event.proc] = event['MSGS']
                            sent = sum(pub_messages)
                            recv = sum(worker_messages)
                            logging.info('sent', sent, '| recv', recv)

                            if pub_last_update + 30 < time.time(): # rate limit updates
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
    parser.add_argument('--parallel', action='store_true', help='run pubs/workers in parallel, 10x per slot')
    parser.add_argument('--msg-size', type=int, default=100, help='message size in bytes')
    parser.add_argument('--delay', type=decimal1, default=0, help='delay in seconds')
    parser.add_argument('--scratch', type=mkpath, default='/scratch/dschultz/queue-benchmarks', help='scratch location')
    parser.add_argument('--loglevel', default='info', help='log level')
    args = vars(parser.parse_args())

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(level=args['loglevel'].upper(), format=logformat)

    job_info = create_jobs(**args)
    monitor_jobs(job_info)

if __name__ == '__main__':
    main()