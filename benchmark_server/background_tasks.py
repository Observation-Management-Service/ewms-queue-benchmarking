"""Run the background tasks once.  Call in a cronjob."""
import asyncio
import logging

import motor.motor_asyncio
from wipac_dev_tools import from_environment

from .server import sum_all_msgs


async def main():
    # handle logging
    setlevel = {
        'CRITICAL': logging.CRITICAL,  # execution cannot continue
        'FATAL': logging.CRITICAL,
        'ERROR': logging.ERROR,  # something is wrong, but try to continue
        'WARNING': logging.WARNING,  # non-ideal behavior, important event
        'WARN': logging.WARNING,
        'INFO': logging.INFO,  # initial debug information
        'DEBUG': logging.DEBUG  # the things no one wants to see
    }

    default_config = {
        'LOG_LEVEL': 'INFO',
        'DB_URL': 'mongodb://localhost/es_benchmarks',
    }
    config = from_environment(default_config)
    if config['LOG_LEVEL'].upper() not in setlevel:
        raise Exception('LOG_LEVEL is not a proper log level')
    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'

    logging.basicConfig(format=logformat, level=setlevel[config['LOG_LEVEL'].upper()])

    logging_url = config["DB_URL"].split('@')[-1] if '@' in config["DB_URL"] else config["DB_URL"]
    logging.info(f'DB: {logging_url}')
    db_url, db_name = config['DB_URL'].rsplit('/', 1)
    db_client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
    logging.info(f'DB name: {db_name}')
    db = db_client[db_name]

    await sum_all_msgs(db)


if __name__ == '__main__':
    asyncio.run(main())
