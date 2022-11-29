"""
Server for queue management
"""
import asyncio
from datetime import datetime
import logging
import os
import string

from elasticsearch import AsyncElasticsearch
import elasticsearch.exceptions
import pymongo.errors
import motor.motor_asyncio
from rest_tools.server import RestServer, RestHandler, RestHandlerSetup
from rest_tools.server.handler import keycloak_role_auth, catch_error
from tornado.escape import json_decode
from tornado.web import RequestHandler, HTTPError
from wipac_dev_tools import from_environment


AUTH_SERVICE_ACCOUNT = 'ewms-service-account'
VALID_ID = set(string.ascii_letters+string.digits+'-')

if os.environ.get('CI_TEST'):
    def service_account_auth(**kwargs):  # type: ignore
        def make_wrapper(method):
            async def wrapper(self, *args, **kwargs):
                logging.warning('TESTING: auth disabled')
                return await method(self, *args, **kwargs)
            return wrapper
        return make_wrapper
else:
    service_account_auth = keycloak_role_auth


class Error(RequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class APIBase(RestHandler):
    def initialize(self, db=None, es_client=None, **kwargs):
        super().initialize(**kwargs)
        self.db = db
        self.es = es_client

    async def create_es_entry(self, benchmark_data, entry_id, entry_type, messages=0, total_messages=0, latency=0., total_latency=0., throughput=0., total_throughput=0.):
        doc = {
            '@timestamp': datetime.utcnow().isoformat(),
            'benchmark': benchmark_data['name'],
            'msg-size': benchmark_data['message-size'],
            'num-pubs': benchmark_data['pubs'],
            'num-workers': benchmark_data['workers'],
            'delay': benchmark_data['delay'],
            'id': entry_id,
            'type': entry_type,
            'messages': messages,
            'total_messages': total_messages,
            'latency': latency,
            'total_latency': total_latency,
            'throughput': throughput,
            'total_throughput': total_throughput,
        }
        await self.es.index(index='benchmarks', document=doc)


async def sum_msgs(db, benchmark, ret=None):
    if not ret:
        ret = await db.benchmarks.find_one({'name': benchmark}, projection={'_id': False})

    if ret:
        # sum messages
        pub_msgs = 0
        worker_msgs = 0
        pipeline = [{'$match': {'benchmark': benchmark}}, {'$group': {'_id': '$type', 'total': {'$sum': '$messages'}}}]
        async for row in db.clients.aggregate(pipeline):
            if row['_id'] == 'pub':
                pub_msgs = row['total']
            elif row['_id'] == 'worker':
                worker_msgs = row['total']

        if pub_msgs < ret['pub-messages']:
            logging.debug('pub-messages decreased! %d < %d', pub_msgs, ret.get('pub-messages', 0))
            raise HTTPError(500, reason='invalid number of pub messages')
        if worker_msgs < ret['worker-messages']:
            logging.debug('worker-messages decreased! %d < %d', worker_msgs, ret.get('worker-messages', 0))
            raise HTTPError(500, reason='invalid number of pub messages')

        if ret['pub-messages'] != pub_msgs or ret['worker-messages'] != worker_msgs:
            update = {'pub-messages': pub_msgs, 'worker-messages': worker_msgs}
            ret.update(update)
            await db.benchmarks.update_one({'name': benchmark}, {'$set': update})

    return ret


class MultiBenchmarks(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self):
        benchmark = self.get_argument('name')
        if (not benchmark) or not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        doc = {
            'name': benchmark,
            'created': datetime.utcnow().isoformat(),
            'pubs': 0,
            'pub-messages': 0,
            'expected-messages': 0,
            'workers': 0,
            'worker-messages': 0,
            'messages-per-pub': 0,
            'message-size': 0,
        }
        doc.update(json_decode(self.request.body))
        try:
            await self.db.benchmarks.insert_one(doc)
        except pymongo.errors.DuplicateKeyError:
            raise HTTPError(409, reason='already existing benchmark')
        logging.info('create benchmark: %r', doc)

        self.write({'id': benchmark})


class Benchmarks(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def get(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')

        ret = await sum_msgs(self.db, benchmark)
        if not ret:
            raise HTTPError(404)

        self.write(ret)

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def delete(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')

        await self.db.benchmarks.delete_one({'name': benchmark})
        await self.db.clients.delete_many({'benchmark': benchmark})

        self.write({})


class MultiPubs(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        benchmark_data = await self.db.benchmarks.find_one({'name': benchmark})
        if not benchmark_data:
            raise HTTPError(404)

        pub_id = self.get_argument('id')
        if not VALID_ID.issuperset(pub_id):
            raise HTTPError(400, reason='invalid pub id')

        now = datetime.utcnow().isoformat()
        doc = {
            'id': pub_id,
            'benchmark': benchmark,
            'type': 'pub',
            'messages': 0,
            'delay': self.get_argument('delay', 0., type=float),
            'created': now,
            'updated': now,
        }
        await self.db.clients.insert_one(doc)

        await self.create_es_entry(benchmark_data, pub_id, 'pub')


class Pubs(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def put(self, benchmark, pub_id):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        if not VALID_ID.issuperset(pub_id):
            raise HTTPError(400, reason='invalid pub id')
        benchmark_data = await self.db.benchmarks.find_one({'name': benchmark}, projection={'_id': False})
        if not benchmark_data:
            raise HTTPError(404)

        delay = self.get_argument('delay', 0., type=float)

        extra_pub_msgs = benchmark_data['pub-messages']-benchmark_data['worker-messages']
        if extra_pub_msgs > 100000:
            logging.info(f'{benchmark} {pub_id} - 100k messages extra, hard backoff')
            delay += 100.
        elif benchmark_data['expected-messages'] > 0 and extra_pub_msgs * 100. / max(1, benchmark_data['expected-messages']) > 10:
            logging.info(f'{benchmark} {pub_id} - 10% of total messages are buffered, backoff')
            delay = delay * 2 + 1.
        elif extra_pub_msgs > 10000:
            logging.info(f'{benchmark} {pub_id} - 10k messages extra, slow backoff')
            delay += 1.
        elif extra_pub_msgs < 1000:
            logging.info(f'{benchmark} {pub_id} - need to queue now, no delay')
            delay = 0.
        elif extra_pub_msgs < 10000:
            logging.info(f'{benchmark} {pub_id} - shrink delay')
            delay = max(0., delay / 2 - 1)

        total_msgs = self.get_argument('total_messages', 0, type=int)
        ret = await self.db.clients.update_one({'id': pub_id}, {'$set': {'messages': total_msgs, 'delay': delay}})
        if ret.matched_count < 1:
            raise HTTPError(404)
        msgs = self.get_argument('messages', 0, type=int)
        throughput = self.get_argument('throughput', 0., type=float)
        total_throughput = self.get_argument('total_throughput', 0., type=float)
        await self.create_es_entry(benchmark_data, pub_id, 'pub',
                                   messages=msgs, total_messages=total_msgs,
                                   throughput=throughput, total_throughput=total_throughput)

        self.write({'delay': delay})


class MultiWorkers(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        benchmark_data = await self.db.benchmarks.find_one({'name': benchmark})
        if not benchmark_data:
            raise HTTPError(404)

        worker_id = self.get_argument('id')
        if not VALID_ID.issuperset(worker_id):
            raise HTTPError(400, reason='invalid worker id')

        now = datetime.utcnow().isoformat()
        doc = {
            'id': worker_id,
            'benchmark': benchmark,
            'type': 'worker',
            'messages': 0,
            'delay': self.get_argument('delay', 0., type=float),
            'created': now,
            'updated': now,
        }
        await self.db.clients.insert_one(doc)

        await self.create_es_entry(benchmark_data, worker_id, 'worker')


class Workers(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def put(self, benchmark, worker_id):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        if not VALID_ID.issuperset(worker_id):
            raise HTTPError(400, reason='invalid worker id')
        benchmark_data = await self.db.benchmarks.find_one({'name': benchmark})
        if not benchmark_data:
            raise HTTPError(404)

        total_msgs = self.get_argument('total_messages', 0, type=int)
        delay = self.get_argument('delay', 0., type=float)
        ret = await self.db.clients.update_one({'id': worker_id}, {'$set': {'messages': total_msgs, 'delay': delay}})
        if ret.matched_count < 1:
            raise HTTPError(404)
        msgs = self.get_argument('messages', 0, type=int)
        latency = self.get_argument('latency', 0., type=float)
        total_latency = self.get_argument('total_latency', 0., type=float)
        throughput = self.get_argument('throughput', 0., type=float)
        total_throughput = self.get_argument('total_throughput', 0., type=float)
        await self.create_es_entry(benchmark_data, worker_id, 'worker',
                                   messages=msgs, total_messages=total_msgs,
                                   latency=latency, total_latency=total_latency,
                                   throughput=throughput, total_throughput=total_throughput)

        self.write({})


class Health(APIBase):
    @catch_error
    async def get(self):
        try:
            health = await self.es.cluster.health()
            if health['status'] == 'red':
                raise HTTPError(503, reason='ES down')
        except elasticsearch.exceptions.ConnectionTimeout:
            raise HTTPError(503, reason='ES down')
        count = await self.db.benchmarks.estimated_document_count()
        logging.debug('output: %r %r', health, count)
        self.write({
            'benchmarks': count,
            'index-status': health['status'],
        })


class Server:
    def __init__(self):
        static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

        default_config = {
            'HOST': 'localhost',
            'PORT': 8080,
            'DEBUG': False,
            'OPENID_URL': '',
            'OPENID_AUDIENCE': '',
            'DB_URL': 'mongodb://localhost/es_benchmarks',
            'ES_ADDRESS': 'http://localhost:9200',
            'ES_TIMEOUT': 10.,
        }
        config = from_environment(default_config)

        rest_config = {
            'debug': config['DEBUG'],
        }
        if config['OPENID_URL']:
            logging.info(f'enabling auth via {config["OPENID_URL"]} for aud "{config["OPENID_AUDIENCE"]}"')
            rest_config.update({
                'auth': {
                    'openid_url': config['OPENID_URL'],
                    'audience': config['OPENID_AUDIENCE'],
                }
            })

        kwargs = RestHandlerSetup(rest_config)

        logging.info(f'DB: {config["DB_URL"]}')
        db_url, db_name = config['DB_URL'].rsplit('/', 1)
        db = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        logging.info(f'DB name: {db_name}')
        self.db = db[db_name]
        kwargs['db'] = self.db

        self.es_client = AsyncElasticsearch(config['ES_ADDRESS'], request_timeout=config['ES_TIMEOUT'])
        kwargs['es_client'] = self.es_client
        self.rest_kwargs = kwargs

        server = RestServer(static_path=static_path, template_path=static_path, debug=config['DEBUG'])
        server.add_route('/benchmarks', MultiBenchmarks, kwargs)
        server.add_route(r'/benchmarks/(?P<benchmark>[\w\-]+)', Benchmarks, kwargs)
        server.add_route(r'/benchmarks/(?P<benchmark>[\w\-]+)/pubs', MultiPubs, kwargs)
        server.add_route(r'/benchmarks/(?P<benchmark>[\w\-]+)/pubs/(?P<pub_id>[\w\-]+)', Pubs, kwargs)
        server.add_route(r'/benchmarks/(?P<benchmark>[\w\-]+)/workers', MultiWorkers, kwargs)
        server.add_route(r'/benchmarks/(?P<benchmark>[\w\-]+)/workers/(?P<worker_id>[\w\-]+)', Workers, kwargs)
        server.add_route('/healthz', Health, kwargs)
        server.add_route(r'/(.*)', Error)

        server.startup(address=config['HOST'], port=config['PORT'])

        self.server = server

        self.background_task = None

    async def start(self):
        indexes = await self.db.benchmarks.index_information()
        if 'name' not in indexes:
            logging.info('DB: creating index benchmarks:name')
            await self.db.benchmarks.create_index('name', unique=True, name='name')

        indexes = await self.db.clients.index_information()
        if 'benchmark' not in indexes:
            logging.info('DB: creating index clients:benchmark')
            await self.db.clients.create_index('benchmark', name='benchmark')
        if 'id' not in indexes:
            logging.info('DB: creating index clients:id')
            await self.db.clients.create_index('id', unique=True, name='id')

        # recurring benchmark msg summing
        if self.background_task is None:
            self.background_task = asyncio.create_task(self.run_background_task())

    async def run_background_task(self):
        while True:
            try:
                async for row in self.db.benchmarks.find({}, projection={'_id': False}):
                    exp = row['expected-messages']
                    if exp >= row['pub-messages'] and exp >= row['worker-messages']:
                        continue
                    await sum_msgs(self.db, row['name'], ret=row)
            except Exception:
                pass
            await asyncio.sleep(1)

    async def stop(self):
        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
            self.background_task = None
        await self.server.stop()
        await self.es_client.close()
