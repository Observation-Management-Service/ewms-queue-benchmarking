"""
Server for queue management
"""
from datetime import datetime
import logging
import os
import string

import elasticsearch
from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.helpers import async_scan
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
    def initialize(self, es_client=None, **kwargs):
        super().initialize(**kwargs)
        self.es = es_client


class MultiBenchmarks(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self):
        name = self.get_argument('name')
        if (not name) or not VALID_ID.issuperset(name):
            raise HTTPError(400, reason='invalid benchmark name')
        doc = {
            'name': name,
            'created': datetime.utcnow().isoformat(),
            'pubs': 0,
            'pub-messages': 0,
            'expected-messages': 0,
            'workers': 0,
            'worker-messages': 0,
        }
        doc.update(json_decode(self.request.body))
        ret = await self.es.index(index='benchmarks', id=name, document=doc)
        logging.info('es index ret: %r', ret)

        self.write({'id': name})


class Benchmarks(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def get(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        try:
            ret = await self.es.get_source(index='benchmarks', id=benchmark)
        except elasticsearch.NotFoundError:
            raise HTTPError(404)
        ret = dict(ret)

        # sum messages
        pub_msgs = 0
        worker_msgs = 0
        try:
            await self.es.indices.refresh(index='benchmark-'+benchmark)
        except elasticsearch.NotFoundError:
            pass
        else:
            async for ret2 in async_scan(client=self.es, query={'query': {'match_all': {}}}, index='benchmark-'+benchmark):
                doc = ret2['_source']
                logging.debug('doc: %r', doc)
                if doc['type'] == 'pub':
                    pub_msgs += doc['messages']
                else:
                    worker_msgs += doc['messages']

        if pub_msgs < ret.get('pub-messages', 0):
            logging.debug('pub-messages decreased! %d < %d', pub_msgs, ret.get('pub-messages', 0))
            raise HTTPError(500, reason='invalid number of pub messages')
        if worker_msgs < ret.get('worker-messages', 0):
            logging.debug('worker-messages decreased! %d < %d', worker_msgs, ret.get('worker-messages', 0))
            raise HTTPError(500, reason='invalid number of pub messages')
        ret['pub-messages'] = pub_msgs
        ret['worker-messages'] = worker_msgs

        # update benchmark doc
        doc = {k:v for k,v in ret.items() if not k.startswith('_')}
        await self.es.index(index='benchmarks', id=benchmark, document=doc)

        self.write(ret)


class MultiPubs(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        try:
            await self.es.get_source(index='benchmarks', id=benchmark)
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        pub_id = self.get_argument('id')
        if not VALID_ID.issuperset(pub_id):
            raise HTTPError(400, reason='invalid pub id')

        doc = {
            'id': pub_id,
            'type': 'pub',
            'messages': 0,
            'delay': self.get_argument('delay', 0., type=float),
            'created': datetime.utcnow().isoformat(),
            'updated': datetime.utcnow().isoformat(),
        }
        await self.es.index(index='benchmark-'+benchmark, id=pub_id, document=doc)


class Pubs(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def put(self, benchmark, pub_id):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        if not VALID_ID.issuperset(pub_id):
            raise HTTPError(400, reason='invalid pub id')
        try:
            ret = await self.es.get_source(index='benchmarks', id=benchmark)
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        delay = self.get_argument('delay', 0., type=float)

        extra_pub_msgs = ret['pub-messages']-ret['worker-messages']
        if extra_pub_msgs > 100000:
            logging.info(f'{benchmark} {pub_id} - 100k messages extra, hard backoff')
            delay += 100
        elif ret['expected-messages'] > 0 and extra_pub_msgs * 100. / max(1, ret['expected-messages']) > 10:
            logging.info(f'{benchmark} {pub_id} - 10% of total messages are buffered, backoff')
            delay = delay * 2 + 1
        elif extra_pub_msgs > 10000:
            logging.info(f'{benchmark} {pub_id} - 10k messages extra, slow backoff')
            delay += 1
        elif extra_pub_msgs < 1000:
            logging.info(f'{benchmark} {pub_id} - need to queue now, no delay')
            delay = 0
        elif extra_pub_msgs < 10000:
            logging.info(f'{benchmark} {pub_id} - shrink delay')
            delay = max(0, delay / 2 - 1)

        doc = {
            'id': pub_id,
            'type': 'pub',
            'messages': self.get_argument('messages', 0, type=int),
            'delay': delay,
            'updated': datetime.utcnow().isoformat(),
        }
        await self.es.index(index='benchmark-'+benchmark, id=pub_id, document=doc)

        self.write({'delay': delay})


class MultiWorkers(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def post(self, benchmark):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        try:
            await self.es.get_source(index='benchmarks', id=benchmark)
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        worker_id = self.get_argument('id')
        if not VALID_ID.issuperset(worker_id):
            raise HTTPError(400, reason='invalid worker id')

        doc = {
            'id': worker_id,
            'type': 'worker',
            'messages': 0,
            'delay': self.get_argument('delay', 0., type=float),
            'created': datetime.utcnow().isoformat(),
            'updated': datetime.utcnow().isoformat(),
        }
        await self.es.index(index='benchmark-'+benchmark, id=worker_id, document=doc)


class Workers(APIBase):
    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])
    async def put(self, benchmark, worker_id):
        if not VALID_ID.issuperset(benchmark):
            raise HTTPError(400, reason='invalid benchmark name')
        if not VALID_ID.issuperset(worker_id):
            raise HTTPError(400, reason='invalid worker id')
        try:
            await self.es.get_source(index='benchmarks', id=benchmark)
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        doc = {
            'id': worker_id,
            'type': 'worker',
            'messages': self.get_argument('messages', 0, type=int),
            'delay': self.get_argument('delay', 0., type=float),
            'updated': datetime.utcnow().isoformat(),
        }
        await self.es.index(index='benchmark-'+benchmark, id=worker_id, document=doc)

        self.write({})


class Health(APIBase):
    @catch_error
    async def get(self):
        try:
            health = await self.es.cluster.health(index='benchmarks')
            if health['status'] == 'red':
                raise HTTPError(503, reason='ES down')
            count = await self.es.count(index='benchmarks')
        except elasticsearch.NotFoundError:
            raise HTTPError(500)
        logging.debug('output: %r %r', health, count)
        self.write({
            'benchmarks': count['count'],
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
            'ES_ADDRESS': 'http://localhost:9200',
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
        self._setup_es(config['ES_ADDRESS'])
        self.es_client = AsyncElasticsearch(config['ES_ADDRESS'])
        kwargs['es_client'] = self.es_client

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

    def _setup_es(self, es_address):
        es = Elasticsearch(es_address)
        try:
            es.cat.indices(index='benchmarks', format='json')
        except elasticsearch.NotFoundError:
            es.indices.create(index='benchmarks')

    async def stop(self):
        await self.server.stop()
        await self.es_client.close()


def create_server():
    return Server()
