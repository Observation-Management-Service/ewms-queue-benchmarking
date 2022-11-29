import asyncio
import logging
import socket
import os

import elasticsearch
from elasticsearch import AsyncElasticsearch
import motor.motor_asyncio
import pytest
import pytest_asyncio
from rest_tools.client import RestClient
import requests.exceptions
from wipac_dev_tools import from_environment

from benchmark_server.server import Server


@pytest.fixture
def port():
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port

@pytest_asyncio.fixture
async def es_clear():
    default_config = {
        'ES_ADDRESS': 'http://localhost:9200',
    }
    config = from_environment(default_config)
    es = AsyncElasticsearch(config['ES_ADDRESS'])

    async def clean():
        indices = await es.cat.indices(index='benchmark*', expand_wildcards='open', format='json')
        for index in indices:
            logging.warning('index exists: %r', index)
            name = index['index']
            if name.startswith('benchmark'):
                logging.debug('deleting index %s', name)
                await es.indices.delete(index=name)

    try:
        await clean()
        yield
    finally:
        await clean()
        await es.close()

@pytest_asyncio.fixture
async def mongo_clear():
    default_config = {
        'DB_URL': 'mongodb://localhost/es_benchmarks',
    }
    config = from_environment(default_config)
    db_url, db_name = config['DB_URL'].rsplit('/', 1)
    client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
    db = client[db_name]

    try:
        await db.benchmarks.drop()
        await db.clients.drop()
        yield
    finally:
        await db.benchmarks.drop()
        await db.clients.drop()

@pytest_asyncio.fixture
async def server(monkeypatch, port, es_clear, mongo_clear):
    monkeypatch.setenv('PORT', str(port))
    monkeypatch.setenv('ES_TIMEOUT', str(1))

    s = Server()
    await s.start()

    def client(timeout=10):
        return RestClient(f'http://localhost:{port}', timeout=timeout, retries=0)

    try:
        yield client
    finally:
        await s.stop()


async def test_bad_route(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/foo')
    assert exc_info.value.response.status_code == 404

async def test_bad_method(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/benchmarks')
    assert exc_info.value.response.status_code == 405

async def test_health(server):
    client = server()
    ret = await client.request('GET', '/healthz')
    assert ret['benchmarks'] == 0

async def test_benchmarks_bad_name(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/benchmarks', {})
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/benchmarks', {'name': '#$@'})
    assert exc_info.value.response.status_code == 400

async def test_benchmarks(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    ret = await client.request('GET', '/benchmarks/foo-bar')
    assert ret['name'] == 'foo-bar'
    assert ret['pub-messages'] == 0
    assert ret['worker-messages'] == 0

async def test_benchmarks_double_post(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    await client.request('GET', '/benchmarks/foo-bar')
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    assert exc_info.value.response.status_code == 409

async def test_benchmark_pub_before_exists(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '1234'})
    assert exc_info.value.response.status_code == 404

async def test_benchmark_pub(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '1234'})
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1234', {'total_messages': 1})
    assert ret['delay'] == 0

async def test_benchmark_worker(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    await client.request('POST', '/benchmarks/foo-bar/workers', {'id': '1234'})
    await client.request('PUT', '/benchmarks/foo-bar/workers/1234', {'total_messages': 1})

async def test_benchmark_totals(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar'})
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '1'})
    await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 2})
    await client.request('POST', '/benchmarks/foo-bar/workers', {'id': '2'})
    await client.request('PUT', '/benchmarks/foo-bar/workers/2', {'total_messages': 1})
    
    ret = await client.request('GET', '/benchmarks/foo-bar')
    assert ret['pub-messages'] == 2
    assert ret['worker-messages'] == 1

async def test_backoff(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar', 'expected-messages': 10000000})
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '1'})
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5000})
    assert ret['delay'] == 0
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '2'})
    await client.request('PUT', '/benchmarks/foo-bar/pubs/2', {'total_messages': 5000})
    assert ret['delay'] == 0
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 0.})
    assert ret['delay'] == 0  # cached result

    # update cached values
    ret = await client.request('GET', '/benchmarks/foo-bar')
    logging.debug('benchmark status: %r', ret)
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001})
    assert ret['delay'] == 1
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 3.5})
    assert ret['delay'] == 4.5  # should go up by 1

    # add a worker
    await client.request('POST', '/benchmarks/foo-bar/workers', {'id': '3'})
    await client.request('PUT', '/benchmarks/foo-bar/workers/3', {'total_messages': 1})
    await client.request('GET', '/benchmarks/foo-bar')
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 3.5})
    assert ret['delay'] == 3.5  # should stay constant

    await client.request('PUT', '/benchmarks/foo-bar/workers/3', {'total_messages': 2})
    await client.request('GET', '/benchmarks/foo-bar')
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 3.5})
    assert ret['delay'] == 3.5/2-1  # should go down sharply

    await client.request('PUT', '/benchmarks/foo-bar/workers/3', {'total_messages': 10000})
    await client.request('GET', '/benchmarks/foo-bar')
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 3.5})
    assert ret['delay'] == 0  # should emergency queue

    await client.request('PUT', '/benchmarks/foo-bar/pubs/2', {'total_messages': 150000})
    ret = await client.request('GET', '/benchmarks/foo-bar')
    logging.debug('benchmark status: %r', ret)
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 5001, 'delay': 3.5})
    assert ret['delay'] == 3.5+100  # should sharply rise

async def test_backoff_10pct(server):
    client = server()
    await client.request('POST', '/benchmarks', {'name': 'foo-bar', 'expected-messages': 10000})
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '1'})
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 900})
    assert ret['delay'] == 0
    await client.request('POST', '/benchmarks/foo-bar/pubs', {'id': '2'})
    await client.request('PUT', '/benchmarks/foo-bar/pubs/2', {'total_messages': 101})
    ret = await client.request('GET', '/benchmarks/foo-bar')
    logging.debug('benchmark status: %r', ret)
    ret = await client.request('PUT', '/benchmarks/foo-bar/pubs/1', {'total_messages': 101, 'delay': 3.5})
    assert ret['delay'] == 3.5*2+1  # should be a mid rise

    