import asyncio

import motor.motor_asyncio
import pytest
from asyncio_mongo_reflection.mongodict import *

test_data = []

loop = asyncio.new_event_loop()
loop.set_debug(False)
asyncio.set_event_loop(loop)
run = lambda coro: loop.run_until_complete(coro)

client = motor.motor_asyncio.AsyncIOMotorClient()
db = client.test_db

run(db['test_dict'].remove())

col = db['test_dict']
obj_ref = {'dict_id': 'test_dict'}
dkey = 'test_dict.inner'
mongo_dict = run(MongoDictReflection.create({'a': 1, 'b': 2, 'c': 3, 'd': {'e': 4}},
                                            col=col, obj_ref=obj_ref,
                                            key=dkey, dumps=None))

async def mongo_compare(ex, col_name):
    obj = await db[col_name].find_one(obj_ref)

    nested = dkey.split(sep='.')
    for key in nested:
        obj = obj[key]

    assert obj == ex


def db_compare(m, o):
    if m._dumps:
        o = {key: m._dumps(val) for key, val in o.items()}
    run(mongo_compare(o, m.col.name))


@pytest.yield_fixture(scope='session', autouse=True)
def db_conn():
    global mongo_dict
    yield 1
    # clear remaining tasks
    del mongo_dict

    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()
    run(asyncio.sleep(1))
    loop.close()


@pytest.fixture(scope="function",
                params=[mongo_dict],
                ids=['dict'])
def _(request):
    return request.param, dict(request.param)


def test_create(_):
    m, o = _[0], _[1]
    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_pop(_):
    m, o = _[0], _[1]

    m.pop('a')
    o.pop('a')

    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_popitem(_):
    m, o = _[0], _[1]

    m.popitem()
    o.popitem()

    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_update(_):
    m, o = _[0], _[1]

    m.update({'e': 3}, i=3, ww=2)
    m.update({'g': 3})
    m.update([('f', 4), ('j', 6)])

    o.update({'e': 3}, i=3, ww=2)
    o.update({'g': 3})
    o.update([('f', 4), ('j', 6)])

    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_set(_):
    m, o = _[0], _[1]

    m['g'] = 66
    m['i'] = 's'

    o['g'] = 66
    o['i'] = 's'

    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_set_nested(_):
    m, o = _[0], _[1]

    m['g'] = {'f': 5}
    m['g'].update(h=3)

    o['g'] = {'f': 5}
    o['g'].update(h=3)

    run(m.mongo_pending.join())
    run(m['g'].mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_more_nested(_):
    m, o = _[0], _[1]

    m['g']['j'] = {'i': 5}
    m['n'] = m['g']['j']
    m['n'].update(st=555)
    m['g']['j'].update(st=43, d=34)
    m['g']['j'].popitem()

    o['g']['j'] = {'i': 5}
    o['n'] = o['g']['j'].copy()
    o['n'].update(st=555)
    o['g']['j'].update(st=43, d=34)
    o['g']['j'].popitem()

    run(m.mongo_pending.join())
    run(m['g'].mongo_pending.join())
    run(m['g']['j'].mongo_pending.join())
    assert m == o
    db_compare(m, o)


def test_del(_):
    m, o = _[0], _[1]

    del m['g']
    del m['i']
    del o['g']
    del o['i']

    run(m.mongo_pending.join())
    assert m == o
    db_compare(m, o)

'''
def test_clear(_):
    m, o = _[0], _[1]

    m.clear()
    o.clear()

    run(asyncio.sleep(0.1))
    assert m == o
    db_compare(m, o)
'''