from tests.test_asyncio_prepare import *

lrun_uc(db['test_dict'].remove())

mongo_dict = lrun_uc(
    MongoDictReflection(
        {
            'a': 1,
            'b': {'g': {'t': 43}},
            'c': 3,
            'd': {'e': 4},
            'p': deque([1, 2, 3], maxlen=10)
        },
        col=db['test_dict'], obj_ref={'dict_id': 'test_dict'},
        key='test_dict.inner', dumps=None)
)


async def db_compare(m, o):
    await mongo_compare(flattern_dict_nested(dict(o), m._dumps), m)


@pytest.fixture(scope="function",
                params=[mongo_dict],
                ids=['dict'])
def _(request):
    return request.param, flattern_dict_nested(dict(request.param), lists_to_deque=True)


@async_test
async def test_default_dict_loaded(_):
    m, o = _[0], _[1]

    m_loaded = await MongoDictReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                         dumps=m._dumps, loads=m._loads)

    compare_nested_dict(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_pop(_):
    m, o = _[0], _[1]

    m.pop('a')
    o.pop('a')

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_popitem(_):
    m, o = _[0], _[1]

    m.popitem()
    o.popitem()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_update(_):
    m, o = _[0], _[1]

    m.update({'e': 3}, i=3, ww=2)
    m.update({'g': 3})
    m.update([('f', 4), ('j', 6)])

    o.update({'e': 3}, i=3, ww=2)
    o.update({'g': 3})
    o.update([('f', 4), ('j', 6)])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_set(_):
    m, o = _[0], _[1]

    m['g'] = 66
    m['i'] = 's'

    o['g'] = 66
    o['i'] = 's'

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_set_nested(_):
    m, o = _[0], _[1]

    m['g'] = {'f': 5}
    m['g'].update(h=3)

    o['g'] = {'f': 5}
    o['g'].update(h=3)

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_more_nested(_):
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

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_del(_):
    m, o = _[0], _[1]

    del m['g']
    del m['i']
    del o['g']
    del o['i']

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_dict_loaded(_):
    m, o = _[0], _[1]

    m_loaded = await MongoDictReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                         dumps=m._dumps, loads=m._loads)

    compare_nested_dict(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_clear(_):
    m, o = _[0], _[1]

    m.clear()
    o.clear()

    await asyncio.sleep(0.1)
    assert m == o
    await db_compare(m, o)
