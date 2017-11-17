from asyncio_mongo_reflection.mongodict import *
from tests.test_asyncio_prepare import *

test_data = []

lrun_uc(db['test_dict'].remove())

col = db['test_dict']
obj_ref = {'dict_id': 'test_dict'}
dkey = 'test_dict.inner'
mongo_dict = lrun_uc(MongoDictReflection.create({'a': 1, 'b': {'g':{'t': 43}}, 'c': 3, 'd': {'e': 4}},
                                                col=col, obj_ref=obj_ref,
                                                key=dkey, dumps=None))

async def mongo_compare(ex, col_name):
    obj = await db[col_name].find_one(obj_ref)

    nested = dkey.split(sep='.')
    for key in nested:
        obj = obj[key]

    assert obj == ex


def flattern_nested(dct, dumps=None):
    fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

    for key, val in dct.items():
        if isinstance(val, MongoDictReflection) or isinstance(val, dict):
            dct[key] = flattern_nested(dict(val), dumps)
        else:
            dct[key] = fdumps(val)
    return dct


async def db_compare(m, o):
    await mongo_compare(flattern_nested(dict(o), m._dumps), m.col.name)


@pytest.fixture(scope="function",
                params=[mongo_dict],
                ids=['dict'])
def _(request):
    return request.param, flattern_nested(dict(request.param))


@async_test
async def test_create(_):
    m, o = _[0], _[1]

    with pytest.raises(TypeError):
        MongoDictReflection()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


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
async def test_loaded(_):
    m, o = _[0], _[1]

    m_loaded = await MongoDictReflection.create(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                                dumps=m._dumps, loads=m._loads)

    def compare_nested(m, tested):
        for key, val in tested.items():
            if isinstance(val, MongoDictReflection):
                compare_nested(m[key], val)

            assert m[key] == val
            assert m.key == tested.key

    compare_nested(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_clear(_):
    m, o = _[0], _[1]

    m.clear()
    o.clear()

    await asyncio.sleep(0.1)
    assert m == o
    await db_compare(m, o)
