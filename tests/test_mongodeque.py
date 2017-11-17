from copy import deepcopy
from json import dumps, loads

from asyncio_mongo_reflection.mongodeque import *
from tests.test_asyncio_prepare import *

test_data = []

lrun_uc(db['test_arr_int'].remove())
lrun_uc(db['test_arr_str'].remove())
lrun_uc(db['test_arr_obj'].remove())

MAX_LEN = 7


mongo_int = lrun_uc(MongoDequeReflection.create([0, 4, 3, 33, 5, deque([1, 2, 3], maxlen=5), 2, 53, 4, 5],
                                                col=db['test_arr_int'],
                                                obj_ref={'array_id': 'test_deque'},
                                                key='inner.arr',
                                                dumps=None, maxlen=MAX_LEN))

mongo_str = lrun_uc(MongoDequeReflection.create([1, 2, 3, 4, 5],
                                                col=db['test_arr_str'],
                                                obj_ref={'array_id': 'test_deque'},
                                                key='inner.arr',
                                                dumps=str, loads=int, maxlen=MAX_LEN+1))

mongo_obj = lrun_uc(MongoDequeReflection.create([{'a': 1}, {'b': 1}, {'c': 1}, {'d': 1}, {'e': 1}],
                                                col=db['test_arr_obj'],
                                                obj_ref={'array_id': 'test_deque'},
                                                key='inner.arr',
                                                dumps=dumps,
                                                loads=loads, maxlen=MAX_LEN+2))


async def mongo_compare(ex, col, obj_ref, akey):
    obj = await col.find_one(obj_ref)

    nested = akey.split(sep='.')
    for key in nested:
        obj = obj[key]

    assert obj == ex


async def db_compare(m, o):
    await mongo_compare(flattern_nested(list(o), dumps=m._dumps, to_deque=False),
                        m.col, m.obj_ref, m.key)


def flattern_nested(nlist, dumps=None, to_deque=True):
    fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

    for ix, el in enumerate(nlist):
        if isinstance(el, MongoDequeReflection) or isinstance(el, deque):
            if to_deque:
                nlist[ix] = deque(list(el), maxlen=el.maxlen)
            else:
                nlist[ix] = list(el)
                flattern_nested(nlist[ix], dumps=dumps, to_deque=to_deque)
        else:
            nlist[ix] = fdumps(el)

    return nlist


@pytest.fixture(scope="function",
                params=[mongo_int, mongo_str, mongo_obj],
                ids=['int', 'str', 'obj'])
def _(request):
    return request.param, flattern_nested(deque(list(request.param), maxlen=request.param.maxlen))


@async_test
async def test_create(_):
    m, o = _[0], _[1]

    with pytest.raises(TypeError):
        MongoDequeReflection()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_append(_):
    m, o = _[0], _[1]

    m.append(m[-1])
    o.append(o[-1])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_appendleft(_):
    m, o = _[0], _[1]

    m.appendleft(m[-1])
    o.appendleft(o[-1])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_extend(_):
    m, o = _[0], _[1]

    m.extend([m[0], m[-2]])
    o.extend([o[0], o[-2]])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_extendleft(_):
    m, o = _[0], _[1]

    m.extendleft([m[0], m[-2]])
    o.extendleft([o[0], o[-2]])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_insert(_):
    m, o = _[0], _[1]

    try:
        m.insert(3, 55)
        o.insert(3, 55)
    except IndexError:
        pass

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_pop(_):
    m, o = _[0], _[1]

    m.pop()
    o.pop()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_popleft(_):
    m, o = _[0], _[1]

    m.popleft()
    o.popleft()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_nested(_):
    m, o = _[0], _[1]

    # make sure there's no 'loop already lrun_ucning' mistakes
    async def inner_coro():
        m[1] = [1, 2, 53]
        m[1].appendleft(m[2])
        m[1].append(m[3])
        m[2] = deque([23, 41, 2])
        m[2].append(m[1])
        m[2][-1].append(777)
    await inner_coro()

    o[1] = deque([1, 2, 53])
    o[1].appendleft(o[2])
    o[1].append(o[3])
    o[2] = deque([23, 41, 2])
    o[2].append(deepcopy(o[1]))
    o[2][-1].append(777)

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_remove(_):
    m, o = _[0], _[1]

    m.remove(m[1])
    o.remove(o[1])

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_reverse(_):
    m, o = _[0], _[1]

    m.reverse()
    o.reverse()

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_rotate(_):
    m, o = _[0], _[1]

    m.rotate(2)
    o.rotate(2)

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_rotate_neg(_):
    m, o = _[0], _[1]

    m.rotate(-2)
    o.rotate(-2)

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_setitem(_):
    m, o = _[0], _[1]

    m[0] = m[-1]
    o[0] = o[-1]

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_delitem(_):
    m, o = _[0], _[1]

    del m[0]
    del o[0]

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_add(_):
    m, o = _[0], _[1]

    m = m + m
    o = o + o

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_iadd(_):
    m, o = _[0], _[1]

    m += m
    o += o

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_imul(_):
    m, o = _[0], _[1]

    m *= 3
    o *= 3

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_mul(_):
    m, o = _[0], _[1]

    m = m * 3
    o = o * 3

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_rmul(_):
    m, o = _[0], _[1]

    m = 3 * m
    o = 3 * o

    await m.mongo_pending.join()
    assert m == o
    await db_compare(m, o)


@async_test
async def test_loaded(_):
    m, o = _[0], _[1]

    m_loaded = await MongoDequeReflection.create(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                               dumps=m._dumps, loads=m._loads, maxlen=m.maxlen)

    def compare_nested(m, tested):
        for ix, el in enumerate(tested):
            if isinstance(el, MongoDequeReflection):
                compare_nested(m[ix], el)

            assert m[ix] == el
            assert m.maxlen == tested.maxlen
            assert m.key == tested.key

    compare_nested(m, m_loaded)

    assert m_loaded == o
