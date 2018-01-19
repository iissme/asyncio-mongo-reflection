from copy import deepcopy
from json import dumps, loads

from tests.test_asyncio_prepare import *

lrun_uc(db['test_arr_int'].remove())
lrun_uc(db['test_arr_str'].remove())
lrun_uc(db['test_arr_obj'].remove())

MAX_LEN = 7


mongo_int = lrun_uc(MongoDequeReflection([0, 4, 3, 33, 5, deque([1, 2, 3]), 2, 53, 4, 5],
                                         col=db['test_arr_int'],
                                         obj_ref={'array_id': 'test_deque'},
                                         key='inner.arr',
                                         dumps=None, maxlen=MAX_LEN))

mongo_str = lrun_uc(MongoDequeReflection([1, 2, 3, 4, 5],
                                         col=db['test_arr_str'],
                                         obj_ref={'array_id': 'test_deque'},
                                         key='inner.arr',
                                         dumps=str, loads=int, maxlen=MAX_LEN+1))

mongo_obj = lrun_uc(MongoDequeReflection([{'a': 1}, {'b': 1}, {'c': 1}, {'d': 1}, {'e': 1}],
                                         col=db['test_arr_obj'],
                                         obj_ref={'array_id': 'test_deque'},
                                         key='inner.arr',
                                         dumps=dumps,
                                         loads=loads, maxlen=MAX_LEN+2))


async def db_compare(m, o):
    await mongo_compare(flattern_list_nested(list(o), dumps=m._dumps, lists_to_deque=False), m)


@pytest.fixture(scope="function",
                params=[mongo_int, mongo_str, mongo_obj],
                ids=['int', 'str', 'obj'])
def _(request):
    return request.param, flattern_list_nested(deque(list(request.param), maxlen=request.param.maxlen))


@async_test
async def test_default_loaded(_):
    m, o = _[0], _[1]

    m_loaded = await MongoDequeReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                          dumps=m._dumps, loads=m._loads, maxlen=m.maxlen)

    compare_nested_list(m, m_loaded)

    assert m_loaded == o


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

    # make sure there's no 'loop already' mistakes
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

    m_loaded = await MongoDequeReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                          dumps=m._dumps, loads=m._loads, maxlen=m.maxlen)

    compare_nested_list(m, m_loaded)

    assert m_loaded == o
