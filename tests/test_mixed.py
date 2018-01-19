from tests.test_asyncio_prepare import *

lrun_uc(db['test_mixed'].remove())

col = db['test_mixed']
obj_ref = {'mixed_id': 'test_mixed'}
key = 'test_mixed.inner'

mongo_mixed_dict = lrun_uc(
    MongoDictReflection(
        {
            'a': 1,
            'b': [1, {'a': 1, 'b': 2}, [1, 2, 3], 4, 5],
            'c': {'e': 4},
            'd': 2
        },
        col=col, obj_ref=obj_ref,
        key=key + '_dict', dumps=None)
)

mongo_mixed_list = lrun_uc(
    MongoDequeReflection(
        [
            1,
            [1, 2, 3],
            {'a': {'a': 1, 'b': 2, 'p': [6, 7, 8]}, 'b': [1, 2, 3], 'c': 3},
            2,
            3
        ],
        col=col, obj_ref=obj_ref,
        key=key + '_list', dumps=None)
)


async def mongo_compare(ex, m):
    obj = await db[m.col.name].find_one(m.obj_ref)

    nested = m.key.split(sep='.')
    for k in nested:
        obj = obj[k]

    assert obj == ex


@pytest.fixture(scope="function",
                params=[mongo_mixed_dict],
                ids=['dict'])
def _d(request):
    return request.param, flattern_dict_nested(dict(request.param), lists_to_deque=True)


@pytest.fixture(scope="function",
                params=[mongo_mixed_list],
                ids=['list'])
def _l(request):
    return request.param, flattern_list_nested(deque(list(request.param)))


@async_test
async def test_default_list_loaded(_l):
    m, o = _l[0], _l[1]

    m_loaded = await MongoDequeReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                          dumps=m._dumps, loads=m._loads, maxlen=m.maxlen)

    compare_nested_list(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_default_dict_loaded(_d):
    m, o = _d[0], _d[1]

    m_loaded = await MongoDictReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                         dumps=m._dumps, loads=m._loads)

    compare_nested_dict(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_dict_pop(_d):
    m, o = _d[0], _d[1]

    # dict - list
    m['b'].pop()
    o['b'].pop()
    # dict - list (new)
    m['g'] = [6, 7, 8]
    m['g'].pop()
    o['g'] = deque([6, 7, 8])
    o['g'].pop()

    # dict - list - list
    m['b'][2].pop()
    o['b'][2].pop()
    # dict - list - list (new)
    m['b'].append([1, 2, 3])
    m['b'][-1].pop()
    o['b'].append(deque([1, 2, 3]))
    o['b'][-1].pop()

    # dict - list - dict
    m['b'][1].popitem()
    o['b'][1].popitem()
    # dict - list - dict (new)
    m['b'].append({'f': 1, 'g': 2})
    m['b'][-1].popitem()
    o['b'].append({'f': 1, 'g': 2})
    o['b'][-1].popitem()

    await m.mongo_pending.join()

    assert m == o
    await mongo_compare(flattern_dict_nested(dict(m), m._dumps), m)


@async_test
async def test_list_pop(_l):
    m, o = _l[0], _l[1]

    # list - dict
    m[2].popitem()
    o[2].popitem()
    # list - dict (new)
    m[-1] = {'f': 1, 'g': 2}
    m[-1].popitem()
    o[-1] = {'f': 1, 'g': 2}
    o[-1].popitem()

    # list - dict - list
    m[2]['b'].pop()
    o[2]['b'].pop()
    # list - dict - list (new)
    m[2]['g'] = [1, 2, 3]
    m[2]['g'].pop()
    o[2]['g'] = deque([1, 2, 3])
    o[2]['g'].pop()

    # list - dict - dict
    m[2]['a'].popitem()
    o[2]['a'].popitem()
    # list - dict - dict (new)
    m[2]['h'] = {'j': 1, 'i': 2}
    m[2]['h'].popitem()
    o[2]['h'] = {'j': 1, 'i': 2}
    o[2]['h'].popitem()

    await m.mongo_pending.join()

    assert m == o
    await mongo_compare(flattern_list_nested(list(m), m._dumps, lists_to_deque=False), m)


@async_test
async def test_list_slice(_l):
    m, o = _l[0], list(_l[1])
    # get slice
    m_slice = m[2:6:2]
    o_slice = o[2:6:2]
    assert m_slice == flattern_list_nested(o_slice, m._dumps, lists_to_deque=False)
    # add deep nesting to check ixs after slices
    m[2]['b'].append({'g': 4})
    o[2]['b'].append({'g': 4})
    # slice from lef
    m[:2] = [3, 4, 6, 7]
    o[:2] = [3, 4, 6, 7]
    # slice with step
    m[0:4:2] = [33, 66]
    o[0:4:2] = [33, 66]
    # insert 2 instead of 1 element
    m[-2:-1] = [{'b': 10}, {'c': 10}]
    o[-2:-1] = [{'b': 10}, {'c': 10}]
    # insert from right
    m[5:] = [8, 9, 10, 11]
    o[5:] = [8, 9, 10, 11]
    # insert at negative index with step
    m[:-1:5] = [88, 77]
    o[:-1:5] = [88, 77]
    # negative step
    m[::-3] = [66, 77, 88]
    o[::-3] = [66, 77, 88]
    # insert uncommon iterable
    m[-2:-2] = {'b': 10}
    o[-2:-2] = {'b': 10}
    # start > stop insert
    m[4:3] = {'q': 10, 'c': 10}
    o[4:3] = {'q': 10, 'c': 10}

    await m.mongo_pending.join()

    assert m == deque(o)
    await mongo_compare(flattern_list_nested(list(m), m._dumps, lists_to_deque=False), m)


@async_test
async def test_final_list_loaded(_l):
    m, o = _l[0], _l[1]

    m_loaded = await MongoDequeReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                          dumps=m._dumps, loads=m._loads, maxlen=m.maxlen)

    compare_nested_list(m, m_loaded)

    assert m_loaded == o


@async_test
async def test_final_dict_loaded(_d):
    m, o = _d[0], _d[1]

    m_loaded = await MongoDictReflection(col=m.col, obj_ref=m.obj_ref, key=m.key,
                                         dumps=m._dumps, loads=m._loads)

    compare_nested_dict(m, m_loaded)

    assert m_loaded == o
