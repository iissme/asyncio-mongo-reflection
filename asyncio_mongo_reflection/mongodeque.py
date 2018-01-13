import asyncio
import inspect
import random
from abc import ABC, abstractmethod
from collections import deque, Iterable
from weakref import proxy

from .base import _SyncObjBase, MongoReflectionError
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument


class MongoDequeSimple(deque, ABC):  # pragma: no cover
    @classmethod
    async def create(cls, **kwargs):
        self = cls()
        self.loop = asyncio.get_event_loop()
        self._run_coro = lambda coro: self.loop.create_task(coro)

        maxlen = kwargs.pop('maxlen', None)
        for name, arg in kwargs.items():
            setattr(self, name, arg)

        items = await self._mongo_get()
        super(MongoDequeSimple, self).__init__(items, maxlen=maxlen)
        return self

    @abstractmethod
    async def _mongo_get(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_update(self):
        raise NotImplementedError

    def __add__(self, other):
        arr = list(other).copy()
        self.loop.run_until_complete(self.extend(arr))
        return self

    def __iadd__(self, other):
        return self.__add__(other)

    def __mul__(self, other):
        arr = list(self).copy() * (other - 1)
        self.loop.run_until_complete(self.extend(arr))
        return self

    def __imul__(self, other):
        return self.__mul__(other)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __setitem__(self, key, value):
        super(MongoDequeSimple, self).__setitem__(key, value)
        self._run_coro(self._mongo_update())

    def __delitem__(self, key):
        super(MongoDequeSimple, self).__delitem__(key)
        self._run_coro(self._mongo_update())

    def __getattribute__(self, name):
        def cb(func):
            async def inner(*args, **kwargs):
                r = func(*args, **kwargs)
                try:
                    await self._mongo_update()
                except Exception as e:
                    raise MongoReflectionError(e)
                return r

            return inner

        res = super().__getattribute__(name)
        if name in ('append', 'appendleft', 'clear', 'extend', 'extendleft', 'insert',
                    'pop', 'popleft', 'remove', 'reverse', 'rotate'):
            res = cb(res)
        return res


class MongoDeque(deque, _SyncObjBase):

    @abstractmethod
    async def _mongo_get(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_append(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_appendleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_clear(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_extend(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_extendleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_insert(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_pop(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_popleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_remove(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_reverse(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_rotate(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_setitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_delitem(self):
        raise NotImplementedError

    def __add__(self, other):
        other = self._flattern(list(other))
        arr = list(other)
        self.extend(arr)
        return self

    def __iadd__(self, other):
        return self.__add__(other)

    def __mul__(self, num):
        flat_self = self._flattern(list(self))
        arr = flat_self * (num - 1)
        self.extend(arr)
        return self

    def __imul__(self, num):
        return self.__mul__(num)

    def __rmul__(self, num):
        return self.__mul__(num)

    @staticmethod
    def _instance_from_outside(v):
        # dirty, but haven't found another way yet
        if isinstance(v, MongoDequeReflection):
            stack = inspect.stack()
            if stack[2][3] != '_proc_pushed' and stack[2][3] != '_proc_loaded':
                return True

    def __setitem__(self, key, value):
        super(MongoDeque, self).__setitem__(key, value)

        if not hasattr(value, '_parent') or self._instance_from_outside(value):
            if self._check_nested_type(value):
                value = self._run_now(self._proc_pushed(self, [value]))
            else:
                value = [self._dumps(value)]
            self._enqueue_coro(self._mongo_setitem(key, value), self._tree_depth)

    def __delitem__(self,  key):
        super(MongoDeque, self).__delitem__(key)
        self._move_nested_ixs(self)
        self._enqueue_coro(self._mongo_delitem(key), self._tree_depth)

    @classmethod
    def _flattern(cls, nlist, dumps=None):
        fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

        for ix, el in enumerate(nlist):
            if isinstance(el, MongoDequeReflection) or isinstance(el, deque):
                nlist[ix] = list(el)
                cls._flattern(nlist[ix], dumps)
            elif MongoDictReflection._check_nested_type(el):
                nlist[ix] = MongoDictReflection._flattern(dict(el), dumps)
            else:
                nlist[ix] = fdumps(el)

        return nlist

    @staticmethod
    def _check_nested_type(el):
        return isinstance(el, list) or isinstance(el, deque) or isinstance(el, MongoDequeReflection)

    @staticmethod
    def _move_nested_ixs(self):
        for ix, el in enumerate(self):
            if isinstance(el, MongoDeque):
                exp_key = f'{self.key}.{ix}'
                if el.key != exp_key:
                    el.nested_ix = ix
                    el.key = exp_key
                    self._move_nested_ixs(el)

    def _find_el(self, el):
        found_mod_at = []
        found_flat_at = []
        for ix, val in enumerate(self):
            if val == el:
                if isinstance(val, MongoDictReflection) or isinstance(val, MongoDequeReflection):
                    found_mod_at.append(ix)
                else:
                    found_flat_at.append(ix)

        if not found_flat_at and not found_mod_at:
            raise ValueError

        if not found_flat_at:
            return found_mod_at[0]
        else:
            return found_flat_at[0]

    @staticmethod
    async def _proc_pushed(self, arg, from_left=False):
        push_arr = []

        if not isinstance(arg, Iterable):
            return arg

        for el in arg:
            if MongoDictReflection._check_nested_type(el):
                try:
                    ix = self._find_el(el)
                except ValueError:  # trimmed by maxlen
                    el = await MongoDictReflection._proc_pushed(self, dict(el), recursive_call=True)
                else:
                    self[ix] = await MongoDictReflection._create_nested(self, ix, el)
                    el = await MongoDictReflection._proc_pushed(self[ix], dict(el), recursive_call=True)

            elif self._check_nested_type(el):
                try:
                    ix = self._find_el(el)
                except ValueError:
                    el = await self._proc_pushed(self, list(el))
                else:
                    self[ix] = await self._create_nested(self, ix, el)
                    el = await self._proc_pushed(self[ix], list(el))

            else:
                el = self._dumps(el)

            if from_left:
                push_arr.insert(0, el)
            else:
                push_arr.append(el)

        return push_arr

    def __getattribute__(self, name):
        def cb(func, deque_method):
            def inner(*args, **kwargs):
                func_res = func(*args, **kwargs)

                if name in {'append', 'appendleft', 'extend', 'extendleft', 'insert'}:
                    args = list(args)
                    p_ix = 0 if name is not 'insert' else 1
                    if name in {'append', 'appendleft', 'insert'}:
                        args[p_ix] = [args[p_ix]]

                    args[p_ix] = self._run_now(self._proc_pushed(
                        self, args[p_ix], from_left=True if name is 'extendleft' else False))

                self._enqueue_coro(getattr(self, f'_mongo_{deque_method}')(*args), self._tree_depth)
                self._move_nested_ixs(self)
                return func_res

            return inner

        res = super().__getattribute__(name)
        if name in {'append', 'appendleft', 'clear', 'extend', 'extendleft', 'insert',
                    'pop', 'popleft', 'remove', 'reverse', 'rotate'}:
            res = cb(res, name)
        return res


class MongoDequeReflection(MongoDeque):

    async def __ainit__(self, lst=list(), *, dumps=None, loads=None, **kwargs):

        if not hasattr(self, '_dumps'):
            self._dumps = lambda arg: dumps(arg) if callable(dumps) else arg
        if not hasattr(self, '_loads'):
            self._loads = lambda arg: loads(arg) if callable(loads) else arg

        if 'col' in kwargs:
            self.col = kwargs.pop('col')
        if 'obj_ref' in kwargs:
            self.obj_ref = kwargs.pop('obj_ref')
        if 'key' in kwargs:
            self.key = kwargs.pop('key')

        if not hasattr(self, 'col') or not hasattr(self, 'obj_ref') or not hasattr(self, 'key'):
            raise MongoReflectionError('You need to provide "col", "obj_ref" and "key" named arguments!')
        elif not isinstance(self.col, AsyncIOMotorCollection):
            raise TypeError('"col" argument must be a AsyncIOMotorCollection instance!')

        await super().__ainit__(lst, **kwargs)

    @classmethod
    async def _create_nested(cls, parent, ix, val):
        self = cls.__cnew__(cls)
        self.__dict__ = parent.__dict__.copy()
        maxlen = getattr(val, 'maxlen', None)
        self.nested_ix = ix
        return await cls.init(self, list(val), key=f'{self.key}.{ix}',
                              maxlen=maxlen, _parent=proxy(parent))

    async def _mongo_get(self):
        mongo_arr = await self.col.find_one(self.obj_ref, projection={self.key: 1})

        if not mongo_arr:
            mongo_arr = await self.col.find_one_and_update(self.obj_ref, {'$set': {self.key: []}},
                                                           upsert=True, projection={self.key: 1},
                                                           return_document=ReturnDocument.AFTER)

        nested = self.key.split(sep='.')
        for key in nested:
            mongo_arr = mongo_arr[key if not key.isdecimal() else int(key)]
            if not mongo_arr:
                break

        if not isinstance(mongo_arr, list) or not mongo_arr:
            return []

        return await self._proc_loaded(self, mongo_arr, self._loads)

    @classmethod
    async def _proc_loaded(cls, parent, arr, loads, parent_ix=None):
        for ix, el in enumerate(arr):
            if isinstance(el, list):
                set_ix = f'{parent_ix}.{ix}' if parent_ix else ix
                el = await cls._proc_loaded(parent, el, loads, set_ix)
                arr[ix] = await cls._create_nested(parent, set_ix, el)
            elif isinstance(el, dict):
                set_ix = f'{parent_ix}.{ix}' if parent_ix else ix
                el = await MongoDictReflection._proc_loaded(parent, el, loads, set_ix)
                arr[ix] = await MongoDictReflection._create_nested(parent, set_ix, el)
            else:
                arr[ix] = loads(el)
        return arr

    async def _mongo_append(self, el):
        return await self._mongo_extend(el)

    async def _mongo_appendleft(self, el):
        return await self._mongo_extendleft(el)

    async def _mongo_clear(self):
        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}': []}})

    async def _mongo_extend(self, arr, maxlen=None, position=None):
        maxlen = maxlen or self.maxlen
        mongo_slice = {'$slice': -maxlen} if maxlen else {}
        mongo_position = {'$position': position} if position is not None else {}

        push_val = {'$each': arr}
        push_val.update(mongo_slice)
        push_val.update(mongo_position)

        return await self.col.update_one(self.obj_ref,
                                         {'$push': {f'{self.key}': push_val}}, upsert=True)

    async def _mongo_extendleft(self, arr):
        maxlen = -self.maxlen if self.maxlen is not None else None
        return await self._mongo_extend(arr, maxlen, 0)

    async def _mongo_insert(self, ix, el):
        return await self._mongo_extend(el, position=ix)

    async def _mongo_pop(self):
        return await self.col.update_one(self.obj_ref, {'$pop': {f'{self.key}': 1}})

    async def _mongo_popleft(self):
        return await self.col.update_one(self.obj_ref, {'$pop': {f'{self.key}': -1}})

    async def _mongo_remove(self, el):
        h = random.getrandbits(32)

        if self._check_nested_type(el):
            el = self._flattern(list(el), self._dumps)
        if MongoDictReflection._check_nested_type(el):
            el = MongoDictReflection._flattern(dict(el), self._dumps)

        ref = self.obj_ref.copy()
        ref.update({f'{self.key}': el})
        await self.col.update_one(ref, {'$set': {f'{self.key}.$': h}})

        await self.col.update_one(self.obj_ref, {'$pull': {f'{self.key}': h}})

    async def _mongo_reverse(self):

        pipeline = [{'$match': self.obj_ref},
                    {'$project': {f'{self.key}': {'$reverseArray': f'${self.key}'}}}]

        doc = await self.col.aggregate(pipeline).__anext__()

        nested = self.key.split(sep='.')
        for key in nested:
            doc = doc[key]

        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}': doc}})

    async def _mongo_rotate(self, num):
        # haven't found any mongo eq. (could be replaced with full reflection update)
        def rotate(a, r=1):
            if len(a) == 0:
                return a
            r = -r % len(a)
            return a[r:] + a[:r]

        obj = await self.col.find_one(self.obj_ref, projection={self.key: 1})
        nested = self.key.split(sep='.')
        for key in nested:
            obj = obj[key]

        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}': rotate(obj, num)}})

    async def _mongo_setitem(self, ix, el):
        el = el[0]
        if isinstance(self[ix], MongoDequeReflection) and getattr(el, 'nested_ix', None) == ix:
            return

        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}.{ix}': el}})

    async def _mongo_delitem(self, ix):
        h = random.getrandbits(32)

        await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}.{ix}': h}})
        return await self.col.update_one(self.obj_ref, {'$pull': {f'{self.key}': h}})


from .mongodict import MongoDictReflection
