import asyncio
import inspect
import random
from abc import ABC, abstractmethod
from collections import deque, Iterable
from weakref import proxy
from hashlib import sha256
from itertools import zip_longest, islice

from .base import _SyncObjBase, MongoReflectionError
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument


class MongoDequeSimple(deque, ABC):  # pragma: no cover
    """
    Additional class to support compatibility with older versions.
    Will be deprecated soon.
    """
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


class DequeReflection(deque, _SyncObjBase):

    @abstractmethod
    async def _reflection_get(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_append(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_appendleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_clear(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_extend(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_extendleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_insert(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_pop(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_popleft(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_remove(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_reverse(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_rotate(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_setitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_delitem(self):
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

    def __getitem__(self, index):
        if isinstance(index, slice):  # get slices as flat list

            start = index.start
            start = len(self) + start if start and start < 0 else start

            stop = index.stop
            stop = len(self) + stop if stop and stop < 0 else stop

            step = index.step
            sl_step = -1 * step if step and step < 0 else step

            dslice = self._flattern([i for i in islice(self, start, stop, sl_step)], self._dumps)
            if step and step < 0:
                dslice.reverse()

            return dslice

        return super(DequeReflection, self).__getitem__(index)

    def __setitem__(self, key, value):
        set_kvs = []
        ins_vs = []
        ins_ix = None
        from_left = False

        if type(key) is int:
            key = len(self) + key if key < 0 else key
            set_kvs.append((key, value))
            super(DequeReflection, self).__setitem__(key, value)

        else:
            # slices assigned iterable support
            if self[key] is []:
                return

            value = [i for i in value.__iter__()]
            start = key.start
            stop = key.stop

            # pure insertion slice
            if start and stop and (start == stop or start > stop):
                stop = start
                ins_vs = value[:]
            # pure replacement slice or mixed with insertion
            else:
                changed_ixs = [i for i in range(*key.indices(len(self)))]
                from_left = start is None and len(value) > len(changed_ixs)

                if from_left:
                    split_at = len(changed_ixs)
                    ins_vs = value[:split_at]
                    ins_vs.reverse()
                    value = value[split_at:]

                for ix, val in zip_longest(changed_ixs, value):
                    if ix is not None:
                        set_kvs.append((ix, val))
                        super(DequeReflection, self).__setitem__(ix, val)
                    else:
                        ins_vs.append(val)

            ins_val_at = ins_ix = (stop if stop else len(self)) if not from_left else 0
            for val in ins_vs:
                super(DequeReflection, self).insert(ins_val_at, val)
                if not from_left:
                    ins_val_at += 1

        self._move_nested_ixs(self)

        # separate db operations for replacements
        for key, value in set_kvs:
            if self._check_nested_type(value):
                value = self._run_now(self._proc_pushed(self, [value]))

            elif DictReflection._check_nested_type(value):
                nested = self._run_now(self._dict_cls._create_nested(self, key, value))
                super(DequeReflection, self).__setitem__(key, nested)
                value = [self._run_now(DictReflection._proc_pushed(nested, dict(value), recursive_call=True))]

            else:
                value = [self._dumps(value)]

            self._enqueue_coro(self._reflection_setitem(key, value), self._tree_depth)

        # insert with one reflection extend db operation
        if ins_vs:
            if from_left:
                ins_vs.reverse()

            if ins_ix < 0:
                ins_ix = len(self) + ins_ix - 1

            self._run_now(self._proc_pushed(self, ins_vs))
            self._enqueue_coro(self._reflection_extend(ins_vs, position=ins_ix), self._tree_depth)

    def __delitem__(self,  key):
        super(DequeReflection, self).__delitem__(key)
        self._move_nested_ixs(self)
        self._enqueue_coro(self._reflection_delitem(key), self._tree_depth)

    @classmethod
    def _flattern(cls, nlist, dumps=None):
        fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

        for ix, el in enumerate(nlist):
            if isinstance(el, DequeReflection) or isinstance(el, deque):
                nlist[ix] = cls._flattern(list(el), dumps)
            elif DictReflection._check_nested_type(el):
                nlist[ix] = DictReflection._flattern(dict(el), dumps)
            else:
                nlist[ix] = fdumps(el)

        return nlist

    @classmethod
    def _move_nested_ixs(cls, self):
        """
        Keeps up right keys for nested reflections after deletion/insertion.
        """
        for ix, el in enumerate(self):
            if isinstance(el, DequeReflection) or isinstance(el, DictReflection):
                exp_key = f'{self.key}.{ix}'
                if el.key != exp_key:
                    el.key = exp_key
                    type(el)._move_nested_ixs(el)

    @classmethod
    async def _create_nested(cls, parent, ix, val):
        self = cls.__cnew__(cls)
        self.__dict__ = parent.__dict__.copy()
        maxlen = getattr(val, 'maxlen', getattr(parent, 'maxlen', None))
        return await cls.init(self, list(val), key=f'{self.key}.{ix}',
                              maxlen=maxlen, _parent=proxy(parent))

    @classmethod
    async def _proc_loaded(cls, parent, arr, loads, parent_ix=None):
        """
        Creates nested classes after data is loaded from db.
        """
        for ix, el in enumerate(arr):

            if isinstance(el, list):
                set_ix = f'{parent_ix}.{ix}' if parent_ix else ix
                el = await cls._proc_loaded(parent, el, loads, set_ix)
                nested_cls = parent.__class__ if isinstance(parent, DequeReflection) else parent._deque_cls
                arr[ix] = await nested_cls._create_nested(parent, set_ix, el)

            elif isinstance(el, dict):
                set_ix = f'{parent_ix}.{ix}' if parent_ix else ix
                el = await DictReflection._proc_loaded(parent, el, loads, set_ix)
                nested_cls = parent.__class__ if isinstance(parent, DictReflection) else parent._dict_cls
                arr[ix] = await nested_cls._create_nested(parent, set_ix, el)

            else:
                arr[ix] = loads(el)

        return arr

    def _find_el(self, el):
        """
        Support same elements in list
        """
        found_mod_at = []
        found_flat_at = []
        for ix, val in enumerate(self):
            if val == el:
                if isinstance(val, DictReflection) or isinstance(val, DequeReflection):
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
    def _check_nested_type(el):
        return isinstance(el, list) or isinstance(el, deque) or isinstance(el, DequeReflection)

    @staticmethod
    async def _proc_pushed(self, arg, from_left=False):
        """
        Check elements pushed to deque and create nested classes.
        """
        push_arr = []

        if not isinstance(arg, Iterable):
            return arg

        for el in arg:
            if DictReflection._check_nested_type(el):
                try:
                    ix = self._find_el(el)
                except ValueError:  # trimmed by maxlen
                    el = await DictReflection._proc_pushed(self, dict(el), recursive_call=True)
                else:
                    nested = await self._dict_cls._create_nested(self, ix, el)
                    super(DequeReflection, self).__setitem__(ix, nested)
                    el = await DictReflection._proc_pushed(self[ix], dict(el), recursive_call=True)

            elif self._check_nested_type(el):
                try:
                    ix = self._find_el(el)
                except ValueError:   # trimmed by maxlen
                    el = await self._proc_pushed(self, list(el))
                else:
                    nested = await self._create_nested(self, ix, el)
                    super(DequeReflection, self).__setitem__(ix, nested)
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

                self._enqueue_coro(getattr(self, f'_reflection_{deque_method}')(*args), self._tree_depth)
                self._move_nested_ixs(self)
                return func_res

            return inner

        res = super().__getattribute__(name)
        if name in {'append', 'appendleft', 'clear', 'extend', 'extendleft', 'insert',
                    'pop', 'popleft', 'remove', 'reverse', 'rotate'}:
            res = cb(res, name)
        return res


class MongoDequeReflection(DequeReflection):

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

        self._dict_cls = MongoDictReflection
        await super().__ainit__(lst, **kwargs)

    async def _reflection_get(self):
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

    async def _reflection_append(self, el):
        return await self._reflection_extend(el)

    async def _reflection_appendleft(self, el):
        return await self._reflection_extendleft(el)

    async def _reflection_clear(self):
        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}': []}})

    async def _reflection_extend(self, arr, maxlen=None, position=None):
        maxlen = maxlen or self.maxlen
        mongo_slice = {'$slice': -maxlen} if maxlen else {}
        mongo_position = {'$position': position} if position is not None else {}

        push_val = {'$each': arr}
        push_val.update(mongo_slice)
        push_val.update(mongo_position)

        return await self.col.update_one(self.obj_ref,
                                         {'$push': {f'{self.key}': push_val}}, upsert=True)

    async def _reflection_extendleft(self, arr):
        maxlen = -self.maxlen if self.maxlen is not None else None
        return await self._reflection_extend(arr, maxlen, 0)

    async def _reflection_insert(self, ix, el):
        return await self._reflection_extend(el, position=ix)

    async def _reflection_pop(self):
        return await self.col.update_one(self.obj_ref, {'$pop': {f'{self.key}': 1}})

    async def _reflection_popleft(self):
        return await self.col.update_one(self.obj_ref, {'$pop': {f'{self.key}': -1}})

    async def _reflection_remove(self, el):
        h = sha256(str(random.getrandbits(256)).encode('utf-8')).hexdigest()

        if self._check_nested_type(el):
            el = self._flattern(list(el), self._dumps)
        if DictReflection._check_nested_type(el):
            el = DictReflection._flattern(dict(el), self._dumps)

        if type(el) not in (list, dict):
            el = self._dumps(el)

        ref = self.obj_ref.copy()
        ref.update({f'{self.key}': el})
        await self.col.update_one(ref, {'$set': {f'{self.key}.$': h}})

        await self.col.update_one(self.obj_ref, {'$pull': {f'{self.key}': h}})

    async def _reflection_reverse(self):

        pipeline = [{'$match': self.obj_ref},
                    {'$project': {f'{self.key}': {'$reverseArray': f'${self.key}'}}}]

        doc = await self.col.aggregate(pipeline).__anext__()

        nested = self.key.split(sep='.')
        for key in nested:
            doc = doc[key]

        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}': doc}})

    async def _reflection_rotate(self, num):
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

    async def _reflection_setitem(self, ix, el):
        el = el[0]
        if isinstance(self[ix], DequeReflection) and getattr(el, 'nested_ix', None) == ix:
            return

        return await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}.{ix}': el}})

    async def _reflection_delitem(self, ix):
        h = random.getrandbits(32)

        await self.col.update_one(self.obj_ref, {'$set': {f'{self.key}.{ix}': h}})
        return await self.col.update_one(self.obj_ref, {'$pull': {f'{self.key}': h}})


from .dict_reflection import MongoDictReflection, DictReflection
