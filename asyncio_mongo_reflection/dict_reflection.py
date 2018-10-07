import inspect
from abc import ABC, abstractmethod
from weakref import proxy

from .base import _SyncObjBase, MongoReflectionError
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument


class DictReflection(dict, _SyncObjBase):
    @abstractmethod
    async def _reflection_get(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_clear(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_pop(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_popitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_update(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_setitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _reflection_delitem(self):
        raise NotImplementedError

    def __setitem__(self, key, value):
        super(DictReflection, self).__setitem__(key, value)

        if self._check_nested_type(value):
            value = self._run_now(self._proc_pushed(self, {key: value}))

        elif DequeReflection._check_nested_type(value):
            nested = self._run_now(self._deque_cls._create_nested(self, key, value))
            super(DictReflection, self).__setitem__(key, nested)
            value = {f'{self.key}.{key}': self._run_now(DequeReflection._proc_pushed(nested, value))}

        else:
            value = {f'{self.key}.{key}': self._dumps(value)}

        self._enqueue_coro(self._reflection_setitem(value), self._tree_depth)

    def __delitem__(self, key):
        self._enqueue_coro(self._reflection_delitem(key), self._tree_depth)
        super(DictReflection, self).__delitem__(key)

    @classmethod
    def _flattern(cls, dct, dumps=None):
        fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

        for key, val in dct.items():

            if isinstance(val, DictReflection) or isinstance(val, dict):
                dct[key] = cls._flattern(dict(val), dumps)

            elif DequeReflection._check_nested_type(val):
                dct[key] = DequeReflection._flattern(list(val), dumps)

            else:
                dct[key] = fdumps(val)

        return dct

    @classmethod
    def _move_nested_ixs(cls, self):
        """
        Keeps up right keys for nested reflections after deletion/insertion in higher deque.
        """
        for key, val in self.items():
            if isinstance(val, DequeReflection) or isinstance(val, DictReflection):
                exp_key = f'{self.key}.{key}'
                if val.key != exp_key:
                    val.key = exp_key
                    type(val)._move_nested_ixs(val)

    @classmethod
    async def _create_nested(cls, parent, key, val):
        self = cls.__new__(cls)
        self.__dict__ = parent.__dict__.copy()
        return await cls.init(self, dict(val), key=f'{self.key}.{key}', _parent=proxy(parent))

    @classmethod
    async def _proc_loaded(cls, parent, dct, loads, parent_key=None):
        """
        Creates nested classes after data is loaded from db.
        """
        for key, val in dct.items():

            if isinstance(val, dict):
                set_key = f'{parent_key}.{key}' if parent_key else key
                val = await cls._proc_loaded(parent, val, loads, set_key)
                nested_cls = parent.__class__ if isinstance(parent, DictReflection) else parent._dict_cls
                dct[key] = await nested_cls._create_nested(parent, set_key, val)

            elif isinstance(val, list):
                set_key = f'{parent_key}.{key}' if parent_key else key
                val = await DequeReflection._proc_loaded(parent, val, loads, set_key)
                nested_cls = parent.__class__ if isinstance(parent, DequeReflection) else parent._deque_cls
                dct[key] = await nested_cls._create_nested(parent, set_key, val)

            else:
                dct[key] = loads(val)

        return dct

    @staticmethod
    def _check_nested_type(val):
        return isinstance(val, dict) or isinstance(val, DictReflection)

    @staticmethod
    async def _proc_pushed(self, pdict, recursive_call=False):
        """
        Check elements pushed to dict and create nested classes.
        """
        proc_dict = {}

        for key, val in pdict.items():

            if DequeReflection._check_nested_type(val):
                nested = await self._deque_cls._create_nested(self, key, val)
                super(DictReflection, self).__setitem__(key, nested)
                val = await DequeReflection._proc_pushed(nested, val)

            elif self._check_nested_type(val):
                nested = await self._create_nested(self, key, dict(val))
                super(DictReflection, self).__setitem__(key, nested)
                val = await self._proc_pushed(nested, dict(val), recursive_call=True)

            else:
                val = self._dumps(val)

            if recursive_call:
                proc_dict[key] = val
            else:
                proc_dict[f'{self.key}.{key}'] = val

        return proc_dict

    def __getattribute__(self, name):
        def cb(func, deque_method):

            def inner(*args, **kwargs):
                func_res = func(*args, **kwargs)

                if name == 'popitem':
                    args = list()
                    args.append(func_res[0])

                elif name == 'update':
                    args = list(args)
                    upd_dict = args.pop() if len(args) else None
                    merged_dict = dict(upd_dict, **kwargs) if upd_dict else dict(**kwargs)
                    args.append(self._run_now(self._proc_pushed(self, merged_dict)))
                    kwargs = {}

                coro = getattr(self, f'_reflection_{deque_method}')(*args, **kwargs)
                self._enqueue_coro(coro, self._tree_depth)

                return func_res
            return inner

        res = super().__getattribute__(name)
        if name in ('clear', 'pop', 'popitem', 'remove', 'update'):
            res = cb(res, name)
        return res


class MongoDictReflection(DictReflection):

    async def __ainit__(self, d=None, *, dumps=None, loads=None, **kwargs):

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

        self._deque_cls = MongoDequeReflection
        await super().__ainit__(d, **kwargs)

    async def _reflection_get(self):
        mongo_dict = await self.col.find_one(self.obj_ref, projection={self.key: 1})

        if not mongo_dict:
            mongo_dict = await self.col.find_one_and_update(self.obj_ref, {'$set': {self.key: {}}},
                                                            upsert=True, projection={self.key: 1},
                                                            return_document=ReturnDocument.AFTER)
        nested = self.key.split(sep='.')
        for key in nested:
            mongo_dict = mongo_dict.get(key, None)
            if not mongo_dict:
                break

        if not isinstance(mongo_dict, dict) or not mongo_dict:
            return {}

        return await self._proc_loaded(self, mongo_dict, self._loads)

    async def _reflection_clear(self):
        return await self.col.update_one(self.obj_ref, {'$set': {self.key: {}}})

    async def _reflection_pop(self, pop_key, default=None):
        return await self.col.update_one(self.obj_ref, {'$unset': {f'{self.key}.{pop_key}': ''}})

    async def _reflection_popitem(self, popped_key):
        return await self._reflection_pop(popped_key)

    async def _reflection_update(self, upd_dict):
        return await self.col.update_one(self.obj_ref, {'$set': upd_dict}, upsert=True)

    async def _reflection_setitem(self, val):
        return await self._reflection_update(val)

    async def _reflection_delitem(self, key):
        return await self._reflection_pop(key)


from .deque_reflection import MongoDequeReflection, DequeReflection
