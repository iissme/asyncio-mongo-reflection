import inspect
from abc import ABC, abstractmethod
from weakref import proxy

from .base import _SyncObjBase, MongoReflectionError
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument


class MongoDict(dict, _SyncObjBase):
    @abstractmethod
    async def _mongo_get(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_clear(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_pop(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_popitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_update(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_setitem(self):
        raise NotImplementedError

    @abstractmethod
    async def _mongo_delitem(self):
        raise NotImplementedError

    @staticmethod
    def _instance_from_outside(v):
        # dirty, but haven't found another way yet
        if isinstance(v, MongoDictReflection):
            stack = inspect.stack()
            if stack[2][3] != '_proc_pushed' and stack[2][3] != '_proc_loaded':
                return True

    def __setitem__(self, key, value):
        super(MongoDict, self).__setitem__(key, value)

        if not hasattr(value, '_parent') or self._instance_from_outside(value):
            if isinstance(value, dict) or isinstance(value, MongoDictReflection):
                value = self._run_now(self._proc_pushed(self, {key: value}))
            else:
                value = {f'{self.key}.{key}': self._dumps(value)}

            self._enqueue_coro(self._mongo_setitem(value), self._tree_depth)

    def __delitem__(self, key):
        self._enqueue_coro(self._mongo_delitem(key), self._tree_depth)
        super(MongoDict, self).__delitem__(key)

    @classmethod
    def _flattern(cls, dct, dumps=None):
        fdumps = lambda arg: dumps(arg) if callable(dumps) else arg

        for key, val in dct.items():
            if isinstance(val, MongoDictReflection) or isinstance(val, dict):
                dct[key] = cls._flattern(dict(val), dumps)
            elif MongoDequeReflection._check_nested_type(val):
                dct[key] = MongoDequeReflection._flattern(val, dumps)
            else:
                dct[key] = fdumps(val)
        return dct

    @staticmethod
    def _check_nested_type(val):
        return isinstance(val, dict) or isinstance(val, MongoDictReflection)

    @staticmethod
    async def _proc_pushed(self, pdict, recursive_call=False):

        to_mongo_dict = {}
        for key, val in pdict.items():
            if MongoDequeReflection._check_nested_type(val):
                self[key] = rec_self = await MongoDequeReflection._create_nested(self, key, list(val))
                val = await rec_self._proc_pushed(rec_self, val)
            elif self._check_nested_type(val):
                self[key] = rec_self = await self._create_nested(self, key, dict(val))
                val = await self._proc_pushed(rec_self, dict(val), recursive_call=True)
            else:
                val = self._dumps(val)

            if recursive_call:
                to_mongo_dict[key] = val
            else:
                to_mongo_dict[f'{self.key}.{key}'] = val

        return to_mongo_dict

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

                self._enqueue_coro(getattr(self, f'_mongo_{deque_method}')(*args, **kwargs), self._tree_depth)
                return func_res

            return inner

        res = super().__getattribute__(name)
        if name in ('clear', 'pop', 'popitem', 'remove', 'update'):
            res = cb(res, name)
        return res


class MongoDictReflection(MongoDict):

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

        await super().__ainit__(d, **kwargs)

    @classmethod
    async def _create_nested(cls, parent, key, val):
        self = cls.__cnew__(cls)
        self.__dict__ = parent.__dict__.copy()
        return await cls.init(self, val, key=f'{self.key}.{key}', _parent=proxy(parent))

    async def _mongo_get(self):
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

    @classmethod
    async def _proc_loaded(cls, parent, dct, loads, parent_key=None):
        for key, val in dct.items():
            if isinstance(val, dict):
                set_key = f'{parent_key}.{key}' if parent_key else key
                val = await cls._proc_loaded(parent, val, loads, set_key)
                dct[key] = await cls._create_nested(parent, set_key, val)
            elif isinstance(val, list):
                set_key = f'{parent_key}.{key}' if parent_key else key
                val = await MongoDequeReflection._proc_loaded(parent, val, loads, set_key)
                dct[key] = await MongoDequeReflection._create_nested(parent, set_key, val)
            else:
                dct[key] = loads(val)
        return dct

    async def _mongo_clear(self):
        return await self.col.update_one(self.obj_ref, {'$set': {self.key: {}}})

    async def _mongo_pop(self, pop_key, default=None):
        return await self.col.update_one(self.obj_ref, {'$unset': {f'{self.key}.{pop_key}': ''}})

    async def _mongo_popitem(self, popped_key):
        return await self._mongo_pop(popped_key)

    async def _mongo_update(self, upd_dict):
        return await self.col.update_one(self.obj_ref, {'$set': upd_dict}, upsert=True)

    async def _mongo_setitem(self, val):
        return await self._mongo_update(val)

    async def _mongo_delitem(self, key):
        return await self._mongo_pop(key)


from .mongodeque import MongoDequeReflection
