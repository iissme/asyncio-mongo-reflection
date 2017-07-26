import asyncio
import weakref
import functools

from pymongo.collection import UpdateResult


class MongoReflectionError(Exception):
    pass


class AsyncCoroQueueDispatcher:
    """
    Dispatcher gets coroutine from its iternal queue,
    runs is asynchronously (in background) and waits untill it's done,
    then gets next one. It's meant to be embed in another class.

    'Create' method should be run via asyncio.ensure_future or loop.create_task.
    """

    @functools.total_ordering
    class Task:

        def __init__(self, coro, priority):
            self.coro = coro
            self.priority = priority

        def __lt__(self, other):
            return self.priority < other.priority

        def __eq__(self, other):
            return self.priority == other.priority

        def __repr__(self):
            return f'Coro - {repr(self.coro)} Priority - {self.priority}'

    def __init__(self, loop=None, external_cb=None):
        self.loop = loop if loop else asyncio._get_running_loop()
        self.tasks_queue = asyncio.PriorityQueue()
        self.results_queue = asyncio.Queue(maxsize=10)
        self._process_next = asyncio.Event()
        self._dispatcher_task = None
        # Pass cb weakref to prevent gc in some cases and let dispatcher finish all tasks.
        # Cb takes 2 positional arguments: task result and task exception.
        self._external_cb = external_cb if callable(external_cb) else None

    async def _queue_consumer(self):
        while True:
            self._process_next.clear()
            pending_task = await self.tasks_queue.get()
            asyncio.ensure_future(pending_task.coro(), loop=self.loop)
            await self._process_next.wait()
            self.tasks_queue.task_done()

    async def create(self):
        try:
            self._dispatcher_task = self.loop.create_task(self._queue_consumer())
            await asyncio.Future()
        except asyncio.CancelledError:
            # waits for remaining tasks when diispatcher's task is cancelled
            await self.tasks_queue.join()
            self._dispatcher_task.cancel()

    def enqueue_coro(self, coro, priority=1):

        def future_wrapper(coro, future):
            @functools.wraps(coro)
            async def inner():
                try:
                    res = await coro
                except Exception as e:
                    future.set_exception(e)
                else:
                    future.set_result(res)
                finally:
                    self._process_next.set()
            return inner

        def task_cb(future):
            if isinstance(self._external_cb, weakref.ReferenceType):
                external_cb = self._external_cb()
            else:
                external_cb = self._external_cb

            try:
                res = future.result()
                asyncio.ensure_future(self.results_queue.put(res), loop=self.loop)
                if external_cb:
                    external_cb(res, None)
            except Exception as e:
                if external_cb:
                    external_cb(None, exc=e)
                else:
                    raise e

        f = asyncio.Future()
        f.add_done_callback(task_cb)
        self.tasks_queue.put_nowait(self.Task(future_wrapper(coro, f), priority))


class _SyncObjBase:

    def __new__(cls, *args, **kwargs):
        if cls is _SyncObjBase:
            raise TypeError("SyncObjBase class can not be instantiated!")
        return object.__new__(cls, *args, **kwargs)

    @staticmethod
    def _dispatcher_cb(res, exc):
        if exc:
            raise MongoReflectionError(exc)
        else:
            if isinstance(res, UpdateResult):
                info = f'\nMongo task done with: {res.raw_result}'
            else:
                info = '\nDispatcher task done!'
            print(info)

    @classmethod
    async def create(cls, self, base, loop=None, **kwargs):
        if cls == _SyncObjBase:
            raise TypeError('You can\'t create _SyncObjBase explicitly!')

        self.cls = cls
        # get event loop from outside if loop is not provided
        # ('create' is a coro function so there must be one)
        self.loop = loop if loop else asyncio._get_running_loop()
        self._run_now = lambda coro: self.loop.run_until_complete(coro)

        super_kwargs = {}
        maxlen = kwargs.pop('maxlen', None)
        if maxlen:
            super_kwargs.update(maxlen=maxlen)

        for name, arg in kwargs.items():
            setattr(self, name, arg)

        if not hasattr(self, '_parent'):
            self._tree_depth = 1

            dispatcher = AsyncCoroQueueDispatcher(self.loop, weakref.ref(self._dispatcher_cb))
            self._enqueue_coro = dispatcher.enqueue_coro
            self.last_mongo_op_results = dispatcher.results_queue
            self.mongo_pending = dispatcher.tasks_queue
            self._mongo_consumer_task = self.loop.create_task(dispatcher.create())
        else:
            self._tree_depth = self._parent._tree_depth + 1

        if isinstance(self, dict):
            base = base if isinstance(base, dict) else {}
        else:
            base = base if isinstance(base, list) else list()

        cached_base = None
        if not hasattr(self, '_parent'):
            cached_base = await self._mongo_get()
            if base and base != cached_base:
                cached_base = None
                await self._mongo_clear()

        super(self.cls, self).__init__(base or cached_base, **super_kwargs)

        if not cached_base and not hasattr(self, '_parent'):
            base = await self._proc_pushed(self, base)
            if isinstance(self, dict):
                await self._mongo_update(base)
            else:
                await self._mongo_extend(base, maxlen=maxlen)

        return self

    def __del__(self):
        if not hasattr(self, '_parent'):
            self._mongo_consumer_task.cancel()