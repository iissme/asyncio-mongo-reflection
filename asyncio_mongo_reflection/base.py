import asyncio
import weakref
import functools
import logging
from threading import Thread
from concurrent.futures import Executor
from time import clock
from abc import ABCMeta

from pymongo.collection import UpdateResult


log = logging.getLogger(__name__)


class MongoReflectionError(Exception):
    pass


class SyncCoroExecutor(Executor):
    """
    Allows to wait for a given coroutine execution synchronously from a main thread.
    Replaces loop.run_until_complete.
    """
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(daemon=True,
                              target=self._start_shadow_loop,
                              name='sync corutine executor')
        self._thread.start()

    def _start_shadow_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def shutdown(self, wait=True):
        self._loop.call_soon_threadsafe(self._loop.stop)
        if wait:
            self._thread.join()


class AsyncCoroQueueDispatcher:
    """
    Dispatcher gets coroutine from its iternal queue,
    runs is asynchronously (in background) and waits untill it's done,
    then gets next one. It's meant to be embed in another class.

    'Create' method should be run via asyncio.ensure_future or loop.create_task.
    """

    @functools.total_ordering
    class Task:

        __slots__ = ('coro', 'priority', 'locals', '_insertion_clock')

        def __init__(self, coro, priority, locals, clock):
            self.coro = coro
            self.priority = priority
            self.locals = locals
            self._insertion_clock = clock

        def __lt__(self, other):
            if self == other:
                return self._insertion_clock < other._insertion_clock
            else:
                return self.priority < other.priority

        def __eq__(self, other):
            return self.priority == other.priority

        def __repr__(self):
            return f'Coro - {repr(self.coro)} Priority - {self.priority} Locals - {self.locals}'

    __slots__ = ('loop', 'tasks_queue', 'results_queue',
                 '_process_next', '_dispatcher_task', '_external_cb')

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
            # waits for remaining tasks when dispatcher's task is cancelled
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

                if self.results_queue.full():
                    self.results_queue.get_nowait()
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

        coro_locals = {key: repr(val) for key, val in coro.cr_frame.f_locals.items()}
        self.tasks_queue.put_nowait(self.Task(future_wrapper(coro, f), priority,
                                              coro_locals, clock()))


def asyncinit(cls):
    __new__ = cls.__new__

    async def init(obj, *args, **kwargs):
        await obj.__ainit__(*args, **kwargs)
        return obj

    def new(cls, *args, **kwargs):
        obj = __new__(cls)
        coro = init(obj, *args, **kwargs)
        return coro

    cls.__new__ = new
    return cls


class AsyncInit(type):

    @staticmethod
    async def init(obj, *args, **kwargs):
        await obj.__ainit__(*args, **kwargs)
        return obj

    @classmethod
    def new(mcs, cls, *args, **kwargs):
        obj = cls.__cnew__(cls)
        coro = mcs.init(obj, *args, **kwargs)
        return coro

    def __new__(mcs, name, bases, attrs, **kwargs):

        if len(bases):
            if hasattr(bases[0], '__cnew__'):
                attrs['__cnew__'] = bases[0].__cnew__
            else:
                attrs['__cnew__'] = bases[0].__new__
        else:
            attrs['__cnew__'] = object.__new__

        attrs['__new__'] = mcs.new
        return super().__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, attrs, **kwargs):
        return super().__init__(name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class ABCAsyncInit(AsyncInit, ABCMeta):
    pass


class _SyncObjBase(metaclass=ABCAsyncInit):
    sync_executor = SyncCoroExecutor()

    async def __ainit__(self, new_base, loop=None, **kwargs):
        # get event loop from outside if loop is not provided
        # ('create' is a coro function so there must be one)
        self.loop = loop if loop else asyncio._get_running_loop()

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
            self._mongo_dispatcher_task = self.loop.create_task(dispatcher.create())
            self._finalizer = weakref.finalize(self, self._cancel_dispatcher)
        else:
            self._tree_depth = self._parent._tree_depth + 1

        if isinstance(self, dict):
            new_base = new_base if isinstance(new_base, dict) else {}
        else:
            new_base = new_base if isinstance(new_base, list) else list()

        cached_base = None
        if not hasattr(self, '_parent'):
            cached_base = await self._mongo_get()
            if new_base and new_base != cached_base and getattr(self, 'rewrite', True):
                cached_base = None
                await self._mongo_clear()

        super(type(self), self).__init__(new_base or cached_base, **super_kwargs)

        if new_base and not cached_base and not hasattr(self, '_parent'):
            new_base = await self._proc_pushed(self, new_base)
            if isinstance(self, dict):
                await self._mongo_update(new_base)
            else:
                await self._mongo_extend(new_base, maxlen=maxlen)

    def _run_now(self, coro):
        coro_future = self.sync_executor.submit(coro)
        return coro_future.result()

    @staticmethod
    def _dispatcher_cb(res, exc):
        if exc:
            raise MongoReflectionError(exc)
        else:
            if isinstance(res, UpdateResult):
                info = f'\nMongo task done with: {res.raw_result}'
            else:
                info = '\nDispatcher task done!'
            log.debug(info)

    def _cancel_dispatcher(self):
        if not hasattr(self, '_parent') and not self.loop.is_closed():
            self._mongo_dispatcher_task.cancel()
