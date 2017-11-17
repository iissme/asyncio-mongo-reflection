import asyncio
import motor.motor_asyncio
import pytest

from functools import wraps

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(False)
loop._close = loop.close
loop.close = lambda: None

lrun_uc = loop.run_until_complete

client = motor.motor_asyncio.AsyncIOMotorClient()
db = client.test_db


@pytest.fixture(scope="session", autouse=True)
def test_shutdown():
    yield None
    # clear all tasks before loop stops
    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()

    lrun_uc(asyncio.sleep(1))
    loop.close()


def async_test(testf):
    @wraps(testf)
    def tmp(*args, **kwargs):
        return lrun_uc(testf(*args, **kwargs))
    return tmp
