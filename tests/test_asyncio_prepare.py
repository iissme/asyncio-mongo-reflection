import asyncio

import motor.motor_asyncio
import pytest

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(False)
loop._close = loop.close
loop.close = lambda: None

run = lambda coro: loop.run_until_complete(coro)

client = motor.motor_asyncio.AsyncIOMotorClient()
db = client.test_db


@pytest.fixture(scope="session", autouse=True)
def test_shutdown():
    yield None
    # in session yield fixture loop stops too early so do this in test
    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()

    run(asyncio.sleep(1))
    loop.close()
