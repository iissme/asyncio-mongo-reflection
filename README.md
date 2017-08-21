# asyncio-mongo-reflection
[![Build Status](https://travis-ci.org/isanich/asyncio-mongo-reflection.svg?branch=master)](https://travis-ci.org/isanich/asyncio-mongo-reflection)
[![Coverage Status](https://coveralls.io/repos/github/isanich/asyncio-mongo-reflection/badge.svg?branch=master)](https://coveralls.io/github/isanich/asyncio-mongo-reflection?branch=master)

Python 3.6+

asyncio-mongo-reflection writes each change on python's deque(list) or dict to [mongodb][mongodb_link] objects asynchronously in background using asyncio and [motor][motor_link] mongodb driver.

## About
Library is in early stage of development so please be patient.
* Reflections support nesting (i.e. dicts inside dicts or deques inside deques). Mixed nesting is work in progress.
* You can choose where to store your reflections: in existing mongodb objects or create new ones.
* For each operation on python object there is a minimal equivalent for mongo. For example you want to insert something in deque that is nested deeply inside your reflection. This roughfly reflects to:
 ```{'$push': {'nested.nested.nested': {'$each': [your_val], '$position': insert_position'}}```

## Install
Work in progress.

## Documentation
Work in progress. Look at example below.

```python
import asyncio
from motor import motor_asyncio
from mongodeque import MongoDequeReflection

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

client = motor_asyncio.AsyncIOMotorClient()
db = client.test_db

# you should wait for the first reflection instance creation
# than there is no difference with python's deque (every mongo writing op will be done in background)

async def create_reflection():
    # first arg is optional, without it [] will be created
    # or list will be loaded from mongo (if any by provided obj_ref/key)
    return await MongoDequeReflection.create([1, 2, [6, 7, 8]],
                                             col=db['example_reflection'],
                                             obj_ref={'array_id': 'example'},
                                             key='inner.arr')

mongo_reflection = loop.run_until_complete(create_reflection())

mongo_reflection.append(9)
mongo_reflection.popleft()
# nested reflections are created immediately so you can perform operations on them
mongo_reflection[1].extend(['a', 'b', [4, 5, 6]])
mongo_reflection[1][-1].pop()

# with mongo_reflection.mongo_pending.join() you can wait synchronously
# for mongo operations completion if needed
loop.run_until_complete(mongo_reflection.mongo_pending.join())

'''
# mongo db object
# note that 'obj_ref' could be ref to any existing mongo object
# or new one will be created like below

{"_id": {"$oid": "59761ba93e5bb7435c1f6c9b"},
 "array_id": "example",
 "inner": {
     "arr": [2, [6, 7, 8, "a", "b", [4, 5]], 9]}}
'''

'''
# also try this in aioconsole
# type in terminal "pip install aioconsole" then "apython" and paste:

from asyncio_mongo_reflection import MongoDequeReflection
import motor.motor_asyncio
client = motor.motor_asyncio.AsyncIOMotorClient()
db = client.test_db
ref = await MongoDequeReflection.create(col=db['example_reflection'],
                                      obj_ref={'array_id': 'interacive_example'},
                                      key='inner.arr', maxlen=10)

# now you can try to modify ref and trace changes in any mongodb client
'''
```

## Dependencies
* asyncio
* [motor][motor_link]

[mongodb_link]: https://www.mongodb.com/
[motor_link]: https://github.com/mongodb/motor