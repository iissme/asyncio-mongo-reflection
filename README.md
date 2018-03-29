# asyncio-mongo-reflection
[![Build Status](https://travis-ci.org/isanich/asyncio-mongo-reflection.svg?branch=master)](https://travis-ci.org/isanich/asyncio-mongo-reflection)
[![Coverage Status](https://coveralls.io/repos/github/isanich/asyncio-mongo-reflection/badge.svg?branch=master)](https://coveralls.io/github/isanich/asyncio-mongo-reflection?branch=master)

Python 3.6+

asyncio-mongo-reflection is an ORM like library. It provides reflections that write each separate change of wrapped python's list/deque, dict or any their nested combination to [mongodb][mongodb_link] objects asynchronously in background using asyncio and [motor][motor_link] mongodb driver.


## About
* Reflections support nesting (i.e. dicts inside dicts or deques inside deques). Mixed nesting is supported too (dicts inside deques for ex.)!
* You can choose where to store your reflections: in existing mongodb objects or create new ones.
* Existing reflections can be automatically recreated from db at thier last state (if 'rewrite=False' is set or no initial list/dict is passed).
* For each operation on python object there is a minimal equivalent for mongo. For example you want to insert something in deque that is nested deeply inside your reflection. This roughfly reflects to:
 `{'$push': {'nested.nested.nested': {'$each': [your_val], '$position': insert_position'}}`

## Install
Clone from git and install via setup.py.
Or `pip install -U https://github.com/isanich/asyncio-mongo-reflection/archive/master.zip`.


## Documentation
Look at example below.

```python
import asyncio
from motor import motor_asyncio
from mongodeque import MongoDequeReflection, MongoDictReflection

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

client = motor_asyncio.AsyncIOMotorClient()
db = client.test_db


# you should 'await' while reflection instance is created
# than there is no difference with python's deque (every mongo writing op will be done in background)
# with 'rewrite=False' flag initial list '[1, 2, [6, 7, 8]]' will be ignored next time (data will be loaded from db).
async def create_reflection():
    # first arg is optional, without it empty reflection will be created
    # or list will be loaded from mongo (if any found using provided obj_ref/key)
    return await MongoDequeReflection([1, 2, [6, 7, 8]], col=db['example_reflection'],
                                      obj_ref={'array_id': 'example'}, key='inner.arr',
                                      rewrite=False)

# MongoDictReflection is similar to MongoDequeReflection but wraps python's dict.
# Note that you can create dicts inside MongoDequeReflection and lists inside MongoDictReflection
# All actions above that dicts and lists are reflected too.

mongo_reflection = loop.run_until_complete(create_reflection())

mongo_reflection.append(9)
mongo_reflection.popleft()
# nested reflections are created immediately so you can perform operations on them
mongo_reflection[1].extend(['a', 'b', [4, 5, 6]])
mongo_reflection[1][-1].pop()

# with mongo_reflection.mongo_pending.join() you can wait synchronously
# for mongo operation completion if needed
loop.run_until_complete(mongo_reflection.mongo_pending.join())

'''
# mongo db object
# note that 'obj_ref' could be ref to any existing mongo object
# or new one will be created like below:

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
ref = await MongoDequeReflection(col=db['example_reflection'],
                                obj_ref={'array_id': 'interacive_example'},
                                key='inner.arr', maxlen=10)

# empty reflection is created
# now you can try to modify ref and trace changes in any mongodb client
'''
```

## Dependencies
* asyncio
* [motor][motor_link]

[mongodb_link]: https://www.mongodb.com/
[motor_link]: https://github.com/mongodb/motor