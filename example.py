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