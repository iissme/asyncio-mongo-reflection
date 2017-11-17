import asyncio

from asyncio_mongo_reflection.mongodeque import MongoDequeReflection
from motor import motor_asyncio

loop = asyncio.new_event_loop()
loop.set_debug(False)
asyncio.set_event_loop(loop)

client = motor_asyncio.AsyncIOMotorClient()
db = client.test_db

# you should wait for the first reflection instance creation
# than there is no difference with python's deque (every mongo writing op will be done in background)

async def create_reflection():
    # first arg is optional, without it [] will be created
    # or list will be loaded from mongo (if any by provided obj_ref/key)
    return await MongoDequeReflection([1, 2, [6, 7, 8]], col=db['example_reflection'],
                                      obj_ref={'array_id': 'example'}, key='inner.arr', maxlen=10)

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
# mongo db object (note that 'obj_ref' could be ref to any existing mongo object)

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

# now you can try to modify ref and trace changes in any mongodb client
'''