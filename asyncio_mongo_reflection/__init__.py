"""
asyncio-mongo-reflection
~~~~~~~~~~~~~~~~~~~
Reflects python's deque and dict objects in mongodb asynchronously in background.
:copyright: (c) 2017 isanich
:license: MIT, see LICENSE for more details.
"""
import logging
from .deque_reflection import MongoDequeReflection
from .dict_reflection import MongoDictReflection

logging.getLogger(__name__).addHandler(logging.NullHandler())
