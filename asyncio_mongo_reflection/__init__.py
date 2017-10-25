"""
asyncio-mongo-reflection
~~~~~~~~~~~~~~~~~~~
Reflects python's deque and dict objects to mongodb asynchronously in background.
:copyright: (c) 2017 isanich
:license: MIT, see LICENSE for more details.
"""
import logging
from .mongodeque import MongoDequeReflection
from .mongodict import MongoDictReflection

logging.getLogger(__name__).addHandler(logging.NullHandler())
