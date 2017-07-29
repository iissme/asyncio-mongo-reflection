from setuptools import setup
import asyncio_mongo_reflection as lib
import re
import os

root = os.path.dirname(os.path.abspath(__file__))

requirements = []
with open(os.path.join(root, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

readme = ''
with open(os.path.join(root, 'README.md')) as f:
    readme = f.read()

setup(name=lib.__title__,
      author=lib.__author__,
      author_email='ishlyakhov@gmail.com',
      url='http://github.com/isanich/asyncio-mongo-reflection',
      version=lib.__version__,
      license=lib.__license__,
      description='Reflects python\'s deque and dict objects to mongodb asynchronously in background.',
      long_description=readme,
      packages=['asyncio_mongo_reflection'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      include_package_data=True,
      install_requires=requirements,
      platforms='any',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ]
      )
