from setuptools import setup
import os

root = os.path.dirname(os.path.abspath(__file__))

about = {}
with open(os.path.join(root, 'asyncio_mongo_reflection', '__version__.py'), 'r') as f:
    exec(f.read(), about)

requirements = []
with open(os.path.join(root, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

readme = ''
with open(os.path.join(root, 'README.md')) as f:
    readme = f.read()

setup(name=about['__title__'],
      author=about['__author__'],
      author_email='ishlyakhov@gmail.com',
      url='http://github.com/isanich/asyncio-mongo-reflection',
      version=about['__version__'],
      license=about['__license__'],
      description='Reflects each separate change of python\'s deque, dict objects or any their nested combination'
                  ' to mongodb asynchronously in background.',
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
