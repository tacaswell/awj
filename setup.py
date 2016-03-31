from setuptools import setup

setup(name='awj',
      version='0.0.1',
      author='Thomas A Caswell',
      author_email='tcaswell@gmail.com',
      py_modules=['awj'],
      description='LRU cache for DataFrames backed by on-disk feather files',
      url='http://github.com/tacaswell/awj',
      platforms='Cross platform (Linux, Mac OSX, Windows)',
      install_requires=['feather-format'],
      license="BSD",
      classifiers=['Development Status :: 4 - Beta',
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.3',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   ],
      )
