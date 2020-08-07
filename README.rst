.. _Redlock Algorithm: https://redis.io/topics/distlock#the-redlock-algorithm

========
Redlock+
========

.. image:: https://img.shields.io/pypi/implementation/redlock-plus?style=flat-square
  :target: https://pypi.org/project/redlock-plus/
.. image:: https://img.shields.io/pypi/pyversions/redlock-plus?style=flat-square
  :target: https://pypi.org/project/redlock-plus/
.. image:: https://github.com/provinzkraut/redlock-plus/workflows/Tests/badge.svg
  :target: https://github.com/provinzkraut/redlock-plus/actions?query=workflow%3ATests

Redlock+ is an up to date, feature complete implementation of the `Redlock Algorithm`_
It's a spiritual successor to `glasslion/redlock <https://github.com/glasslion/redlock>`_, which is no longer maintained.


Features
=========

- Compliant with the standard library `Lock <https://docs.python.org/3/library/threading.html#threading.Lock>`_
  and `RLock <https://docs.python.org/3/library/threading.html#threading.RLock>`_ so it can be used as a drop-in replacement
- Complete implementation of the `Redlock Algorithm`_
- Autoextend functionality to make redlock safer and easier to use
- Well tested (Python 3.6+, PyPy3)
- Type hinted


Documentation
=============

https://redlock-plus.readthedocs.io/en/latest/


Installation
============

Redlock+ is available on `PyPi <https://pypi.org/project/redlock-plus/>`_:

.. code-block:: bash

  pip install redlock-plus


Basic usage
===========

.. code-block:: python

    from redlock_plus import Lock

    redis_instance_configs = [
        {"url": "redis://localhost/0"},
        {"url": "redis://example.com:1234/1"},
        {"url": "redis://other.host:5678/2"},
    ]

    with Lock("my_resource", redis_instance_configs):
        # do work

or speed up things for repeated use using the factory

.. code-block:: python

    from redlock_plus import LockFactory

    redlock_factory = LockFactory(
        [
            {"url": "redis://localhost/0"},
            {"url": "redis://localhost/1"},
            {"url": "redis://localhost/2"},
        ]
    )

    with redlock_factory("my_resource"):
        # do work
