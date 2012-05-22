py-analytics
============
Py-Analytics is a library designed to make it easy to provide analytics as part of any project.

The project's goal is to make it easy to store and retrieve analytics data. It does not provide
any means to visualize this data.

Currently, only ``Redis`` is supported for storing data.

Requirements
------------

Required
~~~~~~~~

Requirements **should** be handled by setuptools, but if they are not, you will need the following Python packages:

* nydus
* redis
* dateutil

Optional
~~~~~~~~
* hiredis

analytics.create_analytic_backend
----------------------------------

Creates an analytics object that allows to to store and retrieve metrics::

    >>> from analytics import create_analytic
    >>>
    >>> analytics = create_analytic({
    >>>     'backend': 'analytics.backends.redis.Redis',
    >>>     'settings': {
    >>>         'defaults': {
    >>>             'host': 'localhost',
    >>>             'port': 6379,
    >>>             'db': 0,
    >>>         },
    >>>         'hosts': [{'db': 0}, {'db': 1}, {'host': 'redis.example.org'}]
    >>>     },
    >>> })

Internally, the ``Redis`` analytics backend uses ``nydus`` to distribute your metrics data over your cluster of redis instances.

There are two required arguements:

* ``backend``: full path to the backend class, which should extend analytics.backends.base.BaseAnalyticsBackend

* ``settings``: settings required to initialize the backend. For the ``Redis`` backend, this is a list of hosts
in your redis cluster.

TODO
----

* Add more backends (MySQL, Postgres, ...)
* Add an API so it can be deployed as a stand alone service
