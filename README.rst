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

    >>> from analytics import create_analytic_backend
    >>>
    >>> analytics = create_analytic_backend({
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

Example Usage
-------------

::

    from analytics import create_analytic_backend
    import datetime

    analytics = create_analytic_backend({
        "backend": "analytics.backends.redis.Redis",
        "settings": {
            "hosts": [{"db": 5}]
        },
    })

    #create some analytics data
    analytics.track_metric("user:1234", "comment", datetime.date.today())
    analytics.track_metric("user:1234", "comment", datetime.date.today(), inc_amt=3)

    #retrieve analytics data:
    analytics.get_metric_by_day("user:1234", "comment", datetime.date.today(), limit=20)
    analytics.get_metric_by_week("user:1234", "comment", datetime.date.today(), limit=10)
    analytics.get_metric_by_month("user:1234", "comment", datetime.date.today(), limit=6)

    #create a counter
    analytics.track_count("user:1245", "login")
    analytics.track_count("user:1245", "login", inc_amt=3)

    #retrieve a count
    analytics.get_count("user:1245", "login")


TODO
----

* Add more backends (MySQL, Postgres, ...)
* Add an API so it can be deployed as a stand alone service
