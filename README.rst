
py-analytics
============

.. image:: https://secure.travis-ci.org/numan/py-analytics.png?branch=master
        :target: https://secure.travis-ci.org/numan/py-analytics

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
* ``settings``: settings required to initialize the backend. For the ``Redis`` backend, this is a list of hosts in your redis cluster.

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

    year_ago = datetime.date.today() - datetime.timedelta(days=265)

    #create some analytics data
    analytics.track_metric("user:1234", "comment", year_ago)
    analytics.track_metric("user:1234", "comment", year_ago, inc_amt=3)
    #we can even track multiple metrics at the same time for a particular user
    analytics.track_metric("user:1234", ["comments", "likes"], year_ago)
    #or track the same metric for multiple users (or a combination or both)
    analytics.track_metric(["user:1234", "user:4567"], "comment", year_ago)

    #retrieve analytics data:
    analytics.get_metric_by_day("user:1234", "comment", year_ago, limit=20)
    analytics.get_metric_by_week("user:1234", "comment", year_ago, limit=10)
    analytics.get_metric_by_month("user:1234", "comment", year_ago, limit=6)

    #create a counter
    analytics.track_count("user:1245", "login")
    analytics.track_count("user:1245", "login", inc_amt=3)

    #retrieve multiple metrics at the same time
    #group_by is one of ``month``, ``week`` or ``day``
    analytics.get_metrics([("user:1234", "login",), ("user:4567", "login",)], year_ago, group_by="day")
    >> [....]

    #retrieve a count
    analytics.get_count("user:1245", "login")

    #retrieve a count between 2 dates
    analytics.get_count("user:1245", "login", start_date=datetime.date(month=1, day=5, year=2011), end_date=datetime.date(month=5, day=15, year=2011))

    #retrieve counts
    analytics.get_counts([("user:1245", "login",), ("user:1245", "logout",)])


BACKWARDS INCOMPATIBLE CHANGES
-------------------------------

v0.5.2
~~~~~~
* ``get_metric_by_day``, ``get_metric_by_week`` and ``get_metric_by_month`` return ``series`` as a set of strings instead of a list of date/datetime objects


TODO
----

* Add more backends possibly...?
* Add an API so it can be deployed as a stand alone service (http, protocolbuffers, ...)
