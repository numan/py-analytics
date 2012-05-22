from analytics.utils import import_string


def create_analytic_backend(settings):
    """
    Creates a new Analytics backend from the settings

    :param settings: Dictionary of settings for the analytics backend
    :returns: A backend object implementing the analytics api

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
    """
    backend = settings.get('backend')
    if isinstance(backend, basestring):
        backend = import_string(backend)
    elif backend:
        backend = backend
    else:
        raise KeyError('backend')

    return backend(settings.get("settings", {}))
