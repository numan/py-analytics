"""
Copyright 2012 Numan Sachwani <numan@7Geese.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from analytics.utils import import_string

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('analytics').version
except Exception, e:
    VERSION = 'unknown'


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
