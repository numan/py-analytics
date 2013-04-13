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


class BaseAnalyticsBackend(object):
    _analytics_backend = None

    def __init__(self, settings, **kwargs):
        pass

    def track_count(self, unique_identifier, metric, inc_amt=1, **kwargs):
        """
        Tracks a metric just by count. If you track a metric this way, you won't be able
        to query the metric by day, week or month.

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param inc_amt: The amount you want to increment the ``metric`` for the ``unique_identifier``
        :return: ``True`` if successful ``False`` otherwise
        """
        return NotImplementedError()

    def track_metric(self, unique_identifier, metric, date, inc_amt=1, **kwargs):
        """
        Tracks a metric for a specific ``unique_identifier`` for a certain date.

        TODO: Possibly default date to the current date.

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param date: A python date object indicating when this event occured
        :param inc_amt: The amount you want to increment the ``metric`` for the ``unique_identifier``
        :return: ``True`` if successful ``False`` otherwise
        """
        raise NotImplementedError()

    def get_metric_by_day(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        """
        Returns the ``metric`` for ``unique_identifier`` segmented by day
        starting from``from_date``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param from_date: A python date object
        :param limit: The total number of days to retrive starting from ``from_date``
        """
        raise NotImplementedError()

    def get_metric_by_week(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        """
        Returns the ``metric`` for ``unique_identifier`` segmented by week
        starting from``from_date``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param from_date: A python date object
        :param limit: The total number of weeks to retrive starting from ``from_date``
        """
        raise NotImplementedError()

    def get_metric_by_month(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        """
        Returns the ``metric`` for ``unique_identifier`` segmented by month
        starting from``from_date``. It will retrieve metrics data starting from the 1st of the
        month specified in ``from_date``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param from_date: A python date object
        :param limit: The total number of months to retrive starting from ``from_date``
        """
        raise NotImplementedError()

    def get_metrics(self, metric_identifiers, from_date, limit=10, group_by="week", **kwargs):
        """
        Retrieves a multiple metrics as efficiently as possible.

        :param metric_identifiers: a list of tuples of the form `(unique_identifier, metric_name`) identifying which metrics to retrieve.
        For example [('user:1', 'people_invited',), ('user:2', 'people_invited',), ('user:1', 'comments_posted',), ('user:2', 'comments_posted',)]
        :param from_date: A python date object
        :param limit: The total number of months to retrive starting from ``from_date``
        :param group_by: The type of aggregation to perform on the metric. Choices are: ``day``, ``week`` or ``month``
        """
        raise NotImplementedError()

    def get_count(self, unique_identifier, metric, **kwargs):
        """
        Gets the count for the ``metric`` for ``unique_identifier``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        """
        raise NotImplementedError()

    def get_counts(self, metric_identifiers, **kwargs):
        """
        Retrieves a multiple metrics as efficiently as possible.

        :param metric_identifiers: a list of tuples of the form `(unique_identifier, metric_name`) identifying which metrics to retrieve.
        For example [('user:1', 'people_invited',), ('user:2', 'people_invited',), ('user:1', 'comments_posted',), ('user:2', 'comments_posted',)]
        """
        raise NotImplementedError()

    def get_backend(self):
        return self._analytics_backend
