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
from analytics.backends.base import BaseAnalyticsBackend

from nydus.db import create_cluster

from dateutil.relativedelta import relativedelta
from dateutil import rrule

import datetime
import itertools
import calendar
import types


class Redis(BaseAnalyticsBackend):
    def __init__(self, settings, **kwargs):
        nydus_hosts = {}

        hosts = settings.get("hosts", [])
        if not hosts:
            raise Exception("No redis hosts specified")

        for i, host in enumerate(hosts):
            nydus_hosts[i] = host

        defaults = settings.get("defaults",
            {
                'host': 'localhost',
                'port': 6379,
            })

        self._analytics_backend = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.ConsistentHashingRouter',
            'hosts': nydus_hosts,
            'defaults': defaults,
        })

    def _get_closest_week(self, metric_date):
        """
        Gets the closest monday to the date provided.
        """
        #find the offset to the closest monday
        days_after_monday = metric_date.isoweekday() - 1

        return metric_date - datetime.timedelta(days=days_after_monday)

    def _get_daily_metric_key(self, unique_identifier, metric_date):
        """
        Redis key for daily metric
        """
        return "user:%s:analy:%s" % (unique_identifier, metric_date.strftime("%y-%m"),)

    def _get_weekly_metric_key(self, unique_identifier, metric_date):
        """
        Redis key for weekly metric
        """
        return "user:%s:analy:%s" % (unique_identifier, metric_date.strftime("%y"),)

    def _get_daily_metric_name(self, metric, metric_date):
        """
        Hash key for daily metric
        """
        return "%s:%s" % (metric, metric_date.strftime("%y-%m-%d"),)

    def _get_weekly_metric_name(self, metric, metric_date):
        """
        Hash key for weekly metric
        """
        return "%s:%s" % (metric, metric_date.strftime("%y-%m-%d"),)

    def _get_monthly_metric_name(self, metric, metric_date):
        """
        Hash key for monthly metric
        """
        return "%s:%s" % (metric, metric_date.strftime("%y-%m"),)

    def _get_daily_date_range(self, metric_date, delta):
        """
        Get the range of months that we need to use as keys to scan redis.
        """
        dates = [metric_date]
        start_date = metric_date
        end_date = metric_date + delta

        while start_date.month < end_date.month or start_date.year < end_date.year:
            days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
            #shift along to the next month as one of the months we will have to see. We don't care that the exact date
            #is the 1st in each subsequent date range as we only care about the year and the month
            start_date = start_date + datetime.timedelta(days=days_in_month - start_date.day + 1)
            dates.append(start_date)

        return dates

    def _get_weekly_date_range(self, metric_date, delta):
        """
        Gets the range of years that we need to use as keys to get metrics from redis.
        """
        dates = [metric_date]
        end_date = metric_date + delta
        #Figure out how many years our metric range spans
        spanning_years = end_date.year - metric_date.year
        for i in range(spanning_years):
            #for the weekly keys, we only care about the year
            dates.append(datetime.date(year=metric_date.year + (i + 1), \
                month=1, day=1))
        return dates

    def _parse_and_process_metrics(self, series, list_of_metrics):
        formatted_result_list = []
        series = [dt.strftime("%Y-%m-%d") for dt in series]
        for result in list_of_metrics:
            values = {}
            for index, date_string in enumerate(series):
                values[date_string] = int(result[index]) if result[index] is not None else 0
            formatted_result_list.append(values)

        merged_values = reduce(lambda a, b: dict((n, a.get(n, 0) + b.get(n, 0)) for n in set(a) | set(b)), \
            formatted_result_list)

        return set(series), merged_values

    def track_count(self, unique_identifier, metric, inc_amt=1, **kwargs):
        """
        Tracks a metric just by count. If you track a metric this way, you won't be able
        to query the metric by day, week or month.

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param inc_amt: The amount you want to increment the ``metric`` for the ``unique_identifier``
        :return: ``True`` if successful ``False`` otherwise
        """
        return self._analytics_backend.incr("analy:%s:count:%s" % (unique_identifier, metric), inc_amt)

    def track_metric(self, unique_identifier, metric, date, inc_amt=1, **kwargs):
        """
        Tracks a metric for a specific ``unique_identifier`` for a certain date. The redis backend supports
        lists for both ``unique_identifier`` and ``metric`` allowing for tracking of multiple metrics for multiple
        unique_identifiers efficiently. Not all backends may support this.

        TODO: Possibly default date to the current date.

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track. This can be a list or a string.
        :param date: A python date object indicating when this event occured
        :param inc_amt: The amount you want to increment the ``metric`` for the ``unique_identifier``
        :return: ``True`` if successful ``False`` otherwise
        """
        metric = [metric] if isinstance(metric, basestring) else metric
        unique_identifier = [unique_identifier] if not isinstance(unique_identifier, (types.ListType, types.TupleType, types.GeneratorType,)) else unique_identifier
        results = []
        with self._analytics_backend.map() as conn:
            for uid in unique_identifier:

                hash_key_daily = self._get_daily_metric_key(uid, date)
                closest_monday = self._get_closest_week(date)
                hash_key_weekly = self._get_weekly_metric_key(uid, date)

                for single_metric in metric:
                    daily_metric_name = self._get_daily_metric_name(single_metric, date)
                    weekly_metric_name = self._get_weekly_metric_name(single_metric, closest_monday)
                    monthly_metric_name = self._get_monthly_metric_name(single_metric, date)

                    results.append([conn.hincrby(hash_key_daily, daily_metric_name, inc_amt),\
                        conn.hincrby(hash_key_weekly, weekly_metric_name, inc_amt),\
                        conn.hincrby(hash_key_weekly, monthly_metric_name, inc_amt),\
                        conn.incr("analy:%s:count:%s" % (uid, single_metric), inc_amt)
                    ])

        return results

    def get_metric_by_day(self, unique_identifier, metric, from_date, limit=30, **kwargs):
        """
        Returns the ``metric`` for ``unique_identifier`` segmented by day
        starting from``from_date``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param from_date: A python date object
        :param limit: The total number of days to retrive starting from ``from_date``
        """
        conn = kwargs.get("connection", None)
        date_generator = (from_date + datetime.timedelta(days=i) for i in itertools.count())
        metric_key_date_range = self._get_daily_date_range(from_date, datetime.timedelta(days=limit))
        #generate a list of mondays in between the start date and the end date
        series = list(itertools.islice(date_generator, limit))

        metric_keys = [self._get_daily_metric_name(metric, daily_date) for daily_date in series]

        metric_func = lambda conn: [conn.hmget(self._get_daily_metric_key(unique_identifier, \
                    metric_key_date), metric_keys) for metric_key_date in metric_key_date_range]

        if conn is not None:
            results = metric_func(conn)
        else:
            with self._analytics_backend.map() as conn:
                results = metric_func(conn)
            series, results = self._parse_and_process_metrics(series, results)

        return series, results

    def get_metric_by_week(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        """
        Returns the ``metric`` for ``unique_identifier`` segmented by week
        starting from``from_date``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :param from_date: A python date object
        :param limit: The total number of weeks to retrive starting from ``from_date``
        """
        conn = kwargs.get("connection", None)
        closest_monday_from_date = self._get_closest_week(from_date)
        metric_key_date_range = self._get_weekly_date_range(closest_monday_from_date, datetime.timedelta(weeks=limit))

        date_generator = (closest_monday_from_date + datetime.timedelta(days=i) for i in itertools.count(step=7))
        #generate a list of mondays in between the start date and the end date
        series = list(itertools.islice(date_generator, limit))

        metric_keys = [self._get_weekly_metric_name(metric, monday_date) for monday_date in series]

        metric_func = lambda conn: [conn.hmget(self._get_weekly_metric_key(unique_identifier, \
                metric_key_date), metric_keys) for metric_key_date in metric_key_date_range]

        if conn is not None:
            results = metric_func(conn)
        else:
            with self._analytics_backend.map() as conn:
                results = metric_func(conn)
            series, results = self._parse_and_process_metrics(series, results)

        return series, results

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
        conn = kwargs.get("connection", None)
        first_of_month = datetime.date(year=from_date.year, month=from_date.month, day=1)
        metric_key_date_range = self._get_weekly_date_range(first_of_month, \
            relativedelta(months=limit))

        date_generator = (first_of_month + relativedelta(months=i) for i in itertools.count())
        #generate a list of mondays in between the start date and the end date
        series = list(itertools.islice(date_generator, limit))

        metric_keys = [self._get_monthly_metric_name(metric, month_date) for month_date in series]

        metric_func = lambda conn: [conn.hmget(self._get_weekly_metric_key(unique_identifier, \
                    metric_key_date), metric_keys) for metric_key_date in metric_key_date_range]

        if conn is not None:
            results = metric_func(conn)
        else:
            with self._analytics_backend.map() as conn:
                results = metric_func(conn)
            series, results = self._parse_and_process_metrics(series, results)

        return series, results

    def get_metrics(self, metric_identifiers, from_date, limit=10, group_by="week", **kwargs):
        """
        Retrieves a multiple metrics as efficiently as possible.

        :param metric_identifiers: a list of tuples of the form `(unique_identifier, metric_name`) identifying which metrics to retrieve.
        For example [('user:1', 'people_invited',), ('user:2', 'people_invited',), ('user:1', 'comments_posted',), ('user:2', 'comments_posted',)]
        :param from_date: A python date object
        :param limit: The total number of months to retrive starting from ``from_date``
        :param group_by: The type of aggregation to perform on the metric. Choices are: ``day``, ``week`` or ``month``
        """
        results = []
        #validation of types:
        allowed_types = {"day": self.get_metric_by_day,
            "week": self.get_metric_by_week,
            "month": self.get_metric_by_month,
            }
        if group_by.lower() not in allowed_types:
            raise Exception("Allowed values for group_by are day, week or month.")

        group_by_func = allowed_types[group_by.lower()]
        #pass a connection object so we can pipeline as much as possible
        with self._analytics_backend.map() as conn:
            for unique_identifier, metric in metric_identifiers:
                results.append(group_by_func(unique_identifier, metric, from_date, limit=limit, connection=conn))

        #we have to merge all the metric results afterwards because we are using a custom context processor
        return [self._parse_and_process_metrics(series, list_of_metrics) for \
            series, list_of_metrics in results]

    def get_count(self, unique_identifier, metric, start_date=None, end_date=None, **kwargs):
        """
        Gets the count for the ``metric`` for ``unique_identifier``

        :param unique_identifier: Unique string indetifying the object this metric is for
        :param metric: A unique name for the metric you want to track
        :return: The count for the metric, 0 otherwise
        """
        result = None
        if start_date and end_date:
            start_date, end_date = (start_date, end_date,) if start_date < end_date else (end_date, start_date,)

            start_date = start_date if hasattr(start_date, 'date') else datetime.datetime.combine(start_date, datetime.time())
            end_date = end_date if hasattr(end_date, 'date') else datetime.datetime.combine(end_date, datetime.time())

            monthly_metrics_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=start_date, bymonthday=1, until=end_date))

            #We can sorta optimize this by getting most of the data by month
            if len(monthly_metrics_dates) >= 3:
                start_diff = monthly_metrics_dates[0] - start_date
                end_diff = end_date - monthly_metrics_dates[-1]

                with self._analytics_backend.map() as conn:
                    monthly_metric_series, monthly_metric_results = self.get_metric_by_month(unique_identifier, metric, monthly_metrics_dates[0], limit=len(monthly_metrics_dates) - 1, connection=conn)

                    #get the difference from the date to the start date and get all dates in between
                    starting_metric_series, starting_metric_results = self.get_metric_by_day(unique_identifier, metric, start_date, limit=start_diff.days, connection=conn) if start_diff.days > 0 else ([], [[]],)
                    ending_metric_series, ending_metric_results = self.get_metric_by_day(unique_identifier, metric, monthly_metrics_dates[-1], limit=end_diff.days + 1, connection=conn)

                monthly_metric_series, monthly_metric_results = self._parse_and_process_metrics(monthly_metric_series, monthly_metric_results)
                starting_metric_series, starting_metric_results = self._parse_and_process_metrics(starting_metric_series, starting_metric_results)
                ending_metric_series, ending_metric_results = self._parse_and_process_metrics(ending_metric_series, ending_metric_results)

                result = sum(monthly_metric_results.values()) + sum(starting_metric_results.values()) + sum(ending_metric_results.values())
            else:
                diff = end_date - start_date
                metric_results = self.get_metric_by_day(unique_identifier, metric, start_date, limit=diff.days + 1)
                result = sum(metric_results[1].values())

        else:
            try:
                result = int(self._analytics_backend.get("analy:%s:count:%s" % (unique_identifier, metric,)))
            except TypeError:
                result = 0

        return result

    def get_counts(self, metric_identifiers, **kwargs):
        """
        Retrieves a multiple metrics as efficiently as possible.

        :param metric_identifiers: a list of tuples of the form `(unique_identifier, metric_name`) identifying which metrics to retrieve.
        For example [('user:1', 'people_invited',), ('user:2', 'people_invited',), ('user:1', 'comments_posted',), ('user:2', 'comments_posted',)]
        """
        parsed_results = []
        with self._analytics_backend.map() as conn:
            results = [conn.get("analy:%s:count:%s" % (unique_identifier, metric,)) for \
                unique_identifier, metric in metric_identifiers]

        for result in results:
            try:
                parsed_result = int(result)
            except TypeError:
                parsed_result = 0

            parsed_results.append(parsed_result)

        return parsed_results
