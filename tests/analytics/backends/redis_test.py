from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from analytics import create_analytic_backend

import datetime
import itertools


class TestRedisAnalyticsBackend(object):
    def setUp(self):
        self._backend = create_analytic_backend({
            "backend": "analytics.backends.redis.Redis",
            "settings": {
                "hosts": [{"db": 3}, {"db": 4}, {"db": 5}]
            },
        })

        self._redis_backend = self._backend.get_backend()

        #clear the redis database so we are in a consistent state
        self._redis_backend.flushdb()

    def tearDown(self):
        self._redis_backend.flushdb()

    def test_track_metric(self):
        user_id = 1234
        metric = "badge:25"
        datetime_obj = datetime.datetime(year=2012, month=1, day=1)

        ok_(self._backend.track_metric(user_id, metric, datetime_obj))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        keys.sort()
        eq_(len(keys), 3)

        daily = self._redis_backend.hgetall(keys[2])
        weekly = self._redis_backend.hgetall(keys[1])
        aggregated = self._redis_backend.get("analy:%s:count:%s" % (user_id, metric, ))

        #each metric should be at 1
        [eq_(int(value), 1) for value in daily.values()]
        [eq_(int(value), 1) for value in weekly.values()]
        eq_(int(aggregated), 1)

        #each hash should only have one key
        eq_(len(daily.keys()), 1)
        eq_(len(weekly.keys()), 2)

        #try incrementing by the non default value
        ok_(self._backend.track_metric(user_id, metric, datetime_obj, inc_amt=3))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        keys.sort()
        eq_(len(keys), 3)

        daily = self._redis_backend.hgetall(keys[2])
        weekly = self._redis_backend.hgetall(keys[1])
        aggregated = self._redis_backend.get("analy:%s:count:%s" % (user_id, metric, ))

        #each metric should be at 4
        [eq_(int(value), 4) for value in daily.values()]
        [eq_(int(value), 4) for value in weekly.values()]
        eq_(int(aggregated), 4)

    def test_track_count(self):
        user_id = 1234
        metric = "badge:25"

        ok_(self._backend.track_count(user_id, metric))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 1)

        aggregated = self._redis_backend.get("analy:%s:count:%s" % (user_id, metric, ))

        #count should be at 1
        eq_(int(aggregated), 1)

        #try incrementing by the non default value
        ok_(self._backend.track_count(user_id, metric, inc_amt=3))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 1)

        aggregated = self._redis_backend.get("analy:%s:count:%s" % (user_id, metric, ))

        #count should be at 4
        eq_(int(aggregated), 4)

    def test_get_count(self):
        user_id = 1234
        metric = "badge:25"

        ok_(self._backend.track_count(user_id, metric))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 1)

        count = self._backend.get_count(user_id, metric)

        #count should be at 1
        eq_(count, 1)

        #try incrementing by the non default value
        ok_(self._backend.track_count(user_id, metric, inc_amt=3))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 1)

        count = self._backend.get_count(user_id, metric)

        #count should be at 4
        eq_(count, 4)

    def test_get_count_invalid_key(self):
        user_id = 1234
        metric = "badge:25"

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 0)

        count = self._backend.get_count(user_id, metric)

        #count should be at 0
        eq_(count, 0)

    def test_get_counts(self):
        user_id = 1234
        metric = "badge:25"
        metric2 = "badge:26"
        does_not_exist = "key:does:not:exist"

        ok_(self._backend.track_count(user_id, metric))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 1)

        #try incrementing by the non default value
        ok_(self._backend.track_count(user_id, metric2, inc_amt=3))

        keys = self._redis_backend.keys()
        #flatten list to lists incase we have a cluster of redis servers
        keys = list(itertools.chain.from_iterable(keys))
        eq_(len(keys), 2)

        counts = self._backend.get_counts([(user_id, metric,), (user_id, metric2,), (user_id, does_not_exist,)])

        #check the counts for each of the metrics
        eq_(len(counts), 3)
        eq_(counts[0], 1)
        eq_(counts[1], 3)
        eq_(counts[2], 0)

    def test_get_closest_week(self):
        """
        Gets the closest Monday to the provided date.
        """
        date_april_1 = datetime.date(year=2012, month=4, day=1)
        date_april_2 = datetime.date(year=2012, month=4, day=2)
        date_april_7 = datetime.date(year=2012, month=4, day=7)
        date_april_8 = datetime.date(year=2012, month=4, day=8)
        date_april_9 = datetime.date(year=2012, month=4, day=9)

        monday_march_26 = datetime.date(year=2012, month=3, day=26)
        monday_april_2 = datetime.date(year=2012, month=4, day=2)
        monday_april_9 = datetime.date(year=2012, month=4, day=9)

        eq_(self._backend._get_closest_week(date_april_1), monday_march_26)
        eq_(self._backend._get_closest_week(date_april_2), monday_april_2)
        eq_(self._backend._get_closest_week(date_april_7), monday_april_2)
        eq_(self._backend._get_closest_week(date_april_8), monday_april_2)
        eq_(self._backend._get_closest_week(date_april_9), monday_april_9)

    def test_metric_by_month_over_several_months(self):
        user_id = 1234
        metric = "badge:25"
        from_date = datetime.date(year=2012, month=4, day=2)

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=5, day=11), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=6, day=18), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=30)))

        series, values = self._backend.get_metric_by_month(user_id, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-01"], 7)
        eq_(values["2012-05-01"], 2)
        eq_(values["2012-06-01"], 3)
        eq_(values["2012-07-01"], 0)
        eq_(values["2012-08-01"], 0)

    def test_metric_by_month_over_several_months_crossing_year_boundry(self):
        user_id = 1234
        metric = "badge:25"
        from_date = datetime.date(year=2011, month=12, day=1)

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=8), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=30), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=1, day=1), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=1, day=5), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7)))

        series, values = self._backend.get_metric_by_month(user_id, metric, from_date, limit=6)
        eq_(len(series), 6)
        eq_(values["2011-12-01"], 6)
        eq_(values["2012-01-01"], 5)
        eq_(values["2012-02-01"], 0)
        eq_(values["2012-03-01"], 0)
        eq_(values["2012-04-01"], 1)
        eq_(values["2012-05-01"], 0)

    def test_metric_by_week_over_several_weeks(self):
        user_id = 1234
        metric = "badge:25"
        from_date = datetime.date(year=2012, month=4, day=2)

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=11), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=18), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=30)))

        series, values = self._backend.get_metric_by_week(user_id, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

    def test_metric_by_week_over_several_weeks_crossing_year_boundry(self):
        user_id = 1234
        metric = "badge:25"
        from_date = datetime.date(year=2011, month=12, day=1)

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=8), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=30), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=1, day=1), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=1, day=5), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7)))

        series, values = self._backend.get_metric_by_week(user_id, metric, from_date, limit=6)
        eq_(len(series), 6)
        eq_(values["2011-11-28"], 0)
        eq_(values["2011-12-05"], 4)
        eq_(values["2011-12-12"], 0)
        eq_(values["2011-12-19"], 0)
        eq_(values["2011-12-26"], 4)
        eq_(values["2012-01-02"], 3)

    def test_get_weekly_date_range(self):
        date = datetime.date(year=2011, month=11, day=1)

        result = self._backend._get_weekly_date_range(date, datetime.timedelta(weeks=12))
        eq_(len(result), 2)
        eq_(result[0], datetime.date(year=2011, month=11, day=1))
        eq_(result[1], datetime.date(year=2012, month=1, day=1))

    def test_get_daily_date_range(self):
        date = datetime.date(year=2011, month=11, day=15)

        result = self._backend._get_daily_date_range(date, datetime.timedelta(days=30))
        eq_(len(result), 2)
        eq_(result[0], datetime.date(year=2011, month=11, day=15))
        eq_(result[1], datetime.date(year=2011, month=12, day=1))

    def test_get_daily_date_range_spans_month_and_year(self):
        date = datetime.date(year=2011, month=11, day=15)

        result = self._backend._get_daily_date_range(date, datetime.timedelta(days=65))
        eq_(len(result), 3)
        eq_(result[0], datetime.date(year=2011, month=11, day=15))
        eq_(result[1], datetime.date(year=2011, month=12, day=1))
        eq_(result[2], datetime.date(year=2012, month=1, day=1))

    def test_metric_by_day(self):
        date = datetime.date(year=2011, month=12, day=1)
        user_id = "user1234"
        metric = "badges:21"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=30), inc_amt=5))

        series, values = self._backend.get_metric_by_day(user_id, metric, date, 30)

        eq_(len(series), 30)
        eq_(len(values.keys()), 30)
        eq_(values["2011-12-05"], 2)
        eq_(values["2011-12-08"], 3)
        eq_(values["2011-12-30"], 5)

    def test_metric_by_count_start_end_date(self):
        start_date = datetime.date(year=2011, month=9, day=1)
        end_date = datetime.date(year=2011, month=11, day=1)
        user_id = "user1234"
        metric = "badges:21"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=5, day=30), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=7, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=8, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=9, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=10, day=1), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=11, day=5), inc_amt=2))

        count = self._backend.get_count(user_id, metric, start_date=start_date, end_date=end_date)
        eq_(count, 8)

    def test_parse_and_process_metrics(self):
        series = [datetime.datetime(year=2011, month=5, day=30), datetime.datetime(year=2011, month=7, day=8), datetime.datetime(year=2011, month=8, day=5),
            datetime.datetime(year=2011, month=9, day=8), datetime.datetime(year=2011, month=9, day=8), datetime.datetime(year=2011, month=10, day=1)]
        metrics = [[None, None, None, None, None, None]]

        new_series, new_metrics = self._backend._parse_and_process_metrics(series, metrics)
        eq_(set(['2011-10-01', '2011-07-08', '2011-09-08', '2011-08-05', '2011-05-30']), new_series)
        eq_({'2011-10-01': 0, '2011-07-08': 0, '2011-09-08': 0, '2011-08-05': 0, '2011-05-30': 0}, new_metrics)

    def test_metric_by_count_start_end_date_within_a_month(self):
        start_date = datetime.date(year=2011, month=9, day=1)
        end_date = datetime.date(year=2011, month=9, day=15)
        user_id = "user1234"
        metric = "badges:21"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=5, day=30), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=7, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=8, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=9, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=10, day=1), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=11, day=5), inc_amt=2))

        count = self._backend.get_count(user_id, metric, start_date=start_date, end_date=end_date)
        eq_(count, 3)

    def test_metric_by_count_start_end_date_with_metric_on_end_date(self):
        start_date = datetime.date(year=2011, month=9, day=1)
        end_date = datetime.date(year=2011, month=9, day=8)
        user_id = "user1234"
        metric = "badges:21"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=5, day=30), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=7, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=8, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=9, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=10, day=1), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=11, day=5), inc_amt=2))

        count = self._backend.get_count(user_id, metric, start_date=start_date, end_date=end_date)
        eq_(count, 3)

    def test_metric_by_count_start_end_date_with_metric_on_start_date(self):
        start_date = datetime.date(year=2011, month=9, day=8)
        end_date = datetime.date(year=2011, month=9, day=15)
        user_id = "user1234"
        metric = "badges:21"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=5, day=30), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=7, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=8, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=9, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=10, day=1), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=11, day=5), inc_amt=2))

        count = self._backend.get_count(user_id, metric, start_date=start_date, end_date=end_date)
        eq_(count, 3)

    @raises(Exception)
    def test_get_metrics_invalid_args(self):
        date = datetime.date(year=2011, month=12, day=1)

        self._backend.get_metrics([], date, group_by="leapyear")

    def test_get_count_in_time_period(self):
        date = datetime.date(year=2011, month=12, day=1)
        user_id = "user1234"
        metric = "badges:21"
        metric2 = "badge:22"

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2012, month=4, day=11), inc_amt=2))

    def test_get_metrics_by_day(self):
        date = datetime.date(year=2011, month=12, day=1)
        user_id = "user1234"
        metric = "badges:21"
        metric2 = "badge:22"

        #track some metrics
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2011, month=12, day=30), inc_amt=5))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2011, month=12, day=5), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2011, month=12, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2011, month=12, day=30), inc_amt=5))

        results = self._backend.get_metrics([(user_id, metric,), (user_id, metric2,)], date, limit=30, group_by="day")

        #metric
        eq_(len(results[0][0]), 30)
        eq_(len(results[0][1].keys()), 30)
        eq_(results[0][1]["2011-12-05"], 2)
        eq_(results[0][1]["2011-12-08"], 3)
        eq_(results[0][1]["2011-12-30"], 5)

        #metric 2
        eq_(len(results[1][0]), 30)
        eq_(len(results[1][1].keys()), 30)
        eq_(results[1][1]["2011-12-05"], 3)
        eq_(results[1][1]["2011-12-08"], 3)
        eq_(results[1][1]["2011-12-30"], 5)

    def test_get_metrics_by_week(self):
        user_id = 1234
        metric = "badge:25"
        metric2 = "badge:26"
        from_date = datetime.date(year=2012, month=4, day=2)

        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric, datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2012, month=4, day=11), inc_amt=2))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2012, month=4, day=18), inc_amt=3))
        ok_(self._backend.track_metric(user_id, metric2, datetime.datetime(year=2012, month=4, day=30)))

        results = self._backend.get_metrics([(user_id, metric,), (user_id, metric2)], from_date, limit=5, group_by="week")

        #metric 1
        eq_(len(results[0][0]), 5)
        eq_(results[0][1]["2012-04-02"], 4)
        eq_(results[0][1]["2012-04-09"], 2)
        eq_(results[0][1]["2012-04-16"], 0)
        eq_(results[0][1]["2012-04-23"], 0)
        eq_(results[0][1]["2012-04-30"], 0)

        #metric 2
        eq_(len(results[1][0]), 5)
        eq_(results[1][1]["2012-04-02"], 0)
        eq_(results[1][1]["2012-04-09"], 2)
        eq_(results[1][1]["2012-04-16"], 3)
        eq_(results[1][1]["2012-04-23"], 0)
        eq_(results[1][1]["2012-04-30"], 1)

    def test_track_metric_for_multi_users_at_the_same_time(self):
        user_id = 1234
        user_id2 = "user:5678"
        metric = "badge:25"
        from_date = datetime.date(year=2012, month=4, day=2)

        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=11), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=18), inc_amt=3))
        ok_(self._backend.track_metric([user_id, user_id2], metric, datetime.datetime(year=2012, month=4, day=30)))

        series, values = self._backend.get_metric_by_week(user_id, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

        series, values = self._backend.get_metric_by_week(user_id2, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

    def test_track_metric_multiple_metrics_at_the_same_time(self):
        date = datetime.date(year=2011, month=12, day=1)
        user_id = "user1234"
        metric = "badges:21"
        metric2 = "badge:22"

        #track some metrics
        ok_(self._backend.track_metric(user_id, [metric, metric2], datetime.datetime(year=2011, month=12, day=5), inc_amt=2))
        ok_(self._backend.track_metric(user_id, [metric, metric2], datetime.datetime(year=2011, month=12, day=8), inc_amt=3))
        ok_(self._backend.track_metric(user_id, [metric, metric2], datetime.datetime(year=2011, month=12, day=30), inc_amt=5))

        results = self._backend.get_metrics([(user_id, metric,), (user_id, metric2,)], date, limit=30, group_by="day")

        #metric
        eq_(len(results[0][0]), 30)
        eq_(len(results[0][1].keys()), 30)
        eq_(results[0][1]["2011-12-05"], 2)
        eq_(results[0][1]["2011-12-08"], 3)
        eq_(results[0][1]["2011-12-30"], 5)

        #metric 2
        eq_(len(results[1][0]), 30)
        eq_(len(results[1][1].keys()), 30)
        eq_(results[1][1]["2011-12-05"], 2)
        eq_(results[1][1]["2011-12-08"], 3)
        eq_(results[1][1]["2011-12-30"], 5)

    def test_track_multi_metrics_for_multi_users_at_the_same_time(self):
        user_id = 1234
        user_id2 = "user:5678"
        metric = "metric1"
        metric2 = "metric2"
        from_date = datetime.date(year=2012, month=4, day=2)

        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=5), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=7), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=9), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=11), inc_amt=2))
        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=18), inc_amt=3))
        ok_(self._backend.track_metric([user_id, user_id2], [metric, metric2], datetime.datetime(year=2012, month=4, day=30)))

        series, values = self._backend.get_metric_by_week(user_id, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

        series, values = self._backend.get_metric_by_week(user_id2, metric, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

        series, values = self._backend.get_metric_by_week(user_id, metric2, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)

        series, values = self._backend.get_metric_by_week(user_id2, metric2, from_date, limit=5)
        eq_(len(series), 5)
        eq_(values["2012-04-02"], 4)
        eq_(values["2012-04-09"], 4)
        eq_(values["2012-04-16"], 3)
        eq_(values["2012-04-23"], 0)
        eq_(values["2012-04-30"], 1)
