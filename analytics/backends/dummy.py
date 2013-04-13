from analytics.backends.base import BaseAnalyticsBackend

class Dummy(BaseAnalyticsBackend):
    def track_count(self, unique_identifier, metric, inc_amt=1, **kwargs):
        pass

    def track_metric(self, unique_identifier, metric, date, inc_amt=1, **kwargs):
        pass

    def get_metric_by_day(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        pass

    def get_metric_by_week(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        pass

    def get_metric_by_month(self, unique_identifier, metric, from_date, limit=10, **kwargs):
        pass

    def get_metrics(self, metric_identifiers, from_date, limit=10, group_by="week", **kwargs):
        pass

    def get_count(self, unique_identifier, metric, **kwargs):
        pass

    def get_counts(self, metric_identifiers, **kwargs):
        pass