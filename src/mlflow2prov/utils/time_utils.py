import datetime
from types import NoneType

import pytz


def unix_timestamp_to_datetime(msec: int | float | NoneType) -> datetime.datetime:
    """
    Converts a UNIX/POSIX timestamp to a datetime object in UTC time.
    """

    if msec:
        # If timestamp value is given, we assume it is in UTC time (see also https://github.com/mlflow/mlflow/issues/30#issuecomment-396094191).
        # In the UI, this time may be displayed in local time.
        return datetime.datetime.fromtimestamp(float(msec or 0.0) / 1000.0).replace(
            tzinfo=pytz.utc
        )
    else:
        return datetime.datetime.utcfromtimestamp(0.0).replace(tzinfo=pytz.utc)
