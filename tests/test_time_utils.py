import datetime
import time

import pytz

from mlflow2prov.utils.time_utils import unix_timestamp_to_datetime


class TestTimeUtils:
    def test_unix_timestamp_to_datetime(self):
        dt = datetime.datetime(2022, 2, 1, 8, 42, tzinfo=pytz.utc)
        ts = time.mktime(dt.timetuple()) * 1000.0

        assert unix_timestamp_to_datetime(ts) == dt
        assert unix_timestamp_to_datetime(int(ts)) == dt
        assert unix_timestamp_to_datetime(None) == datetime.datetime(
            1970, 1, 1, 0, 0
        ).replace(tzinfo=pytz.utc)
