import pandas as pd
from utils.utils import create_dim_date_table as my_module


def test_create_dim_date_table():
    df = pd.DataFrame({"Date": pd.date_range("2023-01-01", "2080-12-31")})
    df["week_day"] = df.Date.dt.weekday
    df["day_name"] = df.Date.dt.day_name()
    df["day"] = df.Date.dt.day
    df["month"] = df.Date.dt.month
    df["month_name"] = df.Date.dt.month_name()
    df["week"] = df.Date.dt.isocalendar().week
    df["quarter"] = df.Date.dt.quarter
    df["year"] = df.Date.dt.year
    df["is_month_start"] = df.Date.dt.is_month_start
    df["is_month_end"] = df.Date.dt.is_month_end
    df.insert(
        0,
        "date_id",
        (
            df.year.astype(str)
            + df.month.astype(str).str.zfill(2)
            + df.day.astype(str).str.zfill(2)
        ).astype(int),
    )
    df.name = "dim_date"
    expected_output = df
    actual_ouput = my_module()
    pd.testing.assert_frame_equal(actual_ouput, expected_output)
