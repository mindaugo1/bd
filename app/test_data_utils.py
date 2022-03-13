from data_utils import data_utils
import pandas as pd
from pandas.api.types import is_object_dtype, is_integer_dtype, is_float_dtype


def get_data():
    integer_column_names = ["customer_id", "event_type"]
    values = {
        "customer_id": [1, 2],
        "event_start_time": ["2016-01-26 07:18:46", "2016-02-26 07:18:46"],
        "event_type": [1, 2],
        "rate_plan_id": [1, 2],
        "billing_flag_1": [1, 2],
        "duration": [32, 33],
        "charge": [0.212325, 0.212326],
        "month": ["2016-02", "2016-03"],
    }
    return values, integer_column_names


def test_DataUtils_validate_convert_integer_columns_no_errors():
    values, integer_column_names = get_data()

    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_integer_columns(
        df, integer_column_names
    )
    assert data.equals(df)
    assert error_ds.empty

    values["customer_id"] = [1, "1"]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_integer_columns(
        df, integer_column_names
    )
    assert is_object_dtype(df["customer_id"])
    assert is_integer_dtype(data["customer_id"])
    df[integer_column_names] = df[integer_column_names].astype(int)
    assert data.equals(df)
    assert error_ds.empty


def test_DataUtils_validate_convert_integer_columns_float_with_decimal_point():
    values, integer_column_names = get_data()
    values["customer_id"] = [2.2, 1]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_integer_columns(
        df, integer_column_names
    )
    assert is_float_dtype(df["customer_id"])
    assert is_integer_dtype(data["customer_id"])
    data_to_compare_to = {key: list(value)[1] for key, value in values.items()}
    error_to_compare_to = {key: list(value)[0] for key, value in values.items()}
    assert data.equals(pd.DataFrame(data_to_compare_to, index=[1]))
    error_to_compare_to["reason"] = "not an integer"
    assert error_ds.equals(pd.DataFrame(error_to_compare_to, index=[0]))


def test_DataUtils_validate_convert_integer_columns_only_errors():
    values, integer_column_names = get_data()
    values["customer_id"] = [2.2, "3.1"]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_integer_columns(
        df, integer_column_names
    )
    assert data.empty
    values["customer_id"] = [2.2, 3.1]
    values["reason"] = "not an integer"
    assert error_ds.equals(pd.DataFrame(values))


def test_DataUtils_validate_convert_integer_columns_errors_in_different_rows():
    values, integer_column_names = get_data()
    values["customer_id"] = [2.0, "hi"]
    values["event_type"] = [2.2, 1]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_integer_columns(
        df, integer_column_names
    )
    assert data.empty
    error_ds = error_ds.sort_index()
    values["reason"] = "not an integer"
    assert error_ds.equals(pd.DataFrame(values))


def test_DataUtils_validate_convert_date_columns_correct_format():
    values, integer_column_names = get_data()
    values["event_start_time"] = [
        "2016-01-22T05:34:48.000+02:00",
        "2016-01-22T05:34:48.000+02:00",
    ]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_date_columns(df, ["event_start_time"])
    df["event_start_time"] = pd.to_datetime(df["event_start_time"])
    assert error_ds.empty
    assert data.equals(df)


def test_DataUtils_validate_convert_date_columns_incorect_format_all():
    values, integer_column_names = get_data()
    values["event_start_time"] = [
        "2016-01-22T05:34:8.00002:00",
        "201601-22t05:34:48000+02:00",
    ]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_date_columns(df, ["event_start_time"])
    df["reason"] = "Date format is not: '%Y-%m-%dT%H:%M:%S.%f%z'"
    assert data.empty
    assert error_ds.equals(df)


def test_DataUtils_validate_convert_date_columns_one_record_correct_one_incorect():
    values, integer_column_names = get_data()
    values["event_start_time"] = [
        "2016-01-22T05:34:48.000+02:00",
        "2016-01-2205:34:48.000+02:00",
    ]
    df = pd.DataFrame(values)
    data, error_ds = data_utils.validate_convert_date_columns(df, ["event_start_time"])
    data_to_compare_to = pd.DataFrame(df.iloc[:1])
    data_to_compare_to["event_start_time"] = pd.to_datetime(
        data_to_compare_to["event_start_time"]
    )
    errors_to_compare_to = df.iloc[1:]
    errors_to_compare_to["reason"] = "Date format is not: '%Y-%m-%dT%H:%M:%S.%f%z'"
    assert error_ds.equals(errors_to_compare_to)
    assert data.equals(data_to_compare_to)


def test_DataUtils_validate_currency_one_record_corect_one_incorect():
    values, integer_column_names = get_data()
    values["charge"] = [0.22212325, 0.212326]
    data, error_ds = data_utils.validate_currency_columns(
        pd.DataFrame(values), ["charge"]
    )
    data_to_compare_to = pd.DataFrame(
        [{key: value[1] for key, value in values.items()}], index=[1]
    )
    errors_to_compare_to = pd.DataFrame(
        [{key: value[0] for key, value in values.items()}], index=[0]
    )
    errors_to_compare_to[
        "reason"
    ] = "wrong currency format. Tip: should be a maximum 6 digits after a decimal point"
    assert data.equals(data_to_compare_to)
    assert error_ds.equals(errors_to_compare_to)


def test_DataUtils_validate_currency_all_incorect():
    values, integer_column_names = get_data()
    values["charge"] = [0.22212325, "k"]
    data, error_ds = data_utils.validate_currency_columns(
        pd.DataFrame(values), ["charge"]
    )
    error_ds = error_ds.sort_index()
    errors_to_compare_to = pd.DataFrame(values)
    errors_to_compare_to[
        "reason"
    ] = "wrong currency format. Tip: should be a maximum 6 digits after a decimal point"
    assert data.empty
    assert error_ds.equals(errors_to_compare_to.sort_index())
