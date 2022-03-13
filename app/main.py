from datetime import datetime
from collections import defaultdict
import logging
from typing import List, Tuple, DefaultDict

import pandas as pd
from tqdm import tqdm

from crud import customer, service_type, rate_plan, event
from data_utils import data_utils
from setup import (
    CHUNK_SIZE,
    ERROR_FILE_PATH,
    COLUMN_NAMES,
    INTEGER_COLUMNS,
    DATE_COLUMNS,
    CURRENCY_COLUMNS,
)

logger = logging.getLogger("__name__")
logging.basicConfig(level=logging.INFO)


def validate_and_clean_data(
    chunk: pd.DataFrame,
    integer_columns: List[str],
    date_columns: List[str],
    currency_columns: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validates and, if needed, cleans data by splitting it into two dataframes: data (cleaned) and error_ds

    Args:
        chunk (pd.DataFrame): dataframe to perform validation and cleaning on
        integer_columns (List[str]): names of integer columns
        date_columns (List[str]): names of date columns
        currency_columns (List[str]): names of currency columns

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: a tuple that contains two dataframes: cleaned data and
         not valid data that can't be cleaned
    """

    error_ds = pd.DataFrame(columns=["reason"])
    data, errors_null = data_utils.drop_nans(chunk)
    data, errors_int = data_utils.validate_convert_integer_columns(
        data, integer_columns
    )
    data, errors_date = data_utils.validate_convert_date_columns(data, date_columns)
    data, errors_currency = data_utils.validate_currency_columns(data, currency_columns)
    error_ds = pd.concat(
        [error_ds, errors_null, errors_int, errors_date, errors_currency]
    )

    return data, error_ds


def populate_db(df: pd.DataFrame) -> DefaultDict[str, int]:
    """Populates database tables (event, customer, service_type, rate_plan_id)

    Args:
        df (pd.DataFrame): a dataframe containing values to be inserted into DB

    Returns:
        DefaultDict[str, int]: dictionary where keys represent a table name with
        values representing how many records were inserted
    """
    records_inserted = defaultdict(lambda: 0)

    for crud, column_name in zip(
        (customer, service_type, rate_plan),
        ("customer_id", "service_type", "rate_plan_id"),
    ):
        values_to_insert_into_db = df[column_name].unique()
        db_data = crud.get_all()

        if not db_data.empty:
            values_to_insert_into_db = values_to_insert_into_db[
                ~pd.Series(values_to_insert_into_db).isin(db_data[column_name])
            ]

        if values_to_insert_into_db.size != 0:
            validated_data = [
                crud.schema(**{column_name: value}).dict()
                for value in values_to_insert_into_db
            ]
            response = crud.bulk_insert(validated_data)
            records_inserted[crud.model.__tablename__] = response.shape[0]

        value_pk_mappings = crud.get_all().set_index(column_name)["id"].to_dict()
        df[column_name] = df[column_name].map(value_pk_mappings)

    df.columns = event.schema.get_field_names()
    values_to_insert_into_db = df.to_dict("records")
    validated_data = [
        event.schema(**value).dict() for value in values_to_insert_into_db
    ]
    response = event.bulk_insert(validated_data)
    records_inserted[event.model.__tablename__] += response.shape[0]

    return records_inserted


if __name__ == "__main__":
    total_records_inserted = defaultdict(lambda: 0)

    for chunk in tqdm(
        data_utils.read_csv_chunks(
            columns=COLUMN_NAMES,
            chunksize=CHUNK_SIZE,
        )
    ):
        df, df_error = validate_and_clean_data(
            chunk=chunk,
            integer_columns=INTEGER_COLUMNS,
            date_columns=DATE_COLUMNS,
            currency_columns=CURRENCY_COLUMNS,
        )
        if not df_error.empty:
            path_ = f"{ERROR_FILE_PATH}/df_errors_{datetime.now()}.csv"
            df_error.to_csv(path_)
            logger.warning(f"Validation errors found. Exported to a file: '{path_}")
        if not df.empty:
            records_inserted = populate_db(df)

            for key, value in records_inserted.items():
                logger.info(f"{value} records inserted into the '{key}' table")
                total_records_inserted[key] += records_inserted[key]

    for key, value in total_records_inserted.items():
        logger.info(f"inserted total: '{value}' records into a '{key}' table")
