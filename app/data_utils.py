from typing import List, Tuple

import pandas as pd
from pandas.api.types import is_numeric_dtype


class DataUtils:
    def read_csv_chunks(
        self, columns: List[str], chunksize: int = 500000
    ) -> pd.DataFrame:
        """Reads a dataset in chunks

        Args:
            columns (List[str]): column names to asign to a dataframe
            chunksize (int, optional): number of rows to read in one chunk. Defaults to 500000.

        Yields:
            Iterator[pd.DataFrame]: a dataframe with column names defined
        """
        with pd.read_csv("usage.csv", chunksize=chunksize) as reader:
            for chunk in reader:
                chunk.columns = columns
                yield chunk

    def drop_nans(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Drops rows with nan in any column. Returns two dataframes (cleaned data and dropped data)

        Args:
            data (pd.DataFrame): a dataframe to perform nan validation non

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: a tuple with cleaned data and dropped data
        """
        error_ds = data[data.isna().any(axis=1)]
        data = data[~data.isnull().any(axis=1)]
        return data, error_ds

    def validate_convert_date_columns(
        self, data: pd.DataFrame, columns: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validates date columns. Converts valid strings to a datetime

        Args:
            data (pd.DataFrame): dataframe to perform a validation on
            columns (List[str]): list containing date column names

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: a tuple with cleaned/converted data and data that cant be validated
        """
        data_to_clean = data.copy()
        error_list = []
        for column in columns:
            date_column = pd.to_datetime(
                data[column], errors="coerce", format=f"%Y-%m-%dT%H:%M:%S.%f%z"
            )
            error_ds = data_to_clean[date_column.isna()]
            if not error_ds.empty:
                error_list.append(error_ds)
            data_to_clean[column] = date_column
            data_to_clean = data_to_clean[date_column.notna()]

        if error_list:
            error_ds = pd.concat(error_list)
            error_ds["reason"] = "Date format is not: '%Y-%m-%dT%H:%M:%S.%f%z'"
        else:
            error_ds = pd.DataFrame()
        return data_to_clean, error_ds

    def _check_remainder(
        self, data: pd.DataFrame, column: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validates numeric column. Cheks if number of digits after a decimal point == 0)

        Args:
            data (pd.DataFrame): dataframe to perform a validation on
            column (str): column name to perform a validation on

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: a tuple with cleaned/converted data and data that cant be validated
        """
        mask = data[column] % 1 == 0
        data_to_clean = data[mask]
        error_ds = data[~mask]
        return data_to_clean, error_ds

    def validate_convert_integer_columns(
        self, data: pd.DataFrame, columns: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validates integer columns and converts them to an int64 type

        Args:
            data (pd.DataFrame): dataframe to perform a validation on
            columns (List[str]): column names to perform a validation on

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: a tuple with cleaned/converted data and data that cant be validated
        """
        data_to_clean = data.copy()
        error_list = []
        for column in columns:
            if is_numeric_dtype(data_to_clean[column]):
                data_to_clean, errors = self._check_remainder(data_to_clean, column)
                if not errors.empty:
                    error_list.append(errors)
            else:
                data_to_clean[column] = pd.to_numeric(
                    data_to_clean[column], errors="coerce"
                )
                errors = data[data_to_clean[column].isna()]
                data_to_clean = data_to_clean[data_to_clean[column].notna()]
                if not errors.empty:
                    error_list.append(errors)

                data_to_clean, errors = self._check_remainder(data_to_clean, column)
                if not errors.empty:
                    error_list.append(errors)

            data_to_clean[column] = data_to_clean[column].astype(int)

        if error_list:
            error_ds = pd.concat(error_list)
            error_ds["reason"] = "not an integer"
        else:
            error_ds = pd.DataFrame()

        return data_to_clean, error_ds

    def validate_currency_columns(
        self,
        data: pd.DataFrame,
        columns: List[str],
        num_digits_after_decimal_point: int = 6,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validates provided columns by checking if they can be converted to numeric type and
            digits after a decimal point does not exceed a value of num_digits_after_decimal_point

        Args:
            data (pd.DataFrame): dataframe to perform a validation on
            columns (List[str]): column names to perform a validation on
            num_digits_after_decimal_point (int, optional): precision after a decimal point. Defaults to 6.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: a tuple with cleaned/converted data and data that cant be validated
        """
        data_to_clean = data.copy()
        error_list = []
        for column in columns:
            mask = pd.to_numeric(data_to_clean[column], errors="coerce").notnull()
            error_list.append(data_to_clean[~mask])
            data_to_clean = data_to_clean[mask]

            str_column = data_to_clean[column].astype(str)
            mask = str_column.apply(
                lambda x: len(x.split(".")[1]) <= num_digits_after_decimal_point
            )
            errors_ds = data_to_clean[~mask]
            data_to_clean = data_to_clean[mask]
            error_list.append(errors_ds)

        if error_list:
            errors_ds = pd.concat(error_list)
            errors_ds[
                "reason"
            ] = "wrong currency format. Tip: should be a maximum 6 digits after a decimal point"
        else:
            errors_ds = pd.DataFrame()

        return data_to_clean, errors_ds


data_utils = DataUtils()
