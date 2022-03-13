from typing import List, Dict, Union, Optional
import datetime

import pandas as pd
from sqlalchemy import insert, select

from models import Customer, Event, ServiceType, RatePlan, engine
from schemas import CustomerSchema, RatePlanSchema, ServiceTypeSchema, EventSchema


class Crud:
    def __init__(self):
        self.schema = None
        self.model = None

    def execute_statement(
        self,
        statement: Union[select, insert],
        data: Optional[List[Dict[str, Union[str, int, datetime.datetime]]]] = None,
    ) -> pd.DataFrame:
        """Executes a statement with/without provided data

        Args:
            statement (Union[select, insert]): database statement
            data (Optional[List[Dict[str, Union[str, int, datetime.datetime]]]], optional): data to use in statemt execution.
             Defaults to None.

        Returns:
            pd.DataFrame: DataFrame representing the result of statement execution
        """
        with engine.connect() as connection:
            return pd.DataFrame(
                connection.execute(
                    statement,
                    data,
                )
                .mappings()
                .all()
            )

    def bulk_insert(
        self,
        data: List[Dict[str, Union[str, int, datetime.datetime]]],
    ) -> pd.DataFrame:
        """Bulk insert into a table

        Args:
            data (List[Dict[str, Union[str, int, datetime.datetime]]]): data to be inserted

        Returns:
            pd.DataFrame: DataFrame with id's of inserted records
        """
        statement = insert(self.model).returning(self.model.id)
        return self.execute_statement(statement, data)

    def get_all(self) -> pd.DataFrame:
        """Get all records from a table

        Returns:
            pd.DataFrame: DataFrame with inserted records (and columns representing DB table structure)
        """
        return self.execute_statement(select(self.model))


class CustomerCrud(Crud):
    """Customer crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a CustomerSchema
        model: defaults to a Customer
    """

    def __init__(self):
        self.schema = CustomerSchema
        self.model = Customer


class ServiceTypeCrud(Crud):
    """ServiceType crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a ServiceTypeSchema
        model: defaults to a ServiceType
    """

    def __init__(self):
        self.schema = ServiceTypeSchema
        self.model = ServiceType


class RatePlanCrud(Crud):
    """RatePlan crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a RatePlanSchema
        model: defaults to a RatePlan
    """

    def __init__(self):
        self.schema = RatePlanSchema
        self.model = RatePlan


class EventCrud(Crud):
    """Event crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a EventSchema
        model: defaults to a Event
    """

    def __init__(self):
        self.schema = EventSchema
        self.model = Event


customer, service_type, rate_plan, event, crud = (
    CustomerCrud(),
    ServiceTypeCrud(),
    RatePlanCrud(),
    EventCrud(),
    Crud(),
)
