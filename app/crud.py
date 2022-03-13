from typing import List, Dict, Union, Optional
import datetime

import pandas as pd
from sqlalchemy import insert, select, text

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

    def delete_orphans(self) -> pd.DataFrame:
        """Deletes orphans from 'customer' table

        Returns:
            pd.DataFrame: DataFrame with id's of deleted records
        """
        return self.execute_statement(
            text(
                """DELETE FROM customer 
                   WHERE customer.id NOT IN (SELECT DISTINCT event.customer_fk FROM event) 
                   RETURNING customer.id"""
            )
        )


class ServiceTypeCrud(Crud):
    """ServiceType crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a ServiceTypeSchema
        model: defaults to a ServiceType
    """

    def __init__(self):
        self.schema = ServiceTypeSchema
        self.model = ServiceType

    def delete_orphans(self) -> pd.DataFrame:
        """Deletes orphans from 'service_type' table

        Returns:
            pd.DataFrame: DataFrame with id's of deleted records
        """
        return self.execute_statement(
            text(
                """DELETE FROM service 
                   WHERE service.id NOT IN (SELECT DISTINCT event.service_type_fk FROM event) 
                   RETURNING service.id"""
            )
        )


class RatePlanCrud(Crud):
    """RatePlan crud class. Inherits from Crud class

    Attributes:
        schema: defaults to a RatePlanSchema
        model: defaults to a RatePlan
    """

    def __init__(self):
        self.schema = RatePlanSchema
        self.model = RatePlan

    def delete_orphans(self) -> pd.DataFrame:
        """Deletes orphans from 'plan' table

        Returns:
            pd.DataFrame: DataFrame with id's of deleted records
        """
        return self.execute_statement(
            text(
                """DELETE FROM plan 
                   WHERE plan.id NOT IN (SELECT DISTINCT event.rate_plan_fk FROM event) 
                   RETURNING plan.id"""
            )
        )


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
