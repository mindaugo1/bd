import datetime
import logging

from sqlalchemy import delete
from sqlalchemy.sql import text

from crud import event, crud

logger = logging.getLogger("__name__")
logging.basicConfig(level=logging.INFO)

DELETE_AFTER_DAYS = 180


def log_deleted(response, table):
    logger.info(f"deleted: {response.shape[0]} records from a '{table}' table")


if __name__ == "__main__":
    statement = (
        delete(event.model)
        .where(
            event.model.created_at
            < (datetime.datetime.now() - datetime.timedelta(days=DELETE_AFTER_DAYS))
        )
        .returning(event.model.id)
    )
    deleted_events = event.execute_statement(statement)
    log_deleted(deleted_events, "event")

    response = crud.execute_statement(
        text(
            """DELETE FROM customer 
               WHERE customer.id NOT IN (SELECT DISTINCT event.customer_fk FROM event) 
               RETURNING customer.id"""
        )
    )
    log_deleted(response, "customer")

    response = crud.execute_statement(
        text(
            """DELETE FROM plan 
               WHERE plan.id NOT IN (SELECT DISTINCT event.rate_plan_fk FROM event) 
               RETURNING plan.id"""
        )
    )
    log_deleted(response, "plan")

    response = crud.execute_statement(
        text(
            """DELETE FROM service 
               WHERE service.id NOT IN (SELECT DISTINCT event.service_type_fk FROM event) 
               RETURNING service.id"""
        )
    )
    log_deleted(response, "service")
