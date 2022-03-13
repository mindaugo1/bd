import datetime
import logging

from sqlalchemy import delete
from sqlalchemy.sql import text

from crud import event, customer, rate_plan, service_type
from setup import DELETE_AFTER_DAYS

logger = logging.getLogger("__name__")
logging.basicConfig(level=logging.INFO)


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

    response = customer.delete_orphans()
    log_deleted(response, "customer")

    response = rate_plan.delete_orphans()
    log_deleted(response, "plan")

    response = service_type.delete_orphans()
    log_deleted(response, "service")
