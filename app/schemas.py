from pydantic import BaseModel
from datetime import datetime
from pydantic import condecimal

class CustomerSchema(BaseModel):
    customer_id: int


class RatePlanSchema(BaseModel):
    rate_plan_id: int


class ServiceTypeSchema(BaseModel):
    service_type: str

class EventSchema(BaseModel):
    customer_fk: int
    start_date: datetime
    service_type_fk: int
    rate_plan_fk: int
    billing_flag_1: int
    billing_flag_2: int
    duration: int
    charge: condecimal(decimal_places=6)
    month: str

    @classmethod
    def get_field_names(cls):
        return list(cls.schema().get("properties").keys())