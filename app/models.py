from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    DateTime,
    Numeric,
    create_engine
)
from sqlalchemy.sql import func

from setup import SQLALCHEMY_CON_STR

engine = create_engine(SQLALCHEMY_CON_STR)
Base = declarative_base()


class Event(Base):
    __tablename__ = "event"
    id = Column(Integer, primary_key=True, index=True)
    customer_fk = Column(Integer, ForeignKey("customer.id"), index=True)
    start_date = Column(DateTime)
    service_type_fk = Column(Integer, ForeignKey("service.id"), index=True)
    rate_plan_fk = Column(Integer, ForeignKey("plan.id"), index=True)
    billing_flag_1 = Column(Integer)
    billing_flag_2 = Column(Integer)
    duration = Column(Integer)
    charge = Column(Numeric(scale=6))
    month = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    event = relationship("Event")


class ServiceType(Base):
    __tablename__ = "service"
    id = Column(Integer, primary_key=True, index=True)
    service_type = Column(String, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    event = relationship("Event")


class RatePlan(Base):
    __tablename__ = "plan"
    id = Column(Integer, primary_key=True, index=True)
    rate_plan_id = Column(Integer, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    event = relationship("Event")
