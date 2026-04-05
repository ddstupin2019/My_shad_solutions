from sqlalchemy import Integer, Column, ForeignKey, DateTime, Boolean, String
from sqlalchemy.orm import relationship

from .base import Base


class Event(Base):  # type: ignore
    __tablename__ = 'events'

    event_id = Column(Integer, primary_key=True)
    trip_id = Column(Integer, ForeignKey("trips.trip_id"), nullable=False)
    title = Column(String, nullable=False)
    happened_datetime = Column(DateTime,)
    settled_up = Column(Boolean, nullable=False)

    trip = relationship("Trip", back_populates="events")

    debts = relationship('Debt', back_populates='event')
    expenses = relationship('Expense', back_populates='event')
