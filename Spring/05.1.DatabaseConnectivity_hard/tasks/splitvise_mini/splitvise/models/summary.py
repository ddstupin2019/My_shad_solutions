from sqlalchemy import Integer, Column, ForeignKey, Numeric
from sqlalchemy.orm import relationship

from .base import Base


class Summary(Base):  # type: ignore
    __tablename__ = 'summaries'

    summary_id = Column(Integer, primary_key=True)
    trip_id = Column(Integer, ForeignKey('trips.trip_id'), nullable=False)
    user_from_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    user_to_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    value = Column(Numeric, nullable=False)

    trip = relationship('Trip', back_populates='summaries')
    user_from = relationship('User', foreign_keys=[user_from_id], back_populates='summary_user_from')
    user_to = relationship('User', foreign_keys=[user_to_id], back_populates='summary_user_to')
