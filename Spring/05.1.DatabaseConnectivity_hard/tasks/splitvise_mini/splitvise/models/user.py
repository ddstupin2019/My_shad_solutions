from sqlalchemy import Integer, Column, String
from sqlalchemy.orm import relationship

from .base import Base
from .trip import UserTrip


class User(Base):  # type: ignore
    __tablename__ = 'users'

    user_id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)

    trips = relationship('Trip', secondary=UserTrip, back_populates='users')

    debts = relationship('Debt', back_populates='debtor')
    expenses = relationship('Expense', back_populates='payer')
    summary_user_from = relationship(
        'Summary', foreign_keys="Summary.user_from_id", back_populates='user_from')
    summary_user_to = relationship(
        'Summary', foreign_keys="Summary.user_to_id", back_populates='user_to')

    def __repr__(self) -> str:
        return f'<User user_id={self.user_id}, username={self.username}>'
