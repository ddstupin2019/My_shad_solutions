import typing as tp
from decimal import Decimal

from .models.base import Session
from .models import User, Expense, Trip, Debt, Event, Summary
from .exceptions import SplitViseException

MoneyType = Decimal


def create_user(
        username: str,
        *,
        session: Session
) -> User:
    """
    Create new User; validate user exists
    :param username: username to create
    :param session: active session to perform operations with
    :return: orm User object
    :exception: username already taken
    """
    existing_user = session.query(User).filter(
        User.username == username).first()
    if existing_user:
        raise SplitViseException(f"Username '{username}' is already taken")

    user = User(username=username)
    session.add(user)
    session.commit()

    return user


def create_event(
        trip_id: int,
        people_debt: tp.Mapping[int, MoneyType],
        people_payment: tp.Mapping[int, MoneyType],
        title: str,
        *,
        session: Session) -> Event:
    """
    Create Event in database, automatically creates Debts and Expenses; validates sum
    :param trip_id: Trip.trip_id from the database
    :param people_debt: mapping of User.user_id to theirs debt in that event
    :param people_payment: mapping of User.user_id to theirs payments in that event
    :param title: title of the event
    :param session: active session to perform operations with
    :return: orm Event object
    :exception: Trip not found by id, Can not create debt for user not in trip,
                Can not create payment for user not in trip, Sum of debts and sum of payments are not equal
    """
    trip = session.get(Trip, trip_id)
    if not trip:
        raise SplitViseException('Trip not found')
    for p in people_debt.keys():
        user = session.get(User, p)
        if not user:
            raise SplitViseException('User not found')
        if user not in trip.users:
            raise SplitViseException('User not in trip')
    for p in people_payment.keys():
        user = session.get(User, p)
        if not user:
            raise SplitViseException('User not found')
        if user not in trip.users:
            raise SplitViseException('User not in trip')
    if sum(people_payment.values()) != sum(people_debt.values()):
        raise SplitViseException(
            'Sum of debts and sum of payments are not equal')

    event = Event(
        trip_id=trip_id,
        title=title,
        settled_up=False
    )
    session.add(event)
    session.flush()

    for p, v in people_debt.items():
        session.add(Debt(event_id=event.event_id, debtor_id=p, value=v))
    for p, v in people_payment.items():
        session.add(Expense(event_id=event.event_id, payer_id=p, value=v))

    session.commit()
    return event


def create_trip(
        creator_id: int,
        title: str,
        description: str,
        *,
        session: Session) -> Trip:
    """
    Create Trip. Automatically add creator to the trip. Validate input: the title should not be empty and the creator
    should exist in the users table
    :param creator_id: User.user_id from the database to create trip by
    :param title: Title of the trip
    :param description: Long (or not so long) description of the trip
    :param session: active session to perform operations with
    :return: orm Trip object
    :exception: Title of a trip should not be empty, User not found by id
    """
    user = session.query(User).filter(User.user_id == creator_id).first()
    if not user:
        raise SplitViseException('User not found by id')
    if len(title) == 0:
        raise SplitViseException('Title of a trip should not be empty')
    trip = Trip(title=title, description=description,)
    trip.users.append(user)
    session.add(trip)
    session.commit()
    return trip


def add_user_to_trip(
        guest_id: int,
        trip_id: int,
        *,
        session: Session
) -> None:
    """
    Mark that the user with guest_id takes part in the trip. Check that the user and the trip do exist and the user has
    not been added to the trip yet.
    :param guest_id: User.user_id from the database to add to the trip
    :param trip_id: Trip.trip_id from the database
    :param session: active session to perform operations with
    :return: None
    :exception: Trip not found by id, User already in trip
    """
    trip = session.get(Trip, trip_id)
    user = session.get(User, guest_id)
    if not trip:
        raise SplitViseException('Trip not found by id')
    if not user:
        raise SplitViseException('User not found by id')
    if user in trip.users:
        raise SplitViseException('User already in trip')
    trip.users.append(user)


def get_trip_users(
        trip_id: int,
        *,
        session: Session
) -> list[User]:
    """
    Get Users from Trip; validate Trip exists
    :param trip_id: Trip.trip_id from the database
    :param session: active session to perform operations with
    :return: list of orm User objects
    :exception: Trip not found by id
    """
    trip = session.get(Trip, trip_id)
    if not trip:
        raise SplitViseException('Trip not found by id')
    return trip.users


def make_summary(
        trip_id: int,
        *,
        session: Session
) -> None:
    """
    Make trip summary. Mark all the events of the trip as settled up. Validate at least the existence of the trip
    being calculated
    :param trip_id: Trip.trip_id from the database
    :param session: active session to perform operations with
    :return: None
    :exception: Trip not found by id
    """
    trip = session.get(Trip, trip_id)
    if not trip:
        raise SplitViseException('Trip not found by id')
    balance: dict[int, Decimal] = {}

    for event in trip.events:
        if event.settled_up:
            continue
        for pay in event.expenses:
            balance[pay.payer_id] = balance.get(pay.payer_id, 0) - pay.value
        for debter in event.debts:
            balance[debter.debtor_id] = balance.get(
                debter.debtor_id, 0) + debter.value
        event.settled_up = True

    credit_and_payer = [[sm, id] for id, sm in balance.items()]
    credit_and_payer.sort()

    debter = 0
    payer = len(credit_and_payer) - 1
    while True:
        while debter < len(credit_and_payer) and credit_and_payer[debter][0] == Decimal(0):
            debter += 1
        while payer > 0 and credit_and_payer[payer][0] == Decimal(0):
            payer -= 1
        if debter >= payer:
            break

        if -credit_and_payer[debter][0] <= credit_and_payer[payer][0]:
            session.add(Summary(trip_id=trip_id,
                                user_from_id=credit_and_payer[debter][1],
                                user_to_id=credit_and_payer[payer][1],
                                value=-credit_and_payer[debter][0]))
            credit_and_payer[payer][0] += credit_and_payer[debter][0]
            credit_and_payer[debter][0] = Decimal(0)
        else:
            session.add(Summary(trip_id=trip_id,
                                user_from_id=credit_and_payer[debter][1],
                                user_to_id=credit_and_payer[payer][1],
                                value=credit_and_payer[payer][0]))
            credit_and_payer[debter][0] += credit_and_payer[payer][0]
            credit_and_payer[payer][0] = Decimal(0)

    session.commit()
