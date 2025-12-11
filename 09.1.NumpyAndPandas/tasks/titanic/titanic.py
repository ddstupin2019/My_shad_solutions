import typing as tp

import pandas as pd


def male_age(df: pd.DataFrame) -> float:
    """
    Return mean age of survived men, embarked in Southampton with fare > 30
    :param df: dataframe
    :return: mean age
    """
    return float(df.query(('Survived == 1 and Sex == "male" and ' +
                     'Embarked == "S" and Fare > 30'))['Age'].mean())


def nan_columns(df: pd.DataFrame) -> tp.Iterable[str]:
    """
    Return list of columns containing nans
    :param df: dataframe
    :return: series of columns
    """
    return df.columns[df.isna().any()].tolist()


def class_distribution(df: pd.DataFrame) -> pd.Series:
    """
    Return Pclass distrubution
    :param df: dataframe
    :return: series with ratios
    """
    return df['Pclass'].value_counts().sort_index() / len(df)


def families_count(df: pd.DataFrame, k: int) -> int:
    """
    Compute number of families with more than k members
    :param df: dataframe,
    :param k: number of members,
    :return: number of families
    """
    return sum(df['Name'].str.split(',').str[0].value_counts() > k)


def mean_price(df: pd.DataFrame, tickets: tp.Iterable[str]) -> float:
    """
    Return mean price for specific tickets list
    :param df: dataframe,
    :param tickets: list of tickets,
    :return: mean fare for this tickets
    """
    tickets = tuple(tickets)
    return float(df[df['Ticket'].isin(tickets)]['Fare'].mean())


def max_size_group(df: pd.DataFrame, columns: list[str]) -> tp.Iterable[tp.Any]:
    """
    For given set of columns compute most common combination of values of these columns
    :param df: dataframe,
    :param columns: columns for grouping,
    :return: list of most common combination
    """
    tmp: pd.Series = pd.Series(df.groupby(columns).size().idxmax())
    return tuple(tmp.to_list())


def dead_lucky(df: pd.DataFrame) -> float:
    """
    Compute dead ratio of passengers with lucky tickets.
    A ticket is considered lucky when it contains an even number of digits in it
    and the sum of the first half of digits equals the sum of the second part of digits
    ex:
    lucky: 123222, 2671, 935755
    not lucky: 123456, 62869, 568290
    :param df: dataframe,
    :return: ratio of dead lucky passengers
    """
    return 1 - df[df['Ticket'].apply(is_lucky)]['Survived'].mean()

def is_lucky(ticket: str) -> bool:
    if ticket.isdigit() and len(ticket) % 2 == 0:
        return sum(map(int, list(ticket[:len(ticket)//2])))== \
                    sum(map(int, list(ticket[len(ticket)//2:])))
    return False
