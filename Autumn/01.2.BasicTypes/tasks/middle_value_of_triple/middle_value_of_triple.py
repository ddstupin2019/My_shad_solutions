def get_middle_value(a: int, b: int, c: int) -> int:
    """
    Takes three values and returns middle value.
    """
    if min(a, c) <= b <= max(a, c):
        return b
    elif min(a, b) <= c <= max(a, b):
        return c
    else:
        return a
