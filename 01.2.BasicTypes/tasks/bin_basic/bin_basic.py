def find_value(nums: list[int] | range, value: int) -> bool:
    """
        Find value in sorted sequence
        :param nums: sequence of integers. Could be empty
        :param value: integer to find
        :return: True if value exists, False otherwise
    """
    if not nums:
        return False

    left_border = 0
    right_border = len(nums)

    while right_border - left_border > 1:
        midle = (left_border + right_border) // 2

        if nums[midle] > value:
            right_border = midle
        else:
            left_border = midle

    return nums[left_border] == value
