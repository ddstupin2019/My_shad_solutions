from collections.abc import Sequence


def find_median(nums1: Sequence[int], nums2: Sequence[int]) -> float:
    """
    Find median of two sorted sequences. At least one of sequences should be not empty.
    :param nums1: sorted sequence of integers
    :param nums2: sorted sequence of integers
    :return: middle value if sum of sequences' lengths is odd
             average of two middle values if sum of sequences' lengths is even
    """
    if len(nums1) > len(nums2):
        return find_median(nums2, nums1)

    if not nums1:
        if len(nums2) % 2 == 0:
            return (nums2[len(nums2) // 2 - 1] + nums2[len(nums2) // 2]) / 2
        else:
            return float(nums2[len(nums2) // 2])

    l_border = 0
    r_border = len(nums1)
    while l_border <= r_border:
        m1 = (l_border + r_border) // 2
        m2 = (len(nums1) + len(nums2) + 1) // 2 - m1

        l_1 = nums1[m1 - 1] if m1 > 0 else min(nums1[0], nums2[0]) - 1
        r_1 = nums1[m1] if m1 < len(nums1) else max(nums1[-1], nums2[-1]) + 1
        l_2 = nums2[m2 - 1] if m2 > 0 else min(nums1[0], nums2[0]) - 1
        r_2 = nums2[m2] if m2 < len(nums2) else max(nums1[-1], nums2[-1]) + 1

        if l_1 <= r_2 and l_2 <= r_1:
            if (len(nums1) + len(nums2)) % 2 == 0:
                return (max(l_1, l_2) + min(r_1, r_2)) / 2
            else:
                return float(max(l_1, l_2))
        if l_1 > r_2:
            r_border = m1 - 1
        else:
            l_border = m1 + 1
    return 0.0
