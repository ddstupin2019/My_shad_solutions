import heapq
import string


def normalize(
        text: str
) -> str:
    """
    Removes punctuation and digits and convert to lower case
    :param text: text to normalize
    :return: normalized query
    """
    return text.translate(str.maketrans('', '', string.punctuation + string.digits)).lower()


def get_words(
        query: str
) -> list[str]:
    """
    Split by words and leave only words with letters greater than 3
    :param query: query to split
    :return: filtered and split query by words
    """
    return [i for i in query.split() if len(i)>3]


def build_index(
        banners: list[str]
) -> dict[str, list[int]]:
    """
    Create index from words to banners ids with preserving order and without repetitions
    :param banners: list of banners for indexation
    :return: mapping from word to banners ids
    """
    r = {}
    for i in range(len(banners)):
        for j in set(get_words(normalize(banners[i]))):
            if j in r:
                r[j].append(i)
            else:
                r[j] = [i]
    return r


class Elem:
    val : int
    it = iter([])

    def __init__(self, val: int, it):
        self.val = val
        self.it = it
    def __lt__(self, other):
        return self.val < other.val

def get_banner_indices_by_query(
        query: str,
        index: dict[str, list[int]]
) -> list[int]:
    """
    Extract banners indices from index, if all words from query contains in indexed banner
    :param query: query to find banners
    :param index: index to search banners
    :return: list of indices of suitable banners
    """
    r = []
    heap = []
    zapros =  get_words(normalize(query))

    if not zapros:
        return []

    for i in zapros:
        if i in index:
            q = iter(index[i])
            heap.append(Elem(next(q), q))
        else:
            return []
    heapq.heapify(heap)


    while heap:
        elem = heapq.heappop(heap)
        r.append(elem.val)
        try:
            heapq.heappush(heap, Elem(next(elem.it), elem.it))
        except StopIteration:
            pass

    result = []
    for i in range(len(r)-len(zapros)+1):
        if r[i]==r[i+len(zapros)-1]:
            result.append(r[i])
    return result




#########################
# Don't change this code
#########################

def get_banners(
        query: str,
        index: dict[str, list[int]],
        banners: list[str]
) -> list[str]:
    """
    Extract banners matched to queries
    :param query: query to match
    :param index: word-banner_ids index
    :param banners: list of banners
    :return: list of matched banners
    """
    indices = get_banner_indices_by_query(query, index)
    return [banners[i] for i in indices]

#########################
