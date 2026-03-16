import enum


class Status(enum.Enum):
    NEW = 0
    EXTRACTED = 1
    FINISHED = 2


def extract_alphabet(
        graph: dict[str, set[str]]
) -> list[str]:
    """
    Extract alphabet from graph
    :param graph: graph with partial order
    :return: alphabet
    """
    top_sort = []
    vis = {i: True for i in graph.keys()}

    for v in graph.keys():
        if vis[v]:
            qu = [v]
            vis[v] = False
            while qu:
                fl = True
                ver = qu[-1]
                for i in graph[ver]:
                    if vis[i]:
                        fl = False
                        vis[i] = False
                        qu.append(i)
                        break
                if fl:
                    top_sort.append(qu.pop())

    return top_sort[::-1]


def build_graph(
        words: list[str]
) -> dict[str, set[str]]:
    """
    Build graph from ordered words. Graph should contain all letters from words
    :param words: ordered words
    :return: graph
    """
    r = {j: set() for i in words for j in i}
    for w1, w2 in zip(words, words[1:]):
        for i in range(min(len(w1), len(w2))):
            if w1[i] != w2[i]:
                r[w1[i]].add(w2[i])
                break
    return r


#########################
# Don't change this code
#########################

def get_alphabet(
        words: list[str]
) -> list[str]:
    """
    Extract alphabet from sorted words
    :param words: sorted words
    :return: alphabet
    """
    graph = build_graph(words)
    return extract_alphabet(graph)

#########################
