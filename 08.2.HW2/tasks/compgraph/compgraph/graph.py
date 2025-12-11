import typing as tp

from compgraph.operations import Operation, Mapper, Joiner, Reducer, TRowsIterable, TRow
from compgraph.operations import Map, Join, Reduce, ReadIterFactory, Read
from compgraph.external_sort import ExternalSort


class Graph:
    """Computational graph implementation"""

    def __init__(self):
        self.__operations: list[Operation] = []
        self.__join_graphs: dict[int, Graph] = {}


    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph.__operations.append(ReadIterFactory(name))
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph = Graph()
        graph.__operations.append(Read(filename, parser))
        return graph

    def map(self, mapper: Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        self.__operations.append(Map(mapper))
        return self

    def reduce(self, reducer: Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        self.__operations.append(Reduce(reducer, keys))
        return self

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        self.__operations.append(ExternalSort(keys))
        return self

    def join(self, joiner: Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        self.__join_graphs[len(self.__operations)] = join_graph
        self.__operations.append(Join(joiner, keys))
        return self

    def run(self, **kwargs: tp.Any) -> TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        tmp_table: TRowsIterable = iter({})
        for operationInd in range(len(self.__operations)):
            if operationInd in self.__join_graphs:
                tmp_table = self.__operations[operationInd](
                    tmp_table, self.__join_graphs[operationInd].run(**kwargs), **kwargs)
            else:
                tmp_table = self.__operations[operationInd](
                    tmp_table, **kwargs)
        return tmp_table
