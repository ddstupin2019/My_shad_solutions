import copy
from compgraph import operations
from compgraph.graph import Graph


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    len_document_column = 'len_document'
    col_doc_with_word_column = 'col_doc_with_word'
    freq_word_column = 'freq_word'

    words_graph: Graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    len_document_graph: Graph = Graph.graph_from_iter(input_stream_name) \
        .sort([doc_column]) \
        .reduce(operations.CountRow(len_document_column), [])

    col_doc_whith_word_graph: Graph = copy.deepcopy(words_graph) \
        .sort([text_column, doc_column]) \
        .reduce(operations.FirstReducer(), [text_column, doc_column]) \
        .reduce(operations.Count(col_doc_with_word_column), [text_column])

    result_graph: Graph = copy.deepcopy(words_graph) \
        .sort([doc_column, text_column]) \
        .reduce(operations.TermFrequency(text_column, freq_word_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner(), copy.deepcopy(col_doc_whith_word_graph), [text_column]) \
        .join(operations.InnerJoiner(), copy.deepcopy(len_document_graph), []) \
        .sort([text_column, doc_column]) \
        .reduce(operations.Tf_idf(freq_word_column, len_document_column, col_doc_with_word_column, result_column),
                [text_column, doc_column]) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column])

    return result_graph


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    freq_word_column = 'freq_word'
    freq_word_all_column = 'freq_word_all'
    count_word_column = 'count_word'

    words_graph: Graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \

    filter_graph: Graph = copy.deepcopy(words_graph) \
        .sort([text_column, doc_column]) \
        .reduce(operations.Count(count_word_column), [text_column, doc_column])

    words_graph = words_graph \
        .sort([text_column, doc_column]) \
        .join(operations.InnerJoiner(), copy.deepcopy(filter_graph), [text_column, doc_column]) \
        .map(operations.Filter(lambda row: (len(row[text_column]) > 4) and row[count_word_column] >= 2))

    all_freq_graph = copy.deepcopy(words_graph) \
        .sort([text_column]) \
        .reduce(operations.TermFrequency(text_column, freq_word_all_column), []) \

    result_graph: Graph = copy.deepcopy(words_graph) \
        .sort([doc_column, text_column]) \
        .reduce(operations.TermFrequency(text_column, freq_word_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner(), copy.deepcopy(all_freq_graph), [text_column]) \
        .sort([text_column, doc_column]) \
        .reduce(operations.Pmi(freq_word_column, freq_word_all_column, result_column), [text_column, doc_column]) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column]) \
        .sort([doc_column])

    return result_graph


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    time_second_column = 'second'
    dist_column = 'dist'

    dist_graph = Graph.graph_from_iter(input_stream_name_length) \
        .sort([edge_id_column]) \
        .map(operations.CalculateDistance(start_coord_column, end_coord_column, dist_column))

    result_graph = Graph.graph_from_iter(input_stream_name_time) \
        .sort([edge_id_column]) \
        .map(operations.CalculateTime(enter_time_column, leave_time_column,
                                      weekday_result_column, hour_result_column, time_second_column)) \
        .join(operations.InnerJoiner(), copy.deepcopy(dist_graph), [edge_id_column]) \
        .map(operations.CalculateSpeed(dist_column, time_second_column, speed_result_column)) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.Mean(speed_result_column), [weekday_result_column, hour_result_column])

    return result_graph
