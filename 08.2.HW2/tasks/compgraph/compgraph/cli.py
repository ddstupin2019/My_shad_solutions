import json
from pathlib import Path

import click

from compgraph import operations, plot_image
from compgraph.algorithms import (
    word_count_graph,
    inverted_index_graph,
    pmi_graph,
    yandex_maps_graph
)
from compgraph.graph import Graph



@click.group()
def cli() -> None:
    """CompGraph CLI"""
    pass


def _read_data(input_file: Path) -> operations.TRowsIterable:
    """Read data from file line by line as JSON"""
    with open(input_file, 'r') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _write_data(rows: operations.TRowsIterable, file_name: Path) -> None:
    with open(file_name, 'w') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')


@cli.command()
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument('output_file', type=click.Path(dir_okay=False, path_type=Path))
def word_count(input_file: Path, output_file: Path) -> None:
    """Word_count cli"""
    graph: Graph = word_count_graph(input_stream_name='docs')

    tmp = list(_read_data(input_file))
    data: operations.TRowsIterable = iter(tmp)
    result: operations.TRowsIterable = graph.run(docs=lambda: data)
    _write_data(result, output_file)

    click.echo("Word count completed")


@cli.command()
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument('output_file', type=click.Path(dir_okay=False, path_type=Path))
def inverted_index(input_file: Path, output_file: Path) -> None:
    """Inverted index cli"""
    graph: Graph = inverted_index_graph(input_stream_name='text')

    data: list[operations.TRow] = list(_read_data(input_file))
    result: operations.TRowsIterable = graph.run(text=lambda: iter(data))
    _write_data(result, output_file)

    click.echo("Inverted index completed")


@cli.command()
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument('output_file', type=click.Path(dir_okay=False, path_type=Path))
def pmi(input_file: Path, output_file: Path) -> None:
    """Calculate PMI cli"""
    graph: Graph = pmi_graph(input_stream_name='text')

    data: list[operations.TRow] = list(_read_data(input_file))
    result: operations.TRowsIterable = graph.run(text=lambda: iter(data))
    _write_data(result, output_file)

    click.echo("Calculate PMI completed")


@cli.command()
@click.argument('travel_times_file', type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument('road_graph_file', type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument('output_file', type=click.Path(dir_okay=False, path_type=Path))
@click.option('--plot', '-p', is_flag=True, help='Generate visualization plot')
@click.option('--plot-file', type=click.Path(dir_okay=False, path_type=Path),
              default='speed_plot.png', show_default=True,
              help='Output file for the plot (if --plot is used)')
def yandex_maps(travel_times_file: Path, road_graph_file: Path,
                output_file: Path, plot: bool, plot_file: Path) -> None:
    """yandex maps cli"""
    graph: Graph = yandex_maps_graph(
        'travel_time', 'edge_length',
        enter_time_column='enter_time', leave_time_column='leave_time', edge_id_column='edge_id',
        start_coord_column='start', end_coord_column='end',
        weekday_result_column='weekday', hour_result_column='hour', speed_result_column='speed'
    )

    travel_data: list[operations.TRow] = list(_read_data(travel_times_file))
    road_data: list[operations.TRow] = list(_read_data(road_graph_file))

    result: operations.TRowsIterable = graph.run(
        travel_time=lambda: iter(travel_data),
        edge_length=lambda: iter(road_data)
    )
    tmp = list(result)
    if plot:
        plot_image.plot_img(tmp, plot_file)

    _write_data(tmp, output_file)
    click.echo("yandex maps completed")


if __name__ == '__main__':
    cli()
