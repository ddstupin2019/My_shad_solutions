import click
from steganography_tool import decode_message, encode_message
from steganography_tool.utils import get_base_file, read_file, write_file

@click.group()
def cli() -> None:
    pass

@cli.command()
@click.argument('args', nargs=2)
def encode(args: tuple[str, str]):
    write_file(encode_message(get_base_file(), args[1]), args[0])


@cli.command()
@click.argument('input_filename')
def decode(input_filename: str):
    click.echo(decode_message(read_file(input_filename)))
