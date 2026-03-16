from pathlib import Path
import subprocess


def python_sort(file_in: Path, file_out: Path) -> None:
    """
    Sort tsv file using python built-in sort
    :param file_in: tsv file to read from
    :param file_out: tsv file to write to
    """
    with open(Path(file_in), 'r', encoding='utf-8') as f:
        a = f.readlines()

    def st(a: str):
        tmp = a.strip().split()
        return int(tmp[1]), tmp[0]

    with open(Path(file_out), 'w', encoding='utf-8') as f1:
        f1.writelines(sorted(a, key=st))


def util_sort(file_in: Path, file_out: Path) -> None:
    """
    Sort tsv file using sort util
    :param file_in: tsv file to read from
    :param file_out: tsv file to write to
    """
    subprocess.run(['sort', '-k2,2n', '-k1,1', '-o', str(file_out), str(file_in)],
                   check=True,
                   encoding='utf-8')
