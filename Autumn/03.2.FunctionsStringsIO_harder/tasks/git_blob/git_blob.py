import zlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class BlobType(Enum):
    """Helper class for holding blob type"""
    COMMIT = b'commit'
    TREE = b'tree'
    DATA = b'blob'

    @classmethod
    def from_bytes(cls, type_: bytes) -> 'BlobType':
        for member in cls:
            if member.value == type_:
                return member
        assert False, f'Unknown type {type_.decode("utf-8")}'


@dataclass
class Blob:
    """Any blob holder"""
    type_: BlobType
    content: bytes


@dataclass
class Commit:
    """Commit blob holder"""
    tree_hash: str
    parents: list[str]
    author: str
    committer: str
    message: str


@dataclass
class Tree:
    """Tree blob holder"""
    children: dict[str, Blob]


def read_blob(path: Path) -> Blob:
    """
    Read blob-file, decompress and parse header
    :param path: path to blob-file
    :return: blob-file type and content
    """
    with open(path, 'rb') as f:
        a = zlib.decompress(f.read())
        z_i = a.index(b'\x00')
        header = a[:z_i]
        body = a[z_i + 1:]
        return Blob(type_=BlobType.from_bytes(header.split(b' ')[0]), content=body)


def traverse_objects(obj_dir: Path) -> dict[str, Blob]:
    """
    Traverse directory with git objects and load them
    :param obj_dir: path to git "objects" directory
    :return: mapping from hash to blob with every blob found
    """
    r = {}
    for path1 in obj_dir.iterdir():
        if path1.is_dir():
            for path in path1.iterdir():
                r[path1.name + path.name] = read_blob(path)
    return r


def parse_commit(blob: Blob) -> Commit:
    """
    Parse commit blob
    :param blob: blob with commit type
    :return: parsed commit
    """
    a = blob.content.decode().split('\n')
    r = Commit('', [], '', '', '')
    for i in a:
        s = i.split(' ', 1)
        if len(s) == 2:
            if s[0] == 'tree':
                r.tree_hash = s[1]
            elif s[0] == 'parent':
                r.parents = [s[1]]
            elif s[0] == 'author':
                r.author = s[1]
            elif s[0] == 'committer':
                r.committer = s[1]
    message_index = a.index('') + 1
    message = '\n'.join(a[message_index:])
    if message[-1] == '\n':
        r.message = message[:-1]
    else:
        r.message = message
    return r


def parse_tree(blobs: dict[str, Blob], tree_root: Blob, ignore_missing: bool = True) -> Tree:
    """
    Parse tree blob
    :param blobs: all read blobs (by traverse_objects)
    :param tree_root: tree blob to parse
    :param ignore_missing: ignore blobs which were not found in objects directory
    :return: tree contains children blobs (or only part of them found in objects directory)
    NB. Children blobs are not being parsed according to type.
        Also nested tree blobs are not being traversed.
    """
    content = tree_root.content
    pos = 0
    children = {}
    while pos < len(content):
        space_pos = content.find(b' ', pos)
        pos = space_pos + 1
        null_pos = content.find(b'\x00', pos)
        name = content[pos:null_pos].decode()
        pos = null_pos + 1
        sha_bytes = content[pos:pos + 20]
        sha = sha_bytes.hex()
        pos += 20
        if sha in blobs:
            children[name] = blobs[sha]
    return Tree(children=children)


def find_initial_commit(blobs: dict[str, Blob]) -> Commit:
    """
    Iterate over blobs and find initial commit (without parents)
    :param blobs: blobs read from objects dir
    :return: initial commit
    """
    for i, j in blobs.items():
        if j.type_ == BlobType.COMMIT:
            blob = parse_commit(j)
            if not blob.parents:
                return blob


def search_file(blobs: dict[str, Blob], tree_root: Blob, filename: str) -> Blob:
    """
    Traverse tree blob (can have nested tree blobs) and find requested file,
    check if file was not found (assertion).
    :param blobs: blobs read from objects dir
    :param tree_root: root blob for traversal
    :param filename: requested file
    :return: requested file blob
    """
    q = parse_tree(blobs, tree_root)
    for i,j in q.children.items():
        if i==filename:
            return j
        elif j.type_==BlobType.TREE:
            w = search_file(blobs, j , filename)
            if w.content != b'':
                return w
    return Blob(BlobType.TREE,b'')


