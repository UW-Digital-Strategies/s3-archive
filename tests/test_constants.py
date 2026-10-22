"""Tests for the canonical streaming chunk size.

The point of :mod:`s3_archive.constants` is that there is exactly one
definition of the streaming read size in the package. These tests guard
that invariant: the value itself, the public defaults bound to it, and
the absence of any private re-definition in a sibling module (six of
which carried their own copy before it was consolidated).
"""

import ast
import inspect
from pathlib import Path

from s3_archive import hashing, manifest, members, retry, seekable, seven_z
from s3_archive.constants import CHUNK_SIZE

_SRC = Path(__file__).resolve().parents[1] / "src" / "s3_archive"

# Modules that pull bytes off a stream in CHUNK_SIZE-sized reads.
_STREAMING_MODULES = (hashing, manifest, members, retry, seekable, seven_z)


_INT_OPS = {
    ast.Mult: lambda a, b: a * b,
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.LShift: lambda a, b: a << b,
    ast.Pow: lambda a, b: a**b,
}


def _fold_int(node: ast.expr) -> int | None:
    """Constant-fold an int expression (``65536``, ``64 * 1024``, ``1 << 16``)."""
    if isinstance(node, ast.Constant):
        return (
            node.value if isinstance(node.value, int) and not isinstance(node.value, bool) else None
        )
    if isinstance(node, ast.BinOp) and type(node.op) in _INT_OPS:
        left, right = _fold_int(node.left), _fold_int(node.right)
        if left is not None and right is not None:
            return _INT_OPS[type(node.op)](left, right)
    return None


def _module_level_int_assignments(path: Path) -> dict[str, int]:
    """Name -> value for every module-level assignment of an int-literal expression."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        elif isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        else:
            continue
        folded = _fold_int(value) if value is not None else None
        if folded is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                found[target.id] = folded
    return found


class TestChunkSize:
    def test_is_64_kib(self):
        assert CHUNK_SIZE == 64 * 1024

    def test_every_streaming_module_uses_the_shared_object(self):
        for module in _STREAMING_MODULES:
            assert module.CHUNK_SIZE is CHUNK_SIZE, module.__name__

    def test_public_chunk_size_defaults_bind_to_it(self):
        defaulted = (
            hashing.body_chunks,
            hashing.stream_hash_object,
            retry.resumable_body_chunks,
        )
        for func in defaulted:
            default = inspect.signature(func).parameters["chunk_size"].default
            assert default == CHUNK_SIZE, func.__qualname__


class TestSingleDefinition:
    def test_no_sibling_module_redefines_the_chunk_size(self):
        """Only constants.py may define a name whose value is the chunk size."""
        offenders = {
            f"{path.name}:{name}"
            for path in sorted(_SRC.glob("*.py"))
            if path.name != "constants.py"
            for name, value in _module_level_int_assignments(path).items()
            if value == CHUNK_SIZE
        }
        assert offenders == set()

    def test_constants_module_is_a_leaf(self):
        """constants.py must import nothing from s3_archive, so anything can import it."""
        tree = ast.parse((_SRC / "constants.py").read_text(encoding="utf-8"))
        imported = [
            node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom | ast.Import)
        ]
        assert imported == []
