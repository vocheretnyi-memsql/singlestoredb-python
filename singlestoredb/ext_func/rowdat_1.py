#!/usr/bin/env python3
import struct
import warnings
from io import BytesIO
from typing import Any
from typing import Iterable

try:
    import _singlestoredb_accel
except ImportError:
    warnings.warn(
        'could not load accelerated data reader for external functions; '
        'using pure Python implementation.',
        RuntimeWarning,
    )
    _singlestoredb_accel = None

from . import dtypes


def _load(colspec: Iterable[tuple[str, int]], data: bytes) -> list[list[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    list[list[Any]]

    '''
    data_len = len(data)
    data_io = BytesIO(data)
    out = []
    val = None
    while data_io.tell() < data_len:
        row = [struct.unpack('<q', data_io.read(8))[0]]
        for _, ctype in colspec:
            is_null = data_io.read(1) == b'\x01'
            if ctype == dtypes.BIGINT:
                val = struct.unpack('<q', data_io.read(8))[0]
            elif ctype == dtypes.DOUBLE:
                val = struct.unpack('<d', data_io.read(8))[0]
            elif ctype == dtypes.TEXT:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen).decode('utf-8')
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            row.append(None if is_null else val)
        out.append(row)
    return out


def _dump(returns: Iterable[int], data: Iterable[tuple[int, Any]]) -> bytes:
    '''
    Convert a list of lists of data into rowdat_1 format.

    Parameters
    ----------
    returns : str
        The returned data type
    data : list[list[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    out = BytesIO()
    for row_id, value in data:
        out.write(struct.pack('<q', row_id))
        for rtype in returns:
            out.write(b'\x01' if value is None else b'\x00')
            if rtype == dtypes.BIGINT:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    out.write(struct.pack('<q', value))
            elif rtype == dtypes.DOUBLE:
                if value is None:
                    out.write(struct.pack('<d', 0.0))
                else:
                    out.write(struct.pack('<d', value))
            elif rtype == dtypes.TEXT:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    sval = value.encode('utf-8')
                    out.write(struct.pack('<q', len(sval)))
                    out.write(sval)
            else:
                raise TypeError(f'unrecognized column type: {rtype}')
    return out.getvalue()


if _singlestoredb_accel is None:
    load = _load
    dump = _dump
else:
    load = _singlestoredb_accel.load_rowdat_1
    dump = _singlestoredb_accel.dump_rowdat_1
