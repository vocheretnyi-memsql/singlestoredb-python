#!/usr/bin/env python3
import re
from collections import OrderedDict
from typing import Any
from typing import Dict

from . import dtypes


type_map = OrderedDict({
    'sequence': 'array',
    'list': 'array',
    'int8': 'tinyint',
    'int16': 'smallint',
    'int32': 'int',
    'int64': 'bigint',
    'uint8': 'unsigned tinyint',
    'uint16': 'unsigned smallint',
    'uint32': 'unsigned int',
    'uint64': 'unsigned bigint',
    'int_': 'bigint',
    'uint': 'unsigned bigint',
    'int': 'bigint',
    'integer': 'bigint',
    'bool': 'bool',
    'float16': 'float',
    'float32': 'float',
    'float64': 'double',
    'float_': 'double',
    'float': 'double',
    'str_': 'text',
    'str': 'text',
    'unicode': 'text',
    'bytes': 'blob',
    'bytearray': 'blob',
})


def translate_annotation(ann: Any) -> Dict[str, Any]:
    '''
    Translate Python type annotations to SingleStoreDB data types.

    Parameters
    ----------
    ann : Python type annotation
        The annotation of the Python object

    Returns
    -------
    Dict[str, Any] : the return value is a dictionary containing all of
        the metadata of the Python annotation in SingleStoreDB terms.

    '''
    out: Dict[str, Any] = dict(type=None, is_nullable=False)

    ann = repr(ann).replace("'", '').replace('"', '').lower()
    ann = re.sub(r'\<\w+\s+(\w+)\>', r'\1', ann)
    ann = re.sub(r'\b(?:\w+\.)+(\w+)\b', r'\1', ann, flags=re.I)
    ann = re.sub(r'(\[|\])', r' ', ann)
    ann = re.sub(r'\s+', r' ', ann).strip()

    if re.search(r'\bunion\b', ann, flags=re.I):
        raise TypeError('unions are not supported')
    if re.search(r'\bdict\b', ann, flags=re.I):
        raise TypeError('dictionaries are not supported')

    for k, v in type_map.items():
        ann = re.sub(k, v, ann, flags=re.I)

    if re.match(r'^optional\b', ann, flags=re.I):
        out['is_nullable'] = True
        ann = re.sub(r'^optional\s*', r'', ann, flags=re.I).strip()

    if re.match(r'^array\s*', ann, flags=re.I):
        out['type'] = dtypes.ARRAY
        ann = re.sub(r'^array\s*', r'', ann, flags=re.I).strip()

    # Handle array item types
    if out['type'] == dtypes.ARRAY:
        out['items'] = dict(is_nullable=False)
        if re.match(r'^optional\b', ann, flags=re.I):
            out['items']['is_nullable'] = True
            ann = re.sub(r'^optional\s*', r'', ann, flags=re.I).strip()
        out['items']['type'] = dtypes.get_scalar_type(ann)

    # Scalar types
    else:
        out['type'] = dtypes.get_scalar_type(ann)

    return {k: v for k, v in out.items() if v is not None}
