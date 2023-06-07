#!/usr/bin/env python3
import inspect
import os
import re
from collections import OrderedDict
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import urljoin

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


def get_signature(name: str, func: Callable[..., Any]) -> Dict[str, Any]:
    '''
    Print the UDF signature of the Python callable.

    Parameters
    ----------
    func : Callable
        The function to extract the signature of

    '''
    args: List[Dict[str, Any]] = []
    out: Dict[str, Any] = dict(name=name, args=args)
    spec = inspect.getfullargspec(func)

    annotations = dict(spec.annotations)

    # Make sure all arguments are annotated
    spec_diff = set(spec.args).difference(set(annotations.keys()))
    if spec_diff:
        raise ValueError(
            'missing annotations for {} in {}'
            .format(', '.join(spec_diff), name),
        )

    for arg in spec.args:
        ann = spec.annotations[arg]
        args.append(dict(name=arg, **translate_annotation(ann)))

    if 'return' not in spec.annotations:
        raise ValueError(f'no return value annotation in function {name}')

    if spec.annotations['return']:
        ann = spec.annotations['return']
        if re.match(r'^(typing\.)?Tuple\[', str(ann), flags=re.I):
            out['returns'] = []
            for item in ann.__args__:
                out['returns'].append(translate_annotation(item))
        else:
            out['returns'] = [translate_annotation(ann)]

    out['endpoint'] = f'/functions/{name}'
    out['doc'] = func.__doc__

    return out


def signature_to_sql(signature: Dict[str, Any], base_url: Optional[str] = None) -> str:
    '''
    Convert a dictionary function signature into SQL.

    Parameters
    ----------
    signature : Dict[str, Any]
        Function signature in the form of a dictionary as returned by
        the `get_signature` function

    Returns
    -------
    str : SQL formatted function signature

    '''
    args = []
    for arg in signature['args']:
        if arg['type'].name == 'array':
            args.append(
                f'`{arg["name"]}` array({arg["items"]["type"].name}'
                ' null)' if arg['items'].get('is_nullable') else ' not null)',
            )
        else:
            args.append(f'`{arg["name"]}` {arg["type"].name}')
        args[-1] += ' null' if arg.get('is_nullable') else ' not null'

    returns = ''
    if signature.get('returns'):
        res = []
        for item in signature['returns']:
            res.append(
                item['type'].name +
                (' null' if item.get('is_nullable') else ' not null'),
            )
        returns = ' RETURNS ' + ', '.join(res)

    host = os.environ.get('SINGLESTOREDB_EXT_HOST', '127.0.0.1')
    port = os.environ.get('SINGLESTOREDB_EXT_PORT', '8000')

    url = urljoin(base_url or f'https://{host}:{port}', signature['endpoint'])

    return (
        f'CREATE OR REPLACE EXTERNAL FUNCTION `{signature["name"]}`' +
        '(' + ', '.join(args) + ')' + returns +
        f' AS REMOTE SERVICE "{url}" FORMAT ROWDAT_1;'
    )
