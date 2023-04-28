#!/usr/bin/env python3
'''
Web application for SingleStoreDB external functions.

This module supplies a function that can create web apps intended for use
with the external function feature of SingleStoreDB. The application
function is a standard ASGI <https://asgi.readthedocs.io/en/latest/index.html>
request handler for use with servers such as Uvicorn <https://www.uvicorn.org>.

An external function web application can be created using the `create_app`
function. By default, the exported Python functions are specified by
environment variables starting with SINGLESTOREDB_EXT_FUNCTIONS. See the
documentation in `create_app` for the full syntax. If the application is
created in Python code rather than from the command-line, exported
functions can be specified in the parameters.

An example of starting a server is shown below.

Example
-------
$ SINGLESTOREDB_EXT_FUNCTIONS='myfuncs.[percentage_90,percentage_95]' \
    uvicorn --factory singlestoredb.ext_func:create_app

'''
import copy
import functools
import importlib
import inspect
import io
import itertools
import os
import re
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import parse_qs
from urllib.parse import urljoin

import ujson

from . import rowdat_1
from .annotations import translate_annotation

# If a number of processes is specified, create a pool of workers
num_processes = max(0, int(os.environ.get('SINGLESTOREDB_EXT_NUM_PROCESSES', 0)))
if num_processes > 1:
    try:
        from ray.util.multiprocessing import Pool
    except ImportError:
        from multiprocessing import Pool
    func_map = Pool(num_processes).map
else:
    func_map = map


def sig_to_sql(signature: Dict[str, Any], base_url: Optional[str] = None) -> str:
    '''
    Convert a dictionary function signature into SQL.

    Parameters
    ----------
    signature : Dict[str, Any]
        Function signature in the form of a dictionary as returned by
        the `get_sig` function

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
                f'{item["type"].name}'
                ' null' if item.get('is_nullable') else ' not null',
            )
        returns = 'RETURNS ' + ', '.join(res)

    host = os.environ.get('SINGLESTOREDB_EXT_HOST', '127.0.0.1')
    port = os.environ.get('SINGLESTOREDB_EXT_PORT', '8000')

    url = urljoin(base_url or f'https://{host}:{port}', signature['endpoint'])

    return (
        f'CREATE OR REPLACE EXTERNAL FUNCTION `{signature["name"]}`' +
        '(' + ', '.join(args) + ')' + returns +
        f' AS REMOTE SERVICE "{url}" FORMAT ROWDAT_1;'
    )


def get_sig(name: str, func: Callable[..., Any]) -> Dict[str, Any]:
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


def get_func_names(funcs: str) -> List[Tuple[str, str]]:
    '''
    Parse all function names from string.

    Parameters
    ----------
    func_names : str
        String containing one or more function names. The syntax is
        as follows: [func-name-1@func-alias-1,func-name-2@func-alias-2,...].
        The optional '@name' portion is an alias if you want the function
        to be renamed.

    Returns
    -------
    List[Tuple[str]] : a list of tuples containing the names and aliases
        of each function.

    '''
    if funcs.startswith('['):
        func_names = funcs.replace('[', '').replace(']', '').split(',')
        func_names = [x.strip() for x in func_names]
    else:
        func_names = [funcs]

    out = []
    for name in func_names:
        alias = name
        if '@' in name:
            name, alias = name.split('@', 1)
        out.append((name, alias))

    return out


def call_func(func: Callable[..., Any], row: list[Any]) -> Tuple[int, Any]:
    '''
    Call a function on a row of data.

    Parameters
    ----------
    func : Callable
        The function to call
    row : list[Any]
        The row of data to use as function parameters

    Returns
    -------
    tuple[int, Any] : a tuple containing the row ID and function result

    '''
    return row[0], func(*row[1:])


def make_func(name: str, func: Callable[..., Any]) -> Callable[..., Any]:
    '''
    Make a function endpoint.

    Parameters
    ----------
    name : str
        Name of the function to create
    func : Callable
        The function to call as the endpoint

    Returns
    -------
    Callable

    '''
    async def do_func(rows: list[Any]) -> List[Any]:
        '''Call function on given rows of data.'''
        return list(func_map(functools.partial(call_func, func), rows))

    do_func.__name__ = name
    do_func.__doc__ = func.__doc__
    sig = get_sig(name, func)
    do_func._ext_func_signature = sig  # type: ignore
    do_func._ext_func_colspec = [(x['name'], x['type'])  # type: ignore
                                 for x in sig['args']]
    do_func._ext_func_returns = [x['type'] for x in sig['returns']]  # type: ignore

    return do_func


def create_app(
    functions: Optional[
        Union[
            str,
            Iterable[str],
            Callable[..., Any],
            Iterable[Callable[..., Any]],
        ]
    ] = None,


) -> Callable[..., Any]:
    '''
    Create an external function application.

    If `functions` is None, the environment is searched for function
    specifications in variables starting with `SINGLESTOREDB_EXT_FUNCTIONS`.
    Any number of environment variables can be specified as long as they
    have this prefix. The format of the environment variable value is the
    same as for the `functions` parameter.

    Parameters
    ----------
    functions : str or Iterable[str], optional
        Python functions are specified using a string format as follows:
            * Single function : <pkg1>.<func1>
            * Multiple functions : <pkg1>.[<func1-name,func2-name,...]
            * Function aliases : <pkg1>.[<func1@alias1,func2@alias2,...]
            * Multiple packages : <pkg1>.<func1>:<pkg2>.<func2>

    Returns
    -------
    Callable : the application request handler

    '''

    # List of functions specs
    specs: List[Union[str, Callable[..., Any]]] = []

    # Look up Python function specifications
    if functions is None:
        for k, v in os.environ.items():
            if k.startswith('SINGLESTOREDB_EXT_FUNCTIONS'):
                specs.append(v)
    elif isinstance(functions, str):
        specs = [functions]
    elif callable(functions):
        specs = [functions]
    else:
        specs = list(functions)

    # Add functions to application
    endpoints = dict()
    for funcs in itertools.chain(specs):
        if isinstance(funcs, str):
            pkg_path, func_names = funcs.rsplit('.', 1)
            pkg = importlib.import_module(pkg_path)

            # Add endpoint for each exported function
            for name, alias in get_func_names(func_names):
                item = getattr(pkg, name)
                endpoints[f'/functions/{alias}'] = make_func(alias, item)
        else:
            alias = funcs.__name__
            endpoints[f'/functions/{alias}'] = make_func(alias, item)

    # Plain text response start
    text_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'text/plain')],
    )

    # JSON response start
    json_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'application/json')],
    )

    # ROWDAT_1 response start
    rowdat_1_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'x-application/rowdat_1')],
    )

    # Path not found response start
    path_not_found_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=404,
    )

    # Response body template
    body_response_dict: Dict[str, Any] = dict(
        type='http.response.body',
    )

    async def app(
        scope: Dict[str, Any],
        receive: Callable[..., Awaitable[Any]],
        send: Callable[..., Awaitable[Any]],
    ) -> None:
        '''
        Application request handler.

        Parameters
        ----------
        scope : dict
            ASGI request scope
        receive : Callable
            Function to receieve request information
        send : Callable
            Function to send response information

        '''
        assert scope['type'] == 'http'

        method = scope['method']

        path = scope['path']
        if path.endswith('/'):
            path = path[:-1]

        # Handle api reflection
        if method == 'GET' and path == '/functions':
            query_string = parse_qs(scope.get('query_string', ''))

            # SQL code
            if query_string.get(b'format', [b'json'])[-1] == b'sql':  # type: ignore
                host = 'localhost:80'
                for k, v in scope['headers']:
                    if k == b'host':
                        host = v.decode('utf-8')
                        break
                url = f'{scope["scheme"]}://{host}{scope["path"]}'
                await send(text_response_dict)
                syntax = []
                for endpoint in endpoints.values():
                    syntax.append(
                        sig_to_sql(
                            endpoint._ext_func_signature,  # type: ignore
                            base_url=url,
                        ),
                    )
                body = '\n'.join(syntax).encode('utf-8')

            # JSON
            else:
                await send(json_response_dict)
                signatures = [
                    x._ext_func_signature for x in endpoints.values()  # type: ignore
                ]
                signatures = copy.deepcopy(signatures)
                for sig in signatures:
                    for i, arg in enumerate(sig['args']):
                        arg['type'] = arg['type'].name
                        if 'items' in arg:
                            arg['items']['type'] = arg['items']['type'].name
                    for i, ret in enumerate(sig['returns']):
                        ret['type'] = ret['type'].name
                        if 'items' in ret:
                            ret['items']['type'] = ret['items']['type'].name
                body = ujson.dumps(signatures).encode('utf-8')

        # Create a new function from source
        elif method == 'PUT' and path == '/create_functions':
            request = await receive()
            info = ujson.loads(request['body'])

            glob: Dict[str, Any] = dict()
            loc: Dict[str, Any] = dict()

            exec(info['code'], glob, loc)

            # This is really needed outside the scope of this app to construct
            # an appropriate environment for the app to run in.
            # imports = info['imports']

            # Add endpoint for each exported function
            for name, alias in get_func_names(info['functions']):
                endpoints[f'/functions/{alias}'] = make_func(alias, loc[name])

            body = b''
            await send(json_response_dict)

        # Call the endpoint
        elif method == 'POST' and path in endpoints:
            data = io.BytesIO()
            more_body = True
            while more_body:
                request = await receive()
                data.write(request['body'])
                more_body = request.get('more_body', False)
#           out = await endpoints[path](ujson.loads(request['body'])['data'])
#           body = ujson.dumps(dict(data=out)).encode('utf-8')
#           await send(json_response_dict)
            func = endpoints[path]
            out = await func(
                rowdat_1.load(
                    func._ext_func_colspec, data.getvalue(),  # type: ignore
                ),
            )
            body = rowdat_1.dump(func._ext_func_returns, out)  # type: ignore
            await send(rowdat_1_response_dict)

        # Path not found
        else:
            body = b''
            await send(path_not_found_response_dict)

        # Send body
        out = body_response_dict.copy()
        out['body'] = body
        await send(out)

    return app
