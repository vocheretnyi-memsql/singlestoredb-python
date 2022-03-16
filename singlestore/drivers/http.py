from __future__ import annotations

import warnings
from typing import Any
from typing import Dict

from .base import Driver


class HTTPDriver(Driver):

    name = 'http'

    pkg_name = 'singlestore.http'
    pypi = 'singlestore'
    anaconda = 'singlestore::singlestore'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('pure_python', False)
        params.pop('driver', None)
        if params.pop('local_infile', False):
            warnings.warn('The HTTP driver does not support file uploads.')
        if params['port'] is None:
            if type(self).name == 'https':
                params['port'] = 443
            else:
                params['port'] = 80
        return params


class HTTPSDriver(HTTPDriver):

    name = 'https'
