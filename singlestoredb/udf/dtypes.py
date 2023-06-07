#!/usr/bin/env python3
from dataclasses import dataclass
from typing import Dict
from typing import Union


@dataclass
class DataType:
    '''SingleStoreDB data types base class.'''
    id: int
    name: str

    def __hash__(self) -> int:
        return id(self.id)


@dataclass
class CollectionType(DataType):
    '''SingleStoreDB collection types base class.'''

    def __hash__(self) -> int:
        return id(self.id)


#
# Scalar types
#
BOOL = DataType(0, 'bool')
BOOLEAN = DataType(0, 'boolean')
BIT = DataType(1, 'bit')
TINYINT = DataType(2, 'tinyint')
UNSIGNED_TINYINT = DataType(3, 'unsigned tinyint')
SMALLINT = DataType(4, 'smallint')
UNSIGNED_SMALLINT = DataType(5, 'unsigned smallint')
MEDIUMINT = DataType(6, 'mediumint')
UNSIGNED_MEDIUMINT = DataType(7, 'unsigned mediumint')
INT = DataType(8, 'int')
INTEGER = DataType(8, 'integer')
UNSIGNED_INT = DataType(9, 'unsigned int')
UNSIGNED_INTEGER = DataType(9, 'unsigned integer')
BIGINT = DataType(10, 'bigint')
UNSIGNED_BIGINT = DataType(11, 'unsigned bigint')
FLOAT = DataType(12, 'float')
DOUBLE = DataType(13, 'double')
REAL = DataType(13, 'real')
DECIMAL = DataType(14, 'decimal')
DEC = DataType(14, 'dec')
FIXED = DataType(14, 'fixed')
NUMERIC = DataType(14, 'numeric')
DATE = DataType(15, 'date')
TIME = DataType(16, 'time')
TIME_6 = DataType(17, 'time(6)')
DATETIME = DataType(18, 'datetime')
DATETIME_6 = DataType(19, 'datetime(6)')
TIMESTAMP = DataType(20, 'timestamp')
TIMESTAMP_6 = DataType(21, 'timestamp(6)')
YEAR = DataType(22, 'year')
CHAR = DataType(23, 'char')
VARCHAR = DataType(24, 'varchar')
LONGTEXT = DataType(25, 'longtext')
MEDIUMTEXT = DataType(26, 'mediumtext')
TEXT = DataType(27, 'text')
TINYTEXT = DataType(28, 'tinytext')
BINARY = DataType(29, 'binary')
VARBINARY = DataType(30, 'varbinary')
LONGBLOB = DataType(31, 'longblob')
MEDIUMBLOB = DataType(32, 'mediumblob')
BLOB = DataType(33, 'blob')
TINYBLOB = DataType(34, 'tinyblob')
JSON = DataType(35, 'json')
GEOGRAPHYPOINT = DataType(36, 'geographypoint')
GEOGRAPHY = DataType(37, 'geography')

_SCALAR_TYPES: Dict[Union[int, str], DataType] = {}
for k, v in list(globals().items()):
    if isinstance(v, DataType):
        _SCALAR_TYPES[v.name] = v
        _SCALAR_TYPES[v.id] = v

#
# Collection types
#
ARRAY = CollectionType(101, 'array')
RECORD = CollectionType(102, 'record')

_COLLECTION_TYPES: Dict[Union[int, str], CollectionType] = {}
for k, v in list(globals().items()):
    if isinstance(v, CollectionType):
        _COLLECTION_TYPES[v.name] = v
        _COLLECTION_TYPES[v.id] = v


def get_scalar_type(dtype: Union[int, str]) -> DataType:
    '''Return the DataType instance for the given name or id.'''
    try:
        return _SCALAR_TYPES[dtype]
    except KeyError:
        raise TypeError(f'no type found for name/id: {dtype}')


def get_collection_type(dtype: Union[int, str]) -> CollectionType:
    '''Return the CollectionType instance for the given name or id.'''
    try:
        return _COLLECTION_TYPES[dtype]
    except KeyError:
        raise TypeError(f'no type found for name/id: {dtype}')


def get_type(dtype: Union[int, str]) -> DataType:
    '''Return the DataType instance for the given name or id.'''
    try:
        return get_scalar_type(dtype)
    except TypeError:
        return get_collection_type(dtype)
