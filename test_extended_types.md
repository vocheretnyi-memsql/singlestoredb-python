
Create table and populate it.
```
memsql [db]> create table t(a int, b bson, c json, d vector(3, i8));
Query OK, 0 rows affected (0.333 sec)

memsql [db]> insert into t values(1, '[]':>json, '{}', '[1,2,3]');
Query OK, 1 row affected (0.070 sec)

```


```
vocheretnyi-ua@vocheretnyi-ua-1:~/singlestoredb-python$ python
Python 3.9.2 (default, Feb 28 2021, 17:03:44) 
[GCC 10.2.1 20210110] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> 
>>> 
>>> import singlestoredb as s2
>>> conn = s2.connect('root:@localhost:3306/db')
>>> cur = conn.cursor()
>>> cur.execute('set @@enable_extended_types_metadata = 1')
0
>>> cur.execute('select * from t')
Len:  12
Len:  13
Extended type code:  1
its BSON
Len:  12
Len:  18
Extended type code:  2
its VECTOR 3 3
1
>>> print(cur.description)
(Description(name='a', type_code=3, display_size=None, internal_size=11, precision=11, scale=0, null_ok=True, flags=0, charset=63), Description(name='b', type_code=251, display_size=None, internal_size=4294967295, precision=4294967295, scale=0, null_ok=True, flags=144, charset=63), Description(name='c', type_code=245, display_size=None, internal_size=4294967295, precision=4294967295, scale=0, null_ok=True, flags=16, charset=33), Description(name='d', type_code=253, display_size=None, internal_size=4294967295, precision=4294967295, scale=0, null_ok=True, flags=16, charset=45))
>>> for item in cur:
...     print(item)
... 
(1, b'\x05\x00\x00\x00\x00\x04', {}, '[1,2,3]')
>>> 

```
