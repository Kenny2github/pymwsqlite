"""
A MediaWiki-style [1]_ wrapper around Python's SQLite3 [2]_ library.

`Example Usage`_
================

.. code-block:: python

    import pymwsqlite
    dbw = pymwsqlite.PyMWSQLite('example.db')
    dbw.create_table('example', {
        'column1': 'INTEGER PRIMARY KEY',
        'column2': 'TEXT',
    })
    dbw.insertmany('example', (
        {'column1': 1, 'column2': 'Hello, World!'},
        {'column1': 2, 'column2': 'Hello, Computer!'}
    ))
    for row in dbw.select('example', ('column1', 'column2')):
        print(*row)

`Expected Output`_
------------------

.. code-block:: bash

    1 Hello, World!
    2 Hello, Computer!

.. [1] https://doc.wikimedia.org/mediawiki-core/master/php/classWikimedia_1_1Rdbms_1_1Database.html
.. [2] https://docs.python.org/3/library/sqlite3.html
"""
from __future__ import annotations
import sqlite3 as sql
from functools import wraps
from typing import Union, Iterable, Mapping, Tuple, Optional, List, Any

def _check_open(func):
    @wraps(func)
    def newfunc(self, *args, **kwargs):
        if self.conn is None:
            raise ValueError('Cannot operate on an unopened database.')
        return func(self, *args, **kwargs)
    return newfunc

Columns = Union[str, Iterable[str], Mapping[str, str]]
Conditions = Union[str, Iterable[Tuple[str, str, str]], None]
Parameters = Optional[Mapping[str, str]]

class PyMWSQLite:
    """The class representing database methods.

    Use as shown in module docs.
    """
    conn: sql.Connection = None
    cursor: sql.Cursor = None

    def __init__(
            self,
            name_or_conn: Union[str, sql.Connection] = None,
            read_only: bool = False
    ):
        """Initialize the database.

        ``name_or_conn`` is one of:
        1. The filename (not URI or anything else SQLite accepts!) to
           open a connection to
        2. An already-created SQLite Connection object to use as the
           internal Connection object.

        If ``read_only`` is True and ``name_or_conn`` is a filename,
            the internal Connection is opened as a file: URI with the
            query parameter ?mode=ro to open it in read-only mode.
        """
        if isinstance(name_or_conn, str):
            if read_only:
                self.open('file:{}?mode=ro'.format(name_or_conn), uri=True)
            else:
                self.open(name_or_conn)
        else:
            self.conn = name_or_conn

    def open(self, *sql_args, close_existing: bool = False, **sql_kwargs):
        """Open a connection. Only to be used when the object was
        not initialized with a connection, or when a previous
        connection should be closed.

        Accepts the same parameters as sqlite3.connect, with the
        exception of the extra ``close_existing`` keyword argument.
        When this argument is True, any previous connection will be
            committed and closed before being discarded.
        When False, if there is a preexisting connection this method
            will raise a ValueError.
        """
        if self.conn is not None:
            if close_existing:
                self.close()
            else:
                raise ValueError('Attempt to open new connection when '
                                 'existing one is not yet closed.')
        self.conn = sql.connect(*sql_args, **sql_kwargs)
        self.conn.row_factory = sql.Row
        self.cursor = self.conn.cursor()

    def close(self):
        """Close the connection. Further operations on the database
        will raise an exception.
        """
        self.conn.commit()
        self.conn.close()
        self.conn = self.cursor = None

    def __enter__(self):
        """Use the database as a context manager."""
        return self

    def __exit__(self, *_):
        """Exit the context.
        Note: This does NOT close the database, only commits it!
        """
        self.conn.commit()

    @_check_open
    def _select(
            self,
            table: str,
            cols: Columns,
            conds: Conditions,
            opts: Optional[str],
            params: Parameters
    ) -> None:
        query = 'SELECT {} FROM {}'
        if isinstance(cols, Mapping):
            cols = ', '.join((
                '{} AS {}'.format(name, alias)
                if alias else name
            ) for name, alias in cols.items())
        elif isinstance(cols, str):
            pass
        else:
            cols = ', '.join(cols)
        query = query.format(cols, table)
        params = (params or {}).copy()
        if isinstance(conds, str):
            query += ' WHERE ' + conds
        elif conds is not None:
            query += ' WHERE ' + ' AND '.join(
                '{0}{1}:{0}'.format(i[0], i[1])
                for i in conds
            )
            params.update({i[0]: i[2] for i in conds})
        if opts:
            query += ' ' + opts
        self.cursor.execute(query, params)

    def select(
            self,
            table: str,
            columns: Columns,
            conditions: Conditions = None,
            options: Optional[str] = None,
            params: Parameters = None
    ) -> Iterable[sql.Row]:
        """Select from a table. Returns an iterator over result rows.
        Note: modifying the database while iterating over result rows
        may result in undefined behavior.

        ``table`` is a directly substituted table name.
        ``columns`` can be either a SQL-valid string list of columns
            to directly substitute into the query, (usually used for
            the * magic column), an iterable of column names to
            select from, or a mapping of column names to column
            aliases (where None means no alias).
        ``conditions`` can be either a SQL-valid string (minus WHERE)
            or an ANDed iterable of 3-tuples:
            (column name, operator, value)
            to check.
        ``options`` is a SQL-valid string of modifiers to the SELECT
            statement.
        When using strings in any previous function parameters,
            specify SQL parameters in :named style. The ``param``
            parameter is a mapping of :names to values for any
            parameters whose values are not specified in the SQL
            strings.
        """
        self._select(table, columns, conditions, options, params)
        return iter(self.cursor)

    def selectall(
            self,
            table: str,
            columns: Columns,
            conditions: Conditions = None,
            options: Optional[str] = None,
            params: Parameters = None
    ) -> List[sql.Row]:
        """Select from a table. Returns a list of result rows.
        Parameters are the same as for ``select``.
        """
        return list(self.select(table, columns, conditions, options, params))

    def selectone(
            self,
            table: str,
            columns: Columns,
            conditions: Conditions = None,
            options: Optional[str] = None,
            params: Parameters = None
    ) -> Optional[sql.Row]:
        """Selects one row from a table. Returns the result row or
        None if no results. Parameters are the same as ``select``.
        """
        self._select(table, columns, conditions, options, params)
        return self.cursor.fetchone()

    @_check_open
    def fetchone(self) -> Optional[sql.Row]:
        """If a previous ``selectone`` selected more than one row,
        this returns the next row. Otherwise, or if there were no
        results, returns None.
        """
        return self.cursor.fetchone()

    #pylint: disable=no-self-use
    @_check_open
    def _insert(
            self,
            table: str,
            columns: Iterable[str]
    ) -> str:
        values = ', '.join(':' + i for i in columns)
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"
    #pylint: enable=no-self-use

    def insert(
            self,
            table: str,
            rows: Mapping[str, Any]
    ) -> PyMWSQLite:
        """Insert a row into a table.

        ``table`` is a directly substituted table name.
        ``rows`` is a mapping (e.g. a dict) of column names
            (substituted directly) to column values.
        """
        query = self._insert(table, rows.keys())
        self.cursor.execute(query, rows)
        return self

    def insertmany(
            self,
            table: str,
            rows: Iterable[Mapping[str, Any]]
    ) -> PyMWSQLite:
        """Insert multiple rows into a table.

        ``table`` is a directly substituted table name.
        ``rows`` is an iterable of mappings valid for insert().
            Specifying mappings with different columns between
            different rows *will* break.
        """
        query = self._insert(table, rows[0].keys())
        self.cursor.executemany(query, rows)
        return self

    #pylint: disable=no-self-use
    @_check_open
    def _update(
            self,
            table: str,
            columns: Iterable[Union[str, Iterable[str]]],
            conditions: Conditions
    ) -> str:
        sets = ('{0}=:{0}'.format(i) for i in columns)
        if isinstance(conditions, str):
            return f"UPDATE {table} SET {', '.join(sets)} WHERE {conditions}"
        if conditions is not None:
            query = f"UPDATE {table} SET {', '.join(sets)} WHERE "
            query += ' AND '.join(
                '{0}{1}:{0}'.format(i[0], i[1])
                for i in conditions
            )
            return query
        return f"UPDATE {table} SET {', '.join(sets)}"
    #pylint: enable=no-self-use

    def update(
            self,
            table: str,
            values: Mapping[str, Any],
            conditions: Conditions = None
    ) -> PyMWSQLite:
        """Update a table with new values.

        ``table`` is a directly substituted table name.
        ``values`` is a mapping of column names (or tuples of column
            names if one value applies to multiple columns) to set
            to values to set them to.
        ``conditions`` is a Conditions value, same as select()'s.
        """
        query = self._update(table, values.keys(), conditions)
        self.cursor.execute(query, {
            **values, **{
                i[0]: i[2] for i in (conditions or ())
            }
        })
        return self

    def updatemany(
            self,
            table: str,
            values: Iterable[Mapping[str, Any]],
            conditions: Conditions = None
    ) -> PyMWSQLite:
        """Update a table with new values, many times.
        Parameters are the same as update().
        Specifying different columns in different mappings
        *will* break things.
        """
        query = self._update(table, values.keys(), conditions)
        condvals = {i[0]: i[2] for i in (conditions or ())}
        self.cursor.executemany(query, ({**val, **condvals} for val in values))
        return self

    @_check_open
    def create_table(
            self,
            table_name: str,
            columns: Columns,
    ) -> PyMWSQLite:
        """Create a table.

        ``table_name`` is the name of the table to create.
        ``columns`` is either a SQL-valid string for column
            declarations, an iterable of column declarations,
            or a mapping of column names to declarations.
        """
        query = 'CREATE TABLE {}({})'
        if isinstance(columns, Mapping):
            columns = ', '.join('{} {}'.format(*i) for i in columns.items())
        elif isinstance(columns, str):
            pass
        else:
            columns = ', '.join(columns)
        query = query.format(table_name, columns)
        self.cursor.execute(query)
        return self

    @_check_open
    def delete(
            self,
            table: str,
            conditions: Conditions = None
    ) -> PyMWSQLite:
        """Delete from a table.

        ``table`` is a directly substituted table name.
        ``conditions`` is a Conditions value, same as select()'s.
        """
        query = 'DELETE FROM ' + table
        params = {}
        if isinstance(conditions, str):
            query += ' WHERE ' + conditions
        elif conditions is not None:
            query += ' WHERE ' + ' AND '.join(
                '{0}{1}:{0}'.format(i[0], i[1])
                for i in conditions
            )
            params = {i[0]: i[2] for i in conditions}
        self.cursor.execute(query, params)
        return self

    @_check_open
    def drop_table(self, table_name: str) -> PyMWSQLite:
        """Delete an entire table.

        ``table_name`` is the name of the table to delete.
        """
        self.cursor.execute('DROP TABLE ' + table_name)
        return self
