# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import logging
# import re
import tempfile

import luigi
from luigi.contrib import rdbms

from luigi.contrib.postgres import default_escape

from luigi import six

logger = logging.getLogger('luigi-interface')

try:
    import pyodbc
except ImportError:
    logger.warning("Loading MSSQL module without the python package pyodbc. \
        This will crash at runtime if SQL Server functionality is used.")


class MSSqlTarget(luigi.Target):
    """
    Target for a resource in Microsoft SQL Server.
    This module is primarily derived from mysqldb.py.  Much of MSSqlTarget,
    MySqlTarget and PostgresTarget are similar enough to potentially add a
    RDBMSTarget abstract base class to rdbms.py that these classes could be
    derived from.
    """

    marker_table = luigi.configuration.get_config().get('mssql', 'marker-table', 'table_updates')
    driver = luigi.configuration.get_config().get('mssql', 'driver', '{ODBC Driver 11 for SQL Server}')

    def __init__(self, host, database, user, password, table, update_id):
        """
        Initializes a MsSqlTarget instance.

        :param host: MsSql server address. Possibly a host:port string.
        :type host: str
        :param database: database name.
        :type database: str
        :param user: database user
        :type user: str
        :param password: password for specified user.
        :type password: str
        :param update_id: an identifier for this data set.
        :type update_id: str
        """
        if ':' in host:
            self.host, self.port = host.split(':')
            self.port = int(self.port)
        else:
            self.host = host
            self.port = 1433
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def start(self, connection=None):
        """
        Start update by inserting row in marker table.

        IMPORTANT, If the marker table doesn't exist,
        the connection transaction will be aborted and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()  # TODO: Is it necessary every time?

        if connection is None:
            connection = self.connect()

        if not self.exists(connection):
            cursor = connection.cursor()
            cursor.execute('INSERT INTO {marker_table}(update_id, target_table) VALUES(?, ?);'.format(
                marker_table=self.marker_table),
                (self.update_id, self.target_table))

            connection.commit()

    def touch(self, connection=None):
        """
        Mark this update as complete by inserting datetime in field completed

        If appropriate row in marker table doesn't exist,
        start row will be created by start method
        """
        self.create_marker_table()  # TODO: Is it necessary every time?

        if connection is None:
            connection = self.connect()

        # Create row
        self.start(connection)

        cursor = connection.cursor()
        cursor.execute('UPDATE {marker_table} SET completed = GETDATE() WHERE update_id = ?;'.format(
            marker_table=self.marker_table),
            (self.update_id, ))
        connection.commit()

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            row = cursor.execute('SELECT 1 FROM {marker_table} WHERE update_id = ?;'.format(
                marker_table=self.marker_table),
                (self.update_id,)).fetchone()
        except pyodbc.DatabaseError as e:
            print(e.args[1])
            raise

        return row is not None

    def connect(self):
        """
        Create a SQL Server connection and return a connection object
        """
        connection_string = 'DRIVER={Driver};SERVER={Server};Database={Database};Uid={User};Pwd={Password};'.format(
            Driver=self.driver,
            Server=self.host,
            Database=self.database,
            User=self.user,
            Password=self.password
        )

        connection = pyodbc.connect(connection_string)

        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Use a separate connection since the transaction might have to be reset.
        """
        # Test if table already exists
        connection = self.connect()
        cursor = connection.cursor()
        # TODO: Try to do in SQLAlchemy
        if not cursor.tables(table=self.marker_table, tableType='TABLE').fetchone():
            print('create')
            cursor.execute(
                """ CREATE TABLE {marker_table} (
                        id            BIGINT    IDENTITY,
                        update_id     VARCHAR(128) NOT NULL,
                        target_table  VARCHAR(128) DEFAULT NULL,
                        inserted      DATETIME DEFAULT(GETDATE()),
                        completed     DATETIME DEFAULT NULL,
                        PRIMARY KEY (update_id)
                    )
                """
                .format(marker_table=self.marker_table)
            )
            connection.commit()

        connection.close()


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into MS SQL

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.
    """

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def map_column(self, value):
        """
        Applied to each column of every row returned by `rows`.

        Default behaviour is to escape special characters and identify any self.null_values.
        """
        if value in self.null_values:
            return r'\\N'
        else:
            return default_escape(six.text_type(value))

# everything below will rarely have to be overridden

    def output(self):
        """
        Returns a MSSqlTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return MSSqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )

    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)'
                            % (self.columns[0],))
        cursor.copy_from(file, self.table, null=r'\\N', sep=self.column_separator, columns=column_names)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        # transform all data generated by rows() using map_column and write data
        # to a temporary file for import using postgres COPY
        tmp_dir = luigi.configuration.get_config().get('postgres', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
            n += 1
            if n % 100000 == 0:
                logger.info("Wrote %d lines", n)
            rowstr = self.column_separator.join(self.map_column(val) for val in row)
            rowstr += "\n"
            tmp_file.write(rowstr.encode('utf-8'))

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into MS SQL
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in range(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
                self.post_copy(connection)
                if self.enable_metadata_columns:
                    self.post_copy_metacolumns(cursor)
            except pyodbc.ProgrammingError as e:
                if e.pgcode == pyodbc.errorcodes.UNDEFINED_TABLE and attempt == 0:
                    # if first attempt fails with "relation not found", try creating table
                    logger.info("Creating table %s", self.table)
                    connection.reset()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
        tmp_file.close()


class MsSqlQuery(rdbms.Query):
    """
    Template task for querying a MS SQL compatible database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.
    Optionally one can override the `autocommit` attribute to put the connection for the query in autocommit mode.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """
    def run(self):
        connection = self.output().connect()
        connection.autocommit = self.autocommit
        cursor = connection.cursor()
        sql = self.query

        logger.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()

    def output(self):
        """
        Returns a MSSQLTarget representing the executed query.

        Normally you don't override this.
        """
        return MSSqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )
