# Варианты Target для разных целей
from pyodbc import DatabaseError

from .mssqldb import MSSqlTarget


class TargetLoadingRegister(MSSqlTarget):
    """ Регистрация загрузочной сессии"""
    pass # Пока использует базовые методы


class TargetClear(MSSqlTarget):
    """Очистка таблицы-получателя"""
    def clear(self, connection=None):
        """
        Delete from target table all rows of session_id specified.
        """
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            str1 = "DELETE FROM {table} WHERE (session_id = ?);".format(
                 table=self.table, )
            row_count = cursor.execute(str1, (self.session_id, )).rowcount
            connection.commit()
        except DatabaseError as e:
            print(e.args[1])
            raise

        return row_count

    def exists(self, connection=None):
        """Выдает True в случае отстствия записей"""
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            row = cursor.execute("SELECT count(*) FROM {table} WHERE (session_id = ?);".format(
                table=self.table),
                (self.session_id,)).fetchone()
        except DatabaseError as e:
            print(e.args[1])
            raise

        return row[0] == 0


class TargetBulkLoad(MSSqlTarget):
    """BULK-загрузка данных"""
    bulk_package_path = ''  # Полное имя bulk-пакета

    # def set_target(self, target_table):
    #     """Установка имени таблицы-получателя"""
    #     # TODO: Сделать поизящнее, например при инициализации (Посмотреть, что в CopyTable)
    #     self.target_table = target_table

    def set_bulk_package_path(self, bulk_package_path):
        """Указание пути расположения файла пакета"""
        self.bulk_package_path = bulk_package_path

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            row = cursor.execute('SELECT count(*) FROM {table} WHERE session_id = ?;'.format(
                table=self.table),
                (self.session_id,)).fetchone()
        except DatabaseError as e:
            print(e.args[1])
            raise
        # TODO: Сверять с исходным количеством строк из файла
        return row is not None and row[0]>0

    def bulk_load(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            str1 = "BULK INSERT {table} FROM '{csv_file}' WITH (CODEPAGE = 'RAW');".format(
                 table=self.table, csv_file=self.bulk_package_path)
            row_count = cursor.execute(str1).rowcount
            connection.commit()
        except DatabaseError as e:
            print(e.args[1])
            raise

        return row_count


class TargetLoadingComplete(MSSqlTarget):
    """Отметка успешного завершения загрузки"""

    def complete(self, connection=None):
        """
        Complete loading by inserting complete date-time in marker table.

        IMPORTANT, If the marker table doesn't exist,
        the connection transaction will be aborted and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()  # TODO: Is it necessary every time?

        if connection is None:
            connection = self.connect()

        if not self.exists(connection):
            cursor = connection.cursor()
            cursor.execute("""
            UPDATE {marker_table} SET completed = GETDATE() 
            WHERE (update_id = ?) AND (target_table = ?);""".format(
                marker_table=self.marker_table),
                (self.update_id, self.table))

            connection.commit()

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()

        try:
            row = cursor.execute("""
            SELECT completed FROM {marker_table} 
            WHERE (update_id = ?) AND (target_table = ?);""".format(
                marker_table=self.marker_table),
                (self.update_id, self.table)).fetchone()
        except DatabaseError as e:
            print(e.args[1])
            raise

        if row is None or row[0] is None:
            # Если строки нет или она пустая
            return False
        return True
