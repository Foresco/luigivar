# Классы взаимодействия с базой данных
import luigi

from .db.mssqldb import MSSqlTarget

from connections import Connection1  # Параметры соединения с базой данных


class RegisterLoadSession(luigi.Task):
    """Класс регистрации сессии загрузки"""
    connection = Connection1  # В дочерних классах соединение можно переопределять

    host = connection().host
    database = connection().database
    user = connection().user
    password = connection().password
    
    def __init__(self):
        # Соединение с базой данных
        self.target = MSSqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.target_table,
            update_id=self.update_id
        )
        super().__init__()

    def run(self):
        self.target.start()

    @property
    def update_id(self):
        # Получаем путь к исходному файлу с данными
        return self.input().path

    # @property
    # def source_file(self):
    #     # Получаем путь к исходному файлу с данными
    #     return self.input

    def output(self):
        return self.target


class BulkLoad(luigi.Task):
    """Класс BULK-загрузки"""
    connection = Connection1  # В дочерних классах соединение можно переопределять
    source_task = luigi.Task  # Задача, выдающая исходный bulk-пакет для загрузки

    host = connection().host
    database = connection().database
    user = connection().user
    password = connection().password

    def __init__(self):
        # Соединение с базой данных
        self.target = MSSqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.target_table,
            update_id=self.update_id
        )
        self.rowcount = 0
        super().__init__()

    def run(self):
        self.rowcount = self.target.bulk_load()

    @property
    def update_id(self):
        # Получаем данное свойство от исходной задачи
        return self.source_task.update_id

    @property
    def bulk_package_path(self):
        # Получаем путь к файлу с bulk-пакетом
        return self.input().path

    def output(self):
        return self.target
