# Классы взаимодействия с базой данных
import luigi

from .db.mssqltargets import TargetLoadingRegister, TargetBulkLoad, TargetLoadingComplete, TargetClear

from connections import Connection1  # Параметры соединения с базой данных (Connection1 - параметры по умолчанию)


class RegisterLoadSession(luigi.Task):
    """Класс регистрации сессии загрузки"""

    # Свойства, переопределяемые в дочерних классах
    connection = Connection1  # В дочерних классах можно не переопределять
    target_table = ''  # Таблица-получатель данных

    def __init__(self):
        self.target = TargetLoadingRegister(
            host=self.connection().host,
            database=self. connection().database,
            user=self. connection().user,
            password=self. connection().password,
            table=self.target_table,
            update_id=self.update_id
        )
        super().__init__()

    def run(self):
        self.target.touch()

    @property
    def update_id(self):
        # Получаем путь к исходному файлу с данными
        return self.input().path

    def output(self):
        return self.target


class ClearTargetTable(luigi.Task):
    """Класс очистки таблицы получателя"""
    connection = Connection1  # В дочерних классах соединение можно переопределять
    reg_task = luigi.Task  # Задача, выдающая исходный файл для загрузки
    target_table = ''  # Имя таблицы-получателя (определяется в дочернем классе)

    def __init__(self):
        # Соединение с базой данных
        self.target = TargetClear(
            host=self.connection().host,
            database=self.connection().database,
            user=self.connection().user,
            password=self.connection().password,
            table=self.target_table,
            update_id=self.update_id
        )
        super().__init__()
        self.target.set_session_id()

    def run(self):
        self.target.clear(self.target_table)

    @property
    def update_id(self):
        # Получаем данное свойство от исходной задачи
        return self.reg_task().update_id

    def output(self):
        return self.target


class BulkLoad(luigi.Task):
    """Класс BULK-загрузки"""
    connection = Connection1  # В дочерних классах соединение можно переопределять
    reg_task = luigi.Task  # Задача, выдающая исходный файл для загрузки
    bulk_task = luigi.Task  # Задача, выдающая bulk-пакет для загрузки
    target_table = ''  # Имя таблицы-получателя (определяется в дочернем классе)

    def __init__(self):
        # Соединение с базой данных
        self.target = TargetBulkLoad(
            host=self.connection().host,
            database=self.connection().database,
            user=self.connection().user,
            password=self.connection().password,
            table=self.target_table,
            update_id=self.update_id
        )
        self.rowcount = 0
        super().__init__()
        self.target.set_session_id()
        self.target.set_bulk_package_path(self.bulk_package_path)

    def run(self):
        self.rowcount = self.target.bulk_load()

    @property
    def update_id(self):
        # Получаем данное свойство от исходной задачи
        return self.reg_task().update_id

    @property
    def bulk_package_path(self):
        # Получаем путь к файлу с bulk-пакетом
        return self.bulk_task().output().path

    def output(self):
        return self.target


class CompleteLoadSession(luigi.Task):
    """Класс регистрации сессии загрузки"""
    connection = Connection1  # В дочерних классах соединение можно переопределять
    reg_task = luigi.Task  # Задача, выдающая исходный файл для загрузки
    target_table = ''  # Имя таблицы-получателя (определяется в дочернем классе)

    def __init__(self):
        # Соединение с базой данных
        self.target = TargetLoadingComplete(
            host=self.connection().host,
            database=self.connection().database,
            user=self.connection().user,
            password=self.connection().password,
            table=self.target_table,
            update_id=self.update_id
        )
        super().__init__()

    def run(self):
        self.target.complete()

    @property
    def update_id(self):
        # Получаем путь к исходному файлу с данными
        return self.reg_task().update_id

    def output(self):
        return self.target
