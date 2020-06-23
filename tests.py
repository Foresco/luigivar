# Тестирование компонентов задач
import unittest
from pyodbc import Connection as PyodbcConnection
from connections import Connection1

from task_classes.db.mssqldb import MSSqlTarget
from task_classes.csv_task_classes import PrepareCsvBulkPackages


class TestMSSqlTarget(unittest.TestCase):
    """Класс тестирования MSSqlTarget"""

    def setUp(self):
        """Проверка создания объекта"""
        self.ms_sql_target = MSSqlTarget(
            host=Connection1().host,
            user=Connection1().user,
            password=Connection1().password,
            database=Connection1().database,
            table=Connection1().table,
            update_id='test1'
        )

        self.assertIsInstance(self.ms_sql_target, MSSqlTarget, "Объект должен создаваться корректно")

    def test_01_connect(self):
        """Провека соединения с базой данных"""
        db_conn = self.ms_sql_target.connect()

        self.assertIsInstance(db_conn, PyodbcConnection, "Соединение с Connection1 должно быть успешным")

    def test_02_touch(self):
        """Проверка записи в таблицу сессий загрузки"""
        self.ms_sql_target.create_marker_table()
        self.ms_sql_target.touch()

        self.assertTrue(self.ms_sql_target.exists(), "Загрузка должа быть зарегистрирована и помечена как выполненная")


class TestCsv(unittest.TestCase):
    """Класс тестирования обработчиков csv-файлов"""

    def setUp(self):
        self.csv_task = PrepareCsvBulkPackages()

    def test_02_package(self):
        # print(self.csv_task.bulk_packages_directory)
        self.assertEqual(self.csv_task.bulk_packages_directory, r'D:\temp\data\packages',
                         'Каталог для Bulk-пакетов должен быть указан верно')

    def test_03_filename(self):
        print(self.csv_task.filename)
        self.assertEqual(self.csv_task.filename, r'D:\temp\data\packages\package.csv',
                         'Файл Bulk-пакетов должен быть указан верно')


if __name__ == '__main__':
    unittest.main(failfast=True)
