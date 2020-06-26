# Тестовый пакет задач
# Берет исходный csv-файл
# Готовит bulk-пакет
# Загружает пакет в таблицу
# Регистрирует загрузку
import os.path
import luigi
from luigi.util import requires

from task_classes.files_task_classes import CheckAnyFileInDirectory
from task_classes.csv_task_classes import PrepareCsvBulkPackages
from task_classes.db_task_classes import RegisterLoadSession, BulkLoad

from connections import Connection1  # Параметры соединения с базой данных

# Общие конфигруационные данные пакета задач
# TODO: Объединить имена каталогов в одну переменную
# TODO: Передавть соединение (MsSqlTarget) по цепочке задач
task_source_directory = 'aaa'  # Поддиректория хранения файлов исходных данных задач пакета
task_package_directory = 'aaa'  # Поддиректория хранения файлов данных задач пакета

task_package_table = 'test_table'  # Наполняемая таблица

parse_pattern = {
    'fld1': {
        'col_num': 0
    },
    'fld2': {
        'col_num': 1
    },
    'id': {
        'const': ''
    }
}

# Подключение дополнительных кофиграций (при необходимости)
# luigi.configuration.core.add_config_path(r'D:\work\Проекты\Предприятия\Алмазов\Python\luigivar\luigi.cfg')

# TODO: Вынести общие параметры в класс

# Проверка наличия исходного файла
class AaaCheckSourceFileExists(CheckAnyFileInDirectory):
    task_source_directory = task_source_directory
    ext = '*.csv'


# Предварительная регистрация загрузки исходного файла в marker-table
@requires(AaaCheckSourceFileExists)
class AaaRegisterLoadSession(RegisterLoadSession):
    target_table = task_package_table
    connection = Connection1  # Класс с настройками соединения с БД


# Генерация bulk-пакета
@requires(AaaRegisterLoadSession)
class AaaPrepareBulkPackages(PrepareCsvBulkPackages):
    target_table = task_package_table  # Имя таблицы, для которой будут готовиться пакеты
    task_package_directory = task_package_directory
    source_task = AaaCheckSourceFileExists  # Задача, выдающая исходный файл
    parse_pattern = parse_pattern  # Паттерн для парсинга строк

# Очистка таблицы-получателя от записей с текущим идентификатором сессии


# Загрузка bulk-пакета в базу данных
@requires(AaaPrepareBulkPackages)
class AaaBulkLoad(BulkLoad):
    target_table = task_package_table  # Имя таблицы, для которой будут готовиться пакеты
    task_package_directory = task_package_directory
    source_task = AaaRegisterLoadSession  # Задача для получения идентификатора массива

# Отметка в marker-table факта загрузки пакета

# Перемещение загруженного bulk-пакета в архив
# На основе наличия отметки о загрузке bulk-пакета

# Пееремещение загруженного исходного пакета в архив
# На основе наличия отметки о загрузке bulk-пакета


