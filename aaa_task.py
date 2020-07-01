# Тестовый пакет задач
# Берет исходный csv-файл
# Готовит bulk-пакет
# Загружает пакет в таблицу
# Регистрирует загрузку
import os.path
import luigi
from luigi.util import requires

from task_classes.files_task_classes import CheckAnyFileInDirectory, MoveFileFromDirectory
from task_classes.csv_task_classes import PrepareCsvBulkPackages
from task_classes.db_task_classes import RegisterLoadSession, BulkLoad, CompleteLoadSession, ClearTargetTable

from connections import Connection1  # Параметры соединения с базой данных

# Общие конфигруационные данные пакета задач
# TODO: Объединить имена каталогов в одну переменную
# TODO: Передавть соединение (MsSqlTarget) по цепочке задач
task_sub_directory = 'aaa'  # Поддиректория хранения файлов данных задач пакета
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


class AaaCheckSourceFileExists(CheckAnyFileInDirectory):
    """Проверка наличия исходного файла для загрузки"""
    directory = os.path.join(luigi.configuration.get_config().get('directories', 'source_files', ''),
                             task_sub_directory)
    ext = '*.csv'


@requires(AaaCheckSourceFileExists)
class AaaRegisterLoadSession(RegisterLoadSession):
    """Предварительная регистрация загрузки исходного файла в marker-table"""
    target_table = task_package_table
    # connection = Connection1  # Класс с настройками соединения с БД


# Генерация bulk-пакета
@requires(AaaRegisterLoadSession)
class AaaPrepareBulkPackages(PrepareCsvBulkPackages):
    # Общие каталоги для обрабатываемых данных
    bulk_package_name = os.path.join(luigi.configuration.get_config().get('directories', 'bulk_packages', 'bulk_packages'),
                                     task_sub_directory, 'package.csv') # Полное имя формируемого пакета
    pattern_name = os.path.join(luigi.configuration.get_config().get('directories', 'bulk_patterns', 'bulk_patterns'),
                                task_package_table + '.xml')

    data_task = AaaCheckSourceFileExists  # Задача, выдающая исходный файл для загрузки
    parse_pattern = parse_pattern  # Паттерн для парсинга строк


@requires(AaaPrepareBulkPackages)
class AaaClearTargetTable(ClearTargetTable):
    """Очистка таблицы-получателя от записей с текущим идентификатором сессии"""
    target_table = task_package_table  # Имя таблицы, которая будет очищаться
    reg_task = AaaRegisterLoadSession  # Задача для получения имени загружаемого файла (для target_id)


# Загрузка bulk-пакета в базу данных
@requires(AaaPrepareBulkPackages, AaaClearTargetTable)
class AaaBulkLoad(BulkLoad):
    target_table = task_package_table  # Имя таблицы, для которой будут готовиться пакеты
    reg_task = AaaRegisterLoadSession  # Задача для получения имени загружаемого файла (для target_id)
    bulk_task = AaaPrepareBulkPackages  # Задача, готовящая bulk-пакет


@requires(AaaBulkLoad)
class AaaCompleteLoadSession(CompleteLoadSession):
    """Отметка в marker-table факта загрузки пакета"""
    target_table = task_package_table
    reg_task = AaaRegisterLoadSession  # Задача для получения имени загружаемого файла (для target_id)
    # connection = Connection1  # Класс с настройками соединения с БД


@requires(AaaCompleteLoadSession)
class AaaMoveBulkPackage(MoveFileFromDirectory):
    """Перемещение загруженного bulk-пакета в архив"""
    target_directory = os.path.join(luigi.configuration.get_config().get('directories', 'bulk_packages_loaded', ''),
                                    task_sub_directory)
    src_task = AaaPrepareBulkPackages  # Задача для получения преремещаемого файла


# Пееремещение загруженного исходного пакета в архив
@requires(AaaCompleteLoadSession, AaaMoveBulkPackage)
class AaaMoveSourceFile(MoveFileFromDirectory):
    target_directory = os.path.join(luigi.configuration.get_config().get('directories', 'source_files_loaded', ''),
                                    task_sub_directory)
    src_task = AaaCheckSourceFileExists # Задача для получения преремещаемого файла
