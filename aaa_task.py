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
from task_classes.db_task_classes import RegisterLoadSession

from connections import Connection1  # Параметры соединения с базой данных

# Общие конфигруационные данные пакета задач
task_source_directory = 'aaa'  # Поддиректория хранения файлов исходных данных задач пакета
task_package_directory = 'aaa'  # Поддиректория хранения файлов данных задач пакета
task_table = 'test_table'  # Наполняемая таблица

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


# Проверка наличия исходного файла
class AaaCheckSourceFileExists(CheckAnyFileInDirectory):
    directory = luigi.configuration.get_config().get('directories', 'source_files', '')
    directory = os.path.join(directory, task_package_directory)
    ext = '*.csv'


# Предварительная регистрация загрузки исходного файла в marker-table
@requires(AaaCheckSourceFileExists)
class AaaRegisterLoadSession(RegisterLoadSession):
    update_id = AaaCheckSourceFileExists().output().path
    target_table = task_table
    connection = Connection1


# Генерация bulk-пакета
@requires(AaaRegisterLoadSession)
class AaaPrepareBulkPackages(PrepareCsvBulkPackages):
    target_table = task_table  # Имя таблицы, для которой будут готовиться пакеты

    # Получение идентификатора сессии загрузки
    # row = AaaRegisterLoadSession()
    # session_id = row['id']

# Загрузка bulk-пакета в базу данных
# Отметка в marker-table факта загрузки пакета

# Перемещение загруженного bulk-пакета в архив
# На основе наличия отметки о загрузке bulk-пакета

# Пееремещение загруженного исходного пакета в архив
# На основе наличия отметки о загрузке bulk-пакета


