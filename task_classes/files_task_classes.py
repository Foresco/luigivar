# Классы задач манипуляций с файлами
import os.path
import luigi

from .utils import get_first_filename


class CheckAnyFileInDirectory(luigi.Task):
    """Проверяет наличие файла в исходной директории"""
    directory = luigi.configuration.get_config().get('directories', 'source_files', '')

    # Свойства, переопределяемые в дочерних классах
    task_source_directory = ''
    ext = '*.csv'

    @property
    def sourcedirectory(self):
        # Получаем путь к каталогу для импорта
        return os.path.join(self.directory, self.task_source_directory)

    @property
    def filename(self):
        # Получаем первый файл список файлов в каталоге для импорта
        return get_first_filename(self.sourcedirectory, self.ext)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.sourcedirectory, self.filename))