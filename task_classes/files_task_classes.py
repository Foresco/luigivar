# Классы задач манипуляций с файлами
import os.path
import luigi

from .utils import get_first_filename


class CheckAnyFileInDirectory(luigi.Task):
    """Проверяет наличие файла в исходной директории"""
    directory = ''
    ext = '*.csv'

    @property
    def filename(self):
        # Получаем первый файл список файлов в каталоге для импорта
        return get_first_filename(self.directory, self.ext)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.filename))