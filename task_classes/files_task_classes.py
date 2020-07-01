# Классы задач манипуляций с файлами
import os.path
import luigi

from .utils import get_first_filename


class LocalTargetEmpty(luigi.LocalTarget):
    """Класс, отображающий результат как отсутсвие файла"""

    def exists(self):
        """Инвертируем наличие файла"""
        return not super().exists()


class CheckAnyFileInDirectory(luigi.Task):
    """Проверяет наличие файла в исходной директории"""

    # Свойства, переопределяемые в дочерних классах
    directory = ''  # Каталог для поиска
    ext = '*.csv'  # Расширение для фильтрации

    @property
    def filename(self):
        # Получаем первый файл список файлов в каталоге для импорта
        return get_first_filename(self.directory, self.ext)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.filename))


class MoveFileFromDirectory(luigi.Task):
    """Перемещает файл из текущего каталога в указанный"""
    # Свойства, переопределяемые в дочерних классах
    target_directory = ''
    src_task = luigi.Task  # Задача, выдающая исходный файл для перемещения

    def __init__(self):
        # Инициализируем, так как потом файл (source_path) будет перемещен
        self.source_path = self.src_task().output().path
        super().__init__()

    def run(self):
        moving_file = os.path.basename(self.source_path)
        target_path = os.path.join(self.target_directory, moving_file)
        self.output().move(new_path=target_path)

    def output(self):
        return LocalTargetEmpty(self.source_path)
