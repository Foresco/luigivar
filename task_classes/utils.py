# Утилиты для работы с xml-файлами
import os.path
import xmltodict
import json
from fnmatch import filter as ext_filter


def get_first_filename(directory: str, ext: str) -> str:
    """Получение имени первого файла с указанным расширением из директории"""
    files_list = ext_filter(os.listdir(directory), ext)
    if files_list:
        return files_list[0]  # Возвращаем первый файл в каталоге
    return ''


class MsSqlBulkParser:
    """Класс разбора FORMATFILE xml для bulk-загрузки MS SQL
    файл должен быть в кодировке UTF-8"""

    def __init__(self, file_name):
        with open(file_name, 'rb') as fd:
            self.content = xmltodict.parse(fd.read())

    def get_columns(self):
        return self.content['BCPFORMAT']['ROW']['COLUMN']

    # Вспомогательный функционал

    def prepare_pattern_json(self):
        pattern = dict()
        columns = self.get_columns()
        for column in columns:
            pattern[column['@NAME']] = dict(
                col_num=column['@SOURCE']-1,
                transformer='set_null'
            )

        with open('pattern.json', 'w', encoding='utf-8') as json_file:
            json.dump(pattern, json_file)


class CsvPatternParser:
    """Класс разбора json-файла с настройками подготовки данных"""

    def __init__(self, file_name):
        with open(file_name, encoding='utf-8') as json_file:
            self.content = json.load(json_file)