# Классы задач обработки csv-файлов

import os.path
import csv

import luigi

from .utils import MsSqlBulkParser, get_first_filename


class PrepareCsvBulkPackages(luigi.Task):
    """Формирует набор файлов для последующей загрузки через BULK"""

    # Свойства, переопределяемые в дочерних классах
    bulk_package_name = ''  # Полное имя файла-пакета
    pattern_name = ''  # Полное имя шаблона для bulk-пакета
    session_id = 0  # Идентификатор загрузочной сессии
    parse_pattern = dict()
    data_task = luigi.Task

    def prepare_row(self, data_row):
        """Подготовка строки csv-файла на основе шаблона и строки данных"""
        bulk_parser = MsSqlBulkParser(self.pattern_name)
        row_columns = bulk_parser.get_columns()
        row = list()
        for column in row_columns:
            column_name = column['@NAME']
            # row.append('aaa')
            # continue
            if column_name in self.parse_pattern:
                if 'col_num' in self.parse_pattern[column_name]:
                    # print(parse_pattern[column_name]['col_num'])
                    row.append(data_row[self.parse_pattern[column_name]['col_num']])
                elif 'const' in self.parse_pattern[column_name]:
                    row.append(self.parse_pattern[column_name]['const'])
            elif column_name == 'session_id':
                row.append(self.session_id)
            # TODO: Сделать обработку отсутствия
        return row

    def run(self):
        self.session_id = self.input().get_session_id()
        # TODO: Заменить open(self.output().path, 'w', newline='') на self.output().open('w') после исправления ошибки
        with self.data_task().output().open() as input_channel, open(self.output().path, 'w', newline='') as output:
            reader = csv.reader(input_channel, delimiter='\t')
            for i in range(0, 1):
                next(reader)  # Пропускаем заголовок или больше, если указано
            writer = csv.writer(output, delimiter='\t')
            for row in reader:
                writer.writerow(self.prepare_row(row))

    def output(self):
        return luigi.LocalTarget(self.bulk_package_name)
