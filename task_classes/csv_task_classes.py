# Классы задач обработки csv-файлов

import os.path
import csv

import luigi

from .utils import MsSqlBulkParser, get_first_filename


class PrepareCsvBulkPackages(luigi.Task):
    """Формирует набор файлов для последующей загрузки через BULK"""
    # Общие каталоги для обрабатываемых данных
    bulk_packages_directory = luigi.configuration.get_config().get('directories', 'bulk_packages', 'bulk_packages')
    patterns_directory = luigi.configuration.get_config().get('directories', 'bulk_patterns', 'bulk_patterns')
    # Свойства, переопределяемые в дочерних классах
    target_table = '' # Имя таблицы, для которой будут готовиться пакеты
    session_id = 0  # Идентификатор загрузочной сессии
    parse_pattern = dict()

    def prepare_row(self, data_row):
        """Подготовка строки csv-файла на основе шаблона и строки данных"""
        bulk_parser = MsSqlBulkParser(os.path.join(self.patterns_directory, self.target_table + '.xml'))
        row_columns = bulk_parser.get_columns()
        row = list()
        for column in row_columns:
            column_name = column['@NAME']
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
        # TODO: Заменить open(self.output().path, 'w', newline='') на self.output().open('w') после исправления ошибки
        with self.input().open() as input, open(self.output().path, 'w', newline='') as output:
            reader = csv.reader(input, delimiter='\t')
            for i in range(0, 1):
                next(reader)  # Пропускаем заголовок или больше, если указано
            writer = csv.writer(output, delimiter='\t')
            for row in reader:
                writer.writerow(self.prepare_row(row))

    @property
    def filename(self):
        # Получаем первый файл список файлов в каталоге для bulk-пакетов
        filename = get_first_filename(self.bulk_packages_directory, '*.csv')
        if filename:
            return filename
        return os.path.join(self.bulk_packages_directory, 'package.csv')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.bulk_packages_directory, self.filename))
