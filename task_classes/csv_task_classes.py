# Классы задач обработки csv-файлов

import os.path
import csv

import luigi

from .utils import MsSqlBulkParser, get_first_filename

parse_pattern_1 = {
    '№': {
        'col_num': 0
    },
    'Ключ_записи': {
        'col_num': 1
    },
    'MXt': {
        'col_num': 2
    },
    'Суточный_диурез_пациента': {
        'col_num': 3
    },
    'Рост_пациента': {
        'col_num': 4
    },
    'Количество_и_периодичность_назначений': {
        'col_num': 5
    },
    'Примечание_лабораторного_эпизода': {
        'col_num': 6
    },
    'Вес_пациента': {
        'col_num': 7
    },
    'Номер_направления_на_госпитализацию': {
        'col_num': 8
    },
    'Predst_o_b': {
        'col_num': 9
    },
    'Группа_крови_реципиента': {
        'col_num': 10
    },
    'Резус-фактор_реципиента': {
        'col_num': 11
    },
    'Дата_направления_на_поступление': {
        'col_num': 12
    },
    'Дата_поступления': {
        'col_num': 13
    },
    'Дефект_догоспитального_этапа': {
        'col_num': 14
    },
    'ФИО_реципиента': {
        'col_num': 15
    },
    'Код_ресурса': {
        'col_num': 16
    },
    'Код_специальности': {
        'col_num': 17
    },
    'NaprSpec': {
        'col_num': 18
    },
    'Лабораторный_Номер': {
        'col_num': 19
    },
    'Массив_последнего_диагноза': {
        'col_num': 20
    },
    'Дата_выполнения': {
        'col_num': 21
    },
    'Дата_завершения_эпизода': {
        'col_num': 22
    },
    'Дата_госпитализации_плановая': {
        'col_num': 23
    },
    'Номер_эпизода': {
        'col_num': 24
    },
    'Цель_поступления': {
        'col_num': 25
    },
    'ФИО_Специалиста': {
        'col_num': 26
    },
    'Регистрационный_номер': {
        'col_num': 27
    },
    'Номер_эпизода_1': {
        'col_num': 28
    },
    'Код_контингент_обследованных': {
        'col_num': 29
    },
    'Ekstrenno': {
        'col_num': 30
    },
    'Дата_последних_menses': {
        'col_num': 31
    },
    'Код_отделения': {
        'col_num': 32
    },
    'Код_сотрудника': {
        'col_num': 33
    },
    'Дата_постановки_клинического_диагноза': {
        'col_num': 34
    },
    'Клинический_диагноз': {
        'col_num': 35
    },
    'Описание_клинического_диагноза': {
        'col_num': 36
    },
    'Канал_поступления': {
        'col_num': 37
    },
    'Категория_пациента': {
        'col_num': 38
    },
    'Диагноз_направления': {
        'col_num': 39
    },
    'Отказ_в_госпитализации': {
        'col_num': 40
    },
    'Патологоанатомическое_вскрытие': {
        'col_num': 41
    },
    'Время_завершения_эпизода': {
        'col_num': 42
    },
    'Отметка_о_посещении_акушерки': {
        'col_num': 43
    },
    'Количество_доз_донации': {
        'col_num': 44
    },
    'Категория_донора': {
        'col_num': 45
    },
    'Объем_донации': {
        'col_num': 46
    },
    'Результат_эпизода': {
        'col_num': 47
    },
    'Результат_эпизода_предыдущий': {
        'col_num': 48
    },
    'Статус_донора_эпизода': {
        'col_num': 49
    },
    'Вид_донации_реальный': {
        'col_num': 50
    },
    'Вид_донорства': {
        'col_num': 51
    },
    'Тип_донорства': {
        'col_num': 52
    },
    'Талон_ВМП': {
        'col_num': 53
    },
    'Исход_Эпизода': {
        'col_num': 54
    },
    'Место_выписки': {
        'col_num': 55
    },
    'Результат_лечения': {
        'col_num': 56
    },
    'Стентирование': {
        'col_num': 57
    },
    'Тромболитическая_терапия': {
        'col_num': 58
    },
    'Должность_Ресурс': {
        'col_num': 59
    },
    'Вич-инфекция_у_пациента': {
        'col_num': 60
    },
    'Вид_поступления': {
        'col_num': 61
    },
    'Зрелость_новорожденного': {
        'col_num': 62
    },
    'Код_клинических_условий': {
        'col_num': 63
    },
    'Тип_штатива': {
        'col_num': 64
    },
    'Квалификационная_категория': {
        'col_num': 65
    },
    'Ученая_степень': {
        'col_num': 66
    },
    'Ученое_звание': {
        'col_num': 67
    },
    'Способ_доставки': {
        'col_num': 68
    },
    'Порядок_госпитализации': {
        'col_num': 69
    },
    'Направившее_учреждение': {
        'col_num': 70
    },
    'Направивший_врач': {
        'col_num': 71
    },
    'Повторность_госпитализации': {
        'col_num': 72
    },
    'Состояние_пациента': {
        'col_num': 73
    },
    'Время_поступления': {
        'col_num': 74
    },
    'Создание': {
        'col_num': 75
    },
    'Коррекция': {
        'col_num': 76
    },
    'Коррекция_дата_время': {
        'col_num': 77
    },
    'Логическое_удаление': {
        'col_num': 78
    },
    'Эпизоды_экз': {
        'col_num': 79
    },
    'Эпизоды_ид': {
        'const': ''
    },
    'Сессия_загрузки': {
        'const': 16
    },
    'wh': {
        'const': 0
    },
    'Дата_время_загрузки': {
        'const': ''
    }
}


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