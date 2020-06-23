# Запуск задачи
```commandline
D:\work\Проекты\Предприятия\Алмазов\Python\luigi\tasks>python -m luigi --module psql_tasks CopyTaxiTripData --date 2019-02 --local-scheduler
```
```commandline
python -m luigi --module csv_tasks PrepareCsvPackages --local-scheduler
```


# Структура приложения
* **config** - конфигация приложения. Индивидуальные настройки конкретного проекта.
* **data** - каталог для исходных и промежуточных данных
* **db** - каталог с инструментариям работы с базо данных (передать в Luigi)
* **tasks** - каталог с задачами
* **test** - функционал тестирования
