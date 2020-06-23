# Запуск задачи
```commandline
python -m luigi --module psql_tasks CopyTaxiTripData --date 2019-02 --local-scheduler
```
```commandline
python -m luigi --module csv_tasks PrepareCsvPackages --local-scheduler
```


