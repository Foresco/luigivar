# Настройки соединений с базами данных
import luigi


class Connection1(luigi.Config):
    """Configuration class for MS SQL"""
    host = luigi.Parameter()
    user = luigi.Parameter()
    database = luigi.Parameter()
    password = luigi.Parameter()
    table = 'foo_table'  # Переопределяется в загрузчике
    marker_table = 'upload_sessions'