# Классы взаимодействия с базой данных
import luigi

from .db.mssqldb import MSSqlTarget


class RegisterLoadSession(MSSqlTarget):
    """Класс регистрации сессии загрузки"""

    def __init__(self, connection):
        # Указание параметров соединения
        super().__init__(
            host = connection.host,
            user = connection.user,
            password = connection.password,
            database = connection.database
        )

    def get_params():
        # Затычка, связанная с неизвестными причинами TODO: Убрать
        return {}

    def run(self):
        # TODO: Использовать общее соединение
        connection = self.connect()
        # Регистрация загрузочной сессии
        self.start(connection)

    def output(self):
        return self.exists()
