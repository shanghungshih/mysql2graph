import pymysql

class MySQL_processor:
    def __init__(self, host, port, user, password, database=None):
        if database is None:
            self.cnx = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password
            )
        else:
            self.cnx = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
        self.cursor = self.cnx.cursor()