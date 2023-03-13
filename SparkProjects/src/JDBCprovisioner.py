import psycopg2


class JDBCprovisioner:
    def __init__(self, ip_server: str, username: str, passwd: str, db: str, port: str):
        self.conn = psycopg2.connect(host=f"{ip_server}:{port}", database=db, user=username, password=passwd)
        self.cursor = self.conn.cursor()

    def checkAllInsert(self) -> None:
        self.cursor.execute("SELECT * FROM py_user_metadata")

        for z in self.cursor.fetchall():
            print(z)

    def checkTables(self) -> None:
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        for table in self.cursor.fetchall():
            print(table)

    def confirmInserted(self) -> bool:
        self.cursor.execute("SELECT * FROM py_user_metadata")
        if len(self.cursor.fetchall()) != 0:
            return True
        return False

    def createTables(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS py_user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS py_bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS py_bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS py_user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP)")

        self.conn.commit()

    def insertUserDefaults(self) -> bool:
        if not self.confirmInserted():
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
            self.cursor.execute(
                "INSERT INTO py_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")

            self.conn.commit()
            return True

        else:
            print("Ya estan insertados")
            return False

    def run(self):
        self.createTables()
        self.checkTables()
        self.insertUserDefaults()
        self.checkAllInsert()
        self.conn.close()
