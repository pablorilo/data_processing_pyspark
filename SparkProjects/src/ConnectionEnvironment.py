import findspark

findspark.init()


class ConnectionEnvironment:
    def __init__(self):
        # -- Variables VM
        self.kafkaServer = "youCloudEngineInstanceExternalIP:9092"  # Los puertos a esta ip deben estar abiertos
        self.topic = "devices"

        # -- Variables PostgreSQL
        self.jdbcUri = "yourPostgreSqlCloudInstanceExternalIp"
        self.jdbcTable = "py_bytes"
        self.user = "yourPostgresUser"
        self.password = "yourPostgresPass"
        self.DB = "postgres"
        self.PORT = "5432"

        # --path
        self.storagePath = "./proyecto/devices/"
