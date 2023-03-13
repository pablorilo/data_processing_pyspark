from JDBCprovisioner import JDBCprovisioner
from DevicesStreamingJob import DevicesStreamingJob
from DevicesBatchJob import DevicesBatchJob
from ConnectionEnvironment import *

if __name__ == '__main__':

    class ExecuteJob(ConnectionEnvironment):
        def __init__(self):
            ConnectionEnvironment.__init__(self)

        def runJob(self):
            # -- Ejecutamos el provisionador para crear la estructura en postgreSQL
            JDBCprovisioner(self.jdbcUri, self.user, self.password, self.DB, self.PORT).run()

            # -- Ejecutamos el streaming job de spark (en primer plano)
            DevicesStreamingJob().run()

            # -- Cuando termine la conexion en streaming (si esta en await) se ejecuta el BatchJob
            DevicesBatchJob().run()

    ExecuteJob().runJob()
