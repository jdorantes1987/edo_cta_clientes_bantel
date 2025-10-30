import sys

sys.path.append("../profit")
from data.mod.ventas.clientes import Clientes  # noqa: E402


class ClientesMonitoreo:
    def __init__(self, db):
        self.db = db
        self.oClientes = Clientes(db)

    def obtener_clientes_activos(self):
        """Obtiene una lista de clientes activos."""
        fields = [
            "co_cli",
            "cli_des",
            "telefonos",
            "tipo_adi",
            "inactivo",
        ]
        data = self.oClientes.get_clientes_profit()[fields]
        return data[(data["inactivo"] == 0) & (data["tipo_adi"] <= 2)]


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # diccionario con las credenciales de la base de datos
    # para manejar multiples conexiones a la vez

    db_credentials = {
        "host": os.getenv("HOST_PRODUCCION_PROFIT"),
        "database": os.getenv("DB_NAME_DERECHA_PROFIT"),
        "user": os.getenv("DB_USER_PROFIT"),
        "password": os.getenv("DB_PASSWORD_PROFIT"),
    }

    # Conexión a la base de datos de la derecha
    sqlserver_connector = SQLServerConnector(**db_credentials)
    db_derecha = DatabaseConnector(sqlserver_connector)
    oClientesMonitoreo = ClientesMonitoreo(db_derecha)
    clientes_derecha = oClientesMonitoreo.obtener_clientes_activos()
    print(clientes_derecha)
    db_derecha.close_connection()
