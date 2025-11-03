import sys

sys.path.append("../profit")
from data.mod.inventario.articulos import Articulos  # noqa: E402


class ArtículosMonitoreo:
    def __init__(self, db):
        self.db = db
        self.oArtículos = Articulos(db)

    def obtener_articulos(self):
        """Obtiene una lista de artículos."""
        fields = [
            "co_art",
            "art_des",
            "anulado",
        ]
        data = self.oArtículos.get_articulos()[fields]
        return data[data["anulado"] == 0]


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
    oArticulos = ArtículosMonitoreo(db_derecha)
    articulos_derecha = oArticulos.obtener_articulos()
    print(articulos_derecha)
    db_derecha.close_connection()
