import os
import sys

sys.path.append("../profit")

from data.mod.ventas.pedidos import Pedidos  # noqa: E402


class EdoCta:

    def __init__(self, db, pedidos: Pedidos):
        self.db = db
        self.pedidos = pedidos

    def get_edo_cta_clientes(
        self,
        numero_d=None,
        numero_h=None,
        fecha_desde=None,
        fecha_hasta=None,
        cliente_d=None,
        cliente_h=None,
        status="TODO",
        anulado="NOT",
    ):
        data = self.pedidos.get_pedidos(
            numero_d=numero_d,
            numero_h=numero_h,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            cliente_d=cliente_d,
            cliente_h=cliente_h,
            status=status,
            anulado=anulado,
        )
        data = data.groupby(
            [
                "doc_num",
                "co_cli",
                "cli_des",
                "descrip",
                "fec_emis",
            ],
            as_index=False,
            sort=False,
        ).agg(
            {
                "total_monto_base": "sum",
                "iva": "sum",
                "total_monto_neto": "sum",
            }
        )
        return data


if __name__ == "__main__":
    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    sqlserver_connector.connect()
    db = DatabaseConnector(sqlserver_connector)
    oPedidos = Pedidos(db)
    oEdoCta = EdoCta(db, oPedidos)
    print(oEdoCta.get_edo_cta_clientes(cliente_d="J407535560", cliente_h="J407535560"))
    db.autocommit(False)
