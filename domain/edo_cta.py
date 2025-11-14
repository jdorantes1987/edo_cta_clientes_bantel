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
        """
        Obtiene el estado de cuenta de los clientes.
        args:
            numero_d (str, optional): Número de pedido desde. Defaults to None.
            numero_h (str, optional): Número de pedido hasta. Defaults to None.
            fecha_desde (date, optional): Fecha desde. Defaults to None.
            fecha_hasta (date, optional): Fecha hasta. Defaults to None.
            cliente_d (str, optional): Código de cliente desde. Defaults to None.
            cliente_h (str, optional): Código de cliente hasta. Defaults to None.
            status (str, optional): Estado de los pedidos:
                'TODO' : Todos los pedidos.
                'SPRO' : Pedidos sin procesar.
                'PRO' : Pedidos procesados.
                'PPRO' : Pedidos parcialmente procesados.
            anulado (str, optional): Estado de anulación de los pedidos:
                'SIT' : Pedidos anulados.
                'NOT' : Pedidos no anulados.
        Returns:
            pd.DataFrame: DataFrame con el estado de cuenta de los clientes.
        """
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

    from sheets.bcv_sheet import HistBCVSheet

    sys.path.append("../conexiones")
    sys.path.append("../manager_sheets")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from manager_sheet import ManagerSheet

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

    SPREADSHEET_ID = os.getenv("HISTORICO_TASAS_BCV_ID")
    SHEET_NAME = os.getenv("FILE_HISTORICO_TASAS_BCV_NAME")
    CREDENTIALS_FILE = os.getenv("HISTORICO_TASAS_BCV_CREDENTIALS")

    oManagerSheet = ManagerSheet(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)

    oHistBCVSheet = HistBCVSheet(oManagerSheet)
    historico_tasas = oHistBCVSheet.get_data_bcv()
    oPedidos = Pedidos(db)
    oEdoCta = EdoCta(db, oPedidos)
    if historico_tasas.empty:
        print("No hay datos históricos disponibles.")
    else:
        print(
            oEdoCta.get_edo_cta_clientes(
                cliente_d="J298823291", cliente_h="J298823291", status="SPRO"
            )
        )
    db.close_connection()
