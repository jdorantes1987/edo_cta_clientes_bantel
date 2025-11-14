import os
import sys
from re import findall
from datetime import datetime

sys.path.append("../profit")

from data.mod.ventas.pedidos import Pedidos  # noqa: E402


class Recibos:

    def __init__(self, db, data_recibos_a_procesar, pedidos: Pedidos):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()
        self.counter_num_recibo = 0
        self.__data_recibos_a_procesar = data_recibos_a_procesar
        self.pedidos = pedidos

    def _set_numero_recibo(self, string_num):
        # Extrae los números dentro de la cadena de texto
        num = findall("[0-9.]+", string_num)
        self.counter_num_recibo += 1
        return str(int(num[0]) + self.counter_num_recibo).zfill(8)

    def get_last_id_recibo(self, fecha_fin) -> str:
        sql = "Select doc_num From saPedidoVenta Where fec_emis <= ? Order By doc_num Desc"
        self.cursor.execute(sql, (fecha_fin,))
        result = self.cursor.fetchone()
        if not result:
            return "RBO-00000000"
        return result[0]

    def get_next_num_recibo(self, id_num_recibo) -> str:
        return "RBO-" + self._set_numero_recibo(id_num_recibo)

    def _data_procesada(self):
        data = self.__data_recibos_a_procesar.copy()
        data["cantidad"] = data["cantidad"].astype(float)
        data["monto_base"] = data["monto_base"].str.replace(
            ".",
            "",
        )  # Eliminar separadores de miles
        data["monto_base"] = data["monto_base"].str.replace(
            ",",
            ".",
        )  # Reemplazar coma decimal por punto
        data["monto_base"] = data["monto_base"].astype(float)
        data["total_monto_base"] = round(data["cantidad"] * data["monto_base"], 2)
        data["iva"] = round(data["total_monto_base"] * 0.16, 2)
        data["total_monto_neto"] = data["total_monto_base"] + data["iva"]
        return data

    def es_data_consistente(self) -> bool:
        """
        Comprueba que, tras procesar y agrupar, cada id_client aparezca solo una vez.
        Devuelve True si no hay duplicados, False si existen clientes repetidos.
        """
        df = self._data_procesada().copy()
        grouped = df.groupby(
            ["enum", "id_client", "razon_social", "descrip_encab_fact", "fecha_recibo"],
            as_index=False,
        ).agg({"total_monto_base": "sum"})

        # Si no hay filas consideramos consistente
        if grouped.empty:
            return True

        # True si no hay enum e id_client repetidos
        return not grouped[["enum", "id_client"]].duplicated().any()

    def _data_encabezados_recibos(self):
        data = self._data_procesada().copy()
        data = data.groupby(
            [
                "enum",
                "id_client",
                "razon_social",
                "descrip_encab_fact",
                "fecha_recibo",
            ],
            sort=False,
            as_index=False,
        ).agg(
            {
                "total_monto_base": "sum",
                "iva": "sum",
                "total_monto_neto": "sum",
            }
        )
        return data

    def _data_detalle_recibos(self):
        data = self._data_procesada()
        return data

    def procesar_recibos_masivos(self):
        resultado = {"success": True, "message": "Datos procesados!."}
        if self.es_data_consistente() is False:
            resultado = {"success": False, "message": "Los datos no son consistentes."}
            return resultado

        encabezados = self._data_encabezados_recibos()
        detalle = self._data_detalle_recibos()

        hoy = datetime.now().strftime("%Y-%m-%d")
        last_id = self.get_last_id_recibo(hoy)
        safe1 = []
        safe2 = []
        for index, row in encabezados.iterrows():
            next_num_recibo = self.get_next_num_recibo(last_id)
            payload_pedido = {
                "doc_num": next_num_recibo,
                "descrip": row["descrip_encab_fact"],
                "co_cli": row["id_client"],
                "co_tran": "NA",
                "co_mone": "US$",
                "co_ven": "0001",
                "co_cond": "01",
                "fec_emis": hoy,
                "fec_venc": hoy,
                "fec_reg": hoy,
                "anulado": 0,
                "status": "0",
                "ven_ter": 0,
                "tasa": 1.00,
                "monto_desc_glob": 0,
                "monto_reca": 0,
                "total_bruto": row["total_monto_base"],
                "monto_imp": row["iva"],
                "monto_imp2": 0,
                "monto_imp3": 0,
                "otros1": 0,
                "otros2": 0,
                "otros3": 0,
                "total_neto": row["total_monto_neto"],
                "saldo": row["total_monto_neto"],
                "contrib": 1,
                "impresa": 0,
                "co_us_in": "JACK",
                "co_sucu_in": "01",
                "co_us_mo": "JACK",
                "co_sucu_mo": "01",
            }
            safe1.append(self.pedidos.normalize_payload_pedido(payload_pedido))

            # # Filtrar los detalles correspondientes a este encabezado
            detalles_recibo = detalle[
                (detalle["id_client"] == row["id_client"])
                & (detalle["enum"] == row["enum"])
            ].reset_index(drop=True)
            detalles_recibo["comentario"] = detalles_recibo[
                ["comentario_l1", "comentario_l2", "comentario_l3"]
            ].agg("\n".join, axis=1)
            for index_det, linea in detalles_recibo.iterrows():
                payload_det_pedido = {
                    "reng_num": index_det + 1,
                    "doc_num": next_num_recibo,
                    "co_art": linea["co_art"],
                    "co_alma": "NA",
                    "total_art": linea["cantidad"],
                    "stotal_art": 0,
                    "co_uni": "001",
                    "co_precio": "01",
                    "prec_vta": linea["monto_base"],
                    "monto_desc": 0,
                    "tipo_imp": "1",
                    "porc_imp": 16.0,
                    "porc_imp2": 0,
                    "porc_imp3": 0,
                    "monto_imp": linea["iva"],
                    "monto_imp2": 0,
                    "monto_imp3": 0,
                    "reng_neto": linea["total_monto_neto"],
                    "pendiente": linea["cantidad"],
                    "pendiente2": 0,
                    "lote_asignado": 0,
                    "monto_desc_glob": 0,
                    "monto_reca_glob": 0,
                    "otros1_glob": 0,
                    "otros2_glob": 0,
                    "otros3_glob": 0,
                    "monto_imp_afec_glob": 0,
                    "monto_imp2_afec_glob": 0,
                    "monto_imp3_afec_glob": 0,
                    "total_dev": 0,
                    "monto_dev": 0,
                    "comentario": linea["comentario"],
                    "otros": 0,
                    "co_us_in": "JACK",
                    "co_sucu_in": "01",
                    "co_us_mo": "JACK",
                    "co_sucu_mo": "01",
                }
                safe2.append(
                    self.pedidos.normalize_payload_det_pedido(payload_det_pedido)
                )

        pedidos_count_rows = self.pedidos.create_pedidos(safe1)
        if not pedidos_count_rows:
            resultado = {
                "success": False,
                "message": "Error al insertar el pedido.",
            }

        det_pedidos_count_rows = self.pedidos.create_det_pedidos(safe2)
        if not det_pedidos_count_rows:
            resultado = {
                "success": False,
                "message": "Error al insertar el detalle del pedido.",
            }
        return resultado


if __name__ == "__main__":
    from dotenv import load_dotenv

    from sheets.recibos_sheet import RecibosSheet

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
    db_credentials = {
        "host": os.getenv("HOST_PRODUCCION_PROFIT"),
        "database": os.getenv("DB_NAME_DERECHA_PROFIT"),
        "user": os.getenv("DB_USER_PROFIT"),
        "password": os.getenv("DB_PASSWORD_PROFIT"),
    }
    sqlserver_connector = SQLServerConnector(**db_credentials)
    sqlserver_connector.connect()
    db = DatabaseConnector(sqlserver_connector)
    db.autocommit(False)
    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("FILE_RECIBOS_ID")
    SHEET_NAME = os.getenv("FILE_RECIBOS_NAME")
    CREDENTIALS_FILE = os.getenv("FILE_RECIBOS_CREDENTIALS")
    oManagerSheet = ManagerSheet(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    data_recibos_a_procesar = RecibosSheet(oManagerSheet).get_data_recibos_a_facturar()
    oPedidos = Pedidos(db)
    oRecibo = Recibos(db, data_recibos_a_procesar, oPedidos)
    last_id = oRecibo.get_last_id_recibo("2025-11-30")
    # print(f"Último ID de recibo: {last_id}")
    # print(f"es data consistente: {oRecibo.es_data_consistente()}")
    if oRecibo.es_data_consistente():
        resultado = oRecibo.procesar_recibos_masivos()
        print(f"Resultado del procesamiento: {resultado}")
        if resultado["success"]:
            db.commit()
        else:
            db.rollback()
    # # Generar los siguientes números de recibo
    # print(oRecibo.get_next_num_recibo(last_id))
    # print(oRecibo.get_next_num_recibo(last_id))
    db.autocommit(True)
    db.close_connection()
