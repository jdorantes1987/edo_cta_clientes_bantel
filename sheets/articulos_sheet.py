class ArticulosSheet:
    def __init__(self, service_sheet, datos_articulos):
        self.service_sheet = service_sheet
        self.service = service_sheet.get_service()
        self.datos_articulos = datos_articulos

    def update_articulos_sheet(
        self,
    ):
        self.clear_articulos_data()
        if not self.datos_articulos.empty:
            # actualizar la hoja de Google Sheets con los datos de artículos desde la fila 2
            self.service.spreadsheets().values().update(
                spreadsheetId=self.service_sheet.get_spreadsheet_id(),
                range=f"{self.service_sheet.get_sheet_name()}!A2",
                valueInputOption="RAW",
                body={"values": self.datos_articulos.values.tolist()},
            ).execute()

    def clear_articulos_data(self):
        range_to_clear = f"{self.service_sheet.get_sheet_name()}!2:1000"  # Ajusta 1000 si esperas más filas
        request = (
            self.service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=self.service_sheet.get_spreadsheet_id(),
                range=range_to_clear,
                body={},
            )
        )
        response = request.execute()
        return response


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv
    from monitoreo.articulos import ArtículosMonitoreo

    sys.path.append("../conexiones")
    sys.path.append("../manager_sheets")

    sys.path.append("../conexiones")
    sys.path.append("../manager_sheets")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from service_sheet import ServiceSheet

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        database=os.getenv("DB_NAME_DERECHA_PROFIT"),
        user=os.getenv("DB_USER_PROFIT"),
        password=os.getenv("DB_PASSWORD_PROFIT"),
    )

    sqlserver_connector.connect()
    db = DatabaseConnector(sqlserver_connector)
    oArticulosMonitoreo = ArtículosMonitoreo(db)
    datos_articulos = oArticulosMonitoreo.obtener_articulos()
    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("FILE_RECIBOS_ID")
    SHEET_NAME = os.getenv("FILE_RECIBOS_NOMBRE_HOJA_ARTICULOS")
    CREDENTIALS_FILE = os.getenv("FILE_RECIBOS_CREDENTIALS")

    try:
        oServiceSheet = ServiceSheet(SPREADSHEET_ID, SHEET_NAME, CREDENTIALS_FILE)
        oSheetArticulos = ArticulosSheet(
            service_sheet=oServiceSheet, datos_articulos=datos_articulos
        )
        oSheetArticulos.update_articulos_sheet()
        print("Hoja de artículos actualizada correctamente.")
    except Exception as e:
        print(f"Error al actualizar la hoja de artículos: {e}")

    db.close_connection()
