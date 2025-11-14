import asyncio
import inspect


class HistBCVSheet:
    def __init__(self, manager_sheet):
        self.manager_sheet = manager_sheet

    async def async_get_data_bcv(self):
        """
        Versión asíncrona: obtiene los datos (await si el getter es coroutine,
        o ejecuta en thread si es síncrono) y realiza el filtrado en thread.
        """
        getter = self.manager_sheet.get_data_hoja

        if inspect.iscoroutinefunction(getter):
            data = await getter("data")
        else:
            data = await asyncio.to_thread(getter, "data")

        def _filter(df):
            return df

        # Ejecutar el filtrado en thread para no bloquear el event loop
        return await asyncio.to_thread(_filter, data)

    def get_data_bcv(self):
        """
        Wrapper síncrono que ejecuta la versión asíncrona.
        Si ya hay un event loop corriendo, lanza RuntimeError y pide usar la versión async.
        """
        try:
            # detecta si hay un event loop en ejecución (no se necesita conservar el objeto)
            asyncio.get_running_loop()
            loop_running = True
        except RuntimeError:
            loop_running = False

        if loop_running:
            raise RuntimeError(
                "Hay un event loop en ejecución. Use 'await async_get_data_bcv()' en código async."
            )

        data = asyncio.run(self.async_get_data_bcv())[["fecha", "venta_ask2"]]

        # Optimización: usar una función para limpiar y convertir columnas
        def limpiar_columna(col):
            return (
                col.str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float)
            )

        data["venta_ask2"] = limpiar_columna(data["venta_ask2"])
        data["fecha"] = data["fecha"].astype("datetime64[ns]")
        return data

    def get_tasa_today(self):
        """
        Obtiene la tasa del día más reciente en el histórico.
        Returns:
            float: Tasa del día más reciente.
        """
        df_bcv = self.get_data_bcv()
        if df_bcv.empty:
            raise ValueError("El DataFrame de tasas está vacío.")
        latest_date = df_bcv["fecha"].max()
        tasa_today = df_bcv.loc[df_bcv["fecha"] == latest_date, "venta_ask2"].values[0]
        return tasa_today


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")
    sys.path.append("../manager_sheets")

    from manager_sheet import ManagerSheet

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    SPREADSHEET_ID = os.getenv("HISTORICO_TASAS_BCV_ID")
    SHEET_NAME = os.getenv("FILE_HISTORICO_TASAS_BCV_NAME")
    CREDENTIALS_FILE = os.getenv("HISTORICO_TASAS_BCV_CREDENTIALS")

    oManagerSheet = ManagerSheet(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    oRecibosSheet = HistBCVSheet(oManagerSheet)
    print(oRecibosSheet.get_data_bcv())
