import asyncio
import inspect


class RecibosSheet:
    def __init__(self, manager_sheet):
        self.manager_sheet = manager_sheet

    async def async_get_data_recibos_a_facturar(self):
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
            return df[
                (df["incluir"] == "SI")
                & (df["razon_social"] != "No existe")
                & (df["desc_art"] != "No existe")
            ]

        # Ejecutar el filtrado en thread para no bloquear el event loop
        return await asyncio.to_thread(_filter, data)

    def get_data_recibos_a_facturar(self):
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
                "Hay un event loop en ejecución. Use 'await async_get_data_recibos_a_facturar()' en código async."
            )

        return asyncio.run(self.async_get_data_recibos_a_facturar())


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

    # Usa variables de entorno o reemplaza por tus valores
    SPREADSHEET_ID = os.getenv("FILE_RECIBOS_ID")
    SHEET_NAME = os.getenv("FILE_RECIBOS_NAME")
    CREDENTIALS_FILE = os.getenv("FILE_RECIBOS_CREDENTIALS")

    oManagerSheet = ManagerSheet(SHEET_NAME, SPREADSHEET_ID, CREDENTIALS_FILE)
    oRecibosSheet = RecibosSheet(oManagerSheet)
    print(oRecibosSheet.get_data_recibos_a_facturar())
