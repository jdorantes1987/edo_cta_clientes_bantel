import os
import sys
from time import sleep

import streamlit as st
from dotenv import load_dotenv

from domain.edo_cta import EdoCta

sys.path.append("../authenticator")
sys.path.append("../profit")
sys.path.append("../conexiones")

from auth import AuthManager  # noqa: E402
from conn.database_connector import DatabaseConnector  # noqa: E402
from conn.sql_server_connector import SQLServerConnector  # noqa: E402
from data.mod.ventas.pedidos import Pedidos  # noqa: E402
from role_manager_db import RoleManagerDB  # noqa: E402

# Configuración de página con fondo personalizado
st.set_page_config(
    page_title="Login",
    layout="centered",
    initial_sidebar_state="expanded",
    page_icon="",
)

MENU_INICIO = "pages/page2.py"

st.title("Inicio de sesión")

# Cargar las claves de session si no existen
for key, default in [
    ("stage", 0),
    ("conexion", None),
    ("auth_manager", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


def set_stage(i):
    st.session_state.stage = i


if st.session_state.stage == 0:
    st.session_state.password = ""
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
    try:
        sqlserver_connector.connect()
    except Exception as e:
        st.error(f"No se pudo conectar a la base de datos: {e}")
        st.stop()
    # Almacenar la conexión
    st.session_state.conexion = DatabaseConnector(sqlserver_connector)
    # Almacenar el gestor de autenticación en session_state
    st.session_state.auth_manager = AuthManager(st.session_state.conexion)
    st.session_state.role_manager = RoleManagerDB(st.session_state.conexion)
    st.session_state.pedidos = Pedidos(st.session_state.conexion)
    st.session_state.edo_cta = EdoCta(
        st.session_state.conexion, st.session_state.pedidos
    )
    set_stage(1)


def existe_user(username):
    return st.session_state.auth_manager.user_existe(username)


def login(user, passw):
    return st.session_state.auth_manager.autenticar(user, passw)


@st.cache_data(show_spinner=False)
def iniciar_sesion(user, password):
    flag, msg = login(user=user, passw=password)

    if not flag:
        st.toast(msg, icon="⚠️")
    else:
        # Verificar permisos
        if st.session_state.rol_user.has_permission("EdoCta", "create"):
            st.toast(msg, icon="✅")
            st.session_state.logged_in = True
            st.session_state.user = user
            # Obtener código de cliente asociado
            cod_cliente = st.session_state.auth_manager.get_data_user(user)[
                "cod_client_asociation"
            ]
            st.session_state.cod_client = cod_cliente
            st.switch_page(MENU_INICIO)
        else:
            st.error("No tienes permisos para acceder a esta aplicación.")
            del st.session_state.usuario
            sleep(0.5)
            st.session_state.logged_in = False
            set_stage(0)
            st.rerun()


if st.session_state.stage == 1:
    if "usuario" not in st.session_state:
        # Si el usuario aún no ha sido ingresado
        user = st.text_input(
            "", placeholder="Ingresa tu usuario y presiona Enter"
        ).lower()
        if existe_user(user):
            st.session_state.usuario = user
            st.success("Usuario validado!")
            st.session_state.rol_user = (
                st.session_state.role_manager.load_user_by_username(user)
            )
            st.rerun()
        else:
            if user:
                st.error("El usuario no existe. Inténtalo de nuevo.")
    else:
        # Si el usuario ya ha sido ingresado, se oculta el input y se muestra el usuario ingresado
        st.write(f"### Usuario ingresado: :blue[{st.session_state.usuario}]")

        # Pedir la contraseña
        pw = st.text_input(
            "",
            type="password",
            key="password",
            placeholder="Ingresa tu contraseña y presiona Enter",
        )
        if st.session_state.password:
            iniciar_sesion(st.session_state.usuario, st.session_state.password)
        else:
            if st.button("Atrás", type="primary"):
                del st.session_state.usuario
                del st.session_state.password
                st.rerun()
