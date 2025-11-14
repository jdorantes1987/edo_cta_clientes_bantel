from time import sleep

import streamlit as st
from streamlit.runtime.scriptrunner import get_script_run_ctx


def get_current_page_name():
    ctx = get_script_run_ctx()
    if ctx is None:
        raise RuntimeError("No se pudo obtener el contexto del script.")

    return ctx.page_script_hash.split("/")[-1]  # type: ignore


def make_sidebar():
    with st.sidebar:
        # Centrar el título
        # quitar margenes
        st.markdown(
            "<h1 style='text-align: center; margin: 0;'>DataPy</h1>",
            unsafe_allow_html=True,
        )
        # imagen desde URL
        # Quitar margenes
        image_url = "images/pexelweb.gif"

        st.image(image_url, use_container_width=True)
        # # imagen local
        st.markdown("---")

        if st.session_state.get("logged_in", False):
            _extracted_from_make_sidebar()
        elif get_current_page_name() != "inicio":
            # If anyone tries to access a secret page without being logged in,
            # redirect them to the login page
            st.switch_page("app.py")


# TODO Rename this here and in `make_sidebar`
def _extracted_from_make_sidebar():
    # st.page_link("pages/page1.py", label="Inicio", icon=None)
    st.page_link("pages/page2.py", label="Estado de cuenta", icon=None)

    if st.button(
        "Cerrar sesión",
        type="primary",
    ):
        logout()

    st.cache_data.clear()


def logout():
    st.session_state.logged_in = False
    st.info("Se ha cerrado la sesión con éxito!")
    sleep(0.5)
    st.switch_page("app.py")
