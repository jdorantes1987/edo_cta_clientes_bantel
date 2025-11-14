import streamlit as st
from pandas import DataFrame

from helpers.navigation import make_sidebar

st.set_page_config(page_title="Estado de cuenta", layout="wide", page_icon="")

make_sidebar()
# Inicialización de estado
for k, v in {
    "stage2": 0,
    "total_sel": 0.0,
    "seleccionados": DataFrame(),
}.items():
    if k not in st.session_state:
        st.session_state[k] = v
        st.session_state[k] = v


st.title("Recibos")

if st.button("Refrescar"):
    st.cache_data.clear()


def set_stage(i):
    st.session_state.stage2 = i


if st.session_state.stage2 == 0:
    st.session_state.total_sel = 0.0
    st.session_state.seleccionados = DataFrame()
    set_stage(1)


@st.cache_data(show_spinner=False)
def get_recibos_pendientes(cod_cliente: str):
    return st.session_state.edo_cta.get_edo_cta_clientes(
        cliente_d=cod_cliente, cliente_h=cod_cliente, status="SPRO"
    )


if st.session_state.stage2 == 1:
    tab1, tab2 = st.tabs(["📑 Recibos pendientes", "📰 Movimientos"])
    with tab1:
        with st.expander("💵 Instrucciones para registrar un pago", expanded=False):
            st.markdown(
                """
                1. Revisa los recibos pendientes en la tabla.
                2. Selecciona los recibos que deseas pagar.
                3. Anota la fecha , referencia bancaria y monto del pago móvil.
                4. Presiona el botón de "Registrar y validar pago".
                """
            )

        # Construye un st.dataframe editable con los recibos pendientes del cliente
        cod_cliente = st.session_state.cod_client
        df_recibos = get_recibos_pendientes(cod_cliente)
        if df_recibos.empty:
            st.info("No hay recibos pendientes para este cliente.")
            st.stop()

        # Inserta columna llamada 'Select'
        df_recibos.insert(0, "sel", False)
        df_edited = st.data_editor(
            df_recibos,
            column_config={
                "cod_cli": None,
                "cli_des": st.column_config.TextColumn(
                    "Razón Social",
                    width="large",
                ),
                "fec_emis": st.column_config.DateColumn(
                    "Fecha de Emisión",
                    help="Fecha de emisión del recibo.",
                    format="DD/MM/YYYY",
                ),
                "doc_num": st.column_config.TextColumn(
                    "Recibo",
                    width="small",
                ),
                "descrip": st.column_config.TextColumn(
                    "Descripción",
                    width="large",
                ),
                "total_monto_base": None,
                "iva": None,
                "total_monto_neto": st.column_config.NumberColumn(
                    "Total",
                    help="Monto total del recibo expresado en USD.",
                    format="$ %.2f",
                    width="small",
                ),
            },
            width="stretch",
            column_order=[
                "sel",
                "doc_num",
                "fec_emis",
                "cli_des",
                "descrip",
                "total_monto_neto",
            ],
            disabled=[
                "fec_emis",
                "doc_num",
                "descrip",
                "total_monto_base",
                "iva",
                "total_monto_neto",
            ],
            use_container_width=True,
            hide_index=True,
        )

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            st.markdown(
                f"**Recibos pendientes**  <span style='color:orange'>{df_edited.shape[0]}</span>",
                unsafe_allow_html=True,
            )
        with col2:
            st.session_state.seleccionados = df_edited[df_edited["sel"]]
            if st.session_state.seleccionados.shape[0] > 0:
                st.metric("Seleccionados", st.session_state.seleccionados.shape[0])
            else:
                st.session_state.seleccionados["total_monto_neto"] = 0.0
        with col3:
            if (
                not st.session_state.seleccionados.empty
                and "total_monto_neto" in st.session_state.seleccionados.columns
            ):
                st.session_state.total_sel = (
                    st.session_state.seleccionados["total_monto_neto"]
                    .astype(float)
                    .sum()
                )
            if st.session_state.total_sel > 0.0:
                st.metric("Total seleccionado", f"${st.session_state.total_sel:,.2f}")

        # Acción principal
        if st.button("Registrar y validar pago"):
            if st.session_state.seleccionados.empty:
                st.warning("No has seleccionado ningún recibo para pagar.")
                st.stop()
            st.success(
                f"{st.session_state.seleccionados.shape[0]} recibos preparados para registro."
            )
            set_stage(2)
            st.rerun()
            # aquí añadir lógica de procesamiento / validación
    with tab2:
        st.subheader("Movimientos realizados")
        st.info("Aquí se mostrarán los movimientos realizados por el cliente.")

if st.session_state.stage2 == 2:
    col1, col2 = st.columns(2)
    st.subheader("Validación de pagos")
    with col1:
        st.metric(
            "Recibos a pagar",
            st.session_state.seleccionados.shape[0],
        )
    with col2:
        st.metric(
            "Total a pagar",
            f"${st.session_state.total_sel:,.2f}",
        )

    col3, col4, col5 = st.columns(3)
    with col3:
        st.date_input(
            "Fecha del pago",
            key="fecha_pago",
        )
    with col4:
        st.text_input(
            "Referencia bancaria",
            key="referencia_bancaria",
            placeholder="Ingresa la referencia bancaria del pago",
        )
    with col5:
        st.number_input(
            "Monto del pago (USD)",
            key="monto_pago",
            min_value=0.0,
            value=float(st.session_state.total_sel),
            format="%.2f",
        )
    st.info(
        "Aquí se mostrarán los detalles para validar y registrar los pagos seleccionados."
    )
    # icono de regresar
    if st.button(" <- recibos pendientes"):
        set_stage(0)
        st.rerun()
