import streamlit as st
from time import sleep
from pandas import DataFrame

from helpers.navigation import make_sidebar

st.set_page_config(page_title="Estado de cuenta", layout="wide", page_icon="")

make_sidebar()
# Inicialización de estado
for k, v in {
    "stage2": 0,
    "total_sel": 0.0,
    "seleccionados": DataFrame(),
    "pagos_realizados": [],
}.items():
    if k not in st.session_state:
        st.session_state[k] = v
        st.session_state[k] = v


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
    if st.button("Refrescar"):
        st.cache_data.clear()

    tab1, tab2 = st.tabs(["📑 Recibos pendientes", "📰 Movimientos"])
    with tab1:
        with st.expander(
            " Hacer **'click'** para ver instrucciones 👇", expanded=False
        ):
            st.markdown(
                """
                1. **Revisa los recibos pendientes en la tabla.**
                2. **Selecciona los recibos que deseas pagar.**
                3. **Presiona el botón de *"validar pago"*.**
                4. **Anota la fecha, referencia bancaria y monto del pago móvil.**
                5. **Presiona el botón de *"Registrar pago"*.**
                """
            )

        # Construye un st.dataframe editable con los recibos pendientes del cliente
        cod_cliente = st.session_state.cod_client
        df_recibos = get_recibos_pendientes(cod_cliente)
        if df_recibos.empty:
            st.info("Cliente solvente!")
            st.stop()
        else:
            st.warning("Tienes recibo(s) pendiente(s) por pagar.")

        # Inserta columna llamada 'Select'
        df_recibos.insert(0, "sel", False)
        df_edited = st.data_editor(
            df_recibos,
            column_config={
                "cod_cli": None,
                "sel": st.column_config.CheckboxColumn(
                    "selec.",
                    help="Selecciona el recibo a validar.",
                    width="small",
                ),
                "cli_des": st.column_config.TextColumn(
                    "Razón Social",
                    width="large",
                ),
                "fec_emis": st.column_config.DateColumn(
                    "Fecha",
                    width="small",
                    help="Fecha de emisión del recibo.",
                    format="DD/MM/YYYY",
                ),
                "doc_num": st.column_config.TextColumn(
                    "Recibo",
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
                "total_monto_neto",
                "cli_des",
                "descrip",
            ],
            disabled=[
                "fec_emis",
                "doc_num",
                "descrip",
                "cli_des",
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
        if st.button("Validar pago"):
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


if st.session_state.stage2 >= 2:
    """
    ## Datos del pago
    """

    def habilitar_boton():
        if (
            st.session_state.referencia_bancaria.strip() != ""
            and len(st.session_state.referencia_bancaria.strip()) >= 1
        ):
            set_stage(3)
        else:
            set_stage(2)

    monto_en_bs = st.session_state.tasa_today * st.session_state.total_sel
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "Tasa del día (Bs/USD)",
            f"Bs {st.session_state.tasa_today:,.4f}",
        )
    with col2:
        st.metric(
            "Total a pagar",
            f"Bs {monto_en_bs:,.2f}",
        )

    with col3:
        st.metric(
            "Recibos a pagar",
            st.session_state.seleccionados.shape[0],
        )

    col4, col5, col6 = st.columns(3)
    with col4:
        st.date_input(
            "📅 Fecha del pago",
            key="fecha_pago",
            format="DD/MM/YYYY",
            disabled=True,
        )
    with col5:
        st.text_input(
            "📋 Referencia bancaria",
            key="referencia_bancaria",
            placeholder="Ingresa la referencia bancaria del pago",
            on_change=habilitar_boton,
            max_chars=13,
        )
    with col6:
        st.number_input(
            "💵 Monto del pago (Bs)",
            key="monto_pago",
            min_value=0.0,
            value=float(monto_en_bs),
            format="%.2f",
            disabled=True,
        )

if st.session_state.stage2 == 3:
    col7, col8 = st.columns(2)
    with col7:
        # icono de regresar
        if st.button("💱 Registrar pago"):
            st.session_state.pagos_realizados.append(
                {
                    "fecha_pago": st.session_state.fecha_pago,
                    "referencia_bancaria": st.session_state.referencia_bancaria,
                    "monto_pago": st.session_state.monto_pago,
                }
            )
            st.success("Pago registrado con éxito!")
            sleep(0.5)
            st.info("Actualizando información...")
            sleep(0.5)
            set_stage(0)
            st.rerun()

    if st.button(" 📊 Ir a recibos pendientes"):
        set_stage(0)
        st.rerun()
