import streamlit as st
import os
import time
import json
import requests
import zipfile
import io
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from workalendar.america import Brazil # <<< CORREÇÃO APLICADA AQUI

# ============================== FUNÇÕES DE LOGIN ============================== #

import bcrypt  # pip install bcrypt

# ─────────────────────────  CONFIG GLOBAL  ──────────────────────────
st.set_page_config(page_title="Dashboard XYZ",
                   page_icon="🔒",
                   layout="centered")

_HIDE = """
<style>
#MainMenu, footer {visibility:hidden;}
body {background:#f5f7fa;}
.login-card {max-width:360px;margin:auto;
             padding:2rem 2.5rem;background:white;
             border-radius:12px;
             box-shadow:0 0 15px rgba(0,0,0,.08);}
.stButton>button {background:#004c97;color:white;width:100%;}
</style>
"""
st.markdown(_HIDE, unsafe_allow_html=True)

# ────────────────────────  FUNÇÕES DE AUTENTICAÇÃO  ─────────────────────────
def _check_password(user: str, pwd: str) -> bool:
    """
    Retorna True se (user, pwd) corresponderem ao que está em st.secrets.
    Aceita senha em texto puro ou hash bcrypt.
    """
    cred = st.secrets["credentials"]
    if user not in cred:
        return False
    stored = cred[user]
    # texto puro
    if stored == pwd:
        return True
    # hash bcrypt
    try:
        return bcrypt.checkpw(pwd.encode(), stored.encode())
    except ValueError:
        return False


def credenciais_inseridas() -> None:
    """Executa a validação e faz update no session_state."""
    user = st.session_state["user_input"].lower()
    pwd  = st.session_state["password_input"]
    ok = _check_password(user, pwd)
    st.session_state["authenticated"] = ok
    if ok:
        st.session_state["username"] = user
        st.toast("✅ Login realizado!", icon="✅")
    else:
        st.toast("❌ Usuário ou senha inválido", icon="❌")


def autenticar_usuario() -> bool:
    """
    Renderiza o card de login; devolve True se o usuário já estiver autenticado.
    """
    if st.session_state.get("authenticated"):
        return True

    st.image("logo.png", width=160)   # coloque seu logo

    st.markdown('<div class="login-card">', unsafe_allow_html=True)
    with st.form("login", clear_on_submit=False):
        st.text_input("Usuário", key="user_input")
        st.text_input("Senha", type="password", key="password_input")
        submitted = st.form_submit_button("Entrar")

    st.markdown('</div>', unsafe_allow_html=True)

    if submitted:
        st.toast("⏳ Verificando…", icon="⏳")
        credenciais_inseridas()
        st.rerun()

    return False

# ============================== CONFIGURAÇÕES ============================== #
TIPO_RELATORIO = 3
TEMPO_ESPERA = 30
PASTA_DESTINO = "download_XML"
CNPJ_MINAS_FIA = "FD11209172000196"
DATA_MARCA_DAGUA_STR = "02/01/2024"
DATA_MARCA_DAGUA_API = "2024-01-02"

FUNDOS = {
    CNPJ_MINAS_FIA: {
        "nome": "MINAS FIA",
        "cota_inicio": 1.9477472,
        "cota_ytd": 1.8726972,
        "marca_dagua": 3.0196718,
    },
    "FD48992682000192": {"nome": "ALFA HORIZON FIA"},
    "FD60096402000163": {"nome": "MINAS DIVIDENDOS FIA"},
    "FD52204085000123": {"nome": "MINAS ONE FIA"},
}
COLUNAS_EXIBIDAS = ["Ticker", "Quantidade de Ações", "Preço Ontem (R$)", "Preço Hoje (R$)", "% no Fundo",
                    "Variação Preço (%)", "Variação Ponderada (%)"]


# ============================== FUNÇÕES DE PROCESSAMENTO DE DADOS ============================== #
@st.cache_data(show_spinner="Obtendo carteiras do dia do BTG (só na 1ª vez)...", ttl=86400)
def obter_dados_base_do_dia(data_str: str):
    token = gerar_token()
    if not token: return {}
    ticket = gerar_ticket(token, data_str)
    mapeamento_xmls = baixar_xmls(token, ticket)

    dados_base = {}
    if mapeamento_xmls:
        for cnpj, xml_path in mapeamento_xmls.items():
            df_base, cota_ontem, qtd_cotas, pl = extrair_xml(xml_path)
            dados_base[cnpj] = {
                "df_base": df_base, "cota_ontem": cota_ontem,
                "qtd_cotas": qtd_cotas, "pl": pl
            }
    return dados_base


@st.cache_data(ttl=86400)
def get_cdi_acumulado(data_inicio: str, data_fim: str) -> float:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_fim}"
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            dados_cdi = response.json()
            if not dados_cdi: return 0.0
            fator_acumulado = 1.0
            for dado in dados_cdi:
                fator_acumulado *= (1 + (float(dado['valor']) / 100))
            return fator_acumulado - 1
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(1)
            else:
                st.error(f"Erro ao buscar dados do CDI após 3 tentativas: {e}")
    return 0.0


@st.cache_data(ttl=86400)
def get_ibov_acumulado(data_inicio: str, data_fim: str) -> float:
    try:
        dados_ibov = yf.download('^BVSP', start=data_inicio, end=data_fim, progress=False, auto_adjust=True)
        if dados_ibov.empty or len(dados_ibov) < 2: return 0.0
        preco_inicio = float(dados_ibov['Close'].iloc[0])
        preco_fim = float(dados_ibov['Close'].iloc[-1])
        return (preco_fim / preco_inicio) - 1
    except Exception as e:
        st.error(f"Erro ao buscar dados do IBOV: {e}")
        return 0.0


def recalcular_metricas(df_base, cota_ontem, qtd_cotas, pl):
    with st.spinner("Buscando preços atuais no Yahoo Finance..."):
        df = df_base.copy()
        df["Preço Hoje (R$)"] = df["Ticker"].map(lambda t: yf.Ticker(f"{t}.SA").info.get("regularMarketPrice", None))
    df["Variação Preço (%)"] = (df["Preço Hoje (R$)"] / df["Preço Ontem (R$)"] - 1).fillna(0)
    df["Valor Hoje (R$)"] = df["Quantidade de Ações"] * df["Preço Hoje (R$)"]
    valor_hoje = df["Valor Hoje (R$)"].fillna(0).sum()
    df["% no Fundo"] = df["Valor Hoje (R$)"] / valor_hoje if valor_hoje != 0 else 0
    df["Variação Ponderada (%)"] = df["Variação Preço (%)"] * df["% no Fundo"]
    valor_ontem, comp_fixos = df["Valor Ontem (R$)"].sum(), pl - df["Valor Ontem (R$)"].sum()
    patrimonio = valor_hoje + comp_fixos
    cota_hoje = patrimonio / qtd_cotas if qtd_cotas != 0 else 0
    var_cota = cota_hoje / cota_ontem - 1 if cota_ontem != 0 else 0
    return {"df": df, "cota_hoje": cota_hoje, "var_cota": var_cota,
            "extras": {"valor_ontem": valor_ontem, "valor_hoje": valor_hoje, "comp_fixos": comp_fixos,
                       "patrimonio": patrimonio, "qtd_cotas": qtd_cotas}}


# ============================== FUNÇÕES AUXILIARES ============================== #
def ultimo_dia_util(delay: int = 1) -> str:
    cal, d = Brazil(), pd.Timestamp.now(tz="America/Sao_Paulo") - timedelta(days=delay)
    while not cal.is_working_day(d.date()): d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


@st.cache_data(ttl=3600)
def gerar_token():
    if "senha_af" not in st.secrets:
        st.error("A chave 'senha_af' não foi encontrada nos segredos do Streamlit.")
        return None
    try:
        resp = requests.post("https://funds.btgpactual.com/connect/token",
                             headers={"Content-Type": "application/x-www-form-urlencoded"},
                             data= st.secrets["senha_af"])
        resp.raise_for_status()
        return resp.json()["access_token"]
    except requests.RequestException as e:
        st.error(f"Falha ao obter token do BTG: {e}")
        return None


def gerar_ticket(token, data):
    payload = json.dumps({"contract": {"startDate": data, "endDate": data, "typeReport": f"{TIPO_RELATORIO}"}})
    resp = requests.post("https://funds.btgpactual.com/reports/Portfolio",
                         headers={"X-SecureConnect-Token": f"Bearer {token}", "Content-Type": "application/json"},
                         data=payload)
    return resp.json()["ticket"]


def baixar_xmls(token, ticket) -> dict[str, str]:
    os.makedirs(PASTA_DESTINO, exist_ok=True)
    url = f"https://funds.btgpactual.com/reports/Ticket?ticketId={ticket}"
    time.sleep(TEMPO_ESPERA)
    resp = requests.get(url, headers={"X-SecureConnect-Token": f"Bearer {token}"})
    mapeamento = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(PASTA_DESTINO)
        for nome in os.listdir(PASTA_DESTINO):
            caminho, cnpj_arquivo = os.path.join(PASTA_DESTINO, nome), nome.split("_")[0]
            if cnpj_arquivo in FUNDOS:
                mapeamento[cnpj_arquivo] = caminho
            else:
                os.remove(caminho)
    except (zipfile.BadZipFile, KeyError):
        st.error("❌ ZIP inválido ou indisponível no BTG. Tente novamente mais tarde.")
    return mapeamento


def extrair_xml(path):
    root = ET.parse(path).getroot()
    head = root.find(".//header")
    cota_ontem, qtd_cotas, pl = float(head.findtext("valorcota")), float(head.findtext("quantidade")), float(
        head.findtext("patliq"))
    linhas = [{"Ticker": ac.findtext("codativo").strip(), "Quantidade de Ações": float(ac.findtext("qtdisponivel")),
               "Preço Ontem (R$)": float(ac.findtext("puposicao")),
               "Valor Ontem (R$)": float(ac.findtext("qtdisponivel")) * float(ac.findtext("puposicao"))} for ac in
              root.findall(".//acoes")]
    return pd.DataFrame(linhas), cota_ontem, qtd_cotas, pl


def css_var(v):
    if isinstance(v, (float, int)):
        if v > 0: return "color: green;"
        if v < 0: return "color: red;"
    return ""




def add_custom_css():
    st.markdown(
        """
        <style>

         /* Alterar a cor de todo o texto na barra lateral */
        section[data-testid="stSidebar"] * {
            color: White; /* Cor padrão para textos na barra lateral */
        }

        div[class="stDateInput"] div[class="st-b8"] input {
                color: white;
                }
            div[role="presentation"] div{
            color: white;
            }

        div[data-baseweb="calendar"] button  {
            color:white;
            }
            
        /* Alterar a cor do texto no campo de entrada do st.number_input */
        input[data-testid="stNumberInput-Input"] {
            color: black !important; /* Define a cor do texto como preto */
        }

        input[data-testid="stNumberInputField"] {
            color: black !important; /* Define a cor do texto como preto */
            }

        /* Estiliza os botões de incremento e decremento */
        button[data-testid="stNumberInputStepDown"], 
        button[data-testid="stNumberInputStepUp"] {
            color: black !important; /* Define a cor do ícone ou texto como preto */
            fill: black !important;  /* Caso o ícone SVG precise ser estilizado */
        }

        /* Estiliza o ícone dentro dos botões */
        button[data-testid="stNumberInputStepDown"] svg, 
        button[data-testid="stNumberInputStepUp"] svg {
            fill: black !important;  /* Garante que os ícones sejam pretos */
        }
        

    /* Estiliza o fundo do container do multiselect */
        div[class="st-ak st-al st-bd st-be st-bf st-as st-bg st-bh st-ar st-bi st-bj st-bk st-bl"] {
            background-color: White !important; /* Altera o fundo para cinza */
        }

        /* Estiliza o fundo do input dentro do multiselect */
        div[class="st-al st-bm st-bn st-bo st-bp st-bq st-br st-bs st-bt st-ak st-bu st-bv st-bw st-bx st-by st-bi st-bj st-bz st-bl st-c0 st-c1"] input {
            background-color: White !important; /* Altera o fundo do campo de entrada para cinza */
        }

        /* Estiliza o fundo do botão ou elemento de "Escolher uma opção" */
        div[class="st-cc st-bn st-ar st-cd st-ce st-cf"] {
            background-color: White !important; /* Altera o fundo do botão de opção para cinza */
        }

        /* Estiliza o ícone dentro do botão de decremento */
        button[data-testid="stNumberInput-StepDown"] svg {
            fill: black !important; /* Garante que o ícone seja preto */
            
            div[data-testid="stNumberInput"] input {
            color: black; /* Define o texto como preto */
        }
        
        div[data-testid="stDateInput"] input {
            color: black;
        };
        </style>

        
        """,
        unsafe_allow_html=True,
    )



# ============================== INTERFACE STREAMLIT ============================== #
st.set_page_config("Carteiras RV AF INVEST", layout="wide")

if autenticar_usuario():
    add_custom_css()
    data_carteira_str = ultimo_dia_util()
    data_formatada = datetime.strptime(data_carteira_str, '%Y-%m-%d').strftime('%d/%m/%Y')
    st.title(f"Carteiras RV AF INVEST - {data_formatada}")

    st.write(f"Usuário: **{st.session_state.get('username', '').capitalize()}**")

    st.session_state.setdefault('dados_calculados_cache', {})
    st.session_state.setdefault('last_update_time', {})

    dados_base_do_dia = obter_dados_base_do_dia(ultimo_dia_util())

    if not dados_base_do_dia:
        st.error(
            "Não foi possível obter os dados da carteira do BTG. Verifique os CNPJs ou a disponibilidade no portal.")
        
        if st.button("🔄 Tentar buscar dados do BTG novamente"):
            st.cache_data.clear()
            st.rerun()
    else:
        nomes_fundos = {cnpj: FUNDOS[cnpj]["nome"] for cnpj in dados_base_do_dia.keys()}
        cnpj_selecionado = st.sidebar.selectbox("Selecione o fundo para visualizar:", options=list(nomes_fundos.keys()),
                                        format_func=lambda c: nomes_fundos[c], key="fundo_selectbox")

        col_header, col_actions = st.columns([3, 2])
        with col_header:
            st.subheader(f"📊 Tabela — {FUNDOS[cnpj_selecionado]['nome']}")
        with col_actions:
            btn1, btn2 = st.columns(2)
            
            with btn1:
                atualizar = st.sidebar.button("🔄 Atualizar Preços")
                if st.session_state.last_update_time.get(cnpj_selecionado):
                    st.sidebar.caption(f"Preços atualizados às {st.session_state.last_update_time[cnpj_selecionado]:%H:%M:%S}")

            with btn2:
                if st.sidebar.button("📥 Puxar Carteira BTG"):
                    with st.sidebar.spinner("Limpando cache e buscando novamente os dados do BTG..."):
                        st.cache_data.clear()
                    st.rerun()
                st.sidebar.caption("Puxe quando o preço D-1 parecer estranho.")


        if atualizar or cnpj_selecionado not in st.session_state.dados_calculados_cache:
            dados_base_fundo = dados_base_do_dia[cnpj_selecionado]
            resultados = recalcular_metricas(dados_base_fundo["df_base"], dados_base_fundo["cota_ontem"],
                                              dados_base_fundo["qtd_cotas"], dados_base_fundo["pl"])
            st.session_state.dados_calculados_cache[cnpj_selecionado] = resultados
            st.session_state.last_update_time[cnpj_selecionado] = datetime.now(tz=ZoneInfo("America/Sao_Paulo"))
            st.rerun()

        if cnpj_selecionado in st.session_state.dados_calculados_cache:
            dados_calculados, cota_ontem_base = st.session_state.dados_calculados_cache[cnpj_selecionado], \
            dados_base_do_dia[cnpj_selecionado]['cota_ontem']
            df_final = dados_calculados["df"]

            fmt = {"Quantidade de Ações": "{:,.0f}", "Preço Ontem (R$)": "R$ {:.2f}", "Preço Hoje (R$)": "R$ {:.2f}",
                   "% no Fundo": "{:.2%}", "Variação Preço (%)": "{:.2%}", "Variação Ponderada (%)": "{:.2%}"}
            st.dataframe(
                df_final[COLUNAS_EXIBIDAS].sort_values("% no Fundo", ascending=False).style.format(fmt).map(css_var,
                                                                                                           subset=[
                                                                                                               "Variação Preço (%)",
                                                                                                               "Variação Ponderada (%)"]),
                use_container_width=True, hide_index=True)

            c1, c2, c3 = st.columns(3)
            c1.metric("Cota de Ontem", f"R$ {cota_ontem_base:.6f}")
            c2.metric("Cota Estimada Hoje", f"R$ {dados_calculados['cota_hoje']:.6f}")
            c3.metric("Variação da Cota", f"{dados_calculados['var_cota']:.4%}")

            if cnpj_selecionado == CNPJ_MINAS_FIA:
                st.divider()
                cota_hoje = dados_calculados['cota_hoje']
                ref_minas_fia = FUNDOS[CNPJ_MINAS_FIA]

                rent_ytd = (cota_hoje / ref_minas_fia['cota_ytd'] - 1) if ref_minas_fia['cota_ytd'] > 0 else 0
                rent_inicio = (cota_hoje / ref_minas_fia['cota_inicio'] - 1) if ref_minas_fia['cota_inicio'] > 0 else 0

                hoje_str, hoje_dt = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime('%d/%m/%Y'), datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d')
                cdi_acumulado = get_cdi_acumulado(data_inicio="15/10/2020", data_fim=hoje_str)
                ibov_acumulado_inicio = get_ibov_acumulado(data_inicio="2020-10-15", data_fim=hoje_dt)

                percentual_cdi = rent_inicio - cdi_acumulado

                marca_dagua = ref_minas_fia['marca_dagua']
                falta_marca_dagua = (marca_dagua / cota_hoje - 1) if cota_hoje > 0 else 0

                ibov_desde_marca_dagua = get_ibov_acumulado(data_inicio=DATA_MARCA_DAGUA_API, data_fim=hoje_dt)
                falta_total = falta_marca_dagua + ibov_desde_marca_dagua

                st.subheader("Análise de Rentabilidade — MINAS FIA")

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Rent. YTD", f"{rent_ytd:.2%}")
                m2.metric("Rent. Início (15/10/20)", f"{rent_inicio:.2%}")
                m3.metric("CDI no período (15/10/20)", f"{cdi_acumulado:.2%}")
                m4.metric("IBOV no período (15/10/20)", f"{ibov_acumulado_inicio:.2%}")

                md_label = f"M. d'Água ({DATA_MARCA_DAGUA_STR})"
                col_md_1, col_md_2, col_md_3 = st.columns(3)
                col_md_1.metric(f"Falta p/ {md_label}", f"{falta_marca_dagua:.2%}")
                col_md_2.metric(f"IBOV desde {md_label}", f"{ibov_desde_marca_dagua:.2%}")
                col_md_3.metric(f"Falta p/ {md_label} + IBOV", f"{falta_total:.2%}")

                texto_relativo_cdi = "acima do CDI" if percentual_cdi >= 0 else "abaixo do CDI"
                valor_display_cdi = f"{abs(percentual_cdi):.2%} {texto_relativo_cdi}"
                st.metric("Performance vs CDI", valor_display_cdi, delta=f"{percentual_cdi:.2%}", delta_color="off")

            with st.expander("🔍 Parâmetros do Cálculo"):
                ex = dados_calculados["extras"]
                st.write(f"📌 Valor das ações ontem: R$ {ex['valor_ontem']:,.2f}")
                st.write(f"📌 Valor das ações hoje:  R$ {ex['valor_hoje']:,.2f}")
                st.write(f"📎 Componentes fixos:     R$ {ex['comp_fixos']:,.2f}")
                st.write(f"💼 Patrimônio estimado:  R$ {ex['patrimonio']:,.2f}")
                st.write(f"🧮 Quantidade de cotas:  {ex['qtd_cotas']:,.2f}")


