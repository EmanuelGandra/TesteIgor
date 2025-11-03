# app.py
# Dashboard reduzido: casar posições, capturar taxa efetiva, filtrar %CDI≠100% (não-IPCA),
# montar tabela com variáveis necessárias e exibir curva DI interpolada com data de referência.

# -*- coding: utf-8 -*-
import io
import re
from pathlib import Path
from typing import Optional, Tuple
import requests

import numpy as np
import pandas as pd
import streamlit as st
from pandas.tseries.offsets import BDay
from functools import lru_cache
import re as _re

st.set_page_config(page_title="PU %CDI — Casamento + Curva DI", layout="wide")

# --------------------------- Paths ---------------------------
DADOS_DIR = Path("Dados"); DADOS_DIR.mkdir(exist_ok=True)
REL_PATH  = None  # se None, o app tenta achar "Relatório de Posição*.xlsx" ao lado
PDF_DIR   = Path("Dados/saida_pdf_cetip_consolidado")
PDF_CSV   = PDF_DIR / "consolidado_pdfs_codativos.csv"
PDF_XLSX  = PDF_DIR / "consolidado_pdfs_codativos.xlsx"
MTM_XLSX  = DADOS_DIR / "ativos_mapeados_com_taxa_efetiva.xlsx"
JUROS_XLSX= DADOS_DIR / "DadosJuros.xlsx"    # mesma estrutura já usada antes
CURVA_DU_PARQUET_DEFAULT = DADOS_DIR / "curva_di_interpolada_por_DU.parquet"
CURVA_DU_PARQUET = (Path("/mnt/data/curva_di_interpolada_por_DU.parquet")
                    if Path("/mnt/data/curva_di_interpolada_por_DU.parquet").exists()
                    else CURVA_DU_PARQUET_DEFAULT)


CDI_AA_DEFAULT = 0.12  # fallback 12% a.a.
MOTOR_PCT_CDI      = "PCT_CDI"        # usa CDI como indexador; pode ser bullet (incorpora) ou cupom
MOTOR_YTC_CHAMADAS = "YTC_CHAMADAS"   # fluxo até próxima CALL (YTC), com % do indexador no desconto

# --------------------------- Helpers ---------------------------
# ========= Exceções (planilha manual) =========
EXC_XLSX = "Dados/Tratamento Exceções.xlsx"

def add_custom_css():
    st.markdown(
        """
        <style>
        /* ===========================
           Sidebar: texto branco geral
        ============================ */
        section[data-testid="stSidebar"] * {
          color: #fff !important;
        }

        /* ===========================
           Global — Select/Multiselect
           (BaseWeb: data-baseweb="select")
        ============================ */

        /* Texto digitado no input do select + placeholder */
        [data-baseweb="select"] input[role="combobox"] {
          color: #fff !important;
          caret-color: #fff !important;
          background: transparent !important;
        }
        [data-baseweb="select"] input[role="combobox"]::placeholder {
          color: rgba(255,255,255,0.75) !important;
        }

        /* Valor “pílula” mostrado quando há um item selecionado (single select) */
        [data-baseweb="select"] div[value] {
          color: #fff !important;
        }

        /* Tags do multiselect (texto do chip e ícone de “x”) */
        [data-baseweb="tag"] *,
        [data-baseweb="select"] [data-baseweb="tag"] * {
          color: #fff !important;
          fill: #fff !important;
        }

        /* Ícones (seta, clear, etc.) */
        [data-baseweb="select"] svg {
          color: #fff !important;
          fill: #fff !important;
        }

        /* Opções selecionadas (quando a lista abre). 
           OBS: se o menu tiver fundo claro, remova este bloco. */
        [role="listbox"] [aria-selected="true"] {
          color: #fff !important;
        }

        /* ===========================
           Calendário/DateInput
        ============================ */
        div[data-baseweb="calendar"] button,
        div[data-testid="stDateInput"] input {
          color: #fff !important;
          background: transparent !important;
          caret-color: #fff !important;
        }

        /* ===========================
           NumberInput
           (mantive seu padrão: texto preto no campo, preto nos botões)
        ============================ */
        input[data-testid="stNumberInput-Input"],
        input[data-testid="stNumberInputField"] {
          color: #000 !important;
          background: #fff !important;
        }
        button[data-testid="stNumberInputStepDown"],
        button[data-testid="stNumberInputStepUp"] {
          color: #000 !important;
          fill: #000 !important;
        }
        button[data-testid="stNumberInputStepDown"] svg,
        button[data-testid="stNumberInputStepUp"] svg {
          fill: #000 !important;
        }

        /* ===========================
           Container do multiselect (fundo claro)
           — ajuste se estiver usando tema escuro total
        ============================ */
        [data-baseweb="select"] [role="combobox"] {
          background: transparent !important; /* ou #1e1e1e no tema dark */
        }

        /* Evita que alguma regra específica do tema sobrescreva */
        [data-baseweb="select"] * {
          text-shadow: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

# Chame assim no início do app:
add_custom_css()

def _warn_once(msg: str, key: str = "_cdi_fallback_warned"):
    # Evita flood de mensagens no Streamlit
    if not st.session_state.get(key, False):
        st.warning(msg)
        st.session_state[key] = True

def _load_cdi_cached_series_once() -> pd.Series:
    """Lê 'cdi_cached.csv' só uma vez por sessão (cache em session_state)."""
    if "_cdi_cached_series" in st.session_state:
        return st.session_state["_cdi_cached_series"]

    from pathlib import Path
    import pandas as pd

    for p in [Path("Dados")/"cdi_cached.csv", Path("cdi_cached.csv")]:
        if p.exists():
            df = pd.read_csv(p, sep=None, engine="python")
            cols = [str(c) for c in df.columns]
            date_col = next((c for c in cols if c.strip().lower() == "data"), cols[0])
            val_candidates = [c for c in cols if c != date_col]
            val_col = next((c for c in val_candidates if "cdi" in c.lower()),
                           (val_candidates[0] if val_candidates else None))
            if not val_col:
                return pd.Series(dtype=float)

            df["Data"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce").dt.normalize()
            vals = (df[val_col].astype(str)
                          .str.replace(".", "", regex=False)   # milhar
                          .str.replace(",", ".", regex=False)) # decimal
            df["valor"] = pd.to_numeric(vals, errors="coerce")
            ser = (df.dropna(subset=["Data","valor"])
                     .sort_values("Data")
                     .drop_duplicates(subset=["Data"], keep="last")
                     .set_index("Data")["valor"] / 100.0)
            st.session_state["_cdi_cached_series"] = ser.astype(float)
            return st.session_state["_cdi_cached_series"]
    return pd.Series(dtype=float)

@st.cache_data
def _formacdi_norm(x: str) -> str:
    t = (str(x or "")).upper().replace(" ", "")
    if "CDI+" in t or t == "CDI+":  return "CDI+"
    if "%CDI" in t or t in {"PCTCDI","CDI"}: return "%CDI"
    if "IPCA+" in t:  return "IPCA+"
    if "IPCA"  in t:  return "IPCA"
    if "PREFIXADO" in t: return "PREFIXADO"
    return t or "UNKNOWN"
# ---------- Exceções ----------
EXC_XLSX = Path("Dados/Tratamento Exceções.xlsx")

@st.cache_data
def _formacdi_norm(x: str) -> str:
    t = (str(x or "")).upper().replace(" ", "")
    if "CDI+" in t or t == "CDI+":  return "CDI+"
    if "%CDI" in t or t in {"PCTCDI","CDI"}: return "%CDI"
    if "IPCA+" in t:  return "IPCA+"
    if "IPCA"  in t:  return "IPCA"
    if "PREFIXADO" in t: return "PREFIXADO"
    return t or "UNKNOWN"

@st.cache_data
def load_exceptions_df(path: Path = EXC_XLSX) -> pd.DataFrame:
    if not Path(path).exists():
        return pd.DataFrame(columns=[
            "cod_Ativo_norm","FormaCDI_final","pct_flutuante_final","CicloJuros_final",
            "Data_Prox_Juros","Data_Emissao","Vencimento_final","Data_Call_Inicial",
            "alpha_norm","PU_emissao_final",
        ])
    df = pd.read_excel(path, dtype=str)
    ren = {}
    for c in df.columns:
        k = strip_accents(str(c)).strip().lower().replace("_", " ").replace("-", " ")
        k = re.sub(r"\s+", " ", k)
        if k == "codigo":                        ren[c] = "Codigo"
        elif k == "tipo":                        ren[c] = "TIPO"
        elif k == "pct flutuante final":         ren[c] = "pct_flutuante_final"
        elif k == "ciclojuros final":            ren[c] = "CicloJuros_final"
        elif k in {"data prox juros","data proximo juros"}: ren[c] = "Data_Prox_Juros"
        elif k == "data emissao":                ren[c] = "Data_Emissao"
        elif k == "vencimento final":            ren[c] = "Vencimento_final"
        elif k == "data call inicial":           ren[c] = "Data_Call_Inicial"
        elif ("beta" in k) and ("indexador" in k or "desconto" in k or "pv" in k): ren[c] = "alpha_norm"
        elif ("pu" in k and "emissao" in k) or k in {"pu emissao","pu de emissao"}: ren[c] = "PU_emissao_final"
    if ren: df = df.rename(columns=ren)

    needed = ["Codigo","TIPO","pct_flutuante_final","CicloJuros_final","Data_Prox_Juros",
              "Data_Emissao","Vencimento_final","Data_Call_Inicial","alpha_norm","PU_emissao_final"]
    for c in needed:
        if c not in df.columns: df[c] = None

    df["cod_Ativo_norm"] = df["Codigo"].map(norm_code)
    df["FormaCDI_final"] = df["TIPO"].map(_formacdi_norm)
    for numcol in ["pct_flutuante_final","alpha_norm","PU_emissao_final"]:
        df[numcol] = df[numcol].map(_to_float)
    for dc in ["Data_Prox_Juros","Data_Emissao","Vencimento_final","Data_Call_Inicial"]:
        df[dc] = pd.to_datetime(df[dc], errors="coerce")
    keep = ["cod_Ativo_norm","FormaCDI_final","pct_flutuante_final","CicloJuros_final",
            "Data_Prox_Juros","Data_Emissao","Vencimento_final","Data_Call_Inicial",
            "alpha_norm","PU_emissao_final"]
    out = (df[keep].dropna(subset=["cod_Ativo_norm"])
                .drop_duplicates(subset=["cod_Ativo_norm"], keep="last")
                .reset_index(drop=True))
    return out

@st.cache_data
def apply_exceptions_to_pdf_idx(pdf_idx: pd.DataFrame, exc: pd.DataFrame) -> pd.DataFrame:
    base = pdf_idx.copy()
    if exc.empty:
        base.attrs["__debug__"] = {"applied": False, "stats": {}, "in_both": [], "only_exc": [], "added_rows": 0}
        return base

    # garantir colunas alvo
    for c in ["FormaCDI_final","pct_flutuante_final","CicloJuros_final",
              "Data_Prox_Juros_final","Data_Emissao_final","Vencimento_final","Data_Call_Inicial_final",
              "alpha_norm","PU_emissao_final"]:
        if c not in base.columns:
            base[c] = None

    if "cod_Ativo_norm" not in base.columns:
        raise KeyError("pdf_idx não possui coluna 'cod_Ativo_norm'.")
    if "cod_Ativo_norm" not in exc.columns:
        raise KeyError("exc_df não possui coluna 'cod_Ativo_norm'.")

    # interseções
    in_both = sorted(set(base["cod_Ativo_norm"]) & set(exc["cod_Ativo_norm"]))
    only_exc = sorted(set(exc["cod_Ativo_norm"]) - set(base["cod_Ativo_norm"]))

    # mapeamento de nomes: exceção -> destino no índice
    colmap = [
        ("FormaCDI_final",     "FormaCDI_final"),
        ("pct_flutuante_final","pct_flutuante_final"),
        ("CicloJuros_final",   "CicloJuros_final"),
        ("Data_Prox_Juros",    "Data_Prox_Juros_final"),
        ("Data_Emissao",       "Data_Emissao_final"),
        ("Vencimento_final",   "Vencimento_final"),
        ("Data_Call_Inicial",  "Data_Call_Inicial_final"),
        ("alpha_norm",         "alpha_norm"),
        ("PU_emissao_final",   "PU_emissao_final"),
    ]

    m = base.set_index("cod_Ativo_norm")
    e = exc.set_index("cod_Ativo_norm")

    debug_stats = {}

    # ----------------- 1) INTERSEÇÃO: limpar e sobrescrever forte -----------------
    if in_both:
        e = e.loc[in_both].copy()  # <<<<<<<<<<<<<< restringe à interseção

        # constrói um 'e2' já com nomes de destino
        e2 = pd.DataFrame(index=e.index)
        for col_exc, col_pdf in colmap:
            if col_exc in e.columns:
                e2[col_pdf] = e[col_exc]
            else:
                e2[col_pdf] = pd.Series(index=e.index, dtype="object")
                
        # normaliza tokens de limpeza
        clear_tokens = {"", "-", "NA", "N/A", "APAGAR", "CLEAR"}
        for col_exc, col_pdf in colmap:
            src = e.get(col_exc)
            if src is None:
                continue
            is_clear = src.astype(str).str.strip().str.upper().isin({t.upper() for t in clear_tokens})
            idx_clear = is_clear[is_clear].index
            if len(idx_clear):
                before = m.loc[idx_clear, col_pdf].copy() if col_pdf in m.columns else pd.Series(dtype="object")
                m.loc[idx_clear, col_pdf] = np.nan
                after  = m.loc[idx_clear, col_pdf].copy()
                debug_stats[col_pdf] = debug_stats.get(col_pdf, {"total_in_both": len(in_both), "changed": 0, "filled_from_null": 0, "kept_same": 0})
                debug_stats[col_pdf]["changed"] += int((before.astype(str).fillna("") != after.astype(str).fillna("")).sum())
                e2.loc[idx_clear, col_pdf] = np.nan

        # overwrite forte: só valores não-nulos de e2 substituem m
        before_all = m.loc[in_both, e2.columns].copy()
        
        # Substitui m.update() por m.loc[] para forçar a escrita de NaNs
        m.loc[in_both, e2.columns] = e2[e2.columns] 
        after_all = m.loc[in_both, e2.columns].copy()

        # estatísticas por coluna (agora compara o B/A total)
        for c in e2.columns:
            # Não usa mais +=, atribui o valor final da comparação B/A
            b = before_all[c]; a = after_all[c]
            b_str = b.astype(str).fillna("")
            a_str = a.astype(str).fillna("")
            
            changed = (b_str != a_str).sum()
            filled  = ((b.isna() | (b_str == "")) & (~a.isna()) & (a_str != "")).sum()
            same    = (b_str == a_str).sum()
            
            debug_stats[c] = {
                "total_in_both": len(in_both),
                "changed": int(changed),
                "filled_from_null": int(filled),
                "kept_same": int(same)
            }

        base = m.reset_index()
    else:
        for _, col_pdf in colmap:
            debug_stats[col_pdf] = {"total_in_both": 0, "changed": 0, "filled_from_null": 0, "kept_same": 0}

    # ----------------- 2) SOMENTE NA EXCEÇÃO: adicionar linhas novas -----------------
    added_rows = 0
    if only_exc:
        add = exc.loc[only_exc].copy()
        add = add.rename(columns={
            "Data_Prox_Juros":"Data_Prox_Juros_final",
            "Data_Emissao":"Data_Emissao_final",
            "Data_Call_Inicial":"Data_Call_Inicial_final",
        })
        for c in ["FormaCDI_final","pct_flutuante_final","CicloJuros_final",
                  "Data_Prox_Juros_final","Data_Emissao_final","Vencimento_final","Data_Call_Inicial_final",
                  "alpha_norm","PU_emissao_final",
                  "taxa_emissao_final","Emissor_pdf_ref","IncorporaJuros_final","AgendaJuros_final"]:
            if c not in add.columns:
                add[c] = None
        base = pd.concat([base, add], ignore_index=True)
        added_rows = len(add)

    base.attrs["__debug__"] = {
        "applied": True,
        "stats": debug_stats,
        "in_both": in_both,
        "only_exc": only_exc,
        "added_rows": added_rows,
        "colmap": colmap
    }
    return base.reset_index(drop=True)

def _naive(ts):
    """Normaliza para Timestamp sem tz e em meia-noite; retorna NaT se inválido."""
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return pd.NaT
    try:
        t = pd.Timestamp(ts)
    except Exception:
        return pd.NaT
    if pd.isna(t):
        return pd.NaT
    if getattr(t, "tz", None) is not None:
        t = t.tz_localize(None)
    return t.normalize()


## ==== Calendário: usa somente 'feriados_nacionais.xls' (leitura simples) ====
import pandas_market_calendars as mcal

cal = mcal.get_calendar("BVMF")  # fallback (não usado quando o arquivo existir)
FERIADOS_PATH = Path("feriados_nacionais.xls")

@lru_cache(maxsize=1)
def _load_feriados_set() -> set:
    """
    Lê 'feriados_nacionais.xls' de forma simples:
      df = pd.read_excel('feriados_nacionais.xls')
      df = df.dropna()
      df = df[[df.columns[0]]]
      feriados = pd.to_datetime(..., dayfirst=True).dropna().dt.date.unique()
    Retorna um set(date).
    """
    if not FERIADOS_PATH.exists():
        return set()

    # leitura simples (como solicitado)
    try:
        df = pd.read_excel(FERIADOS_PATH)
    except Exception as e:
        st.warning(f"Falha ao ler '{FERIADOS_PATH.name}': {e}. Usando calendário B3 (fallback).")
        return set()

    if df.empty or len(df.columns) == 0:
        return set()

    # dropar NaNs e manter somente a PRIMEIRA coluna
    df = df.dropna()
    df = df[[df.columns[0]]]

    # parse DD/MM/AAAA com dayfirst
    feriados = (
        pd.to_datetime(df[df.columns[0]], errors="coerce", dayfirst=True)
          .dropna()
          .dt.normalize()
          .dt.date
          .unique()
    )
    return set(feriados.tolist())

def _using_file_calendar() -> bool:
    return (FERIADOS_PATH.exists() and (len(_load_feriados_set()) > 0))

def _is_file_bizday(ts) -> bool:
    t = _naive(ts)
    if t is pd.NaT: return False
    if t.weekday() >= 5: return False
    fer = _load_feriados_set()
    return t.date() not in fer

def fator_alpha_excel(fator_intervalo: float, alpha: float, D: int) -> float:
    if D <= 0: return 1.0
    r_eq = (float(fator_intervalo) ** (1.0 / D)) - 1.0
    return (1.0 + float(alpha) * r_eq) ** D

def df_beta_excel(fator_intervalo: float, beta: float, D: int) -> float:
    if D <= 0: return 1.0
    r_eq = (float(fator_intervalo) ** (1.0 / D)) - 1.0
    return (1.0 + float(beta) * r_eq) ** D  # DF > 1; VP = Fluxo / DF

def b3_next_session(d):
    """
    Próximo dia útil segundo:
      - se existir 'feriados_nacionais.xls': calendário do arquivo (2ª–6ª menos feriados do arquivo)
      - senão: sessões da B3 (pandas_market_calendars BVMF)
    """
    d0 = _naive(d)
    if d0 is pd.NaT:
        return pd.NaT

    if _using_file_calendar():
        for k in range(0, 3700):
            cand = d0 + pd.Timedelta(days=k)
            if _is_file_bizday(cand):
                return cand.normalize()
        return pd.NaT

    # fallback B3
    try:
        if len(cal.valid_days(d0, d0)) > 0:
            return d0
    except Exception:
        return pd.NaT
    for k in range(1, 33):
        cand = d0 + pd.Timedelta(days=k)
        try:
            if len(cal.valid_days(cand, cand)) > 0:
                return cand.normalize()
        except Exception:
            continue
    return pd.NaT

def b3_sessions(start, end):
    s = _naive(start); e = _naive(end)
    if s is pd.NaT or e is pd.NaT or e < s:
        return pd.DatetimeIndex([], dtype="datetime64[ns]")

    if _using_file_calendar():
        days = pd.date_range(s, e, freq="D")
        mask = [_is_file_bizday(x) for x in days]
        idx = pd.DatetimeIndex(days[mask]).normalize()
    else:
        try:
            days = cal.valid_days(s, e)
        except Exception:
            return pd.DatetimeIndex([], dtype="datetime64[ns]")
        idx = pd.to_datetime(days)
        try:
            idx = idx.tz_localize(None)
        except Exception:
            pass
        idx = idx.normalize()

    return idx

@st.cache_data(ttl=24*60*60)
def fetch_sgs_cdi_raw(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.Series:
    """
    Baixa a série bruta do SGS (12 = CDI % a.d.) no intervalo solicitado
    e retorna um pd.Series (index=Data normalizada, values=retorno diário em DECIMAL).
    NÃO reindexa para dias B3 e NÃO faz ffill: só dias publicados.
    """
    s = _naive(start_date); e = _naive(end_date)
    if s is pd.NaT or e is pd.NaT or e < s:
        return pd.Series(dtype=float)

    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
        f"?formato=json&dataInicial={s.strftime('%d/%m/%Y')}&dataFinal={e.strftime('%d/%m/%Y')}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        j = resp.json()
    except Exception:
        return pd.Series(dtype=float)

    if not j:
        return pd.Series(dtype=float)

    df = pd.DataFrame(j)
    df["Data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce").dt.normalize()
    df["valor"] = (
        df["valor"].astype(str).str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    df = df.dropna(subset=["Data","valor"]).sort_values("Data").set_index("Data")
    return (df["valor"] / 100.0).astype(float)


def b3_prev_session(d: pd.Timestamp) -> pd.Timestamp:
    """Sessão útil imediatamente anterior a 'd' segundo o calendário ativo do app."""
    t = _naive(d)
    if t is pd.NaT:
        return pd.NaT
    for k in range(1, 3700):
        cand = t - pd.Timedelta(days=k)
        s = b3_next_session(cand)
        if s is not pd.NaT and s < t:
            return s.normalize()
    return pd.NaT


def cdi_factor_sensitivity(start_date: pd.Timestamp,
                           end_date: pd.Timestamp,
                           pivot_date: pd.Timestamp | None = None,
                           allow_ffill_insert: bool = True):
    """
    Monta cenários de sensibilidade (±1 dia em início/fim e +/- 1 dia no meio).
    Retorna (df_resumo, dict_series_por_cenario)
    """
    s = _naive(start_date); e = _naive(end_date)
    base_factor, base_du, base_series = compute_cdi_factor_sgs(
            s, e, include_start=True, include_end=False, return_series=True
        )
    series_map: dict[str, pd.Series] = {"BASE_SGS": base_series.copy() if base_series is not None else pd.Series(dtype=float)}
    rows = [{
        "Cenario": "BASE_SGS", "Inicio": (base_series.index.min().date() if not series_map["BASE_SGS"].empty else None),
        "Fim": (base_series.index.max().date() if not series_map["BASE_SGS"].empty else None),
        "DU": base_du, "Fator": base_factor, "Obs": "Série SGS pura (dias publicados)."
    }]

    # --- helpers locais ---
    def _resumo(nome: str, ser: pd.Series, obs: str):
        if ser is None or ser.empty:
            fator = 1.0; ndu = 0
        else:
            fator = float(np.exp(np.log1p(ser.values).sum())); ndu = int(len(ser))
        rows.append({
            "Cenario": nome,
            "Inicio": (ser.index.min().date() if (ser is not None and not ser.empty) else None),
            "Fim": (ser.index.max().date() if (ser is not None and not ser.empty) else None),
            "DU": ndu, "Fator": fator, "Obs": obs
        })
        series_map[nome] = ser if ser is not None else pd.Series(dtype=float)

    ser_base = series_map["BASE_SGS"]

    # --- fim -1 / +1 ---
    if not ser_base.empty and len(ser_base) >= 2:
        _resumo("END_minus_1", ser_base.iloc[:-1].copy(), "Exclui o último dia publicado do intervalo.")
    elif not ser_base.empty:
        _resumo("END_minus_1", pd.Series(dtype=float), "Sem dados suficientes para -1 no fim.")

    # precisa do próximo dia publicado > fim
    ser_ext_plus = fetch_sgs_cdi_raw(s, e + pd.Timedelta(days=30))
    if not ser_base.empty:
        after = ser_ext_plus.index[ser_ext_plus.index > ser_base.index.max()]
        if len(after):
            nxt = after[0]
            _resumo("END_plus_1",
                    pd.concat([ser_base, ser_ext_plus.loc[[nxt]]]).sort_index(),
                    f"Inclui o próximo dia publicado após o fim ({nxt.date()}).")

    # --- início +1 / -1 ---
    if not ser_base.empty and len(ser_base) >= 2:
        _resumo("START_plus_1", ser_base.iloc[1:].copy(), "Exclui o primeiro dia publicado do intervalo.")
    elif not ser_base.empty:
        _resumo("START_plus_1", pd.Series(dtype=float), "Sem dados suficientes para +1 no início.")

    ser_ext_minus = fetch_sgs_cdi_raw(s - pd.Timedelta(days=30), e)
    if not ser_base.empty:
        before = ser_ext_minus.index[ser_ext_minus.index < ser_base.index.min()]
        if len(before):
            prv = before[-1]
            _resumo("START_minus_1",
                    pd.concat([ser_ext_minus.loc[[prv]], ser_base]).sort_index(),
                    f"Inclui o dia publicado imediatamente anterior ao início ({prv.date()}).")

    # --- meio: remover pivô ---
    if pivot_date is not None and not ser_base.empty:
        p = _naive(pivot_date)
        if p in ser_base.index:
            _resumo("DROP_pivot",
                    ser_base.drop(index=p).copy(),
                    f"Remove o dia no meio da série ({p.date()}).")
        else:
            # opcional: simular “inserir” um dia inexistente (feriado) com ffill do SGS
            if allow_ffill_insert:
                # pega o valor do último dia publicado anterior ao pivô
                ser_all = fetch_sgs_cdi_raw(s - pd.Timedelta(days=30), e + pd.Timedelta(days=30))
                prev = ser_all[ser_all.index < p]
                if not prev.empty and (p > ser_base.index.min()) and (p < ser_base.index.max()):
                    val = float(prev.iloc[-1])
                    ser_ins = pd.concat([ser_base, pd.Series([val], index=pd.DatetimeIndex([p]))]).sort_index()
                    _resumo("ADD_pivot_ffill",
                            ser_ins,
                            f"Insere 1 dia no meio ({p.date()}) usando ffill do SGS (simula erro de incluir feriado).")

    # --- DataFrame resumo + deltas vs base ---
    df = pd.DataFrame(rows)
    try:
        base = float(df.loc[df["Cenario"] == "BASE_SGS", "Fator"].iloc[0])
        df["Delta_vs_base"] = df["Fator"]/base - 1.0
    except Exception:
        df["Delta_vs_base"] = np.nan

    df = df.sort_values(["Cenario"]).reset_index(drop=True)
    return df, series_map


def _series_to_sheet(df_ser: pd.Series) -> pd.DataFrame:
    """Converte Series diária para planilha com Data, CDI_%a.d., CDI_decimal, G, DF."""
    if df_ser is None or df_ser.empty:
        return pd.DataFrame(columns=["Data","CDI_%a.d.","CDI_decimal","G","DF"])
    out = (pd.DataFrame({"Data": df_ser.index, "CDI_decimal": df_ser.values.astype(float)})
             .sort_values("Data")
             .reset_index(drop=True))
    out["CDI_%a.d."] = out["CDI_decimal"] * 100.0
    out["G"]  = (1.0 + out["CDI_decimal"]).cumprod()
    out["DF"] = 1.0 / out["G"]
    return out[["Data","CDI_%a.d.","CDI_decimal","G","DF"]]


def make_cdi_sensitivity_xlsx(start_date: pd.Timestamp,
                              end_date: pd.Timestamp,
                              pivot_date: pd.Timestamp | None = None) -> bytes:
    """
    Cria um XLSX com:
      • 'Resumo_Sensibilidade' (cenários, fatores e deltas)
      • 'Serie_BASE_SGS' + 0..N abas por cenário com a série usada em cada cálculo
    """
    resumo, series_map = cdi_factor_sensitivity(start_date, end_date, pivot_date=pivot_date)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        # Resumo
        resumo.to_excel(xw, index=False, sheet_name="Resumo_Sensibilidade")
        ws = xw.sheets["Resumo_Sensibilidade"]
        ws.set_column(0, 0, 22)  # Cenario
        ws.set_column(1, 3, 14)  # datas
        ws.set_column(4, 6, 14)  # DU/Fator
        ws.set_column(7, 7, 16)  # Delta
        ws.set_column(8, 8, 50)  # Obs

        # Séries
        for name, ser in series_map.items():
            sheet = f"Serie_{name}"[:31]
            _series_to_sheet(ser).to_excel(xw, index=False, sheet_name=sheet)
            w = xw.sheets[sheet]
            w.set_column(0, 0, 12)
            w.set_column(1, 4, 16)
    return buf.getvalue()

def _inject_session_if_missing(idx: pd.DatetimeIndex, asof) -> pd.DatetimeIndex:
    """
    Se 'asof' cair dentro do intervalo de 'idx' e não estiver presente,
    injeta 'asof' (normalizado) de forma determinística.
    """
    t = _naive(asof)
    if t is pd.NaT or len(idx) == 0:
        return idx
    t = t.normalize()
    if (t < idx.min()) or (t > idx.max()) or (t in idx):
        return idx
    return pd.DatetimeIndex(sorted(idx.append(pd.DatetimeIndex([t])).unique()))

def b3_first_session_of_month(year: int, month: int) -> Optional[pd.Timestamp]:
    start = pd.Timestamp(year, month, 1)
    end   = start + pd.offsets.MonthEnd(0)
    idx = b3_sessions(start, end)
    return (idx[0] if len(idx) else None)

def b3_range(d0: pd.Timestamp, d1: pd.Timestamp) -> pd.DatetimeIndex:
    """(d0, d1] — remove o 1º se for o mesmo que o next_session(d0)."""
    idx = b3_sessions(d0, d1)
    if len(idx) == 0:
        return idx
    start = b3_next_session(d0)
    if (start is not pd.NaT) and (idx[0] == start):
        return idx[1:]
    return idx

def b3_count(d0: pd.Timestamp, d1: pd.Timestamp) -> int:
    """Nº de dias úteis em [d0, d1] conforme calendário ativo."""
    return int(len(b3_sessions(d0, d1)))

def b3_count_excl(d0: pd.Timestamp, d1: pd.Timestamp) -> int:
    """Nº de dias úteis em (d0, d1] conforme calendário ativo."""
    idx = b3_sessions(d0, d1)
    if len(idx) == 0:
        return 0
    start = b3_next_session(d0)
    return len(idx) - (1 if (start is not pd.NaT and idx[0] == start) else 0)


def strip_accents(s: str) -> str:
    import unicodedata
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _to_float(x):
    if x is None: return None
    s = str(x).strip().replace("\u00a0", "").replace("%", "").strip()
    if s == "" or s.lower() in {"nan", "none"}: return None
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s: s = s.replace(",", ".")
    try: return float(s)
    except: return None

def _norm_percent_or_ratio(v) -> Optional[float]:
    """Transforma 119 -> 1.19, 1.19 -> 1.19, '119,5' -> 1.195, 100 -> 1.0."""
    if v is None: return None
    x = _to_float(v)
    if x is None: return None
    return (x/100.0) if x > 2.5 else x


def code_root(c: str) -> str:
    s = norm_code(c)
    # remove 1 letra final se houver (ex.: CDB725BEF4B → CDB725BEF4)
    return re.sub(r"([A-Z0-9]{6,})([A-Z])$", r"\1", s)

def extract_code_from_free_text(s: str) -> Optional[str]:
    if not s: return None
    t = strip_accents(str(s)).upper()
    m = re.search(r"[A-Z]{2,6}\d{3,12}[A-Z0-9]*", t)
    if m: return m.group(0)
    cand = re.sub(r"[^A-Z0-9]", "", t)
    return cand if 6 <= len(cand) <= 20 else None

def _is_ipca_forma(s: str) -> bool:
    ss = (str(s or "")).upper()
    return any(k in ss for k in ["IPCA", "IP CA"])

def _aa_to_daily_scalar(r_aa: float) -> float:
    return (1.0 + float(r_aa))**(1.0/252.0) - 1.0

def _bday_count(d0: pd.Timestamp, d1: pd.Timestamp) -> int:
    if pd.isna(d0) or pd.isna(d1) or d1 <= d0: return 0
    return len(pd.bdate_range(d0, d1, inclusive="right"))

# --------------------------- Leitura bases ---------------------------
def find_relatorio_path() -> Optional[Path]:
    files = sorted(Path(".").glob("Dados/Relatório de Posição 2025-10-17.xlsx"))
    return files[0] if files else None

@st.cache_data
def read_any(path: Path) -> pd.DataFrame:
    if (path is None) or (not path.exists()): return pd.DataFrame()
    if path.suffix.lower() == ".csv": return pd.read_csv(path, dtype=str, keep_default_na=False)
    return pd.read_excel(path, dtype=str)

@st.cache_data
def prepare_carteira(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.copy()
    mapping = {
        "Data": ["Data","data","DataRef","Data Ref"],
        "Emissor": ["Emissor","Nome do Emissor"],
        "Fundo": ["Fundo","Nome do Fundo"],
        "Ativo": ["Ativo","Código Ativo","Codigo Ativo"],
        "Vencimento do ativo": ["Vencimento do ativo","Vencimento","Vencimento Ativo"],
        "Quantidade": ["Quantidade","Qtde","Qtd"],
        "Pu Posição": ["Pu Posição","PU Posição","PU Posicao","PU Posição","PU"],
        "Valor": ["Valor","Valor Posição","ValorPosicao"],
        "Estratégia": ["Estratégia","Estrategia","Estrategia/Indexador"],
    }
    ren = {}
    for tgt, aliases in mapping.items():
        for a in aliases:
            if a in df.columns:
                ren[a] = tgt; break
    if ren: df = df.rename(columns=ren)
    if "Data" in df.columns: df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    if "Vencimento do ativo" in df.columns:
        df["Vencimento do ativo"] = pd.to_datetime(df["Vencimento do ativo"], errors="coerce", dayfirst=True)
    for col in ["Quantidade","Pu Posição","Valor"]:
        if col in df.columns: df[col + "_num"] = df[col].map(_to_float)
    df["cod_Ativo_guess"] = df.get("Ativo","").map(extract_code_from_free_text)
    df["cod_Ativo_guess_norm"] = df["cod_Ativo_guess"].map(lambda x: norm_code(x) if pd.notna(x) else "")
    return df

def generate_coupons_semester_until(emissao: pd.Timestamp,
                                      first_coupon: pd.Timestamp,
                                      end_inclusive: pd.Timestamp,
                                      months_step: int = 6):
    """
    Agenda semestral (6M) com regra 'modified following' B3,
    igual ao LFSN, TRUNCADA em end_inclusive (ex.: próxima CALL).

    Retorna [(ini, fim), ...] com 'fim' <= end_inclusive.
    """
    if any(pd.isna(x) for x in [emissao, end_inclusive]) or months_step <= 0:
        return []
    periods = generate_periods_semester_b3(emissao, first_coupon, end_inclusive,
                                           months_step=int(months_step))
    out = []
    for (ini, fim) in periods:
        if fim >= end_inclusive:
            # inclui o período FINAL truncado exatamente em end_inclusive
            out.append((ini, end_inclusive))
            break
        out.append((ini, fim))
    return out


@st.cache_data
def prepare_pdf_base(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    # --- renomeações amigáveis (case-insensitive) ---
    ren = {}
    for c in df.columns:
        cl = c.strip().lower()
        cl_norm = cl.replace("_", " ").strip()  # <<< pega nomes com underscore
        if cl_norm == "cod ativo":         ren[c] = "cod_Ativo"
        if cl_norm == "data emissao":    ren[c] = "Data Emissão"
        if cl_norm == "data proximo juros": ren[c] = "Data Proximo Juros"   # <<< aqui
        if cl_norm == "data call inicial": ren[c] = "Data Call Inicial"
        # vários nomes possíveis para "Incorpora Juros"
        if re.search(r"^incorpora(\s|_)*juros$", cl):
            ren[c] = "IncorporaJuros"
        if re.search(r"^agenda(\s|_)*juros$", cl): # <<< ALTERAÇÃO: Reconhecer a nova coluna
            ren[c] = "AgendaJuros"

    if ren:
        df = df.rename(columns=ren)

    # --- garantir colunas mínimas ---
    need = [
        "cod_Ativo","FormaCDI","pct_flutuante","taxa_emissão","PU_emissão",
        "Data Emissão","vencimento","CicloJuros", "AgendaJuros", "Data Proximo Juros","Data Call Inicial", # <<< ALTERAÇÃO: Adicionar AgendaJuros
        "Emissor","IncorporaJuros"  # nova
    ]
    for c in need:
        if c not in df.columns:
            df[c] = None

    # --- normalizações ---
    df["cod_Ativo_norm"] = df["cod_Ativo"].map(norm_code)

    for dc in ["Data Emissão","vencimento","Data Proximo Juros","Data Call Inicial"]:
        df[dc] = pd.to_datetime(df[dc], errors="coerce", dayfirst=True)

    def _formacdi(x: str) -> str:
        t = (x or "").upper().replace(" ", "")
        if "CDI+" in t:  return "CDI+"
        if "%CDI" in t or t in {"PCTCDI","CDI"}: return "%CDI"
        if "IPCA+" in t: return "IPCA+"
        if "IPCA" in t:  return "IPCA"
        return t or "UNKNOWN"
    df["FormaCDI"] = df["FormaCDI"].map(_formacdi)

    for c in ["pct_flutuante","taxa_emissão","PU_emissão"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- IncorporaJuros: normaliza se existir; senão tenta inferir ---
    def _norm_inc(v):
        s = strip_accents(str(v or "")).upper().strip()
        if s in {"SIM","S","YES","Y"}: return "SIM"
        elif s in {"NAO","NAO.","NAO/NAO","N","NO"}: return "NAO"
        else:
            return "SIM"
        return None

    # Se a coluna já existir (pelo rename acima), normaliza
    if "IncorporaJuros" in df.columns:
        df["IncorporaJuros"] = df["IncorporaJuros"].map(_norm_inc)

    # Heurística leve quando vier vazia/ausente:
    # - Se há Data Proximo Juros -> NAO (paga cupom, não capitaliza)
    # - Se sanity_notes mencionar 'incorpora_juros_nao' -> NAO
    # (mantemos None quando não dá pra inferir com segurança)
    if "sanity_notes" not in df.columns:
        df["sanity_notes"] = None

    def _infer_inc_row(r):
        if pd.notna(r.get("Data Proximo Juros")):
            return "NAO"
        sn = str(r.get("sanity_notes") or "").lower()
        if "incorpora_juros_nao" in sn:
            return "NAO"
        return None

    mask = df["IncorporaJuros"].isna()
    if mask.any():
        df.loc[mask, "IncorporaJuros"] = df.loc[mask].apply(_infer_inc_row, axis=1)

    return df

@st.cache_data
def build_pdf_code_index(pdf: pd.DataFrame) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame()

    def first_valid(s: pd.Series):
        x = s.dropna()
        return x.iloc[0] if not x.empty else None

    g = pdf.groupby("cod_Ativo_norm", dropna=False)
    out = g.apply(lambda d: pd.Series({
        "FormaCDI_final":        first_valid(d["FormaCDI"]),
        "pct_flutuante_final":   first_valid(d["pct_flutuante"]),
        "taxa_emissao_final":    first_valid(d["taxa_emissão"]),
        "PU_emissao_final":      first_valid(d["PU_emissão"]),
        "Data_Emissao_final":    first_valid(d["Data Emissão"]),
        "Vencimento_final":      first_valid(d["vencimento"]),
        "Data_Prox_Juros_final": first_valid(d["Data Proximo Juros"]),
        "Data_Call_Inicial_final": first_valid(d["Data Call Inicial"]),
        "CicloJuros_final":      first_valid(d["CicloJuros"]),
        "AgendaJuros_final":     first_valid(d["AgendaJuros"]), # <<< ALTERAÇÃO: Adicionar AgendaJuros_final
        "IncorporaJuros_final":  first_valid(d["IncorporaJuros"]),   # <<< NOVO
        "Emissor_pdf_ref":       first_valid(d["Emissor"]),
    })).reset_index(names="cod_Ativo_norm")

    return out

@st.cache_data
def cruzar(carteira: pd.DataFrame, pdf_idx: pd.DataFrame) -> pd.DataFrame:
    if carteira.empty:
        return pd.DataFrame()
    if pdf_idx.empty:
        out = carteira.copy()
        out["match_pdf"] = False
        return out

    # 1) merge exato pelo código
    m = carteira.merge(
        pdf_idx, how="left",
        left_on="cod_Ativo_guess_norm", right_on="cod_Ativo_norm",
        suffixes=("", "_pdf")
    )
    exact_ok = m["cod_Ativo_norm"].notna()

    # 2) fallback por raiz do código (só nos que não casaram)
    if (~exact_ok).any():
        car_fb = m.loc[~exact_ok, ["cod_Ativo_guess_norm"]].copy()
        car_fb["cod_root"] = car_fb["cod_Ativo_guess_norm"].map(code_root)

        idx_fb = pdf_idx.copy()
        idx_fb["cod_root"] = idx_fb["cod_Ativo_norm"].map(code_root)

        # se houver múltiplos no mesmo root, pegue o primeiro (ou escolha outra prioridade)
        idx_fb = idx_fb.sort_values("cod_Ativo_norm").drop_duplicates("cod_root")

        fb = car_fb.merge(idx_fb, on="cod_root", how="left", suffixes=("", "_fb"))
        rows = m.index[~exact_ok]
        for c in pdf_idx.columns:
            m.loc[rows, c] = fb[c].values

    m["match_pdf"] = m["cod_Ativo_norm"].notna()
    return m


# taxa efetiva MTM por ativo (opcional, para consulta)
@st.cache_data
def load_mtm_table() -> pd.DataFrame:
    if not MTM_XLSX.exists(): return pd.DataFrame()
    df = pd.read_excel(MTM_XLSX)
    for col in ["ISIN", "Ativo", "TAXA_EFETIVA"]:
        if col not in df.columns: df[col] = None
    def _norm_isin(x):
        if x is None: return ""
        return re.sub(r"[^A-Z0-9]", "", strip_accents(str(x)).upper().strip())
    def _norm_name(x):
        if x is None: return ""
        return re.sub(r"\s+", " ", strip_accents(str(x)).upper().strip())
    df["ISIN_norm"]  = df["ISIN"].map(_norm_isin)
    df["Ativo_norm"] = df["Ativo"].map(_norm_name)

    def _parse_aa_from_mtm(v) -> Optional[float]:
        if v is None or (isinstance(v, float) and np.isnan(v)): return None
        if isinstance(v, (int, float)): x = float(v)
        else:
            s = str(v).strip().replace("\u00a0", "")
            if s == "" or s.lower() in {"nan", "none"}: return None
            has_pct = "%" in s; s = s.replace("%","").strip()
            if "." in s and "," in s: s = s.replace(".","").replace(",",".")
            elif "," in s: s = s.replace(",",".")
            try: x = float(s)
            except: return None
            if has_pct: x = x/100.0
        while x > 10.0: x = x/100.0
        if x <= 0.0 or x > 5.0: return None
        return x

    df["taxa_aa_mtm"] = df["TAXA_EFETIVA"].map(_parse_aa_from_mtm)
    df = df[(df["taxa_aa_mtm"].notna()) & (df["ISIN_norm"].astype(bool) | df["Ativo_norm"].astype(bool))]
    return df.reset_index(drop=True)

@st.cache_data(ttl=24*60*60)
def load_cdi_sgs_daily(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.Series:
    """
    Baixa CDI (D) % a.d. do SGS (serie 12) e retorna r_d em DECIMAL por dia útil B3
    no intervalo [start_date, end_date], reindexado para as sessões da B3.
    Cache vence 1x por dia.
    """
    s = _naive(start_date); e = _naive(end_date)
    if s is pd.NaT or e is pd.NaT or e < s:
        return pd.Series(dtype=float)

    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
        f"?formato=json&dataInicial={s.strftime('%d/%m/%Y')}&dataFinal={e.strftime('%d/%m/%Y')}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        j = resp.json()
    except Exception:
        # Falha de rede/serviço → retorna vazio (caller trata fallback)
        return pd.Series(dtype=float)

    if not j:
        return pd.Series(dtype=float)

    df = pd.DataFrame(j)
    # colunas vêm como 'data' (DD/MM/YYYY) e 'valor' com vírgula decimal
    df["Data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df["valor"] = (
        df["valor"].astype(str).str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    df = df.dropna(subset=["Data","valor"]).sort_values("Data").set_index("Data")

    # Converte % a.d. → decimal ao dia
    cdi_daily = (df["valor"] / 100.0).astype(float)

    idx = b3_sessions(s, e)
    # >>> se por acaso idx veio vazio (falha de calendário), devolve vazio
    if len(idx) == 0:
        return pd.Series(dtype=float)
    # a série do SGS geralmente não tem “hoje” cedo; o ffill cobre isso
    return cdi_daily.reindex(idx).ffill()

@st.cache_data
def _juros_available_dates() -> pd.DataFrame:
    """
    Lê DadosJuros.xlsx e devolve DataFrame com as 'Datas' realmente utilizáveis
    e a contagem de vértices OD preenchidos (para mostrar na sidebar).
    """
    if not JUROS_XLSX.exists():
        return pd.DataFrame(columns=["Datas","n_vertices"]).astype({"n_vertices": int})
    try:
        tmp = pd.read_excel(JUROS_XLSX, dtype=str)
    except Exception:
        return pd.DataFrame(columns=["Datas","n_vertices"]).astype({"n_vertices": int})

    if tmp.shape[0] < 2 or tmp.shape[1] < 2:
        return pd.DataFrame(columns=["Datas","n_vertices"]).astype({"n_vertices": int})

    tmp = tmp.drop(tmp.index[1], errors="ignore")
    tmp = tmp.iloc[:, 1:].copy()
    tmp = tmp.rename(columns={tmp.columns[0]: "Datas"})
    tmp["Datas"] = pd.to_datetime(tmp["Datas"], errors="coerce")
    tmp.columns = [str(c).strip() for c in tmp.columns]

    # normaliza números
    for c in tmp.columns[1:]:
        col = (tmp[c].astype(str)
               .str.replace("\u00a0", "", regex=False).str.strip()
               .str.replace("%", "", regex=False).str.replace(",", ".", regex=False))
        tmp[c] = pd.to_numeric(col, errors="coerce")

    vertex_cols = [c for c in tmp.columns if c != "Datas" and str(c).upper().startswith("OD")]
    if not vertex_cols:
        return pd.DataFrame(columns=["Datas","n_vertices"]).astype({"n_vertices": int})

    tmp = tmp.dropna(subset=["Datas"]).sort_values("Datas")
    has_any = tmp[vertex_cols].notna().any(axis=1)
    df = (tmp.loc[has_any, ["Datas"]]
            .assign(n_vertices=tmp.loc[has_any, vertex_cols].notna().sum(axis=1)))
    return df.reset_index(drop=True)

# --------------------------- Curva DI (interp. DU) ---------------------------
@st.cache_data
def get_curve_ref_date_from_parquet() -> pd.Timestamp:
    if not CURVA_DU_PARQUET.exists():
        return pd.NaT
    df = pd.read_parquet(CURVA_DU_PARQUET)
    if df.empty or "ref_date" not in df.columns:
        return pd.NaT
    d = pd.to_datetime(df["ref_date"], errors="coerce").dt.normalize().dropna()
    return (d.max() if len(d) else pd.NaT)

@st.cache_data
def load_di_curve_daily(end_date: pd.Timestamp,
                        ref_date: pd.Timestamp,
                        forced_curve_date: pd.Timestamp | None = None) -> Tuple[pd.Timestamp, pd.Series]:
    """
    Lê a curva DI já interpolada por DU a partir de CURVA_DU_PARQUET e retorna:
      (curve_date_usado, série diária forward em (ref_date, end_date] indexada em sessões B3).
    Mantém a assinatura original para não quebrar o restante do app.

    Espera colunas no parquet:
      - ref_date  (Timestamp, normalizado)
      - data      (Timestamp, cada DU convertido para data útil)
      - DU        (int)
      - di_diaria_interp              (retorno diário em DECIMAL)
      - di_aa_252_interp_pct (opcional; se necessário, convertemos para diário)
    """
    def _fallback_series(base_date: pd.Timestamp, end_dt: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Series]:
        base_date = _naive(base_date); end_dt = _naive(end_dt)
        sess = b3_range(base_date, end_dt)
        if len(sess) == 0:
            return base_date, pd.Series(dtype=float)
        r_d = _aa_to_daily_scalar(CDI_AA_DEFAULT)
        return base_date, pd.Series([r_d] * len(sess), index=sess)

    base = _naive(ref_date)
    end_dt = _naive(end_date)
    if (base is pd.NaT) or (end_dt is pd.NaT) or (end_dt <= base):
        return base, pd.Series(dtype=float)

    if not CURVA_DU_PARQUET.exists():
        # parquet ausente → fallback simples
        return _fallback_series(base, end_dt)

    # Carrega parquet
    try:
        df = pd.read_parquet(CURVA_DU_PARQUET)
    except Exception:
        # erro de leitura → fallback
        return _fallback_series(base, end_dt)

    if df.empty or ("ref_date" not in df.columns) or ("data" not in df.columns):
        return _fallback_series(base, end_dt)

    # Normalizações de data
    df = df.copy()
    df["ref_date"] = pd.to_datetime(df["ref_date"], errors="coerce").dt.normalize()
    df["data"]     = pd.to_datetime(df["data"],     errors="coerce").dt.normalize()
    df = df.dropna(subset=["ref_date","data"])

    # Escolha da linha da curva:
    # - se 'forced_curve_date' vier da UI, tentamos usá-la;
    # - senão, usamos a última ref_date <= base.
    sel_base = _naive(forced_curve_date) if forced_curve_date is not None else base
    cands = df.loc[df["ref_date"] <= sel_base]
    if cands.empty:
        # Se não há <= sel_base, tentar o próprio base
        cands = df.loc[df["ref_date"] == base]
        if cands.empty:
            return _fallback_series(base, end_dt)

    curve_date = cands["ref_date"].max()
    df_curve = df.loc[df["ref_date"] == curve_date].copy()

    # Garante coluna de retorno diário
    daily_col = None
    for cand in ["di_diaria_interp", "di_daily_interp"]:
        if cand in df_curve.columns:
            daily_col = cand
            break

    if daily_col is None:
        # Se não veio a diária, tenta derivar a partir da anual (decimal)
        if "di_aa_252_interp_pct" in df_curve.columns:
            r_aa = pd.to_numeric(df_curve["di_aa_252_interp_pct"], errors="coerce")
            # Se estiver em %, traz como decimal (ex: 14.5 -> 0.145)
            r_aa = np.where(r_aa > 2.5, r_aa/100.0, r_aa).astype(float)
            r_d  = (1.0 + r_aa)**(1.0/252.0) - 1.0
            df_curve["__di_d"] = r_d
            daily_col = "__di_d"
        else:
            return _fallback_series(base, end_dt)

    # Indexa por data e recorta exatamente o horizonte (ref_date, end_date]
    s_all = (pd.to_numeric(df_curve[daily_col], errors="coerce")
               .astype(float)
               .rename("CDI_daily"))
    s_all.index = df_curve["data"]

    sess = b3_range(curve_date, end_dt)  # usa o calendário ativo do app
    if len(sess) == 0:
        return curve_date, pd.Series(dtype=float)

    # Mantém apenas as datas do horizonte (exclui a base)
    sess = sess[sess > curve_date]
    if len(sess) == 0:
        return curve_date, pd.Series(dtype=float)

    # Reindexa para garantir alinhamento perfeito com o calendário do app
    s = s_all.reindex(sess)

    # Se houver algum NaN por pequeno desalinhamento de calendário, tenta um ajuste leve
    if s.isna().any():
        # Estratégia conservadora: descarta datas faltantes; se esvaziar, cai no fallback
        s = s.dropna()
        if s.empty:
            return _fallback_series(base, end_dt)

    return curve_date, s

def _feriados_path_and_count():
    path = FERIADOS_PATH if FERIADOS_PATH.exists() else None
    fer = _load_feriados_set()
    return path, len(fer), fer


# ===================== Funções/constantes NOVAS (utilitárias) =====================
from typing import Optional
import io, re
import numpy as np
import pandas as pd

# 1) Alpha composto estilo Excel (diário equivalente)
def fator_alpha_excel(fator_intervalo: float, alpha: float, n_du: int) -> float:
    """
    Transforma o fator_intervalo (= ∏(1+r_d)) em uma taxa diária constante r_eq,
    aplica alpha nos dias (1 + alpha*r_eq) e recompõe por n_du:
        r_eq = fator_intervalo**(1/n_du) - 1
        fator_alpha = (1 + alpha * r_eq)**n_du
    Para n_du<=0, retorna 1.0.
    """
    try:
        n = int(n_du)
        if n <= 0:
            return 1.0
        r_eq = float(fator_intervalo)**(1.0/n) - 1.0
        return float((1.0 + float(alpha) * r_eq)**n)
    except Exception:
        return 1.0

# 2) Sensibilidade do fator CDI (SGS 12) ±1 dia
def cdi_factor_sensitivity(start_date: pd.Timestamp,
                           end_date: pd.Timestamp,
                           pivot_date: Optional[pd.Timestamp] = None):
    """
    Gera cenários simples de sensibilidade do fator CDI entre [start, end):
      - Base: inclui start, exclui base (como você já usa)
      - Remover pivô (se existir dado naquele dia)
      - Inserir pivô (se não existir dado naquele dia, mas for dia útil B3 no intervalo)
      - Incluir início / Excluir fim / Incluir fim / Excluir início
    Retorna (resumo_df, dict_series)
    """
    _, _, s_base = compute_cdi_factor_sgs(start_date, end_date, include_start=True, include_end=False, return_series=True)
    base_factor = float((1.0 + s_base.values).prod()) if not s_base.empty else 1.0

    def _factor_of(series):
        if series is None or series.empty:
            return 1.0
        return float((1.0 + series.values).prod())

    scenarios = []
    series_map = {"Base": s_base}

    # Bordas
    # (a) incluir fim (se houver dado no próprio end_date)
    _, _, s_inc_end = compute_cdi_factor_sgs(start_date, end_date, include_start=True, include_end=True, return_series=True)
    # (b) excluir início (sem incluir start)
    _, _, s_exc_ini = compute_cdi_factor_sgs(start_date, end_date, include_start=False, include_end=False, return_series=True)

    scenarios.append(["Base", start_date.date(), end_date.date(), len(s_base), _factor_of(s_base), 0.0, "Include start; exclude end"])
    scenarios.append(["Incluir fim", start_date.date(), end_date.date(), len(s_inc_end), _factor_of(s_inc_end), _factor_of(s_inc_end)/base_factor - 1.0, "Include end"])
    scenarios.append(["Excluir início", start_date.date(), end_date.date(), len(s_exc_ini), _factor_of(s_exc_ini), _factor_of(s_exc_ini)/base_factor - 1.0, "Exclude start"])

    series_map["Incluir fim"] = s_inc_end
    series_map["Excluir início"] = s_exc_ini

    # Pivô (se informado)
    if pivot_date is not None and not pd.isna(pivot_date):
        pv = _naive(pd.to_datetime(pivot_date))
        if pv is not pd.NaT and (pv >= _naive(start_date)) and (pv < _naive(end_date)):
            # Remover pivô (se existir na série)
            if not s_base.empty and pv in s_base.index:
                s_rm = s_base.drop(index=[pv])
                scenarios.append(["Remover pivô", start_date.date(), end_date.date(), len(s_rm), _factor_of(s_rm), _factor_of(s_rm)/base_factor - 1.0, "Drop pivot"])
                series_map["Remover pivô"] = s_rm
            else:
                # Inserir pivô (se for sessão B3 válida, tenta trazer só ele e inserir)
                idx_all = b3_sessions(_naive(start_date), _naive(end_date))
                if pv in idx_all:
                    # tenta pegar o valor do dia via SGS (chamada unitária)
                    _, _, s_one = compute_cdi_factor_sgs(pv, pv, include_start=True, include_end=True, return_series=True)
                    if not s_one.empty:
                        s_add = s_base.copy()
                        s_add.loc[pv] = float(s_one.iloc[0])
                        s_add = s_add.sort_index()
                        scenarios.append(["Inserir pivô", start_date.date(), end_date.date(), len(s_add), _factor_of(s_add), _factor_of(s_add)/base_factor - 1.0, "Add pivot"])
                        series_map["Inserir pivô"] = s_add

    resumo = pd.DataFrame(scenarios, columns=["Cenario","Inicio","Fim","DU","Fator","Delta_vs_base","Obs"])
    return resumo, series_map

# 3) XLSX da sensibilidade
def make_cdi_sensitivity_xlsx(start_date: pd.Timestamp,
                              end_date: pd.Timestamp,
                              pivot_date: Optional[pd.Timestamp] = None) -> bytes:
    resumo, series_map = cdi_factor_sensitivity(start_date, end_date, pivot_date)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        resumo.to_excel(xw, index=False, sheet_name="Resumo")
        xw.sheets["Resumo"].set_column(0, resumo.shape[1]-1, 18)
        for name, ser in series_map.items():
            df = _series_to_sheet(ser)
            # limita nome da aba a 31 chars
            sheet = (name[:28] + "...") if len(name) > 31 else name
            df.to_excel(xw, index=False, sheet_name=sheet)
            xw.sheets[sheet].set_column(0, df.shape[1]-1, 18)
    return buf.getvalue()

# 4) DataFrame bonitinho para a série
def _series_to_sheet(ser: pd.Series) -> pd.DataFrame:
    if ser is None or ser.empty:
        return pd.DataFrame(columns=["Data","CDI_%a.d.","CDI_decimal","G","DF"])
    df = pd.DataFrame({"Data": ser.index, "CDI_decimal": ser.values.astype(float)}).sort_values("Data").reset_index(drop=True)
    df["CDI_%a.d."] = df["CDI_decimal"] * 100.0
    df["G"] = (1.0 + df["CDI_decimal"]).cumprod()
    df["DF"] = 1.0 / df["G"]
    return df[["Data","CDI_%a.d.","CDI_decimal","G","DF"]]

# 5) Normalização de códigos
def norm_code(s) -> str:
    try:
        from unicodedata import normalize
        ss = str(s or "").strip().upper()
        ss = normalize("NFKD", ss).encode("ASCII", "ignore").decode("ASCII")
        ss = re.sub(r"\s+", " ", ss)
        return ss
    except Exception:
        return str(s).strip().upper()

# 6) Série diária via SGS (reuso do seu helper principal)
def load_cdi_sgs_daily(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.Series:
    """
    Retorna série diária do CDI (decimal ao dia) entre [start, end], usando a própria
    compute_cdi_factor_sgs(return_series=True). Inclui o início; exclui o fim.
    """
    _, _, ser = compute_cdi_factor_sgs(start_date, end_date, include_start=True, include_end=False, return_series=True)
    return ser

# 7) Fallback para diário a partir do a.a.
CDI_AA_DEFAULT = 0.12  # <— ajuste se quiser outro padrão
def _aa_to_daily_scalar(aa: float) -> float:
    try:
        return (1.0 + float(aa))**(1.0/252.0) - 1.0
    except Exception:
        return (1.0 + CDI_AA_DEFAULT)**(1.0/252.0) - 1.0


# ===================== Sidebar: Calendário =====================
st.sidebar.write('---')
st.sidebar.title("Configurações de Calendário")
calendario = st.sidebar.checkbox("Mostrar Feriados", key="debug_mode", value=False)
if calendario == True:
    with st.expander("🩺 Diagnóstico de Calendário (DU/feriados)", expanded=False):
        c1, c2, c3 = st.columns(3)
        path, nfer, fer_set = _feriados_path_and_count()

        c1.metric("Usando planilha de feriados?", "SIM" if _using_file_calendar() else "NÃO")
        c2.metric("Qtde de feriados carregados", nfer if nfer else 0)
        c3.write(f"Arquivo detectado: **{path.name if path else '— (não encontrado)'}**")

        st.caption("Se você trocou/substituiu o arquivo durante a sessão, limpe o cache para forçar nova leitura.")
        if st.button("🔄 Recarregar planilha de feriados (limpar cache)"):
            try:
                _load_feriados_set.cache_clear()  # lru_cache
                # reavalia
                path, nfer, fer_set = _feriados_path_and_count()
                st.success(f"Cache limpo. Arquivo: {path.name if path else '—'}. Feriados carregados: {nfer}.")
            except Exception as e:
                st.error(f"Falhou ao limpar cache: {e}")

        st.markdown("---")
        st.write("### Comparar contagem de DU e diferenças de datas")
        colA, colB = st.columns(2)
        d_ini = colA.date_input("Início (inclusive)", value=pd.Timestamp.today().date() - pd.DateOffset(months=6))
        d_fim = colB.date_input("Fim (inclusive)", value=pd.Timestamp.today().date() + pd.DateOffset(months=6))

        d0 = _naive(pd.to_datetime(d_ini))
        d1 = _naive(pd.to_datetime(d_fim))

        if d0 is pd.NaT or d1 is pd.NaT or d1 < d0:
            st.error("Intervalo inválido.")
        else:
            # sessões por ARQUIVO (se houver)
            sess_file = pd.DatetimeIndex([], dtype="datetime64[ns]")
            if _using_file_calendar():
                days = pd.date_range(d0, d1, freq="D")
                mask = [_is_file_bizday(x) for x in days]
                sess_file = pd.DatetimeIndex(days[mask]).normalize()

            # sessões por B3 (pandas_market_calendars)
            try:
                sess_b3 = cal.valid_days(d0, d1)
                sess_b3 = pd.to_datetime(sess_b3).tz_localize(None).normalize()
            except Exception:
                sess_b3 = pd.DatetimeIndex([], dtype="datetime64[ns]")

            # sessões segundo calendário ATIVO do app
            sess_active = b3_sessions(d0, d1)

            # contagens
            col1, col2, col3 = st.columns(3)
            col1.metric("DU (app: calendário ativo)", len(sess_active))
            col2.metric("DU (apenas planilha)", len(sess_file) if len(sess_file) else 0)
            col3.metric("DU (B3 / pandas_market_calendars)", len(sess_b3))

            # diferenças
            if len(sess_file) and len(sess_b3):
                set_file = set(pd.to_datetime(sess_file).date)
                set_b3   = set(pd.to_datetime(sess_b3).date)

                only_file = sorted(list(set_file - set_b3))
                only_b3   = sorted(list(set_b3 - set_file))

                st.write("#### Diferenças de sessões úteis")
                st.write(f"**(Arquivo − B3)**: {len(only_file)} dia(s)")
                if only_file:
                    st.write(", ".join([pd.Timestamp(x).strftime("%Y-%m-%d") for x in only_file]))

                st.write(f"**(B3 − Arquivo)**: {len(only_b3)} dia(s)")
                if only_b3:
                    st.write(", ".join([pd.Timestamp(x).strftime("%Y-%m-%d") for x in only_b3]))

                st.caption(
                    "• Se 'Arquivo − B3' contiver **Carnaval, Sexta-feira Santa, Corpus Christi, 24/12, 31/12**, "
                    "então sua planilha é bancária (ANBIMA) e está correta; o B3 não fecha nesses dias."
                )
            else:
                if not _using_file_calendar():
                    st.info("O aplicativo NÃO está usando a planilha (verifique nome/local do arquivo).")
                if len(sess_b3) == 0:
                    st.warning("Falha ao consultar calendário B3 para este intervalo.")

# ===================== Sidebar: Visão =====================
st.sidebar.write('---')
visao = st.sidebar.radio(
    "Visão",
    options=["Match de Ativos e DI", "Calculadora de Ativos", "Calculadora de Fundos"],
    index=0,
    key="visao_mode"
)

# ===================== Sidebar: Linha da Curva (DadosJuros) =====================
@st.cache_data
def _parquet_available_dates() -> pd.DataFrame:
    if not CURVA_DU_PARQUET.exists():
        return pd.DataFrame(columns=["Datas","n_du"]).astype({"n_du": int})
    df = pd.read_parquet(CURVA_DU_PARQUET)
    if df.empty: 
        return pd.DataFrame(columns=["Datas","n_du"]).astype({"n_du": int})
    df["ref_date"] = pd.to_datetime(df["ref_date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["ref_date"])
    g = df.groupby("ref_date", as_index=False).agg(n_du=("DU","max"))
    return g.rename(columns={"ref_date":"Datas","n_du":"n_vertices"}).sort_values("Datas")

@st.cache_data
def carregar_R_implied_por_DU() -> pd.Series:
    if not CURVA_DU_PARQUET.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {CURVA_DU_PARQUET}")
    df = pd.read_parquet(CURVA_DU_PARQUET).copy()

    col = "R_implied_aa" if "R_implied_aa" in df.columns else (
          "di_aa_252_interp_pct" if "di_aa_252_interp_pct" in df.columns else None)
    if col is None:
        raise ValueError("Preciso de 'R_implied_aa' (ou 'di_aa_252_interp_pct') no parquet.")

    curva = df[["DU", col]].dropna().copy()
    curva["DU"] = pd.to_numeric(curva["DU"], errors="coerce").astype("Int64").dropna()
    vals = pd.to_numeric(curva[col], errors="coerce").astype(float)
    vals = np.where(vals > 2.5, vals/100.0, vals)  # aceita % ou decimal
    s = pd.Series(vals, index=curva["DU"].astype(int)).sort_index()
    if len(s.index):
        s = s.reindex(range(int(s.index.min()), int(s.index.max())+1)).ffill()
    return s

def R_implied_at(s_R: pd.Series, DU: int) -> float:
    if DU <= 0:
        return 0.0
    if DU in s_R.index:
        return float(s_R.loc[DU])
    DU_max = int(s_R.index.max())
    return float(s_R.loc[DU_max])

# ===================== Data de referência global (fixa pela curva) =====================
REF_DATE_CURVA = get_curve_ref_date_from_parquet()
if REF_DATE_CURVA is pd.NaT:
    st.error("❌ Não encontrei 'ref_date' no parquet da curva.")
    st.stop()

st.sidebar.success(f"Data de referência da curva (fixa): {REF_DATE_CURVA.strftime('%d/%m/%Y')}")

# ===================== Uploads =====================
#with st.expander("📤 Upload (opcional)", expanded=False):
#    up_rel = st.file_uploader("Relatório de Posição (*.xlsx)", type=["xlsx"], key="up_rel")
#    up_pdf_csv = st.file_uploader("Base PDF consolidada (.csv)", type=["csv"], key="up_pdf_csv")
#    up_pdf_xls = st.file_uploader("Base PDF consolidada (.xlsx)", type=["xlsx"], key="up_pdf_xls")

# Ler arquivos
#if up_rel is not None:
#    rel_df = pd.read_excel(up_rel, dtype=str)
#else:
rel_path = find_relatorio_path()
rel_df = read_any(rel_path) if rel_path else pd.DataFrame()

#if up_pdf_csv is not None:
#    pdf_df = pd.read_csv(up_pdf_csv, dtype=str, keep_default_na=False)
#elif up_pdf_xls is not None:
#    pdf_df = pd.read_excel(up_pdf_xls, dtype=str)
#else:
PDF_CSV = PDF_DIR / "consolidado_pdfs_codativos.csv"
PDF_XLSX = PDF_DIR / "consolidado_pdfs_codativos.xlsx"
pdf_df = read_any(PDF_CSV) if PDF_CSV.exists() else read_any(PDF_XLSX)

if rel_df.empty:
    st.error("❌ Relatório de Posição não encontrado.")
    st.stop()

# ===================== Preparação das bases =====================
carteira = prepare_carteira(rel_df)
pdf_base = prepare_pdf_base(pdf_df) if not pdf_df.empty else pd.DataFrame()

# >>> capture o índice ANTES da exceção (para diffs)
pdf_idx_before = build_pdf_code_index(pdf_base) if not pdf_base.empty else pd.DataFrame()

# Exceções
exc_df = load_exceptions_df(EXC_XLSX)

# >>> aplique exceções gerando índice DEPOIS
pdf_idx_after = apply_exceptions_to_pdf_idx(pdf_idx_before, exc_df) if not exc_df.empty else pdf_idx_before.copy()

DEBUG_MODE = st.sidebar.checkbox("Modo Debug (exceções)", key="debug_mode_exceptions", value=False)
if DEBUG_MODE:
    # ======== PAINEL DE DEBUG DE EXCEÇÕES ========
    with st.expander("🔎 Debug: aplicação de exceções (%CDI / datas / ciclo / etc.)", expanded=False):
        c1, c2, c3 = st.columns([2,2,1])

        c1.metric("Códigos no PDF (antes)", 0 if pdf_idx_before.empty else len(pdf_idx_before["cod_Ativo_norm"].dropna().unique()))
        c2.metric("Códigos após exceção", 0 if pdf_idx_after.empty else len(pdf_idx_after["cod_Ativo_norm"].dropna().unique()))
        if not exc_df.empty:
            c3.metric("Códigos na exceção", len(exc_df["cod_Ativo_norm"].dropna().unique()))
        else:
            c3.metric("Códigos na exceção", 0)

        # stats coluna a coluna
        dbg = pdf_idx_after.attrs.get("__debug__", {})
        stats = dbg.get("stats", {})
        if stats:
            st.write("**Resumo por coluna (interseção PDF × Exceção):**")
            rows = []
            for col, s in stats.items():
                rows.append({
                    "Coluna (destino)": col,
                    "Em ambos": s.get("total_in_both", 0),
                    "Alterados": s.get("changed", 0),
                    "Preenchidos (antes nulo)": s.get("filled_from_null", 0),
                    "Iguais (sem mudança)": s.get("kept_same", 0),
                })
            st.dataframe(pd.DataFrame(rows).sort_values("Coluna (destino)"), use_container_width=True)
        # foco opcional: digite um código (ex: LFSN1800DIG) para ver ANTES/EXCEÇÃO/DEPOIS
        code_focus = st.text_input("Código para inspecionar (ex.: CDB725BEF4B):", value="CDB725BEF4B").strip().upper()
        if code_focus:
            key = norm_code(code_focus)
            # recortes
            before_row = pdf_idx_before.loc[pdf_idx_before["cod_Ativo_norm"] == key] if not pdf_idx_before.empty else pd.DataFrame()
            after_row  = pdf_idx_after.loc[pdf_idx_after["cod_Ativo_norm"] == key]   if not pdf_idx_after.empty  else pd.DataFrame()
            exc_row    = exc_df.loc[exc_df["cod_Ativo_norm"] == key]                 if not exc_df.empty        else pd.DataFrame()

            # colunas relevantes
            cols_dbg = ["cod_Ativo_norm","FormaCDI_final","pct_flutuante_final","CicloJuros_final",
                        "Data_Prox_Juros_final","Data_Emissao_final","Vencimento_final","Data_Call_Inicial_final",
                        "Emissor_pdf_ref"]
            # mapeia as colunas da exceção para as de destino, só para mostrar lado a lado
            exc_map = {
                "cod_Ativo_norm":"cod_Ativo_norm",
                "FormaCDI_final":"FormaCDI_final",
                "pct_flutuante_final":"pct_flutuante_final",
                "CicloJuros_final":"CicloJuros_final",
                "Data_Prox_Juros":"Data_Prox_Juros_final",
                "Data_Emissao":"Data_Emissao_final",
                "Vencimento_final":"Vencimento_final",
                "Data_Call_Inicial":"Data_Call_Inicial_final",
            }

            st.write(f"**Inspeção – {key}**")
            colA, colB, colC = st.columns(3)

            with colA:
                st.caption("ANTES (PDF)")
                if not before_row.empty:
                    st.dataframe(before_row[ [c for c in cols_dbg if c in before_row.columns] ], use_container_width=True)
                else:
                    st.info("— não existe no índice base (PDF)")

            with colB:
                st.caption("EXCEÇÃO (planilha)")
                if not exc_row.empty:
                    show_exc = exc_row.rename(columns=exc_map)
                    st.dataframe(show_exc[ [c for c in cols_dbg if c in show_exc.columns] ], use_container_width=True)
                else:
                    st.info("— não existe na planilha de exceções")

            with colC:
                st.caption("DEPOIS (aplicado)")
                if not after_row.empty:
                    st.dataframe(after_row[ [c for c in cols_dbg if c in after_row.columns] ], use_container_width=True)
                else:
                    st.info("— não consta após a aplicação (não casou ou não foi incluído)")

            # diff célula a célula (antes vs depois)
            if (not before_row.empty) and (not after_row.empty):
                st.caption("Diferenças (ANTES → DEPOIS):")
                diffs = []
                for c in cols_dbg:
                    b = before_row.iloc[0].get(c, None) if c in before_row.columns else None
                    a = after_row.iloc[0].get(c, None)  if c in after_row.columns  else None
                    if (str(b), str(a)) and ( (pd.isna(b) and pd.notna(a)) or (pd.notna(b) and pd.isna(a)) or (str(b) != str(a)) ):
                        diffs.append({"Coluna": c, "Antes": b, "Depois": a})
                st.dataframe(pd.DataFrame(diffs) if diffs else pd.DataFrame([{"Coluna":"—","Antes":"(igual)","Depois":"(igual)"}]), use_container_width=True)

        # botão para limpar caches das funções envolvidas na exceção
        if st.button("🧹 Limpar caches (exceções/índice PDF)"):
            try:
                # Limpa a cadeia de exceções
                load_exceptions_df.clear()
                prepare_pdf_base.clear()
                build_pdf_code_index.clear()
                apply_exceptions_to_pdf_idx.clear()
                
                # --- [MUDANÇA] ---
                # Limpa a função de cruzamento que usa o resultado das exceções
                cruzar.clear() 
                # --- [FIM DA MUDANÇA] ---

                st.success("Caches limpos (exceções, índice e cruzamento). Recarregando...")
                # st.experimental_rerun() # Descomente se quiser forçar o rerun automático
            except Exception as e:
                st.error(f"Falha ao limpar caches: {e}")
        #ATÉ AQUI OS ATIVOS ESTÃO PREENCHIDOS

# ===================== segue o fluxo normal =====================
pdf_idx = pdf_idx_after

# ===================== segue o fluxo normal =====================
merged = cruzar(carteira, pdf_idx)

# MTM — beta_from_mtm
mtm = load_mtm_table()
if not mtm.empty:
    merged["Ativo_norm"] = merged.get("Ativo","").map(lambda x: re.sub(r"\s+", " ", strip_accents(str(x)).upper().strip()))
    mtm_small = mtm[["Ativo_norm", "taxa_aa_mtm"]].dropna().drop_duplicates(subset=["Ativo_norm"])
    merged = merged.merge(mtm_small, how="left", on="Ativo_norm", suffixes=("", "_mtm"))
    merged.rename(columns={"taxa_aa_mtm": "beta_from_mtm"}, inplace=True)
else:
    merged["beta_from_mtm"] = np.nan

# Motor por ativo
def infer_motor(row) -> str:
    forma = str(row.get("FormaCDI_final") or "").upper()
    estr  = strip_accents(str(row.get("Estratégia") or "")).upper()
    has_call = pd.notna(row.get("Data_Call_Inicial_final"))
    if has_call or ("SELIC" in forma) or ("SELIC" in estr) or ("CALL" in estr):
        return MOTOR_YTC_CHAMADAS
    return MOTOR_PCT_CDI
merged["MOTOR_CALC"] = merged.apply(infer_motor, axis=1)

#st.success(f"Bases lidas. Linhas carteira: {len(carteira):,} | Casadas: {int(merged['match_pdf'].sum()):,}")

#Mas todas essas colunas a seguir ficam vazias
# Alpha e flags
def compute_alpha(row: pd.Series) -> Optional[float]:
    a = _norm_percent_or_ratio(row.get("pct_flutuante_final"))
    if a is None: a = _norm_percent_or_ratio(row.get("taxa_emissao_final"))
    if a is None:
        s = strip_accents(str(row.get("Estratégia") or "")).upper().replace(",", ".")
        m = re.search(r"(\d+(?:\.\d+)?)\s*%?\s*CDI", s)
        if m:
            a = _norm_percent_or_ratio(m.group(1))
        elif "CDI+" in s:
            a = 1.0
    return float(a) if a is not None else None

merged["alpha_norm"] = merged.apply(compute_alpha, axis=1)

def flag_potencial_pct_cdi(row) -> bool:
    if _is_ipca_forma(row.get("FormaCDI_final")):
        return False
    a = merged.loc[row.name, "alpha_norm"]
    if a is None: return False
    return abs(a - 1.0) > 1e-8
merged["Flag_%CDI≠100_noIPCA"] = merged.apply(flag_potencial_pct_cdi, axis=1)
flt = merged[merged["Flag_%CDI≠100_noIPCA"] == True].copy()

# Tabela 'out' final
cols_final = [
    "Fundo","PU_emissao_final","Data_Emissao_final","Data_Call_Inicial_final","Data_Prox_Juros_final",
    "Pu Posição_num","alpha_norm","FormaCDI_final","CicloJuros_final", "AgendaJuros_final", "IncorporaJuros_final", # <<< ALTERAÇÃO
    "Vencimento_final","Quantidade_num","Valor_num","Emissor","Ativo","cod_Ativo_guess",
    "MOTOR_CALC","beta_from_mtm"
]
for c in cols_final:
    if c not in flt.columns:
        flt[c] = None

ren_cols = {
    "PU_emissao_final": "PU_emissao",
    "Data_Emissao_final": "Data_emissao",
    "Data_Call_Inicial_final": "Data_call",
    "Data_Prox_Juros_final": "Data_prox_juros",
    "Pu Posição_num": "PU_posicao",
    "alpha_norm": "alpha_(%CDI fator)",
    "CicloJuros_final": "Ciclo_juros",
    "AgendaJuros_final": "AgendaJuros", # <<< ALTERAÇÃO
    "IncorporaJuros_final": "IncorporaJuros",
    "Vencimento_final": "Vencimento",
    "Quantidade_num": "Quantidade",
    "Valor_num": "Valor",
    "cod_Ativo_guess": "Codigo",
    "MOTOR_CALC": "Motor_calc",
    "beta_from_mtm": "beta_from_mtm"
}
out = flt[cols_final].rename(columns=ren_cols)

# Ajustes solicitados
out = out[out['beta_from_mtm'].notna()]
#out = out[out['Codigo'] != 'KLBNA2']

# Curva DI global (até maior vencimento do filtro)
cand_ven = pd.to_datetime(flt.get("Vencimento_final"), errors="coerce")
max_end = cand_ven.max() if len(cand_ven) else (pd.to_datetime(REF_DATE_CURVA) + pd.DateOffset(years=5))
curve_date, di_daily = load_di_curve_daily(max_end, REF_DATE_CURVA, forced_curve_date=None)

def build_di_export_df(curve_date: pd.Timestamp, di_daily: pd.Series, alpha: float|None=None) -> pd.DataFrame:
    if di_daily.empty:
        return pd.DataFrame(columns=["Data","DU","CDI_daily","G","DF"])
    dates = di_daily.index
    du = np.arange(1, len(dates) + 1, dtype=int)
    cdi_daily = di_daily.values.astype(float)

    G  = np.cumprod(1.0 + cdi_daily)
    DF = 1.0 / G

    cdi_daily_aa = (1.0 + cdi_daily)**252 - 1.0
    cdi_implied_aa_to_date = (G**(252.0/du)) - 1.0

    data = {
        "Data": dates.date,
        "DU": du,
        "CDI_daily": cdi_daily,
        "G": G,
        "DF": DF,
        "CDI_daily_aa": cdi_daily_aa,
        "CDI_implied_aa_to_date": cdi_implied_aa_to_date,
    }

    if alpha is not None:
        cdi_a = alpha * cdi_daily
        G_a   = np.cumprod(1.0 + cdi_a)
        DF_a  = 1.0 / G_a
        cdi_alpha_daily_aa = (1.0 + cdi_a)**252 - 1.0
        cdi_alpha_implied_aa_to_date = (G_a**(252.0/du)) - 1.0
        data.update({
            "CDI_alpha_daily": cdi_a,
            "G_alpha": G_a,
            "DF_alpha": DF_a,
            "CDI_alpha_daily_aa": cdi_alpha_daily_aa,
            "CDI_alpha_implied_aa_to_date": cdi_alpha_implied_aa_to_date,
        })
    return pd.DataFrame(data)

df_di = build_di_export_df(curve_date, di_daily)

# ===================== VISÃO 1 — Match de Ativos e DI =====================
if visao == "Match de Ativos e DI":
    st.title("HEDGE DI — Match de Ativos e DI")

    tab1, tab2, tab3 = st.tabs(["📇 Tabela de Match", "📈 Curva DI (implícita a.a.)", "🔍 Confronto %CDI (PDF × Relatório)"])

    # ---- TAB 1: match + download XLSX local ----
    with tab1:
        st.markdown("### Tabela — Match de Ativos e DI")

        display_map = {
            "PU_emissao_final": "PU_emissao",
            "Pu Posição_num":   "PU_posicao",
            "alpha_norm":       "alpha_(%CDI fator)",
            "Quantidade_num":   "Quantidade",
            "Valor_num":        "Valor",
        }
        merged_disp = merged.copy()
        for src, dst in display_map.items():
            if src in merged_disp.columns:
                merged_disp[dst] = pd.to_numeric(merged_disp[src], errors="coerce")

        fmt_map = {}
        if "PU_emissao" in merged_disp.columns:         fmt_map["PU_emissao"] = "{:,.2f}"
        if "PU_posicao" in merged_disp.columns:         fmt_map["PU_posicao"] = "{:,.2f}"
        if "alpha_(%CDI fator)" in merged_disp.columns: fmt_map["alpha_(%CDI fator)"] = "{:,.6f}"
        if "Quantidade" in merged_disp.columns:         fmt_map["Quantidade"] = "{:,.2f}"
        if "Valor" in merged_disp.columns:              fmt_map["Valor"] = "{:,.2f}"

        st.dataframe(
            merged_disp.sort_values(["Fundo","Emissor","Ativo"], na_position="last").style.format(fmt_map),
            use_container_width=True, height=520
        )
        st.caption(
            f"Linhas na carteira: {len(carteira):,} | Casadas com PDF: {int(merged['match_pdf'].sum()):,} "
            f"| Sem match: {int((~merged['match_pdf']).sum()):,}"
        )

        # Download XLSX desta tabela
        buf_match = io.BytesIO()
        with pd.ExcelWriter(buf_match, engine="xlsxwriter") as xw:
            merged_disp.to_excel(xw, index=False, sheet_name="Match")
            xw.sheets["Match"].set_column(0, merged_disp.shape[1]-1, 18)
        st.download_button(
            "⬇️ Baixar XLSX — Match de Ativos",
            data=buf_match.getvalue(),
            file_name="match_ativos.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ---- TAB 2: curva DI (um gráfico) + tabela completa em expander + XLSX local ----
    with tab2:
        st.markdown("### Curva DI — Taxa implícita a.a. (desde a base)")

        df_plot = df_di.rename(columns={"CDI_implied_aa_to_date": "CDI_impl_aa"}).copy()
        if df_plot.empty:
            st.warning("Curva não disponível para o horizonte selecionado.")
        else:
            # [INÍCIO DAS MUDANÇAS]
            import altair as alt, numpy as np
            
            # --- Lógica do Y Máximo (existente) ---
            y_max = float(df_plot["CDI_impl_aa"].max() or 0.0)
            # margem de ~30 bps e arredonda para passos de 0,5pp; teto de 40% por segurança
            y_max = min(0.40, np.ceil((y_max + 0.003) * 200) / 200)

            # --- [NOVO] Lógica do Y Mínimo ---
            y_min = float(df_plot["CDI_impl_aa"].min() or 0.0)
            # margem de ~30 bps e arredonda para passos de 0,5pp; piso de 0%
            y_min = max(0.0, np.floor((y_min - 0.003) * 200) / 200)
            # --- [FIM DO NOVO] ---

            chart = (
                alt.Chart(df_plot)
                .mark_line()
                .encode(
                    x=alt.X("Data:T", title="Data"),
                    y=alt.Y(
                        "CDI_impl_aa:Q",
                        title="Taxa implícita a.a.",
                        axis=alt.Axis(format='%'),
                        # --- [MODIFICADO] Usa o novo y_min ao invés de 0 ---
                        scale=alt.Scale(domain=[y_min, y_max], clamp=True, nice=False)
                    ),
                    tooltip=[
                        alt.Tooltip("Data:T", title="Data", format="%d/%m/%Y"),
                        alt.Tooltip("CDI_impl_aa:Q", title="Implícita a.a.", format=".4%")
                    ]
                )
                .properties(height=520, title=f"Curva DI (linha: {curve_date.strftime('%d/%m/%Y')})")
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)

        # Download XLSX da curva exibida
        buf_di = io.BytesIO()
        with pd.ExcelWriter(buf_di, engine="xlsxwriter") as xw:
            df_di.to_excel(xw, index=False, sheet_name="Curva_DI")
            ws = xw.sheets["Curva_DI"]
            ws.set_column(0, 0, 12)  # Data
            ws.set_column(1, df_di.shape[1]-1, 18)
        st.download_button(
            "⬇️ Baixar XLSX — Curva DI (tabela completa)",
            data=buf_di.getvalue(),
            file_name=f"curva_DI_{curve_date.strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        with st.expander("📋 Tabela completa (todas as datas + forward)"):
            fmt = {
                "CDI_daily":"{:.8f}", "G":"{:.8f}", "DF":"{:.8f}",
                "CDI_daily_aa":"{:.4%}", "CDI_implied_aa_to_date":"{:.4%}"
            }
            for k in ["CDI_alpha_daily","G_alpha","DF_alpha","CDI_alpha_daily_aa","CDI_alpha_implied_aa_to_date"]:
                if k in df_di.columns:
                    fmt[k] = "{:.8f}" if "daily" in k or k in {"G_alpha","DF_alpha"} else "{:.4%}"
            st.dataframe(df_di.style.format(fmt), use_container_width=True, height=420)
    with tab3:
        st.markdown("### Confronto %CDI — o que está na base PDF vs o Relatório")

        # ---------- 1) Regras de classificação %CDI ----------
        def _is_pct_cdi_forma(x: str) -> bool:
            s = (str(x or "")).upper().replace(" ", "")
            if "IPCA" in s:  # exclui qualquer IPCA
                return False
            return ("%CDI" in s) or (s in {"PCTCDI","CDI","CDI+"})  # CDI+, PCTCDI ou %CDI contam como 'família CDI'

        def _estrategia_menciona_cdi(x: str) -> bool:
            s = strip_accents(str(x or "")).upper()
            return ("CDI" in s) and ("IPCA" not in s)

        # ---------- 2) Chaves canônicas ----------
        carteira_codes = merged["cod_Ativo_guess_norm"].fillna("").astype(str)  # do prepare_carteira
        pdf_codes = (pdf_idx["cod_Ativo_norm"].fillna("").astype(str) if not pdf_idx.empty else pd.Series([], dtype=str))

        # ---------- 3) Flags %CDI em cada lado ----------
        rel_is_pct_cdi = (
            merged.get("FormaCDI_final").map(_is_pct_cdi_forma).fillna(False)
            | merged["alpha_norm"].notna()
            | merged.get("Estratégia","").map(_estrategia_menciona_cdi).fillna(False)
        )
        if not pdf_idx.empty:
            pdf_is_pct_cdi_map = pdf_idx.set_index("cod_Ativo_norm")["FormaCDI_final"].map(_is_pct_cdi_forma).fillna(False)
        else:
            pdf_is_pct_cdi_map = pd.Series(dtype=bool)

        # ---------- 4) Conjuntos ----------
        set_rel_pct_cdi = set(carteira_codes[rel_is_pct_cdi & carteira_codes.astype(bool)])
        set_pdf_pct_cdi = set(pdf_codes[pdf_codes.astype(bool) & pdf_codes.map(lambda c: bool(pdf_is_pct_cdi_map.get(c, False)))])

        only_pdf = sorted(list(set_pdf_pct_cdi - set_rel_pct_cdi))
        only_rel = sorted(list(set_rel_pct_cdi - set_pdf_pct_cdi))
        both     = sorted(list(set_rel_pct_cdi & set_pdf_pct_cdi))

        c1, c2, c3 = st.columns(3)
        c1.metric("Somente na base PDF (%CDI)", len(only_pdf))
        c2.metric("Somente no Relatório (%CDI)", len(only_rel))
        c3.metric("Em ambos (%CDI)", len(both))

        st.divider()

        def _alpha_from_any(v) -> float | None:
            x = _norm_percent_or_ratio(v)  # já definida acima no app
            return float(x) if x is not None else None

        df_pdf_alpha_ne_1 = pd.DataFrame()
        if not pdf_idx.empty:
            pdf_alpha = pdf_idx.copy()

            # calcula alpha canônico (__alpha__)
            pdf_alpha["__alpha__"] = pdf_alpha.apply(
                lambda r: (
                    _alpha_from_any(r.get("pct_flutuante_final"))
                    if _alpha_from_any(r.get("pct_flutuante_final")) is not None
                    else _alpha_from_any(r.get("taxa_emissao_final"))
                ),
                axis=1
            )

            mask = pdf_alpha["__alpha__"].notna() & (np.abs(pdf_alpha["__alpha__"] - 1.0) > 1e-8)
            if mask.any():
                # traz contexto do PDF base (emissor, vencimento e campos originais)
                cols_ctx = ["cod_Ativo_norm","Emissor","vencimento","pct_flutuante","taxa_emissão","PU_emissão"]
                for c in cols_ctx:
                    if c not in pdf_base.columns:
                        pdf_base[c] = None
                ctx = (
                    pdf_base[cols_ctx]
                    .drop_duplicates(subset=["cod_Ativo_norm"])
                )

                df_pdf_alpha_ne_1 = (
                    pdf_alpha.loc[mask, ["cod_Ativo_norm","__alpha__","FormaCDI_final","pct_flutuante_final","taxa_emissao_final","PU_emissao_final"]]
                    .merge(ctx, on="cod_Ativo_norm", how="left")
                    .rename(columns={
                        "cod_Ativo_norm": "Codigo",
                        "__alpha__": "alpha_calc",
                        "FormaCDI_final": "FormaCDI(PDF)",
                        "pct_flutuante_final": "pct_flutuante_final(PDF)",
                        "taxa_emissao_final": "taxa_emissao_final(PDF)",
                        "PU_emissao_final": "PU_emissao_final(PDF)",
                        "Emissor": "Emissor_PDF",
                        "vencimento": "Vencimento",
                        "pct_flutuante": "pct_flutuante(orig)",
                        "taxa_emissão": "taxa_emissao(orig)",
                        "PU_emissão": "PU_emissao(orig)",
                    })
                    .sort_values(["Emissor_PDF","Codigo"])
                    .reset_index(drop=True)
                )

        # ======================================================================
        # 5.b) (PRIMEIRA TABELA) Somente no Relatório (não apareceu na base PDF)
        #      — filtrada para alpha ≠ 1 (ou 100%)
        # ======================================================================
        df_only_rel = pd.DataFrame()
        if only_rel:
            # máscara: alpha existe e é diferente de 1.0 (tolerância numérica)
            mask_alpha = (
                merged["alpha_norm"].notna()
                & (np.abs(merged["alpha_norm"] - 1.0) > 1e-8)
            )

            cols_keep = ["Fundo","Emissor","Ativo","cod_Ativo_guess","Estratégia","alpha_norm",
                        "Quantidade_num","Valor_num","Pu Posição_num","Vencimento_final"]
                        
            cols_keep = [c for c in cols_keep if c in merged.columns]

            df_only_rel = (
                merged.loc[
                    merged["cod_Ativo_guess_norm"].isin(only_rel) & mask_alpha,
                    cols_keep
                ]
                .rename(columns={
                    "cod_Ativo_guess":"Codigo",
                    "alpha_norm":"alpha_(%CDI fator)",
                    "Quantidade_num":"Quantidade",
                    "Valor_num":"Valor",
                    "Pu Posição_num":"PU_posicao",
                    "Vencimento_final":"Vencimento"
                })
                .sort_values(["Fundo","Emissor","Ativo"])
                .reset_index(drop=True)
            )
        
        COLUNA_ID_ATIVO_TABELA_1 = 'Codigo' # <-- AJUSTE AQUI SE NECESSÁRIO
            # 2. Pega a 'primeira tabela', SUBSTITUI STRINGS VAZIAS ("") POR NaN,
        #    e DEPOIS remove todas as linhas que contêm QUALQUER valor NaN.
        #    (Certifique-se de ter 'import numpy as np' no início do seu script)
        df_pdf_alpha_ne_2 = df_pdf_alpha_ne_1.copy()
        #Dropar coluna Vencimento
        df_pdf_alpha_ne_2.drop('Vencimento', axis=1, inplace=True)  # <-- LINHA ADICIONADA
        df_pdf_clean = df_pdf_alpha_ne_2.replace("", np.nan).dropna() # <-- LINHA MODIFICADA

        # 3. Cria uma lista de ativos "limpos" (sem NaN/vazios) da tabela 1
        ativos_limpos_lista = df_pdf_clean[COLUNA_ID_ATIVO_TABELA_1].unique()

        with st.expander(f" Somente no Relatório (%CDI) ou Faltando Dados ", expanded=True):
            # --- Segunda tabela (deduplicada por categoria) ---
            def _is_blank_strategy(s: str) -> bool:
                ss = str(s or "").strip().upper()
                return (ss == "") or (ss in {"--", "—", "N/A", "NA", "NULL", "NONE"})

            def _is_pct_cdi_text(s: str) -> bool:
                ss = strip_accents(str(s or "")).upper()
                return ("CDI" in ss) and ("IPCA" not in ss)

            df_cat = df_only_rel.copy()

            # flags por categoria
            flag_pct_text  = df_cat["Estratégia"].map(_is_pct_cdi_text)
            flag_pct_alpha = df_cat["alpha_(%CDI fator)"].notna() & (np.abs(df_cat["alpha_(%CDI fator)"] - 1.0) > 1e-8)
            flag_pct       = flag_pct_text | flag_pct_alpha

            flag_blank     = df_cat["Estratégia"].map(_is_blank_strategy)
            flag_outros    = (~flag_pct) & (~flag_blank)

            # construir tabela única deduplicada por categoria
            def _dedup(df, cat_label):
                if df.empty:
                    return pd.DataFrame(columns=["Categoria","Codigo","Ativo"])
                return (df[["Codigo","Ativo"]]
                        .drop_duplicates()
                        .assign(Categoria=cat_label)
                        [["Categoria","Codigo","Ativo"]])

            df_pct    = _dedup(df_cat.loc[flag_pct],   "%CDI")
            df_blank  = _dedup(df_cat.loc[flag_blank], "Sem Estratégia")
            df_outros = _dedup(df_cat.loc[flag_outros], "Outras Estratégias")

            df_resumo_dedup = pd.concat([df_pct, df_blank, df_outros], ignore_index=True)
            df_resumo_dedup = df_resumo_dedup.sort_values(["Categoria","Codigo","Ativo"]).reset_index(drop=True)
            #Retirar do df os ativos que estão na lista de 'ativos_limpos_lista'
            df_resumo_dedup = df_resumo_dedup[~df_resumo_dedup['Codigo'].isin(ativos_limpos_lista)]

            st.markdown("#### Ativos que estão faltando")
            if df_resumo_dedup.empty:
                st.info("Nada a mostrar nesta visão.")
            else:
                st.dataframe(df_resumo_dedup, use_container_width=True, height=300)

            st.markdown("#### Base completa")
            if df_only_rel.empty:
                st.info("Nada aqui.")
            else:
                fmt = {"alpha_(%CDI fator)":"{:.6f}","Quantidade":"{:,.2f}","Valor":"{:,.2f}","PU_posicao":"{:,.2f}"}
                st.dataframe(df_only_rel.style.format(fmt), use_container_width=True, height=360)

                buf_rel = io.BytesIO()
                with pd.ExcelWriter(buf_rel, engine="xlsxwriter") as xw:
                    df_only_rel.to_excel(xw, index=False, sheet_name="Somente_Relatorio_alpha_ne_100")
                    xw.sheets["Somente_Relatorio_alpha_ne_100"].set_column(0, df_only_rel.shape[1]-1, 18)
                st.download_button("⬇️ Baixar XLSX — Somente no Relatório (alpha≠100%)",
                                data=buf_rel.getvalue(),
                                file_name="somente_relatorio_pctcdi_alpha_ne_100.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # --------------------------------------------------------
        # 5.a) Somente na base PDF (não apareceu no relatório)
        # --------------------------------------------------------
        df_only_pdf = pd.DataFrame()
        if only_pdf:
            df_only_pdf = (
                pdf_idx.loc[pdf_idx["cod_Ativo_norm"].isin(only_pdf)]
                .merge(
                    pdf_base[["cod_Ativo_norm","Emissor","vencimento","pct_flutuante","taxa_emissão","PU_emissão"]]
                    .drop_duplicates("cod_Ativo_norm"),
                    on="cod_Ativo_norm", how="left"
                )
                .rename(columns={
                    "cod_Ativo_norm":"Codigo",
                    "Emissor_pdf_ref":"Emissor_PDF",
                    "vencimento":"Vencimento",
                    "pct_flutuante":"pct_flutuante(%)",
                    "taxa_emissão":"taxa_emissao(aa)",
                    "PU_emissão":"PU_emissao"
                })
                .sort_values(["Emissor_PDF","Codigo"])
            )
        with st.expander(f"📄 Somente na base PDF (%CDI) — {len(only_pdf)} ativo(s)", expanded=False):
            if df_only_pdf.empty:
                st.info("Nada aqui.")
            else:
                st.dataframe(df_only_pdf, use_container_width=True, height=360)
                buf_pdf = io.BytesIO()
                with pd.ExcelWriter(buf_pdf, engine="xlsxwriter") as xw:
                    df_only_pdf.to_excel(xw, index=False, sheet_name="Somente_PDF")
                    xw.sheets["Somente_PDF"].set_column(0, df_only_pdf.shape[1]-1, 18)
                st.download_button("⬇️ Baixar XLSX — Somente na base PDF",
                                data=buf_pdf.getvalue(),
                                file_name="somente_base_pdf_pctcdi.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # --------------------------------------------------------
        # 5.c) Em ambos (match) — útil para %CDI≠100% e conciliar alfa
        # --------------------------------------------------------
        df_both = pd.DataFrame()
        if both:
            cols = ["Fundo","Emissor","Ativo","cod_Ativo_guess","Estratégia","alpha_norm",
                    "Quantidade_num","Valor_num","Pu Posição_num","FormaCDI_final","pct_flutuante_final",
                    "taxa_emissao_final","Vencimento_final","match_pdf"]
            keep = [c for c in cols if c in merged.columns]
            df_both = (
                merged.loc[merged["cod_Ativo_guess_norm"].isin(both), keep]
                .rename(columns={
                    "cod_Ativo_guess":"Codigo",
                    "alpha_norm":"alpha_(%CDI fator)",
                    "Quantidade_num":"Quantidade",
                    "Valor_num":"Valor",
                    "Pu Posição_num":"PU_posicao",
                    "pct_flutuante_final":"pct_flutuante(%)",
                    "taxa_emissao_final":"taxa_emissao(aa)",
                    "Vencimento_final":"Vencimento"
                })
                .assign(Flag_pctCDI_ne_100=lambda d: np.where(
                    d["alpha_(%CDI fator)"].notna() & (np.abs(d["alpha_(%CDI fator)"] - 1.0) > 1e-8), True, False))
                .sort_values(["Fundo","Emissor","Ativo"])
            )
        with st.expander(f"🔗 Em ambos (%CDI) — {len(both)} ativo(s)", expanded=False):
            if df_both.empty:
                st.info("Nada aqui.")
            else:
                fmt = {
                    "alpha_(%CDI fator)":"{:.6f}","Quantidade":"{:,.2f}","Valor":"{:,.2f}","PU_posicao":"{:,.2f}",
                    "pct_flutuante(%)":"{:.6f}","taxa_emissao(aa)":"{:.6f}"
                }
                st.dataframe(df_both.style.format(fmt), use_container_width=True, height=420)
                buf_both = io.BytesIO()
                with pd.ExcelWriter(buf_both, engine="xlsxwriter") as xw:
                    df_both.to_excel(xw, index=False, sheet_name="Em_Ambos")
                    xw.sheets["Em_Ambos"].set_column(0, df_both.shape[1]-1, 18)
                st.download_button("⬇️ Baixar XLSX — Em ambos",
                                data=buf_both.getvalue(),
                                file_name="em_ambos_pctcdi.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # ===== Lista: ativos que caíram por missing data (PDF) =====
        with st.expander("🧩 Ativos que caíram por missing data (PDF)", expanded=False):
            # 'only_rel' contém os códigos %CDI presentes no Relatório mas não presentes (como %CDI) no PDF
            # Vamos tentar explicar o motivo:
            #  - existe no pdf_base, mas leitura_ok == FALSO / sanity_check == 'FAIL'  -> erro de extração
            #  - não existe no pdf_base                                   -> ausente na consolidação PDF

            # Normalizar flags de diagnóstico se existirem
            def _true_false(x):
                s = str(x or "").strip().upper()
                if s in {"TRUE","VERDADEIRO","SIM","S"}: return True
                if s in {"FALSE","FALSO","NAO","NÃO","N"}: return False
                return None

            # garantir colunas
            for c in ["leitura_ok","sanity_check","sanity_notes","extractor_used","source_file","pages_used"]:
                if c not in pdf_base.columns: pdf_base[c] = None

            pdf_diag = pdf_base.copy()
            pdf_diag["leitura_ok_bool"] = pdf_diag["leitura_ok"].map(_true_false)
            pdf_codes_all = set(pdf_base.get("cod_Ativo_norm","").astype(str))

            # montar dataframe de motivos
            miss_rows = []
            # mapeio código -> primeiro par Fundo/Emissor/Ativo do relatório pra contexto
            ctx_cols = ["Fundo","Emissor","Ativo","cod_Ativo_guess_norm","cod_Ativo_guess","Estratégia"]
            ctx_df = (merged[ctx_cols].drop_duplicates(subset=["cod_Ativo_guess_norm"])
                    if all(c in merged.columns for c in ctx_cols) else pd.DataFrame())

            for code in only_rel:
                motivo = "Não encontrado na base PDF"
                src, sanity, extr, pages = None, None, None, None

                # há alguma linha bruta do PDF com esse código?
                diag_hit = pdf_diag.loc[pdf_diag["cod_Ativo_norm"] == code]
                if not diag_hit.empty:
                    # se houver flags de erro, prioriza explicação de erro de leitura
                    any_fail = (
                        (diag_hit["leitura_ok_bool"] == False).any() or
                        (diag_hit["sanity_check"].astype(str).str.upper() == "FAIL").any()
                    )
                    if any_fail:
                        row0 = diag_hit.iloc[0]
                        motivo = "PDF encontrado, mas falha de leitura"
                        sanity = row0.get("sanity_notes")
                        src    = row0.get("source_file")
                        extr   = row0.get("extractor_used")
                        pages  = row0.get("pages_used")
                    else:
                        motivo = "PDF encontrado, porém sem dados necessários"

                # contexto do relatório
                fundo = emissor = ativo = estrategia = None
                if not ctx_df.empty:
                    ctx_row = ctx_df.loc[ctx_df["cod_Ativo_guess_norm"] == code]
                    if not ctx_row.empty:
                        r0 = ctx_row.iloc[0]
                        fundo = r0.get("Fundo"); emissor = r0.get("Emissor"); ativo = r0.get("Ativo"); estrategia = r0.get("Estratégia")

                miss_rows.append({
                    "Codigo": code,
                    "Fundo": fundo,
                    "Emissor_rel": emissor,
                    "Ativo_rel": ativo,
                    "Estrategia_rel": estrategia,
                    "Motivo": motivo,
                    "sanity_notes": sanity,
                    "extractor_used": extr,
                    "source_file": src,
                    "pages_used": pages,
                })

            df_missing = pd.DataFrame(miss_rows).sort_values(["Motivo","Codigo"]).reset_index(drop=True)

            if df_missing.empty:
                st.success("Nenhum ativo caiu por missing data nesta amostra. ✅")
            else:
                st.dataframe(df_missing, use_container_width=True, height=320)

                # export
                buf_miss = io.BytesIO()
                with pd.ExcelWriter(buf_miss, engine="xlsxwriter") as xw:
                    df_missing.to_excel(xw, index=False, sheet_name="Missing_PDF")
                    xw.sheets["Missing_PDF"].set_column(0, df_missing.shape[1]-1, 24)
                st.download_button("⬇️ Baixar XLSX — Missing data (PDF)",
                                data=buf_miss.getvalue(),
                                file_name="missing_pdf_pctcdi.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------- utils locais do bloco (mantidos) ----------
def ln_factor(series_daily, t0, t1, mult=1.0, include_start=False, include_end=True):
    i0, i1 = _naive(t0), _naive(t1)
    if i0 is pd.NaT or i1 is pd.NaT or i1 <= i0:
        return 0.0
    sidx = series_daily.index
    m_start = (sidx >= i0) if include_start else (sidx > i0)
    m_end   = (sidx <= i1) if include_end   else (sidx < i1)
    idx = sidx[m_start & m_end]
    if len(idx) == 0:
        return 0.0
    return float(np.log1p(mult * series_daily.reindex(idx).values).sum())


def _get_implied_at_DU(df_di_local: pd.DataFrame, DU: int) -> float | None:
    if DU <= 0:
        return None
    df = df_di_local[["DU", "CDI_implied_aa_to_date"]].dropna().sort_values("DU")
    DU_max = int(df["DU"].max()) if not df.empty else 0
    if DU > DU_max and DU_max > 0:
        return float(df.loc[df["DU"] == DU_max, "CDI_implied_aa_to_date"].iloc[0])
    row = df.loc[df["DU"] == int(DU), "CDI_implied_aa_to_date"]
    if row.empty:
        s = (df.set_index("DU")["CDI_implied_aa_to_date"].reindex(range(1, DU_max+1)).ffill())
        return float(s.iloc[DU-1])
    return float(row.iloc[0])

def di_period_from_implied(Eprev: int, Ecur: int, R_prev: float | None, R_cur: float) -> tuple[float, float]:
    if Ecur <= Eprev:
        return 1.0, 0.0
    D = float(Ecur - Eprev)
    A_prev = 1.0 if (Eprev == 0 or R_prev is None or np.isnan(R_prev)) else (1.0 + float(R_prev))**(Eprev/252.0)
    A_cur  = (1.0 + float(R_cur))**(Ecur/252.0)
    fator_intervalo = A_cur / A_prev
    di_per_aa       = fator_intervalo**(252.0 / D) - 1.0
    return float(fator_intervalo), float(di_per_aa)


def interval_factor_from_implied(base_dt: pd.Timestamp,
                                 start_dt: pd.Timestamp,
                                 end_dt: pd.Timestamp,
                                 s_R: pd.Series) -> tuple[float, float, int]:
    D_prev = b3_count_excl(base_dt, start_dt)
    D_cur  = b3_count_excl(base_dt, end_dt)
    R_prev = R_implied_at(s_R, D_prev)
    R_cur  = R_implied_at(s_R, D_cur)
    fator_intervalo, di_per_aa = di_period_from_implied(D_prev, D_cur, R_prev, R_cur)
    return fator_intervalo, di_per_aa, int(max(D_cur - D_prev, 0))

def last_and_next_coupon(base_dt: pd.Timestamp, em_dt: pd.Timestamp, first_coupon: pd.Timestamp,
                            ven_dt: pd.Timestamp, months_step: int = 6) -> tuple[pd.Timestamp, pd.Timestamp | None]:
    periods = generate_periods_semester_b3(em_dt, first_coupon, ven_dt, months_step=months_step)
    last_coupon = em_dt
    next_coupon = None
    for (ini, fim) in periods:
        if fim <= base_dt:
            last_coupon = fim
        elif next_coupon is None and fim > base_dt:
            next_coupon = fim
    return last_coupon, next_coupon

def infer_coupon_anchors_from_emissao(base_dt: pd.Timestamp, em_dt: pd.Timestamp, ven_dt: pd.Timestamp,
                                        months_step: int = 6) -> tuple[pd.Timestamp, pd.Timestamp]:
    base_dt = _naive(base_dt); em_dt = _naive(em_dt); ven_dt = _naive(ven_dt)
    if any(pd.isna(x) for x in [base_dt, em_dt, ven_dt]) or months_step <= 0:
        return em_dt, b3_next_session((em_dt + pd.DateOffset(months=max(months_step,1))).normalize())
    first = b3_next_session((em_dt + pd.DateOffset(months=months_step)).normalize())
    if pd.isna(first): first = em_dt
    last_cpn = em_dt; next_cpn = first
    cur = first; guard = 0
    while (cur is not pd.NaT) and (cur <= base_dt) and (cur < ven_dt) and guard < 1000:
        last_cpn = cur
        nxt = b3_next_session((cur + pd.DateOffset(months=months_step)).normalize())
        if pd.isna(nxt) or nxt <= cur:
            nxt = b3_next_session(cur + pd.Timedelta(days=1))
            if pd.isna(nxt) or nxt <= cur:
                break
        cur = nxt
        next_cpn = cur
        guard += 1
    if (next_cpn is pd.NaT) or (next_cpn > ven_dt):
        next_cpn = ven_dt
    return last_cpn, next_cpn

def compute_pu_hoje(
    PU_emissao: float,
    alpha: float,
    curve_acc_full: pd.Series,
    em_dt: pd.Timestamp,
    last_coupon_dt: pd.Timestamp,
    base_dt: pd.Timestamp,
    incorpora: str
) -> float:
    if str(incorpora).upper() == "SIM":
        ln_acc = ln_factor(curve_acc_full, em_dt, base_dt, mult=alpha, include_start=True, include_end=False)
    else:
        ln_acc = ln_factor(curve_acc_full, last_coupon_dt, base_dt, mult=alpha, include_start=True, include_end=False)
    return float(PU_emissao * np.exp(ln_acc))

def roll_following(d):
    return b3_next_session(d)

def generate_periods_semester_b3(emissao, first_coupon, end_date, months_step=6, max_steps=400):
    em = _naive(emissao); e = _naive(end_date)
    if em is pd.NaT or e is pd.NaT or e <= em or months_step <= 0:
        return []
    periods = []
    prev_adj = roll_following(em)

    for k in range(1, max_steps + 1):
        # se já alcançou/ultrapassou o fim, fecha e sai
        if prev_adj >= e:
            periods.append((prev_adj, e))
            break
        try:
            unadj = (em + pd.DateOffset(months=int(months_step) * k)).normalize()
        except Exception:
            # Overflow (atingiu limite do pandas) → fecha no end_date e sai
            periods.append((prev_adj, e))
            break

        adj = roll_following(unadj)
        if pd.isna(adj) or adj <= prev_adj:
            adj = roll_following(prev_adj + pd.Timedelta(days=1))
            if pd.isna(adj) or adj <= prev_adj:
                periods.append((prev_adj, e))
                break

        if adj >= e:
            periods.append((prev_adj, e))
            break

        periods.append((prev_adj, adj))
        prev_adj = adj

    return periods

def _fmt_money(x):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "—"
        return f"{float(x):,.2f}"
    except Exception:
        return "—"

def _safe_date_from_any(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    try:
        t = _naive(pd.to_datetime(x))
        return None if (t is pd.NaT) else t
    except Exception:
        return None

def is_real_date(x):
    try:
        return _naive(pd.to_datetime(x)) is not pd.NaT
    except Exception:
        return False

def safe_add_days(ts: pd.Timestamp, days: int) -> Optional[pd.Timestamp]:
    t = _naive(ts)
    if t is pd.NaT:
        return None
    try:
        return (t + pd.Timedelta(days=int(days))).normalize()
    except Exception:
        return None

def _roll_to_prev_session(d):
    d = _naive(d)
    if d is pd.NaT:
        return pd.NaT
    cand = d
    for _ in range(40):
        cand = cand - pd.Timedelta(days=1)
        s = b3_next_session(cand)
        if s is not pd.NaT and s.month == d.month and s <= d:
            return s
    return pd.NaT

def roll_modified_following(d):
    d = _naive(d)
    if d is pd.NaT:
        return pd.NaT
    fwd = b3_next_session(d)
    if fwd is pd.NaT:
        return pd.NaT
    if fwd.month == d.month:
        return fwd
    return _roll_to_prev_session(d)

def next_call_after(base, call_start, step_days, max_loops=4000, max_years=50):
    d = _naive(call_start); b = _naive(base)
    try:
        step = int(step_days)
    except Exception:
        return pd.NaT
    if d is pd.NaT or b is pd.NaT or step <= 0:
        return pd.NaT
    horizon = b + pd.DateOffset(years=int(max_years))
    loops = 0
    while d < b:
        loops += 1
        if loops > max_loops or d > horizon:
            return pd.NaT
        nd = safe_add_days(d, step)
        if nd is None:
            return pd.NaT
        d = nd
    return roll_modified_following(d)

def next_call_after_months(base, first_coupon, months_step=6):
    """
    Próxima CALL usando a MESMA lógica dos cupons: +N meses com modified-following.
    """
    b = _naive(base)
    d = roll_modified_following(first_coupon)
    guard = 0
    while d < b and guard < 1000:
        d = roll_modified_following(d + pd.DateOffset(months=int(months_step)))
        guard += 1
    return d

def _ytc_anchor(call_start: pd.Timestamp | None, emissao: pd.Timestamp) -> pd.Timestamp:
    """Âncora para YTC: usa Data de início dos CALLs se existir; senão, Emissão."""
    c0 = _naive(call_start)
    return (c0 if (c0 is not pd.NaT) else _naive(emissao))


def generate_coupons_lfsc(emissao, first_coupon, end_exclusive, step_days=180, max_steps=2000):
    d0 = roll_modified_following(emissao)
    d1 = roll_modified_following(first_coupon)
    e  = roll_modified_following(end_exclusive)
    if pd.isna(d0) or pd.isna(d1) or pd.isna(e) or (d1 <= d0) or (e <= d1):
        return []
    periods = [(d0, d1)]
    cur = d1
    cnt = 0
    while True:
        cnt += 1
        if cnt > max_steps:
            break
        nd = safe_add_days(cur, int(step_days))
        if nd is None:
            break
        nxt = roll_modified_following(nd)
        if pd.isna(nxt) or nxt >= e:
            periods.append((cur, e))
            break
        periods.append((cur, nxt))
        cur = nxt
    return periods

def generate_coupons_lfsc2(emissao,
                            first_coupon,           # ignorado no modo Excel
                            end_exclusive,
                            step_days=180,
                            max_steps=2000):
    """
    MODO EXCEL: gera períodos em DIAS CORRIDOS exatos (emissão + k*step_days),
    sem qualquer rolling. O 'first_coupon' é IGNORADO para garantir
    a grade aritmética perfeita.
    Retorna pares (ini, fim_raw) estritamente na grade.
    O último intervalo fecha em 'end_exclusive' (mesmo que 'off-grid').
    """
    em_dt = _naive(emissao)
    e_raw = _naive(end_exclusive)
    step  = int(step_days)

    if (em_dt is pd.NaT) or (e_raw is pd.NaT) or (step <= 0) or (e_raw <= em_dt):
        return []

    periods = []
    prev = em_dt
    # primeira âncora perfeita na grade aritmética
    cur  = (em_dt + pd.Timedelta(days=step))

    # anda em saltos exatos até alcançar/ultrapassar o fim
    n = 0
    while (cur < e_raw) and (n < max_steps):
        periods.append((prev, cur))
        prev = cur
        cur  = (cur + pd.Timedelta(days=step))
        n   += 1

    # fecha no fim (pode ser off-grid)
    periods.append((prev, e_raw))
    return periods


def last_and_next_coupon2(base_dt: pd.Timestamp,
                          em_dt: pd.Timestamp,
                          first_coupon: pd.Timestamp,   # ignorado no modo Excel
                          ven_dt: pd.Timestamp,
                          step_days: int = 180) -> tuple[pd.Timestamp, pd.Timestamp | None]:
    """
    MODO EXCEL: âncoras por fórmula fechada (emissão + k*step).
    'last' = emissão + floor((base - emissão)/step)*step
    'next' = last + step (limitado a ven_dt)
    """
    base = _naive(base_dt); em = _naive(em_dt); ven = _naive(ven_dt)
    step = int(step_days)

    if any(pd.isna(x) for x in [base, em, ven]) or (step <= 0) or (ven <= em):
        return em_dt, None

    # diferença em dias corridos
    delta_days = (base - em).days
    k = max(0, delta_days // step)

    last = em + pd.Timedelta(days=k * step)
    nxt  = last + pd.Timedelta(days=step)

    if nxt > ven:
        nxt = ven

    # se base cai exatamente em 'last', tratamos 'last' como "passado" e 'nxt' como "próximo"
    return last, nxt

def infer_coupon_anchors_from_emissao2(base_dt: pd.Timestamp,
                                       em_dt: pd.Timestamp,
                                       ven_dt: pd.Timestamp,
                                       step_days: int = 180) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    MODO EXCEL: infere (last, next) diretamente pela grade aritmética.
    Ignora 'first_coupon' e qualquer rolling. Consistente com a planilha.
    """
    base = _naive(base_dt); em = _naive(em_dt); ven = _naive(ven_dt)
    step = int(step_days)

    if any(pd.isna(x) for x in [base, em, ven]) or (step <= 0) or (ven <= em):
        # fallback trivial
        return em, em + pd.Timedelta(days=max(step, 1))

    delta_days = (base - em).days
    k = max(0, delta_days // step)

    last = em + pd.Timedelta(days=k * step)
    nxt  = last + pd.Timedelta(days=step)

    if nxt > ven:
        nxt = ven

    return last, nxt

if st.session_state.get("_warned_cdi_fallback") is not True:
    st.session_state["_warned_cdi_fallback"] = True
    st.warning("⚠️ Aviso: SGS indisponível; usando CDI local (CSV).")


@st.cache_data(ttl=24*60*60)
def compute_cdi_factor_sgs(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    include_start: bool = False,
    include_end: bool = True,
    return_series: bool = False,
    cap_to_date: pd.Timestamp | None = None,
):
    # Helpers robustos p/ NaT
    def _isnat(x):
        return (x is None) or (isinstance(x, pd.Timestamp) and pd.isna(x))

    s = _naive(start_date); e = _naive(end_date)
    cap = _naive(cap_to_date) if cap_to_date is not None else None

    if _isnat(s) or _isnat(e) or (e <= s):
        return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    # ---------- 1) Tenta SGS ----------
    use_fallback = False
    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
        f"?formato=json&dataInicial={s.strftime('%d/%m/%Y')}&dataFinal={e.strftime('%d/%m/%Y')}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        js = resp.json()
    except Exception:
        use_fallback = True
        js = None

    # ---------- 2) Monta série (SGS OU fallback) ----------
    if not use_fallback and js:
        df = pd.DataFrame(js)
        df["Data"]  = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce").dt.normalize()
        df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        df = df.dropna(subset=["Data","valor"]).sort_values("Data")
        sgs = (df.set_index("Data")["valor"] / 100.0).astype(float)
    else:
        # <<< CORREÇÃO AQUI: chamar a função certa >>>
        sgs = _load_cdi_cached_series_once()  # era _load_cdi_cached_series_once()
        if sgs.empty:
            # nada a fazer; devolve neutro
            return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

        # Preenche dias úteis até o limite (cap ou end_date)
        R = _naive(cap_to_date) if cap_to_date is not None else None
        end_cap_for_fill = min(e, R) if (not _isnat(R)) else e
        first_csv_date = sgs.index.min()
        if _isnat(first_csv_date):
            return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0
        idx_fill = b3_sessions(first_csv_date, end_cap_for_fill)
        sgs = sgs.reindex(idx_fill).ffill()

    if sgs.empty:
        return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    # ---------- 3) HARD CAP ----------
    if cap_to_date is not None and not _isnat(cap):
        sgs = sgs.loc[sgs.index <= cap]
        if sgs.empty:
            return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    # ---------- 4) Janela e fator ----------
    last_pub = sgs.index.max()
    end_cap = min(e, last_pub)
    if end_cap <= s:
        return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    idx_all = b3_sessions(s, end_cap)
    if include_start and include_end:
        idx_use = idx_all[(idx_all >= s) & (idx_all <= end_cap)]
    elif include_start and not include_end:
        idx_use = idx_all[(idx_all >= s) & (idx_all <  end_cap)]
    elif (not include_start) and include_end:
        idx_use = idx_all[(idx_all >  s) & (idx_all <= end_cap)]
    else:
        idx_use = idx_all[(idx_all >  s) & (idx_all <  end_cap)]

    if len(idx_use) == 0:
        return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    ser = sgs.reindex(idx_use)
    # Se vierem buracos do SGS, você pode escolher:
    # ser = ser.ffill()  # OU manter dropna:
    ser = ser.dropna()
    if ser.empty:
        return (1.0, 0, pd.Series(dtype=float)) if return_series else 1.0

    ln_sum = np.log1p(ser.values).sum()
    fator = float(np.exp(ln_sum))
    n_du  = int(len(ser))
    return (fator, n_du, ser) if return_series else fator



def make_cdi_sgs_xlsx(start_date: pd.Timestamp, end_date: pd.Timestamp, include_end: bool = True) -> bytes:
    fator, n_du, ser = compute_cdi_factor_sgs(
        start_date, end_date, include_end=include_end, return_series=True
    )
    df = (pd.DataFrame({"Data": ser.index, "CDI_decimal": ser.values.astype(float)})
            .sort_values("Data").reset_index(drop=True))
    if not df.empty:
        df["CDI_%a.d."] = df["CDI_decimal"] * 100.0
        df["G"]  = (1.0 + df["CDI_decimal"]).cumprod()
        df["DF"] = 1.0 / df["G"]

    end_utilizado = (df["Data"].max().date() if not df.empty else None)
    cdi_media_aa = ((df["G"].iloc[-1]**(252.0/n_du) - 1.0) if (not df.empty and n_du > 0) else None)
    resumo = pd.DataFrame([
        {"Campo": "Início solicitado", "Valor": pd.to_datetime(start_date).date()},
        {"Campo": "Fim solicitado",    "Valor": pd.to_datetime(end_date).date()},
        {"Campo": "Fim utilizado",     "Valor": end_utilizado},
        {"Campo": "include_end?",      "Valor": bool(include_end)},
        {"Campo": "DU utilizados",     "Valor": int(n_du)},
        {"Campo": "Fator_CDI",         "Valor": float(fator)},
        {"Campo": "CDI médio a.a. (aprox.)", "Valor": (float(cdi_media_aa) if cdi_media_aa is not None else None)},
        {"Campo": "Notas",             "Valor": "Série SGS 12 (CDI %a.d.). Sem forward-fill; acumula só dias com dado publicado."},
    ])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        (df[["Data","CDI_%a.d.","CDI_decimal","G","DF"]] if not df.empty else df)\
            .to_excel(xw, index=False, sheet_name="Historico_CDI_SGS")
        ws1 = xw.sheets["Historico_CDI_SGS"]
        ws1.set_column(0, 0, 12)
        ws1.set_column(1, 4, 16)
        resumo.to_excel(xw, index=False, sheet_name="Resumo")
        ws2 = xw.sheets["Resumo"]
        ws2.set_column(0, 0, 26)
        ws2.set_column(1, 1, 28)
    return buf.getvalue()

@st.cache_data
def build_acc_series_for(em_dt, ven_dt, di_forward, ref_date=None, use_history=True) -> pd.Series:
    em_dt = _naive(em_dt); ven_dt = _naive(ven_dt)
    if em_dt is pd.NaT or ven_dt is pd.NaT or ven_dt <= em_dt:
        return pd.Series(dtype=float)

    idx_full = b3_sessions(em_dt, ven_dt)
    if len(idx_full) == 0:
        return pd.Series(dtype=float)

    s = pd.Series(index=idx_full, dtype=float)

    if use_history and ref_date is not None:
        rb = _naive(ref_date)
        left_end = min(rb, ven_dt) if rb is not pd.NaT else em_dt
        if (rb is not pd.NaT) and (left_end > em_dt):
            hist = load_cdi_sgs_daily(em_dt, left_end)
            if not hist.empty:
                s.loc[s.index <= left_end] = hist.reindex(s.index[s.index <= left_end]).values

    if di_forward is not None and not di_forward.empty:
        if ref_date is None:
            s.loc[s.index.isin(di_forward.index)] = di_forward.values
        else:
            s_fwd_idx = s.index[s.index > _naive(ref_date)]
            if len(s_fwd_idx) > 0:
                s.loc[s_fwd_idx] = di_forward.reindex(s_fwd_idx).values

    if s.isna().any():
        s = s.ffill()
        if s.isna().any():
            r0 = _aa_to_daily_scalar(CDI_AA_DEFAULT)
            s = s.fillna(r0)
    return s.astype(float)


# ---------- helpers para UI de ativo ----------
# ---------- helpers para UI de ativo ----------
def _is_empty_like(v):
    if v is None: return True
    if isinstance(v, float) and np.isnan(v): return True
    if isinstance(v, str) and v.strip().lower() in {"", "nat", "nan", "none", "—", "-", "null"}:
        return True
    return False

def pick_first_nonnull(*vals):
    for v in vals:
        if not _is_empty_like(v):
            return v
    return None

def get_fields_for_code(sel_code: str) -> dict:
    orow = out[out["Codigo"].astype(str) == str(sel_code)].iloc[0].to_dict()
    try:
        mrow = merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel_code)]
        mrow = (mrow.iloc[0].to_dict() if not mrow.empty else {})
    except Exception:
        mrow = {}

    def g(d, *keys):
        for k in keys:
            if k in d and d[k] not in [None, "", np.nan]:
                return d[k]
        return None

    campos = {
        "PU_emissao": pick_first_nonnull(orow.get("PU_emissao"), g(mrow, "PU_emissao_final")),
        "alpha":      pick_first_nonnull(orow.get("alpha_(%CDI fator)"), g(mrow, "alpha_norm")),
        "beta":       None,
        "Data_emissao": pick_first_nonnull(orow.get("Data_emissao"), g(mrow, "Data_Emissao_final", "Data")),
        "Data_prox_juros": pick_first_nonnull(orow.get("Data_prox_juros"), g(mrow, "Data_Prox_Juros_final")),
        "Vencimento": pick_first_nonnull(orow.get("Vencimento"), g(mrow, "Vencimento_final", "Vencimento do ativo")),
        "Data_call":  pick_first_nonnull(orow.get("Data_call"), g(mrow, "Data_Call_Inicial_final")),
        "PU_posicao": pick_first_nonnull(orow.get("PU_posicao"), g(mrow, "Pu Posição_num")),
        "Forma":      pick_first_nonnull(orow.get("FormaCDI_final"), g(mrow, "FormaCDI_final")),
        "Estrategia": pick_first_nonnull(g(orow, "Estratégia", "Estrategia"), g(mrow, "Estratégia")),
        "Ciclo":      pick_first_nonnull(orow.get("Ciclo_juros"), g(mrow, "CicloJuros_final")),
        "AgendaJuros": pick_first_nonnull(orow.get("AgendaJuros"), g(mrow, "AgendaJuros_final")), # <<< ALTERAÇÃO
        "IncorporaJuros": pick_first_nonnull(orow.get("IncorporaJuros"), g(mrow, "IncorporaJuros_final")),
    }

    # <<< ALTERAÇÃO: Mover a extração de 'meses' e 'dias' para cá para ser mais robusto
    meses = None; call_step = None; step_days = None
    if isinstance(campos["Ciclo"], str):
        txt = campos["Ciclo"].upper()
        m_dias = re.search(r"(\d+)\s*(DIAS|DIA|D)$", txt)
        if m_dias:
            step_days = int(m_dias.group(1))
            call_step = step_days  # Mantém compatibilidade com a lógica de call
        m_meses = re.search(r"(\d+)\s*(MES|MESES|M)$", txt)
        if m_meses:
            meses = int(m_meses.group(1))
        m_uteis = re.search(r"(\d+)\s*(UTEIS|ÚTEIS|DU|BUS)$", txt)
        if m_uteis and call_step is None:
            # Mantém a lógica original para dias úteis se não for dias corridos
            call_step = int(m_uteis.group(1))

    campos["meses"] = meses
    campos["call_step"] = call_step
    campos["step_days"] = step_days # <<< NOVO CAMPO para dias corridos

    agenda = None
    step_days = None
    txt_ciclo = str(campos.get("Ciclo") or "").strip().upper()

    # Regra por texto do Ciclo
    if re.search(r"\b(MES|MESES|M)\b", txt_ciclo):
        agenda = "M"
    if re.search(r"\b(DIAS|D)\b", txt_ciclo):
        agenda = "D"
    if re.search(r"\b(UTEIS|ÚTEIS|DU|BUS)\b", txt_ciclo):
        # Tratamos como "D" (dias corridos) para a malha dos cupons,
        # a menos que você implemente contagem por DU também nos cupons.
        agenda = "D"

    # Extrai número de dias do Ciclo quando for "D"
    m_days = re.search(r"(\d+)\s*(DIAS|D|UTEIS|ÚTEIS|DU|BUS)\b", txt_ciclo)
    if m_days:
        try:
            step_days = int(m_days.group(1))
        except Exception:
            step_days = None

    # Heurísticas se o texto do Ciclo não for conclusivo:
    if agenda is None:
        if campos.get("meses"):
            agenda = "M"
        elif campos.get("call_step"):
            agenda = "D"
        else:
            # default conservador
            agenda = "M"

    # Se ainda não temos step_days e a agenda é "D", use um default
    if step_days is None and agenda == "D":
        step_days = int(campos.get("call_step") or 180)

    campos["AgendaJuros"] = agenda          # 'D' (dias) ou 'M' (meses)
    campos["step_days"]   = step_days       # só usado quando AgendaJuros == 'D'

    return campos

if "overrides" not in st.session_state:
    st.session_state["overrides"] = {}

def _normalize_override_row(r: dict) -> dict:
    m = {k.strip(): (None if pd.isna(v) else v) for k, v in r.items()}
    alias = {
        "Codigo": ["codigo", "cod", "Ativo", "ativo"],
        "PU_emissao": ["PU_emissão", "PU", "principal", "Principal"],
        "alpha": ["Alpha", "alpha_(%CDI fator)", "pct_cdi", "%CDI"],
        "beta": ["Beta", "beta_desconto", "beta_mtm"],
        "Data_emissao": ["Data Emissao", "Data_Emissão", "emissao", "Emissão"],
        "Data_prox_juros": ["Data Proximo Juros", "1o_cupom", "Primeiro cupom"],
        "Vencimento": ["venc", "vcto", "Venc"],
        "Data_call": ["Data Call Inicial", "call_start"],
        "call_step": ["callstep", "dias_call"],
        "meses": ["meses_entre_cupons", "periodicidade_meses"],
        "IncorporaJuros": ["incorporajuros", "incorpora", "capitaliza"],
    }
    for k, al in alias.items():
        if k not in m or m[k] is None:
            for a in al:
                if a in m and m[a] is not None:
                    m[k] = m[a]; break
    if isinstance(m.get("IncorporaJuros"), str):
        s = m["IncorporaJuros"].strip().upper()
        if s in {"TRUE","YES","Y","SIM","S"}: m["IncorporaJuros"] = "SIM"
        elif s in {"FALSE","NO","N","NAO","NÃO"}: m["IncorporaJuros"] = "NAO"
    return {k: m.get(k) for k in [
        "Codigo","PU_emissao","alpha","beta","Data_emissao","Data_prox_juros",
        "Vencimento","Data_call","call_step","meses","IncorporaJuros"
    ]}

def _apply_overrides(f: dict, code: str) -> dict:
    ov = st.session_state["overrides"].get(str(code), {})
    if not ov: return f
    out_loc = f.copy()
    for k, v in ov.items():
        if v not in [None, "", np.nan]:
            out_loc[k] = v
    return out_loc


def di_period_from_implied(Eprev: int, Ecur: int, R_prev: float | None, R_cur: float) -> tuple[float, float]:
    """Retorna (fator_intervalo, di_per_aa) entre Eprev e Ecur a partir de R_implied a.a. em cada DU acumulado."""
    if Ecur <= Eprev:
        return 1.0, 0.0
    D = float(Ecur - Eprev)
    A_prev = 1.0 if (Eprev == 0 or R_prev is None or np.isnan(R_prev)) else (1.0 + float(R_prev))**(Eprev/252.0)
    A_cur  = (1.0 + float(R_cur))**(Ecur/252.0)
    fator_intervalo = A_cur / A_prev
    di_per_aa       = fator_intervalo**(252.0 / D) - 1.0
    return float(fator_intervalo), float(di_per_aa)

def R_implied_at(s_R: pd.Series, DU: int) -> float:
    if DU <= 0:
        return 0.0
    if DU in s_R.index:
        return float(s_R.loc[DU])
    return float(s_R.loc[int(s_R.index.max())])

@st.cache_data
def carregar_R_implied_por_DU() -> pd.Series:
    """
    Lê DADOS_DIR/'curva_di_interpolada_por_DU.parquet' (ou /mnt/data se existir),
    e retorna Série com índice DU (int) e valores de taxa a.a. implícita (decimal).
    Aceita % (ex.: 13.5) ou decimal (0.135).
    """
    CURVA_DU_PARQUET_DEFAULT = Path("Dados") / "curva_di_interpolada_por_DU.parquet"
    CURVA_DU_PARQUET = (Path("/mnt/data/curva_di_interpolada_por_DU.parquet")
                        if Path("/mnt/data/curva_di_interpolada_por_DU.parquet").exists()
                        else CURVA_DU_PARQUET_DEFAULT)
    if not CURVA_DU_PARQUET.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {CURVA_DU_PARQUET}")

    df = pd.read_parquet(CURVA_DU_PARQUET).copy()
    col = "R_implied_aa" if "R_implied_aa" in df.columns else (
            "di_aa_252_interp_pct" if "di_aa_252_interp_pct" in df.columns else None)
    if col is None:
        raise ValueError("Preciso de 'R_implied_aa' (ou 'di_aa_252_interp_pct') no parquet.")

    curva = df[["DU", col]].dropna().copy()
    curva["DU"]  = pd.to_numeric(curva["DU"], errors="coerce").astype("Int64").dropna()
    vals         = pd.to_numeric(curva[col], errors="coerce").astype(float)
    vals         = np.where(vals > 2.5, vals/100.0, vals)  # aceita % ou decimal
    s = pd.Series(vals, index=curva["DU"].astype(int)).sort_index()

    if len(s.index):
        s = s.reindex(range(int(s.index.min()), int(s.index.max())+1)).ffill()
    return s

def R_implied_at(s_R: pd.Series, DU: int) -> float:
    if DU <= 0:
        return 0.0
    if DU in s_R.index:
        return float(s_R.loc[DU])
    return float(s_R.loc[int(s_R.index.max())])

def di_period_from_implied(Eprev: int, Ecur: int, R_prev: float | None, R_cur: float) -> tuple[float, float]:
    """
    Igual à sua metodologia: dado Eprev/Ecur (DUs cumulativos) e as taxas a.a.
    implícitas nesses DUs, obtém:
        - fator_intervalo (A_cur/A_prev)
        - di_per_aa (taxa a.a. constante equivalente no intervalo)
    """
    if Ecur <= Eprev:
        return 1.0, 0.0
    D = float(Ecur - Eprev)
    A_prev = 1.0 if (Eprev == 0 or R_prev is None or np.isnan(R_prev)) else (1.0 + float(R_prev))**(Eprev/252.0)
    A_cur  = (1.0 + float(R_cur))**(Ecur/252.0)
    fator_intervalo = A_cur / A_prev
    di_per_aa       = fator_intervalo**(252.0 / D) - 1.0
    return float(fator_intervalo), float(di_per_aa)

def _du_cum_from_base(base_dt: pd.Timestamp, dt: pd.Timestamp) -> int:
    """DU acumulado desde a base (exclui base, inclui 'dt')."""
    if dt <= base_dt:
        return 0
    return int(b3_count_excl(base_dt, dt))

def _df_desconto_desde_base_por_DU(base_dt, fim, beta, s_R):
    D = _du_cum_from_base(base_dt, fim)
    if D <= 0: 
        return 1.0
    DF = 1.0
    A_prev = 1.0
    for k in range(1, D+1):
        Rk   = R_implied_at(s_R, k)
        A_k  = (1.0 + Rk)**(k/252.0)
        # para DU=1, fator_intervalo = A_k / A_prev; a "taxa diária" é esse fator - 1
        r_d  = (A_k / A_prev) - 1.0
        DF  *= (1.0 + beta * r_d)  # aplica β no dia
        A_prev = A_k

    return float(DF)

# ===================== VISÃO 2 — Calculadora de Ativos (CORRIGIDA: metodologia por DU) =====================
if visao == "Calculadora de Ativos":
    st.title("HEDGE DI — Calculadora de Ativos")

    # Escolha do ativo
    if ("Codigo" not in out.columns) or out.empty:
        st.info("Nenhum ativo disponível na filtragem atual.")
        st.stop()
    cods = out["Codigo"].dropna().astype(str).unique().tolist()
    if not cods:
        st.info("Nenhum ativo disponível na filtragem atual.")
        st.stop()
    sel = st.selectbox("Ativo (Código)", sorted(cods))

    # ---------------- Helpers locais p/ datas (inalterados) ----------------
    def _safe_ui_date(val, fallback_date):
        from datetime import date as _date, datetime as _dt
        try:
            if isinstance(fallback_date, _date) and not isinstance(fallback_date, _dt):
                fb = fallback_date
            else:
                fb = pd.Timestamp(fallback_date)
                fb = fb.to_pydatetime().date()
        except Exception:
            fb = pd.Timestamp.today().normalize().to_pydatetime().date()
        if val is None:
            return fb
        if isinstance(val, _date) and not isinstance(val, _dt):
            return val
        try:
            ts = pd.to_datetime(val, errors="coerce", dayfirst=True)
            if pd.isna(ts):
                return fb
            return ts.to_pydatetime().date()
        except Exception:
            return fb

    _ref_base = pd.to_datetime(REF_DATE_CURVA).date()

    # ---------------- NOVOS HELPERS — METODOLOGIA POR DU ----------------
    # Carrega curva por DU (do seu parquet), como você faz na metodologia-alvo
    

    # ========= Expander “Completar dados” (inalterado nas entradas/UI) =========
    with st.expander("🔧 Completar dados faltantes deste ativo"):
        f_cur = _apply_overrides(get_fields_for_code(sel), sel)
        current = {
            "Data_Emissao_final":      f_cur.get("Data_emissao"),
            "Vencimento_final":        f_cur.get("Vencimento"),
            "Data_Prox_Juros_final":   f_cur.get("Data_prox_juros"),
            "Data_Call_Inicial_final": f_cur.get("Data_call"),
            "CicloJuros_final":        f_cur.get("Ciclo"),
            "alpha_norm":              f_cur.get("alpha"),
            "PU_emissao_final":        f_cur.get("PU_emissao"),
            "beta":                    f_cur.get("beta"),
            "meses":                   f_cur.get("meses"),
            "call_step":               f_cur.get("call_step"),
            "IncorporaJuros":          (f_cur.get("IncorporaJuros") or ""),
        }

        with st.form(key=f"override_form_{sel}"):
            col1, col2 = st.columns(2)

            Data_Emissao_final = col1.date_input(
                "Data de Emissão",
                value=_safe_ui_date(current["Data_Emissao_final"], _ref_base),
                key=f"ov2_em_{sel}"
            )
            Vencimento_final = col2.date_input(
                "Vencimento",
                value=_safe_ui_date(current["Vencimento_final"], (_ref_base + pd.DateOffset(years=2)).date()),
                key=f"ov2_ven_{sel}"
            )
            Data_Prox_Juros_final = col1.date_input(
                "Data Próximo Juros (1º cupom, opcional)",
                value=_safe_ui_date(current["Data_Prox_Juros_final"], _ref_base),
                key=f"ov2_fc_{sel}"
            )
            Data_Call_Inicial_final = col2.date_input(
                "Data de Início dos CALLs (LFSC) (opcional)",
                value=_safe_ui_date(current["Data_Call_Inicial_final"], _ref_base),
                key=f"ov2_call_{sel}"
            )

            CicloJuros_final = col1.text_input(
                "Ciclo de Juros (ex: 180 DIAS / 180 UTEIS / 6 MESES / BULLET)",
                value=str(current["CicloJuros_final"] or ""),
                key=f"ov2_ciclo_{sel}"
            )

            alpha_norm_in = col2.number_input(
                "Alpha (fator: 1.19 ≈ 119% do indexador)",
                value=float(pd.to_numeric(current["alpha_norm"], errors="coerce")) if pd.notna(pd.to_numeric(current["alpha_norm"], errors="coerce")) else 1.0,
                step=0.01, format="%.6f", key=f"ov2_alpha_{sel}"
            )
            PU_emissao_final = col1.number_input(
                "PU emissão (por unidade)",
                value=float(pd.to_numeric(current["PU_emissao_final"], errors="coerce")) if pd.notna(pd.to_numeric(current["PU_emissao_final"], errors="coerce")) else 1000.0,
                step=0.01, format="%.2f", key=f"ov2_pu_{sel}"
            )
            beta_in = col2.number_input(
                "Beta p/ desconto PV (%indexador)",
                value=float(pd.to_numeric(current["beta"], errors="coerce")) if pd.notna(pd.to_numeric(current["beta"], errors="coerce")) else 1.00,
                step=0.01, format="%.6f", key=f"ov2_beta_{sel}"
            )

            colA, colB, colC = st.columns(3)
            meses_in = colA.number_input(
                "Meses entre cupons (se não incorpora)",
                value=int(current["meses"] or 6), step=1, key=f"ov2_meses_{sel}"
            )
            call_step_in = colB.number_input(
                "Dias corridos entre CALLs (LFSC)",
                value=int(current["call_step"] or 180), step=1, key=f"ov2_callstep_{sel}"
            )
            inc_sel = colC.selectbox(
                "Incorpora Juros?",
                ["", "SIM", "NAO"],
                index=(["","SIM","NAO"].index(str(current["IncorporaJuros"]).upper())
                       if str(current["IncorporaJuros"]).upper() in ["","SIM","NAO"] else 0),
                key=f"ov2_inc_{sel}"
            )

            submitted_ov = st.form_submit_button("Salvar overrides")
            if submitted_ov:
                patch = {
                    "Data_emissao":    pd.to_datetime(Data_Emissao_final),
                    "Vencimento":      pd.to_datetime(Vencimento_final),
                    "Data_prox_juros": pd.to_datetime(Data_Prox_Juros_final) if Data_Prox_Juros_final else None,
                    "Data_call":       pd.to_datetime(Data_Call_Inicial_final) if Data_Call_Inicial_final else None,
                    "Ciclo":           (CicloJuros_final.strip() or None),
                    "alpha":           float(alpha_norm_in) if alpha_norm_in else None,
                    "PU_emissao":      float(PU_emissao_final) if PU_emissao_final else None,
                    "beta":            float(beta_in) if beta_in else None,
                    "meses":           int(meses_in) if meses_in else None,
                    "call_step":       int(call_step_in) if call_step_in else None,
                    "IncorporaJuros":  (inc_sel or None),
                }
                st.session_state["overrides"].setdefault(str(sel), {}).update(patch)
                st.success("Overrides salvos. Recalcule/role a página para atualizar os cálculos.")

    f = _apply_overrides(get_fields_for_code(sel), sel)
    if sel in st.session_state.get("overrides", {}):
        st.info("✅ Override ativo para este código será aplicado na calculadora.")

    inc_flag = (str(f.get("IncorporaJuros") or "")).strip().upper()
    if inc_flag not in {"SIM","NAO"}:
        inc_flag = ""

    motor_auto = str(merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel), "MOTOR_CALC"].iloc[0] 
                     if (("MOTOR_CALC" in merged.columns) and
                         (not merged.loc[merged["cod_Ativo_guess"].astype(str)==str(sel)].empty))
                     else MOTOR_PCT_CDI)

    motor = st.radio(
        "Motor de cálculo",
        [MOTOR_PCT_CDI, MOTOR_YTC_CHAMADAS],
        index=(0 if motor_auto == MOTOR_PCT_CDI else 1),
        help="PCT_CDI: usa CDI como indexador (bullet/cupom). YTC_CHAMADAS: fluxo até a próxima CALL.",
        key=f"motor_{sel}"
    )
    if inc_flag:
        st.caption(f"**Incorpora Juros:** {inc_flag}")
    if motor == MOTOR_PCT_CDI:
        if inc_flag == "SIM":
            st.caption("Forma de pagamento: **bullet capitalizado** (juros + principal no vencimento).")
        else:
            st.caption("Forma de pagamento: **cupom periódico** (sem capitalizar).")

    # -------------------- Form principal (inalterado na UI) --------------------
    with st.form(key=f"form_calc_{sel}"):
        c1, c2, c3 = st.columns(3)
        pu_default = float(f.get("PU_emissao") or 1000.0)
        PU_emissao = c1.number_input("PU de emissão",
                                     value=pu_default, step=100.0, format="%.2f", key=f"pu_{sel}")
        alpha_default = float(f.get("alpha") or 1.0)
        alpha = c2.number_input("Taxa de Emissão",
                                value=alpha_default, step=0.01, format="%.6f",
                                help="Ex.: 1.18 = 118% do CDI/SELIC", key=f"alpha_{sel}")

        beta_sug = None
        try:
            row_sel = merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel)]
            if not row_sel.empty:
                beta_sug = float(row_sel["beta_from_mtm"].iloc[0]) if pd.notna(row_sel["beta_from_mtm"].iloc[0]) else None
        except Exception:
            pass
        beta_default = float(
            f.get("beta") if f.get("beta") not in [None, "", np.nan] else
            (beta_sug if beta_sug is not None else (1.06 if motor == MOTOR_YTC_CHAMADAS else 1.00))
        )
        beta = c3.number_input(
            "Taxa de Marcação a Mercado",
            value=beta_default, step=0.01, format="%.6f", key=f"beta_{sel}"
        )

        de_val = _safe_ui_date(f.get("Data_emissao"), _ref_base)
        dfc_pref = _safe_date_from_any(f.get("Data_prox_juros"))
        if dfc_pref is not None:
            dfc_val = dfc_pref.to_pydatetime().date()
        else:
            meses_default = int(f.get("meses") or 6)
            try:
                em_ts = pd.Timestamp(de_val)
                unadj = (em_ts + pd.DateOffset(months=meses_default)).normalize()
                fc_guess = b3_next_session(unadj)
                dfc_val = (fc_guess.to_pydatetime().date()
                           if fc_guess is not pd.NaT else _ref_base)
            except Exception:
                dfc_val = _ref_base

        dv_val = _safe_ui_date(f.get("Vencimento"), (_ref_base + pd.DateOffset(years=2)).date())

        dcol1, dcol2, dcol3 = st.columns(3)
        Data_emissao    = dcol1.date_input("Data de Emissão", value=de_val, key=f"inp_em_{sel}")
        Data_prox_juros = dcol2.date_input("Data do 1º Cupom (Data Específica/Emissão + Ciclo de Juros)", value=dfc_val, key=f"inp_fc_{sel}")
        Vencimento      = dcol3.date_input("Vencimento (bullet/último cupom)", value=dv_val, key=f"inp_ven_{sel}")

        if motor == MOTOR_PCT_CDI:
            if (inc_flag == "SIM"):
                meses = None
                Data_call = None; call_step = None
            else:
                meses_default = int(f.get("meses") or 6)
                meses = st.number_input("Meses entre cupons", value=meses_default, step=1, key=f"meses_{sel}")
                Data_call = None; call_step = None
        else:
            cc1, cc2= st.columns(2)
            with cc1:
                dcall_val = _safe_ui_date(f.get("Data_call"), _ref_base)
                Data_call = st.date_input("Data de início dos Calls", value=dcall_val, key=f"inp_call_{sel}")
            with cc2:
                call_months_default = 6
                if f.get("call_step") == 360 or f.get("Ciclo") == "360 DIAS":
                    call_months_default = 12

                call_months_default = int(f.get("meses") or call_months_default)  # ou guarde em outro campo específico de CALLs
                call_months = st.number_input("Meses entre CALLs", value=call_months_default, step=1, key=f"callmonths_{sel}")
                meses = None

        submitted = st.form_submit_button("Calcular fluxo")

    if submitted:
        base_dt = _naive(pd.to_datetime(_ref_base))
        st.caption(f"🗓️ Data-base (ref_date): **{base_dt.date()}**")
        em_dt   = _safe_date_from_any(Data_emissao)
        fcup_dt = _safe_date_from_any(Data_prox_juros)
        ven_dt  = _safe_date_from_any(Vencimento)
        
        # <<< ALTERAÇÃO: Obter o valor da AgendaJuros aqui
        agenda_juros_val = str(f.get("AgendaJuros") or "").strip().upper()

        if (em_dt is None) or (ven_dt is None):
            st.error("Preencha **Emissão** e **Vencimento** para calcular.")
            st.stop()

        if (motor == MOTOR_PCT_CDI) and (inc_flag != "SIM") and (fcup_dt is None):
            meses_est = int(meses or f.get("meses") or 6)
            last_cpn_auto, next_cpn_auto = infer_coupon_anchors_from_emissao(
                base_dt=base_dt, em_dt=em_dt, ven_dt=ven_dt, months_step=meses_est
            )
            fcup_dt = b3_next_session((_naive(em_dt) + pd.DateOffset(months=meses_est)).normalize())
            st.info(
                f"1º cupom não informado — âncoras inferidas por {meses_est}M: "
                f"último={last_cpn_auto.date()}, próximo={next_cpn_auto.date()}."
            )

        if (motor == MOTOR_YTC_CHAMADAS) and (inc_flag != "SIM") and (fcup_dt is None):
            st.error("Não foi possível estimar o 1º cupom. Informe-o manualmente.")
            st.stop()

        if motor == MOTOR_YTC_CHAMADAS:
            if not isinstance(call_months, (int, float)) or int(call_months) <= 0:
                st.error("Informe **Meses entre CALLs** como inteiro positivo.")
                st.stop()

            call0_dt   = _naive(pd.to_datetime(Data_call))

            mes_def = 6
            if f.get("call_step") == 360 or f.get("Ciclo") == "360 DIAS":
                mes_def = 12

            meses_step = int(meses or f.get("meses") or mes_def)

            # 1) Próxima CALL de verdade: usa a regra de CALL (LFSC em dias corridos)
            if agenda_juros_val == 'D':
                # LÓGICA PARA DIAS CORRIDOS
                # Usa os dias corridos extraídos do campo "Ciclo" ou o override "call_step"
                call_step_dias = int(f.get("step_days") or f.get("call_step") or 180)
                next_call_dt = next_call_after(base_dt, call0_dt, step_days=call_step_dias)
                if next_call_dt is pd.NaT:
                    st.error("Não foi possível calcular a próxima CALL pela regra de dias corridos.")
                    st.stop()
            else:
                # LÓGICA PARA MESES (comportamento que já estava)
                # O número de meses entre calls já é derivado de call_step_days ou do input 'call_months'
                next_call_dt = next_call_after_months(base_dt, call0_dt, months_step=int(call_months))
                if next_call_dt is pd.NaT:
                    st.error("Não foi possível calcular a próxima CALL pela regra de meses.")
                    st.stop()

            # 2) Primeiro cupom para accrual/PU_hoje (se não informado, inferir por meses_step na malha de cupons)
            if _safe_date_from_any(fcup_dt) is None:
                fc_use = b3_next_session((_naive(em_dt) + pd.DateOffset(months=meses_step)).normalize())
            else:
                fc_use = _naive(fcup_dt)
            ytc_dt  = next_call_dt    
            ytc_ref = _ytc_anchor(call0_dt, em_dt)   # << NOVO
            if ytc_ref == next_call_dt:
                ytc_ref = em_dt
            # 1º cupom para a malha de cupons (usamos a frequência de CUPOM, não de CALL)
            mes_def = 6
            if f.get("call_step") == 360 or f.get("Ciclo") == "360 DIAS":
                mes_def = 12

            meses_step = int(meses or f.get("meses") or mes_def)

            if _safe_date_from_any(fcup_dt) is None:
                fc_use = b3_next_session((ytc_ref + pd.DateOffset(months=meses_step)).normalize())
            else:
                fc_use = _naive(fcup_dt)

            # 3) Agenda de cupons até a CALL (para calcular juros de cada período)
            periods = generate_coupons_semester_until(
                ytc_ref, fc_use, ytc_dt,
                months_step=meses_step   # <<<< antes estava int(call_months or meses_step)
            )
            
            if not periods:
                st.error("Não foi possível gerar a agenda semestral até a próxima CALL.")
                st.stop()

            # 4) Âncoras de cupom vigentes na base (para PU_hoje/accrual)
            last_cpn, next_cpn = last_and_next_coupon(base_dt, em_dt, fc_use, next_call_dt, months_step=meses_step)
            if next_call_dt is pd.NaT:
                            st.error("Não foi possível calcular a próxima CALL no calendário B3.")
                            st.stop()

        else:
            next_call_dt = None

        if motor == MOTOR_YTC_CHAMADAS:
            st.caption(f"🔔 Próxima CALL projetada: **{next_call_dt.date()}**")

        end_for_curve = ven_dt if (motor == MOTOR_PCT_CDI) else next_call_dt
        if end_for_curve is pd.NaT or end_for_curve is None:
            st.error("Não consegui determinar o horizonte do ativo (vencimento/call).")
            st.stop()
        if base_dt >= end_for_curve:
            st.warning(
                f"Data-base {base_dt.date()} ≥ última data de evento ({end_for_curve.date()}). "
                "Ajuste as datas do ativo (ex.: vencimento/call) e tente novamente."
            )
            st.stop()

        # Curva diária exportada (mantida p/ debug/apoio); forward p/ datas
        curve_date_sel, di_daily_sel = load_di_curve_daily(end_for_curve, REF_DATE_CURVA, forced_curve_date=None)
        df_di_sel = build_di_export_df(curve_date_sel, di_daily_sel)

        # === SÉRIE-CHAVE POR DU (metodologia nova) ===
        s_R = carregar_R_implied_por_DU()

        # Série histórica para PU_hoje e accrual: mantemos exatamente como você fazia
        curve_disc = di_daily_sel
        curve_acc_full = build_acc_series_for(
            em_dt, end_for_curve, di_daily_sel, ref_date=base_dt, use_history=True
        )

        # Guard rails
        last_event = (next_call_dt if motor == MOTOR_YTC_CHAMADAS else end_for_curve)
        if (last_event is not pd.NaT) and (base_dt >= last_event):
            st.warning(
                f"Data-base **{base_dt.date()}** está ≥ última data de evento "
                f"(**{last_event.date()}**). Ajuste a **Data de referência**."
            )
            df_ev = pd.DataFrame(columns=["Data","Tipo","Juros","Amort","Fluxo","DF","VP"])

        eventos = []
        PU_emissao_f = float(PU_emissao)
        alpha_f = float(alpha)
        beta_f  = float(beta)
        primeira_Taxa_desconto = 0.0 
        # ---------------------- MOTOR PCT_CDI ----------------------
        if motor == MOTOR_PCT_CDI:
            if inc_flag == "SIM":
                # bullet capitalizado — juros por DU até o vencimento (PU_hoje multiplicado por alfa por DU)
                # 1) PU_hoje pela sua rotina histórica+forward (mantido)
                PU_hoje = compute_pu_hoje(PU_emissao_f, alpha_f, curve_acc_full,
                                          em_dt, em_dt, base_dt, incorpora="SIM")

                # 2) Fator do período base->vencimento pela metodologia por DU (para juros do período)
                D_cum = _du_cum_from_base(base_dt, end_for_curve)
                R_prev = R_implied_at(s_R, 0)
                R_cur  = R_implied_at(s_R, D_cum)
                fator_intervalo, di_per_aa = di_period_from_implied(0, D_cum, None, R_cur)
                di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0

                fator_alpha_periodo = (1.0 + alpha_f * di_dia) ** D_cum
                fluxo = PU_hoje * fator_alpha_periodo
                amort = PU_emissao_f
                juros = fluxo - amort

                DF = _df_desconto_desde_base_por_DU(base_dt, end_for_curve, beta_f, s_R)
                if primeira_Taxa_desconto == 0:
                    primeira_Taxa_desconto = DF
                pv = fluxo / DF

                eventos.append({
                    "Data": end_for_curve.date(), "Tipo": "BULLET (capitalizado)",
                    "DU_entre": D_cum, "DI_periodo_aa": di_per_aa,
                    "Juros": juros, "Amort": amort, "Fluxo": fluxo, "DF": DF, "VP": pv
                })

            else:
                if agenda_juros_val == 'D':
                    # LÓGICA PARA DIAS CORRIDOS
                    step_dias = int(f.get("step_days") or 180)
                    periods = generate_coupons_lfsc2(em_dt, fcup_dt, end_for_curve, step_days=step_dias)
                    last_cpn, next_cpn = last_and_next_coupon2(base_dt, em_dt, fcup_dt, end_for_curve, step_days=step_dias)
                else:
                    # LÓGICA PARA MESES (comportamento original)
                    meses_step = int(meses or f.get("meses") or 6)
                    if fcup_dt is None:
                        last_cpn, next_cpn = infer_coupon_anchors_from_emissao(base_dt, em_dt, end_for_curve, months_step=meses_step)
                        fc_guess = b3_next_session((_naive(em_dt) + pd.DateOffset(months=meses_step)).normalize())
                        periods = generate_periods_semester_b3(em_dt, fc_guess, end_for_curve, months_step=meses_step)
                    else:
                        periods = generate_periods_semester_b3(em_dt, fcup_dt, end_for_curve, months_step=meses_step)
                        last_cpn, next_cpn = last_and_next_coupon(base_dt, em_dt, fcup_dt, end_for_curve, months_step=meses_step)

                if not periods:
                    st.error("Não foi possível gerar a agenda de cupons (verifique datas e meses entre cupons).")
                    st.stop()

                # PU_hoje (mantido; entra só no 1º fluxo que cruza a base)
                PU_hoje = compute_pu_hoje(PU_emissao_f, alpha_f, curve_acc_full,
                                          em_dt, last_cpn, base_dt, incorpora="NAO")

                # Info histórico base (mantido p/ debug)
                fator_DI_last_to_base, n_du_hist, ser_hist = compute_cdi_factor_sgs(
                    last_cpn, base_dt, include_start=True, include_end=False, return_series=True,
                    cap_to_date=base_dt,                      # <<< AQUI
                )
                st.caption(
                    f"**Fator CDI histórico [inclui último, exclui base] (SGS):** "
                    f"{fator_DI_last_to_base:,.6f}  — dias: {n_du_hist}"
                )
                primeira_Taxa_desconto = 0
                for (ini, fim) in periods:
                    if fim > end_for_curve:
                        continue
                    if fim <= base_dt:
                        continue

                    # DUs do período e DUs acumulados
                    ini_eff = max(ini, base_dt)
                    D_between = int(b3_count_excl(ini_eff, fim))
                    D_cum     = int(b3_count_excl(base_dt, fim))

                    # Taxa implícita por DU no período (metodologia nova)
                    R_prev = R_implied_at(s_R, int(b3_count_excl(base_dt, ini_eff)))
                    R_cur  = R_implied_at(s_R, D_cum)
                    fator_intervalo, di_per_aa = di_period_from_implied(
                        int(b3_count_excl(base_dt, ini_eff)), D_cum, R_prev, R_cur
                    )
                    di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0


                    if ini < base_dt:
                        # primeiro período: juros sobre PU_hoje
                        taxa_periodo = (1.0 + alpha_f * di_dia) ** D_between - 1.0
                        juros = PU_hoje * taxa_periodo
                    else:
                        taxa_periodo = (1.0 + alpha_f * di_dia) ** D_between - 1.0
                        juros = PU_emissao_f * taxa_periodo

                    amort = PU_emissao_f if (fim.date() == end_for_curve.date()) else 0.0
                    fluxo = juros + amort

                    # DF por DU da base até o fim do período
                    DF = _df_desconto_desde_base_por_DU(base_dt, fim, beta_f, s_R)
                    if primeira_Taxa_desconto == 0:
                        primeira_Taxa_desconto = DF
                    pv = fluxo / DF

                    # Extras informativos
                    row_di = df_di_sel.loc[df_di_sel["Data"] == fim.date()]
                    cdi_daily_val     = (float(row_di["CDI_daily"].iloc[0])      if not row_di.empty else np.nan)
                    cdi_daily_aa_val  = (float(row_di["CDI_daily_aa"].iloc[0])   if not row_di.empty else np.nan)

                    eventos.append({
                        "Data": fim.date(),
                        "Tipo": ("CUPOM+PRINCIPAL" if amort>0 else "CUPOM"),
                        "DU_entre": D_between,
                        "DU_desde_base": D_cum,
                        "Fator_DI_periodo": fator_intervalo,
                        "DI_periodo_aa": di_per_aa,
                        "CDI_diario": cdi_daily_val,
                        "CDI_diario_aa": cdi_daily_aa_val,
                        "Taxa_diaria_eq_periodo": di_dia,
                        "Juros": juros, "Amort": amort, "Fluxo": fluxo,
                        "DF": DF, "VP": pv
                    })

        # ---------------------- MOTOR YTC_CHAMADAS ----------------------
        else:
            #st.write(f)
            ytc_dt = end_for_curve  # próxima CALL já calculada
            mes_def = 6
            if f.get("call_step") == 360 or f.get("Ciclo") == "360 DIAS":
                mes_def = 12

            meses_step = int(meses or f.get("meses") or mes_def)

            # >>> NOVO: âncora do YTC
            ytc_ref = _ytc_anchor(call0_dt, em_dt)   # << NOVO
            ytc_dt  = next_call_dt                   # << NOVO (horizonte do YTC é a próxima CALL)
            #st.write(f"YTC Ref: {ytc_ref}, YTC Dt: {ytc_dt}, Emissão: {em_dt}, Call0: {call0_dt}")
            if ytc_ref == next_call_dt:
                ytc_ref = em_dt

            # 1º cupom para a malha de cupons (usamos a frequência de CUPOM, não de CALL)
            if _safe_date_from_any(fcup_dt) is None:
                fc_use = b3_next_session((ytc_ref + pd.DateOffset(months=meses_step)).normalize())
            else:
                fc_use = _naive(fcup_dt)

            # agenda de cupons (6M ou o que você informar em "meses")
            # Bloco Novo (Corrigido com a lógica condicional)
            if agenda_juros_val == 'D':
                # LÓGICA PARA DIAS CORRIDOS
                call_step_dias = int(f.get("step_days") or f.get("call_step") or 180)
                periods = generate_coupons_lfsc2(em_dt, fc_use, ytc_dt, step_days=call_step_dias)
                last_cpn, next_cpn = last_and_next_coupon2(base_dt, em_dt, fc_use, ytc_dt, step_days=call_step_dias)
            else:
                # LÓGICA PARA MESES (comportamento original)
                periods = generate_coupons_semester_until(em_dt, fc_use, ytc_dt, months_step=int(meses_step))
                last_cpn, next_cpn = last_and_next_coupon(base_dt, em_dt, fc_use, ytc_dt, months_step=int(meses_step))
            # O restante da lógica de cálculo do fluxo para YTC_CHAMADAS permanece o mesmo...
            if periods and periods[-1][1] < ytc_dt:
                periods[-1] = (periods[-1][0], ytc_dt)
            if not periods:
                st.error("Não foi possível gerar a agenda semestral até a próxima CALL.")
                st.stop()

            # Âncoras last/next também com a âncora do YTC
            #last_cpn, next_cpn = last_and_next_coupon(base_dt, ytc_ref, fc_use, ytc_dt, months_step=meses_step)
            # PU_hoje (mantido) — entra no 1º fluxo que cruza a base
            curve_acc_hist = curve_acc_full.loc[curve_acc_full.index <= base_dt]

            PU_hoje = compute_pu_hoje(
                PU_emissao_f, alpha_f, curve_acc_hist,   # << use a série capada na base
                em_dt, last_cpn, base_dt, incorpora="NAO"
            )
            data_primeira = True
            primeira_Taxa_desconto = 0

            for (ini, fim) in periods:
                if fim > ytc_dt:
                    continue
                if fim <= base_dt:
                    continue

                
                ini_eff = max(ini, base_dt)
                D_between = int(b3_count_excl(ini_eff, fim))
                D_cum     = int(b3_count_excl(base_dt, fim))

                R_prev = R_implied_at(s_R, int(b3_count_excl(base_dt, ini_eff)))
                R_cur  = R_implied_at(s_R, D_cum)
                fator_intervalo, di_per_aa = di_period_from_implied(
                    int(b3_count_excl(base_dt, ini_eff)), D_cum, R_prev, R_cur
                )

                di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0

                if data_primeira:
                    taxa_periodo = (1.0 + alpha_f * di_dia) ** D_between - 1.0
                    juros = PU_hoje * taxa_periodo
                    data_primeira = False
                else:
                    taxa_periodo = (1.0 + alpha_f * di_dia) ** D_between - 1.0
                    juros = PU_emissao_f * taxa_periodo

                amort = PU_emissao_f if (fim.date() == ytc_dt.date()) else 0.0
                fluxo = juros + amort

                # Desconto desde a base (por DU) — repricing com beta
                DF = _df_desconto_desde_base_por_DU(base_dt, fim, beta_f, s_R)
                if primeira_Taxa_desconto == 0:
                    primeira_Taxa_desconto = DF
                pv = fluxo / DF

                eventos.append({
                    "Data": fim.date(),
                    "Tipo": ("CUPOM+PRINCIPAL@CALL" if amort>0 else "CUPOM"),
                    "DU_entre": D_between, "DI_periodo_aa": di_per_aa,
                    "Juros": juros, "Amort": amort, "Fluxo": fluxo, "DF": DF, "VP": pv
                })

        # ---------- Tabela e métricas ----------
        df_ev = pd.DataFrame(eventos)
        if df_ev.empty:
            st.warning("Não há fluxos futuros após a data-base selecionada.")
        else:
            st.markdown("### Cronograma de Eventos (VP)")
            st.dataframe(
                df_ev.sort_values("Data").style.format({
                    "Juros":"{:,.2f}","Amort":"{:,.2f}","Fluxo":"{:,.2f}",
                    "DF":"{:,.6f}","VP":"{:,.2f}",
                    "Fator_DI_periodo":"{:,.6f}",
                    "DU_entre":"{:,.0f}","DU_desde_base":"{:,.0f}",
                    "CDI_diario":"{:.6%}","CDI_diario_aa":"{:.4%}",
                    "DI_periodo_aa":"{:.4%}",
                    "Taxa_diaria_eq_periodo":"{:.6%}",
                }),
                use_container_width=True
            )
            PU_clean = float(df_ev["VP"].sum())

            # ================== ACCRUAL — MANTIDO EXATAMENTE COMO NO SEU CÓDIGO ==================
            accrued_now = 0.0
            try:
                # 1. Determina a data de início do período de accrual
                start_accrual_date = None
                caption_label = ""

                if inc_flag == "SIM":
                    # Para ativos que capitalizam, o accrual é sempre da emissão até a base.
                    start_accrual_date = em_dt
                    caption_label = f"emissão ({em_dt.date()})"
                else:
                    # Para ativos com cupom (PCT_CDI ou YTC), o accrual é do último cupom pago até a base.
                    # A variável `last_cpn` já foi calculada corretamente para ambos os motores antes deste bloco.
                    start_accrual_date = last_cpn
                    caption_label = f"último cupom ({last_cpn.date()})"

                # 2. Calcula o accrual apenas se houver um período válido
                if start_accrual_date is not None and not pd.isna(start_accrual_date) and start_accrual_date < base_dt:
                    # Calcula o fator histórico de CDI para o período de accrual
                    fator_DI_hist, n_du_hist, _ = compute_cdi_factor_sgs(
                        start_date=start_accrual_date,
                        end_date=base_dt,
                        include_start=True,
                        include_end=False,  # Accrual exclui a data final
                        return_series=True
                    )

                    # Aplica o alpha para obter o fator de juros do ativo
                    if n_du_hist > 0:
                        fator_alpha_hist = fator_alpha_excel(fator_DI_hist, float(alpha_f), n_du_hist)
                        # Juros acumulados = Principal * (Fator de juros - 1)
                        accrued_now = PU_emissao_f * (fator_alpha_hist - 1.0)
                    
                    # Exibe o caption informativo
                    st.caption(
                        f"**Fator CDI histórico ({caption_label} → {base_dt.date()}) (SGS):** "
                        f"{fator_DI_hist:,.6f} — dias: {n_du_hist}"
                    )
                else:
                    # Se não há período de accrual (ex: data-base é a mesma do último cupom), os juros são zero.
                    accrued_now = 0.0

            except Exception as e:
                # Tratamento de exceção aprimorado: informa o usuário sobre o erro.
                st.warning(f"⚠️ Erro ao calcular o accrual de juros: {e}")
                accrued_now = 0.0
            # ================== FIM DO BLOCO CORRIGIDO ==================

            #Trazer o accrued para o valor presente com a taxa bete
            accruado_descontado = accrued_now / primeira_Taxa_desconto if primeira_Taxa_desconto > 0 else accrued_now
            if inc_flag != "SIM":
                PU_dirty = PU_clean + float(accruado_descontado)
            else:
                PU_dirty = PU_clean


            st.metric("PU limpo (VP dos fluxos)", _fmt_money(PU_clean))
            if inc_flag != "SIM": 
                st.metric("Accrual (último cupom → base)", _fmt_money(accrued_now))
            st.metric("PU sujo (limpo + accrual)", _fmt_money(PU_dirty))

            # Caption informativa (mantida quando disponível)
            try:
                if motor == MOTOR_PCT_CDI and inc_flag != "SIM":
                    st.caption(
                        f"**Fator DI (último cupom → {base_dt.date()}):** "
                        f"{fator_DI_last_to_base:,.6f}  "
                        f"(último={last_cpn.date()}, próximo={next_cpn.date()})"
                    )
            except Exception:
                pass

            # Download XLSX (mantido; só a tabela agora reflete a nova metodologia)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
                df_ev.to_excel(xw, index=False, sheet_name="Fluxo_pagamento")
                ws1 = xw.sheets["Fluxo_pagamento"]
                ws1.set_column(0, 0, 12)
                ws1.set_column(1, 1, 22)
                ws1.set_column(2, 6, 16)

                start_row = len(df_ev) + 2
                ws1.write(start_row, 0, "Notas — definição das colunas e metodologia (por DU)")
                notas_fluxo = [
                    "Data: data útil B3 do pagamento do evento.",
                    "Tipo: tipo do evento (CUPOM, CUPOM+PRINCIPAL, BULLET capitalizado, CUPOM+PRINCIPAL@CALL).",
                    "Juros: calculados por DU no período, aplicando 'alpha' sobre a taxa diária equivalente à taxa a.a. implícita por DU.",
                    "Amort: devolução de principal; ocorre no vencimento (bullet/cupom) ou na data de CALL (YTC).",
                    "Fluxo: Juros + Amort.",
                    "DF: fator de desconto desde a base até a data do evento, por DU, aplicando 'beta' sobre a taxa diária equivalente do trecho base→evento.",
                    "VP: Fluxo / DF; soma dos VPs = PU limpo na base.",
                ]
                for i, txt in enumerate(notas_fluxo, start=1):
                    ws1.write(start_row + i, 0, f"- {txt}")

                # Quadro Dados_gerais (mantido)
                PU_calc_local = float(df_ev["VP"].sum()) if not df_ev.empty else 0.0
                def _float_or_nan(v):
                    try:
                        x = float(v); return x if np.isfinite(x) else np.nan
                    except Exception:
                        return np.nan
                pu_alvo_local = _float_or_nan(f.get("PU_posicao"))
                if np.isnan(pu_alvo_local):
                    try:
                        mr = merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel)]
                        if not mr.empty:
                            pu_alvo_local = _float_or_nan(mr["Pu Posição_num"].iloc[0])
                    except Exception:
                        pass
                gap_local = (PU_calc_local - pu_alvo_local) if (pu_alvo_local is not None and not np.isnan(pu_alvo_local)) else np.nan
                ratio_local = (PU_calc_local / pu_alvo_local) if (pu_alvo_local is not None and not np.isnan(pu_alvo_local) and pu_alvo_local != 0) else np.nan

                dados_gerais = pd.DataFrame([
                    {"Campo": "Codigo",                "Valor": sel},
                    {"Campo": "Motor",                 "Valor": motor},
                    {"Campo": "IncorporaJuros",        "Valor": (inc_flag or "—")},
                    {"Campo": "PU_emissao",            "Valor": float(PU_emissao_f)},
                    {"Campo": "Alpha (juros)",         "Valor": float(alpha_f)},
                    {"Campo": "Beta (desconto)",       "Valor": float(beta_f)},
                    {"Campo": "Data_emissao",          "Valor": (em_dt.date() if em_dt is not None else None)},
                    {"Campo": "Data_prox_juros",       "Valor": (fcup_dt.date() if fcup_dt is not None else None)},
                    {"Campo": "Vencimento/Último evento", "Valor": end_for_curve.date()},
                    {"Campo": "Data_call (se YTC)",    "Valor": (next_call_dt.date() if (motor == MOTOR_YTC_CHAMADAS and next_call_dt is not pd.NaT and next_call_dt is not None) else None)},
                    {"Campo": "Meses entre cupons",    "Valor": (meses if (motor == MOTOR_PCT_CDI and inc_flag != 'SIM') else None)},
                    {"Campo": "Dias entre CALLs",      "Valor": (call_months  if motor == MOTOR_YTC_CHAMADAS else None)},
                    {"Campo": "Data-base (ref_date)",  "Valor": base_dt.date()},
                    {"Campo": "PU_calculado (sum VP)", "Valor": PU_calc_local},
                    {"Campo": "PU_posicao (alvo)",     "Valor": (None if (pu_alvo_local is None or np.isnan(pu_alvo_local)) else float(pu_alvo_local))},
                    {"Campo": "Gap (calc − alvo)",     "Valor": (None if (gap_local is None or (isinstance(gap_local, float) and np.isnan(gap_local))) else float(gap_local))},
                    {"Campo": "Ratio (calc / alvo)",   "Valor": (None if (ratio_local is None or (isinstance(ratio_local, float) and np.isnan(ratio_local))) else float(ratio_local))},
                    {"Campo": "PU_limpo_VP",           "Valor": PU_clean},
                    {"Campo": "Accrual_L->Base",       "Valor": float(accrued_now)},
                    {"Campo": "PU_sujo_(limpo+acr)",   "Valor": PU_dirty},
                ])
                dados_gerais.to_excel(xw, index=False, sheet_name="Dados_gerais")
                ws2 = xw.sheets["Dados_gerais"]
                ws2.set_column(0, 0, 28)
                ws2.set_column(1, 1, 28)

                row2 = len(dados_gerais) + 2
                ws2.write(row2, 0, "Notas — metodologia (por DU)")
                for i, txt in enumerate([
                    "Alpha: multiplicador aplicado ao indexador na acumulação de juros por DU em cada período.",
                    "Beta: multiplicador aplicado ao indexador no desconto (base→evento) por DU.",
                    "Motor: PCT_CDI (bullet/cupom) ou YTC_CHAMADAS (fluxo até próxima CALL).",
                    "IncorporaJuros: SIM (bullet capitalizado) ou NAO (cupons periódicos).",
                    "PU_calculado: soma dos VPs; alvo é o PU de posição informado/da base."
                ], start=1):
                    ws2.write(row2 + i, 0, f"- {txt}")

                df_di_sel.to_excel(xw, index=False, sheet_name="Curva_interpolada")
                ws3 = xw.sheets["Curva_interpolada"]
                ws3.set_column(0, 0, 12)
                ws3.set_column(1, 1, 6)
                ws3.set_column(2, 8, 16)

                # histórico SGS (mantido)
                hist_start = _naive(min(em_dt, base_dt))
                hist_end   = _naive(min(base_dt, end_for_curve))
                hist_series = load_cdi_sgs_daily(hist_start, hist_end)
                if not hist_series.empty:
                    df_hist = pd.DataFrame({
                        "Data": hist_series.index.date,
                        "CDI_daily": hist_series.values.astype(float),
                        "Origem": "HIST_SGS"
                    })
                    df_hist.to_excel(xw, index=False, sheet_name="Curva_historica")
                    wsH = xw.sheets["Curva_historica"]
                    wsH.set_column(0, 0, 12)
                    wsH.set_column(1, 1, 16)
                    wsH.set_column(2, 2, 12)

            st.download_button(
                "⬇️ Baixar XLSX (fluxo do ativo + dados + curva)",
                data=buf.getvalue(),
                file_name=f"fluxo_{motor}_{sel}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Verificador (mantido)
        st.markdown("### Verificador — PU calculado × PU posição (MTM)")
        PU_calc = PU_dirty  # sujo (limpo + accrual)
        pu_target = f.get("PU_posicao")
        try:
            pu_target = float(pu_target) if pu_target not in [None, "", np.nan] else np.nan
        except Exception:
            pu_target = np.nan
        if (np.isnan(pu_target)) and ("Pu Posição_num" in merged.columns):
            try:
                mr = merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel)]
                if not mr.empty:
                    pu_target = float(mr["Pu Posição_num"].iloc[0])
            except Exception:
                pass

        c1v, c2v, c3v, c4v = st.columns(4)
        with c1v:
            st.caption("PU calculado (sujo)")
            st.metric(label="", value=_fmt_money(PU_calc))
        with c2v:
            st.caption("PU posição (alvo)")
            if np.isnan(pu_target):
                pu_target = st.number_input("Informar PU posição (alvo)", value=0.0, step=1.0, format="%.2f", key=f"putgt_{sel}")
            st.metric(label="", value=_fmt_money(pu_target))

        gap = np.nan; ratio = np.nan
        if (pu_target is not None) and (not np.isnan(pu_target)) and pu_target != 0:
            gap = PU_calc - float(pu_target)
            ratio = PU_calc / float(pu_target)

        with c3v:
            st.caption("Gap (calc − alvo)")
            st.metric(label="", value=_fmt_money(gap))
        with c4v:
            st.caption("Ratio (calc / alvo)")
            st.metric(label="", value=("—" if np.isnan(ratio) else f"{ratio:,.4f}"))
    
    ytw_on = st.checkbox("Ativar simulação de Yield to Worst (YTW) por datas de CALL", value=False, key=f"ytw_on_{sel}")
    if ytw_on:
        st.markdown("##  Yield to Worst — Simulação por datas de CALL")

        # --- preço de referência (PU sujo) para o cálculo do yield ---
        def _coalesce_float(*vals, default=None):
            for v in vals:
                try:
                    if v is None:
                        continue
                    x = float(v)
                    if np.isfinite(x):
                        return x
                except Exception:
                    pass
            return float(default) if default is not None else None

        pu_ref_default = _coalesce_float(
            locals().get("PU_dirty"),
            locals().get("PU_sujo"),
            st.session_state.get("PU_dirty"),
            st.session_state.get("PU_sujo"),
            (f.get("PU_mercado") if isinstance(f, dict) else None),
            (f.get("PU_sujo") if isinstance(f, dict) else None),
            locals().get("PU_clean"),
            locals().get("PU_limpo"),
            st.session_state.get("PU_clean"),
            st.session_state.get("PU_limpo"),
            (f.get("PU_limpo") if isinstance(f, dict) else None),
            locals().get("PU_emissao"),
            (f.get("PU_emissao") if isinstance(f, dict) else None),
            default=1000.0,
        )

        col_ytw_a, col_ytw_b = st.columns([1, 1])
        pu_ref = col_ytw_a.number_input(
            "PU de referência (mercado)",
            value=float(pu_ref_default),
            step=1.0,
            format="%.6f",
            help="Preço (PU sujo) que será casado pelos yields."
        )

        # --- data-base ---
        try:
            ref_base_dt = _naive(pd.to_datetime(_ref_base))
        except Exception:
            _hoje = pd.Timestamp.today().normalize()
            ref_base_dt = _naive(pd.to_datetime(st.session_state.get("ref_base", _hoje)))

        # --- regra de agenda
        try:
            agenda_juros_val = str(f.get("AgendaJuros") or "").strip().upper()
        except Exception:
            agenda_juros_val = ""

        # --- seed da primeira CALL (> base)
        _call_seed = None
        try:
            if ('next_call_dt' in locals()) and (next_call_dt is not None) and (next_call_dt is not pd.NaT):
                _call_seed = _naive(next_call_dt)
            else:
                dc0 = _naive(pd.to_datetime(f.get("Data_call"))) if f.get("Data_call") \
                    else _safe_date_from_any(Data_emissao)
                if dc0 is not None:
                    if agenda_juros_val == 'D':
                        step_days = int(f.get("step_days") or f.get("call_step") or 180)
                        _call_seed = next_call_after(ref_base_dt, dc0, step_days=step_days)
                    else:
                        step_months = int(f.get("meses") or 6)
                        _call_seed = next_call_after_months(ref_base_dt, dc0, months_step=step_months)
        except Exception:
            _call_seed = None

        if (_call_seed is None) or (_call_seed is pd.NaT) or (_call_seed <= ref_base_dt):
            try:
                _call_seed = _naive(pd.to_datetime(f.get("Vencimento"))) if f.get("Vencimento") \
                            else (ref_base_dt + pd.Timedelta(days=180))
            except Exception:
                _call_seed = ref_base_dt + pd.Timedelta(days=180)

        # --- quantidade de CALLs
        n_calls = col_ytw_b.number_input(
            "Quantidade de CALLs a comparar",
            min_value=1, max_value=12, value=3, step=1
        )

        # --- próxima k-ésima CALL somando períodos sucessivos (fix)
        def _nth_call(seed_dt: pd.Timestamp, k: int) -> pd.Timestamp:
            """Retorna seed_dt + k*(step) já ajustando para dia útil B3."""
            if agenda_juros_val == 'D':
                step_days = int(f.get("step_days") or f.get("call_step") or 180)
                dt = seed_dt
                for _ in range(k):
                    dt = b3_next_session((dt + pd.Timedelta(days=step_days)).normalize())
                return dt
            else:
                step_months = int(f.get("meses") or 6)
                dt = seed_dt
                for _ in range(k):
                    dt = b3_next_session((dt + pd.DateOffset(months=step_months)).normalize())
                return dt

        # --- lista editável de CALLs
        call_dates = []
        for i in range(int(n_calls)):
            call_dates.append(_call_seed if i == 0 else _nth_call(_call_seed, i))

        st.caption("Edite abaixo as datas de CALL para comparar os yields. A primeira é a **próxima CALL** sugerida.")

        try:
            _sel_key = str(sel)
        except NameError:
            _sel_key = str((f.get("ISIN") if isinstance(f, dict) else None) or "global")

        cols = st.columns(min(4, int(n_calls)))
        picked_calls = []
        for i, dt in enumerate(call_dates):
            col = cols[i % len(cols)]
            _default_date = (dt.to_pydatetime().date() if isinstance(dt, pd.Timestamp) else ref_base_dt.date())
            val = col.date_input(
                f"CALL {i+1}",
                value=_default_date,
                key=f"ytw_call_{_sel_key}_{i}"
            )
            picked_calls.append(_naive(pd.to_datetime(val)))

        # ---------------------------
        #  Fluxos até cada CALL
        # ---------------------------
        def _build_flows_until_call(call_dt: pd.Timestamp) -> list[tuple[pd.Timestamp, float]]:
            em_dt = _safe_date_from_any(Data_emissao)

            mes_def = 6
            if f.get("call_step") == 360 or f.get("Ciclo") == "360 DIAS":
                mes_def = 12
            meses_step = int((f.get("meses") or mes_def))

            if _safe_date_from_any(Data_prox_juros) is None:
                call0_dt = _naive(pd.to_datetime(f.get("Data_call"))) if f.get("Data_call") else em_dt
                ytc_ref = _ytc_anchor(call0_dt, em_dt)
                if ytc_ref == call_dt:
                    ytc_ref = em_dt
                fc_use = b3_next_session((ytc_ref + pd.DateOffset(months=meses_step)).normalize())
            else:
                fc_use = _naive(_safe_date_from_any(Data_prox_juros))

            if str(agenda_juros_val) == 'D':
                step_days = int(f.get("step_days") or f.get("call_step") or 180)
                periods = generate_coupons_lfsc2(em_dt, fc_use, call_dt, step_days=step_days)
            else:
                periods = generate_coupons_semester_until(em_dt, fc_use, call_dt, months_step=int(meses_step))
            if not periods:
                return []

            flows = []
            PU_emissao_f = float(PU_emissao) if 'PU_emissao' in locals() else float(f.get("PU_emissao") or 1000.0)
            alpha_f = float(alpha)
            base_dt = ref_base_dt

            s_R = carregar_R_implied_por_DU()

            # curva diária só até a call (para histórico do PU_hoje)
            _, di_daily_local = load_di_curve_daily(call_dt, REF_DATE_CURVA, forced_curve_date=None)
            last_cpn, _ = last_and_next_coupon(base_dt, em_dt, fc_use, call_dt, months_step=int(meses_step))
            curve_acc_hist = build_acc_series_for(em_dt, call_dt, di_daily_local, ref_date=base_dt, use_history=True).loc[
                lambda s: s.index <= base_dt
            ]
            PU_hoje = compute_pu_hoje(PU_emissao_f, alpha_f, curve_acc_hist,
                                    em_dt, last_cpn, base_dt, incorpora="NAO")

            first = True
            for (ini, fim) in periods:
                if fim > call_dt or fim <= base_dt:
                    continue
                ini_eff = max(ini, base_dt)
                D_between = int(b3_count_excl(ini_eff, fim))
                D_cum     = int(b3_count_excl(base_dt, fim))

                R_prev = R_implied_at(s_R, int(b3_count_excl(base_dt, ini_eff)))
                R_cur  = R_implied_at(s_R, D_cum)
                _, di_per_aa = di_period_from_implied(int(b3_count_excl(base_dt, ini_eff)), D_cum, R_prev, R_cur)
                di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0

                taxa_periodo = (1.0 + alpha_f * di_dia) ** D_between - 1.0
                juros = (PU_hoje if first else PU_emissao_f) * taxa_periodo
                first = False

                amort = PU_emissao_f if (fim.date() == call_dt.date()) else 0.0
                fluxo = juros + amort
                flows.append((fim, float(fluxo)))

            if not flows or flows[-1][0].date() != call_dt.date():
                flows.append((call_dt, float(PU_emissao_f)))

            return flows

        # ---------------------------
        #  Solver robusto (evita divisão por zero/underflow)
        # ---------------------------
        def _solve_yield_du_aa(flows: list[tuple[pd.Timestamp, float]], price: float, base_dt: pd.Timestamp) -> float:
            """Retorna yield a.a. (252) tal que sum(cf / (1+y_d)^DU) = price, com proteção a overflow/underflow."""
            import math

            if not flows:
                return np.nan

            def _safe_pow1p(y_daily: float, du: int) -> float:
                # evita (1+y) <= 0 e controla expoente para exp()
                if y_daily <= -0.999999:
                    y_daily = -0.999999
                log_den = du * math.log1p(y_daily)  # = ln((1+y)^du)
                if log_den > 700:     # exp(>700) ~ overflow em float64
                    return float("inf")
                if log_den < -700:    # exp(<-700) ~ underflow -> 0.0
                    return 0.0
                return math.exp(log_den)

            def pv_with_y(y_daily: float) -> float:
                total = 0.0
                for dt, cf in flows:
                    du = int(b3_count_excl(base_dt, dt))
                    if du <= 0:
                        continue
                    den = _safe_pow1p(y_daily, du)
                    if not np.isfinite(den):
                        # den = inf -> termo ~ 0 (desprezável)
                        continue
                    if den == 0.0:
                        # underflow extremo -> evita divisão por zero mantendo sinal correto
                        den = 1e-300
                    total += cf / den
                return total

            # limites mais seguros e bracketing dinâmico
            lo, hi = -0.95, 2.0
            pv_lo, pv_hi = pv_with_y(lo), pv_with_y(hi)

            for _ in range(24):
                if (pv_lo - price) * (pv_hi - price) <= 0:
                    break
                if pv_lo > price and pv_hi > price:
                    # aumentar hi reduz PV; seguimos até cruzar o alvo
                    hi = min(hi * 1.5, 10.0)  # tampa hi p/ evitar exp muito grande
                    pv_hi = pv_with_y(hi)
                else:
                    # expandir para yield mais negativo, mas nunca chega a -1
                    lo = max(lo - 0.05, -0.999)
                    pv_lo = pv_with_y(lo)

            if (pv_lo - price) * (pv_hi - price) > 0:
                return np.nan  # não encontrou intervalo com mudança de sinal

            # bisseção
            y_d = None
            for _ in range(100):
                mid = 0.5 * (lo + hi)
                pv_mid = pv_with_y(mid)
                if abs(pv_mid - price) < 1e-6:
                    y_d = mid
                    break
                if (pv_lo - price) * (pv_mid - price) <= 0:
                    hi, pv_hi = mid, pv_mid
                else:
                    lo, pv_lo = mid, pv_mid
            if y_d is None:
                y_d = 0.5 * (lo + hi)

            try:
                return (1.0 + y_d) ** 252 - 1.0
            except Exception:
                # fallback com log1p se necessário (muito raro)
                return math.exp(252 * math.log1p(y_d)) - 1.0


        # --- calcula yields
        rows = []
        for i, cdt in enumerate(picked_calls, start=1):
            if cdt is None or cdt is pd.NaT or cdt <= ref_base_dt:
                rows.append({"#": i, "CALL": None, "DU base→CALL": None, "Yield a.a. (252)": np.nan})
                continue
            flows_i = _build_flows_until_call(cdt)
            y_aa = _solve_yield_du_aa(flows_i, float(pu_ref), ref_base_dt)
            rows.append({
                "#": i,
                "CALL": cdt.date(),
                "DU base→CALL": int(b3_count_excl(ref_base_dt, cdt)),
                "Yield a.a. (252)": y_aa
            })

        df_y = pd.DataFrame(rows)
        if not df_y.empty and df_y["Yield a.a. (252)"].notna().any():
            idx_worst = df_y["Yield a.a. (252)"].idxmin()
            df_y.loc[idx_worst, "≙ YtW"] = "★"
        else:
            df_y["≙ YtW"] = ""

        st.markdown("### 🧮 Yields por CALL")
        st.dataframe(
            df_y.style.format({
                "Yield a.a. (252)": "{:.4%}",
                "DU base→CALL": "{:,.0f}",
            }),
            use_container_width=True
        )

        if df_y["Yield a.a. (252)"].notna().any():
            ytw_val = float(df_y.loc[df_y["≙ YtW"] == "★", "Yield a.a. (252)"].iloc[0])
            st.metric("Yield to Worst (a.a., 252)", f"{ytw_val:.4%}")
        else:
            st.info("Não foi possível calcular os yields com as datas/fluxos atuais.")


def _norm_bucket(s: pd.Series) -> pd.Series:
    # força DIF/DIN (default F quando vier 'DIyy' puro)
    return _bucket_norm_series(s, default_to_F=True)
# === Helpers de choque direto em R_prev / R_cur e DF ===
def _bps_to_decimal(bps: float) -> float:
    return float(bps) / 10000.0

def _daily_add_from_bps(bps: float) -> float:
    # mantém para o DF (desconto) — conversão anual -> diária
    return (1.0 + (bps/10000.0))**(1.0/252.0) - 1.0

def _bump_R_pair_direct(sR: pd.Series, du_prev: int, du_cur: int, bps: float):
    """
    SHIFT PARALELO nas taxas anuais usadas na curva implícita:
    R_prev_b = R_prev + 1bp
    R_cur_b  = R_cur  + 1bp
    """
    R_prev = R_implied_at(sR, du_prev)
    R_cur  = R_implied_at(sR, du_cur)
    if not bps:
        return R_prev, R_cur
    bump = _bps_to_decimal(bps)               # <<== 0.0001 p/ 1 bp
    return R_prev + bump, R_cur + bump

def _df_bumped_from_base(DF_base: float, beta: float, du_cum: int, bps: float) -> float:
    if (not bps) or (DF_base is None) or (not np.isfinite(DF_base)) or DF_base == 0:
        return DF_base
    d_add = _daily_add_from_bps(bps)
    # aplica +bps (anual) convertido p/ incremento diário, ponderado por beta, no desconto acumulado em DU
    return float(DF_base) * (1.0 + beta * d_add) ** int(du_cum)

# ===================== VISÃO 3 — Calculadora de Fundos (ALINHADA À VISÃO 2, por DU) =====================
if visao == "Calculadora de Fundos":
    st.title("Calculadora de Hedge dos Fundos")

    def _ptbr_to_float(x) -> float:
        """Converte '3.333,00' -> 3333.00, '14,89' -> 14.89."""
        if x is None:
            return 0.0
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none", "null", "nat", "—", "-", "–"}:
            return 0.0
        # remove separador de milhar . e troca , por .
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0


    def _strip_pct(s: str) -> str:
        """Remove '%' e espaços finais/iniciais."""
        s = str(s or "").strip().replace(" ", "")
        if s.endswith("%"):
            s = s[:-1]
        return s


    def _to_alpha_factor(x) -> float:
        """
        Converte '109%', '109,0%', '1,09', '1.09' ou 1.09 para fator 1.09.
        Regra:
        - remove '%'
        - aplica conversão pt-BR -> float
        - se o valor ficar > 3.0, assume que veio em 'percentual' e divide por 100
        """
        s = _strip_pct(x)
        v = _ptbr_to_float(s)
        if v > 3.0:   # pega casos como 109, 110 etc.
            v = v / 100.0
        return max(v, 0.0)


    def _get_deb_extra_df() -> pd.DataFrame:
        """
        Lê o dicionário/CSV dos fluxos manuais.

        Preferências de entrada (em ordem):
        1) st.session_state['deb_extra_df']  (DataFrame)
        2) st.session_state['deb_extra_csv'] (texto CSV)
        3) 'Dados/flux_deb.csv'              (arquivo)

        Retorna DF normalizado com colunas:
        ['Ativo','dt_pagto','du_cum','du_between','amort','alpha','vne','evt_raw']
        """
        import io

        # 1) Escolha da fonte
        df = None
        if "deb_extra_df" in st.session_state and isinstance(st.session_state["deb_extra_df"], pd.DataFrame):
            df = st.session_state["deb_extra_df"].copy()
        elif "deb_extra_csv" in st.session_state and st.session_state["deb_extra_csv"]:
            df = pd.read_csv(io.StringIO(st.session_state["deb_extra_csv"]), sep=",")
        else:
            df = pd.read_csv("Dados/flux_deb.csv", sep=",")

        # 2) Normaliza / padroniza colunas esperadas
        cols = {c.strip().lower(): c for c in df.columns}

        def _pick(*names):
            for n in names:
                if n.lower() in cols:
                    return cols[n.lower()]
            return None

        c_evt   = _pick("Dados do evento", "evento", "descricao", "descrição")
        c_data  = _pick("Data de pagamento", "data")
        c_du    = _pick("Prazos (dias úteis)", "prazos", "prazos du", "prazo_du", "du_cum")
        c_duint = _pick("Dias entre pagamentos", "dias entre", "du entre", "du_between")
        c_am    = _pick("Amortizações", "amortizacao", "amortização", "amort", "amortizacao_r$")
        c_code  = _pick("Ativo", "codigo", "código")
        c_vne   = _pick("VNE", "PU", "PU Emissão", "PU_emissao", "vne_emissao")
        c_tx    = _pick("TaxaEmissao", "Taxa", "alpha", "alpha_(%CDI fator)", "alpha_%cdi")

        required = [c_evt, c_data, c_du, c_duint, c_am, c_code, c_vne, c_tx]
        if any(x is None for x in required):
            # Retorna esquema vazio previsível
            return pd.DataFrame(columns=[
                "Ativo","dt_pagto","du_cum","du_between","amort","alpha","vne","evt_raw"
            ])

        out = pd.DataFrame({
            "Ativo":      df[c_code].astype(str).str.strip(),
            "evt_raw":    df[c_evt].astype(str).str.strip(),
            "dt_pagto":   pd.to_datetime(df[c_data].astype(str).str.strip(), dayfirst=True, errors="coerce"),
            "du_cum":     pd.to_numeric(df[c_du].apply(_ptbr_to_float), errors="coerce").fillna(0).astype(int),
            "du_between": pd.to_numeric(df[c_duint].apply(_ptbr_to_float), errors="coerce").fillna(0).astype(int),
            # 'amort' esperado por unidade (mesma unidade da VNE). Se vier total da posição,
            # a heurística de ajuste é aplicada no motor externo.
            "amort":      df[c_am].apply(_ptbr_to_float),
            # PU unitário de emissão
            "vne":        df[c_vne].apply(_ptbr_to_float),
            # alpha como FATOR (ex.: 1.09 para "109%")
            "alpha":      df[c_tx].apply(_to_alpha_factor),
        })

        # 3) Limpeza e ordenação
        out = out.dropna(subset=["dt_pagto"])
        out = out.sort_values(["Ativo", "dt_pagto", "du_cum"]).reset_index(drop=True)

        # (Opcional) Aviso de alpha fora de faixa "usual"
        # bad = out[(out["alpha"] <= 0.5) | (out["alpha"] >= 2.0)]
        # if not bad.empty:
        #     st.warning(f"alpha fora do intervalo [0.5, 2.0] em {len(bad)} linha(s) do fluxo manual.")

        return out

    def _evt_tipo_from_str(s: str) -> str:
        """Converte 'Juros|Amortização'/'Juros'/'Amortização' → rótulos do seu app."""
        t = (s or "").upper()
        tem_j = "JUROS" in t
        tem_a = "AMORT" in t
        if tem_j and tem_a: return "CUPOM+PRINCIPAL"
        if tem_a and not tem_j: return "PRINCIPAL"
        if tem_j and not tem_a: return "CUPOM"
        return "EVENTO"
        
    # ### NOVO: Função para carregar DV01 dos contratos futuros de DI ###
    def _load_di_fut_dv01_series() -> pd.Series:
        """
        Lê DV01 por contrato de DI (DIF/DIN) e retorna uma série indexada por:
        DIF26, DIN26, DIF27, DIN27, ..., DIN30, DIF31, DIF32, DIF33, DIF35, DIF37.
        Aceita cabeçalhos e tickers com variações (Comdty/Index, etc).
        """
        from unidecode import unidecode
        import re as _re

        xls_path = Path('Dados/AF_Trading.xlsm')
        if not xls_path.exists():
            st.error(f"Arquivo de DV01 não encontrado em: {xls_path.resolve()}")
            return pd.Series(dtype=float)

        # 1) Tenta ler a aba
        sheet_candidates = ['Base CDI', 'Base_CDI', 'BaseCDI']
        last_exc = None
        di_raw = None
        for sh in sheet_candidates:
            try:
                di_raw = pd.read_excel(xls_path, sheet_name=sh, header=0)
                break
            except Exception as e:
                last_exc = e
        if di_raw is None:
            st.error(f"Falha ao ler a aba de DV01 em {xls_path.name}: {last_exc}")
            return pd.Series(dtype=float)

        # 2) Normaliza nomes de colunas (tolerante)
        def _norm(s):
            return _re.sub(r'[^A-Z0-9]', '', unidecode(str(s).strip().upper()))

        colmap = {c: _norm(c) for c in di_raw.columns}

        # 3) Acha coluna DV01 por regex ampla
        #    cobre DV01, DV0I (OCR), PVBP, BPV, FUTTICKVAL, TICKVALUE, R$PORBP, BRLPERBP...
        dv01_candidates = []
        for c, cn in colmap.items():
            if _re.search(r'(DV0?1|PVBP|BPV|TICKVAL|TICKVALUE|RSP?ORB?P|BRLPERBP|BRLPORB?P|VALORPORBP)', cn):
                dv01_candidates.append(c)
        if not dv01_candidates:
            # extra: se a 1ª linha é título e a 2ª tem o cabeçalho real
            try:
                di_try = pd.read_excel(xls_path, sheet_name=sheet_candidates[0], header=1)
                colmap2 = {c: _norm(c) for c in di_try.columns}
                for c, cn in colmap2.items():
                    if _re.search(r'(DV0?1|PVBP|BPV|TICKVAL|TICKVALUE|RSP?ORB?P|BRLPERBP|BRLPORB?P|VALORPORBP)', cn):
                        di_raw = di_try
                        colmap = colmap2
                        dv01_candidates = [c]; break
            except Exception:
                pass

        if not dv01_candidates:
            st.error("Coluna de DV01 não encontrada (procura por DV01/PVBP/BPV/TICKVALUE/R$/bp etc.). "
                    "Dica: confira o cabeçalho da aba 'Base CDI'.")
            # Debug opcional:
            if st.sidebar.checkbox("Debug DV01 headers", value=False, key="dbg_dv01_cols"):
                st.write("Colunas lidas:", list(di_raw.columns))
            return pd.Series(dtype=float)

        col_dv01 = dv01_candidates[0]

        # 4) Descobre coluna do ticker (ou usa parsing por linha)
        ticker_candidates = [c for c, cn in colmap.items() if _re.search(r'(TICKER|ATIVO|CONTRATO|SYMBOL|RIC)', cn)]
        col_ticker = ticker_candidates[0] if ticker_candidates else None

        df = di_raw.copy()
        # Converte DV01 para número
        df[col_dv01] = pd.to_numeric(df[col_dv01], errors='coerce')
        df = df.dropna(subset=[col_dv01])

        def map_bucket_from_str(t: str) -> str | None:
            if not isinstance(t, str): return None
            s = _norm(t)  # já sem espaços/símbolos
            # Aceita ODF26 / ODF26COMDTY / ODF26INDEX ...
            m = _re.search(r'^OD([A-Z])(\d{2})', s)
            if m:
                mon_code, yy = m.group(1), m.group(2)
                if mon_code == 'F':  # Jan
                    return f'DIF{yy}'
                if mon_code == 'N':  # Jun
                    return f'DIN{yy}'
                return None  # outros meses ignorados
            # DI1JAN27 / DI1JAN27INDEX ...
            m2 = _re.search(r'^DI1(JAN|JUN)(\d{2})', s)
            if m2:
                mon, yy = m2.group(1), m2.group(2)
                return f'DI{"F" if mon=="JAN" else "N"}{yy}'
            # Tenta achar "JAN/JUN" + yy solto
            m3 = _re.search(r'(JAN|JUN).*(\d{2})', s)
            if m3:
                mon, yy = m3.group(1), m3.group(2)
                return f'DI{"F" if mon=="JAN" else "N"}{yy}'
            return None

        if col_ticker:
            df['Bucket'] = df[col_ticker].map(map_bucket_from_str)
        else:
            # fallback: varre cada linha concatenando os campos como string
            df['Bucket'] = df.apply(lambda r: map_bucket_from_str(' '.join(map(str, r.values))), axis=1)

        df = df.dropna(subset=['Bucket'])

        # 5) Mantém apenas os buckets alvo
        alvo = set([f"DIF{yy:02d}" for yy in range(26, 38)]) | set([f"DIN{yy:02d}" for yy in range(26, 31)])
        df = df[df['Bucket'].isin(alvo)].copy()
        if df.empty:
            st.warning("Planilha lida, mas nenhum bucket alvo (DIF/DIN) foi encontrado. "
                    "Confira se os tickers estão como ODFyy/ODNyy ou DI1JANyy/DI1JUNyy.")
            return pd.Series(dtype=float)

        s = (df.groupby('Bucket')[col_dv01]
            .mean()
            .sort_index())
        # (Opcional) Debug rápido
        if st.sidebar.checkbox("Debug DV01 buckets", value=False, key="dbg_dv01_buckets"):
            st.write(s.reset_index().rename(columns={'Bucket':'DI_bucket', col_dv01:'DV01_R$/contrato'}))

        return s



    # ---------- Seleção do fundo ----------
    if ("Fundo" not in out.columns) or out.empty:
        st.info("Nenhum fundo disponível na tabela filtrada atual.")
        st.stop()
    fundos = sorted(out["Fundo"].dropna().astype(str).unique().tolist())
    sel_fundo = st.selectbox("Fundo", fundos, key="fundo_calc_sel")

    out_fundo = out[out["Fundo"].astype(str) == sel_fundo].copy()
    cods_fundo = out_fundo["Codigo"].dropna().astype(str).unique().tolist()
    st.caption(f"{len(cods_fundo)} ativo(s) na tabela filtrada para este fundo.")
    df_deb_extra = _get_deb_extra_df()
    codes_extra = set(df_deb_extra["Ativo"].astype(str)) if not df_deb_extra.empty else set()
    if not df_deb_extra.empty:
        st.caption(f"Fluxos manuais carregados para {len(codes_extra)} código(s) em 'Dados/flux_deb.csv'.")

    # ---------- Overrides / coleta de campos p/ cada ativo ----------
    if "overrides" not in st.session_state:
        st.session_state["overrides"] = {}

    def _apply_overrides(base_fields: dict, code: str) -> dict:
        ov = st.session_state["overrides"].get(str(code), {})
        outd = base_fields.copy()
        for k, v in ov.items():
            if v not in [None, "", np.nan]:
                outd[k] = v
        return outd

    def _is_empty_like(v):
        if v is None: return True
        if isinstance(v, float) and np.isnan(v): return True
        if isinstance(v, str) and v.strip().lower() in {"", "nat", "nan", "none", "—", "-", "null"}:
            return True
        return False

    def pick_first_nonnull(*vals):
        for v in vals:
            if not _is_empty_like(v):
                return v
        return None

    def get_fields_for_code(sel_code: str) -> dict:
        orow = out[out["Codigo"].astype(str) == str(sel_code)].iloc[0].to_dict()
        try:
            mrow = merged.loc[merged["cod_Ativo_guess"].astype(str) == str(sel_code)]
            mrow = (mrow.iloc[0].to_dict() if not mrow.empty else {})
        except Exception:
            mrow = {}

        def g(d, *keys):
            for k in keys:
                if k in d and d[k] not in [None, "", np.nan]:
                    return d[k]
            return None

        # NOVO: tenta pegar a quantidade da própria tabela do fundo
        try:
            qty = float(out_fundo.loc[out_fundo["Codigo"].astype(str) == str(sel_code), "Quantidade"].iloc[0])
        except Exception:
            qty = np.nan

        campos = {
            "PU_emissao": pick_first_nonnull(orow.get("PU_emissao"), g(mrow, "PU_emissao_final")),
            "alpha":      pick_first_nonnull(orow.get("alpha_(%CDI fator)"), g(mrow, "alpha_norm")),
            "beta":       pick_first_nonnull(orow.get("beta"), g(mrow, "beta_from_mtm")),
            "Data_emissao": pick_first_nonnull(orow.get("Data_emissao"), g(mrow, "Data_Emissao_final", "Data")),
            "Data_prox_juros": pick_first_nonnull(orow.get("Data_prox_juros"), g(mrow, "Data_Prox_Juros_final")),
            "Vencimento": pick_first_nonnull(orow.get("Vencimento"), g(mrow, "Vencimento_final", "Vencimento do ativo")),
            "Data_call":  pick_first_nonnull(orow.get("Data_call"), g(mrow, "Data_Call_Inicial_final")),
            "PU_posicao": pick_first_nonnull(orow.get("PU_posicao"), g(mrow, "Pu Posição_num")),
            "Ciclo":      pick_first_nonnull(orow.get("Ciclo_juros"), g(mrow, "CicloJuros_final")),
            "IncorporaJuros": pick_first_nonnull(orow.get("IncorporaJuros"), g(mrow, "IncorporaJuros_final")),
            # NOVO:
            "Quantidade": qty,
        }

        meses = None; call_step = None
        if isinstance(campos["Ciclo"], str):
            txt = campos["Ciclo"].upper()
            m = re.search(r"(\d+)\s*(DIAS|DIA|D)$", txt)
            if m: call_step = int(m.group(1))
            m2 = re.search(r"(\d+)\s*(MES|MESES|M)$", txt)
            if m2: meses = int(m2.group(1))
            m3 = re.search(r"(\d+)\s*(UTEIS|ÚTEIS|DU|BUS)$", txt)
            if m3 and call_step is None:
                call_step = int(m3.group(1))

        campos["meses"] = meses
        campos["call_step"] = call_step
        return campos

    def _motor_for_code(code: str) -> str:
        try:
            sel_norm = norm_code(code)
            mrow = merged.loc[merged["cod_Ativo_guess_norm"] == sel_norm]
            if not mrow.empty and "MOTOR_CALC" in mrow.columns:
                m = str(mrow["MOTOR_CALC"].iloc[0])
                return m if m in {MOTOR_PCT_CDI, MOTOR_YTC_CHAMADAS} else MOTOR_PCT_CDI
        except Exception:
            pass
        return MOTOR_PCT_CDI

    # ---------- Base temporal comum e Curvas ----------
    _ref_base = pd.to_datetime(REF_DATE_CURVA).date()
    base_dt   = _naive(pd.to_datetime(_ref_base))
    ven_list = []
    for code in cods_fundo:
        f0 = _apply_overrides(get_fields_for_code(code), code)
        v = _safe_date_from_any(f0.get("Vencimento"))
        if v is not None: ven_list.append(v)
    fund_end = (max(ven_list) if ven_list else (pd.Timestamp(REF_DATE_CURVA) + pd.DateOffset(years=5)))
    curve_date_fundo, di_daily_fundo = load_di_curve_daily(fund_end, REF_DATE_CURVA)
    df_di_sel = build_di_export_df(curve_date_fundo, di_daily_fundo)
    s_R = carregar_R_implied_por_DU()

    # ---------- Preparação e validação por ativo ----------
    auto_fills = []
    prepared: dict[str, dict] = {}
    faltantes_block  = []
    faltantes_nao_block = []   # faltantes que NÃO bloqueiam (estão em codes_extra)


    def _beta_default_for(code: str, motor: str, f: dict) -> float:
        if f.get("beta") not in [None, "", np.nan]:
            return float(f["beta"])
        try:
            sel_norm = norm_code(code)
            mrow = merged.loc[merged["cod_Ativo_guess_norm"] == sel_norm]
            if not mrow.empty and pd.notna(mrow["beta_from_mtm"].iloc[0]):
                return float(mrow["beta_from_mtm"].iloc[0])
        except Exception:
            pass
        return 1.06 if motor == MOTOR_YTC_CHAMADAS else 1.00

    def _fields_for_code_with_defaults(code: str) -> dict:
        f0 = _apply_overrides(get_fields_for_code(code), code)
        mot = _motor_for_code(code)
        inc = (str(f0.get("IncorporaJuros") or "")).strip().upper()
        if inc not in {"SIM","NAO"}: inc = ""

        if f0.get("beta") in [None, "", np.nan]:
            f0["beta"] = _beta_default_for(code, mot, f0)
        if mot == MOTOR_PCT_CDI and inc != "SIM" and _is_empty_like(f0.get("meses")):
            f0["meses"] = 6
        if mot == MOTOR_YTC_CHAMADAS and _is_empty_like(f0.get("call_step")):
            f0["call_step"] = 180

        em_dt  = _safe_date_from_any(f0.get("Data_emissao"))
        fc_dt  = _safe_date_from_any(f0.get("Data_prox_juros"))
        ven_dt = _safe_date_from_any(f0.get("Vencimento"))
        call_dt= _safe_date_from_any(f0.get("Data_call"))

        if ven_dt is None and call_dt is not None:
            f0["Vencimento"] = call_dt
            auto_fills.append({"Codigo": code, "Campo":"Vencimento", "Valor_novo": call_dt.date(), "Motivo":"Vencimento ausente → Data_call"})

        if fc_dt is None and em_dt is not None:
            if mot == MOTOR_PCT_CDI:
                meses_pref = int(f0.get("meses") or 6)
                f0["Data_prox_juros"] = em_dt + pd.DateOffset(months=meses_pref)
                auto_fills.append({"Codigo": code, "Campo":"Data_prox_juros", "Valor_novo": (em_dt + pd.DateOffset(months=meses_pref)).date(),
                                     "Motivo": f"Ausente → Emissão + {meses_pref}M"})
            else:
                f0["Data_prox_juros"] = em_dt + pd.DateOffset(months=6)
                auto_fills.append({"Codigo": code, "Campo":"Data_prox_juros", "Valor_novo": (em_dt + pd.DateOffset(months=6)).date(),
                                     "Motivo": "Ausente → Emissão + 6M (YTC)"})
        # Fail-safe: se por algum motivo AgendaJuros/step_days não vieram do get_fields_for_code
        agenda = str(f0.get("AgendaJuros") or "").strip().upper()
        if agenda not in {"D","M"}:
            if f0.get("meses"): agenda = "M"
            elif f0.get("call_step"): agenda = "D"
            else: agenda = "M"
            f0["AgendaJuros"] = agenda

        if agenda == "D" and f0.get("step_days") in [None, "", np.nan]:
            f0["step_days"] = int(f0.get("call_step") or 180)

        return f0 | {"__motor__": mot, "__inc__": inc}

    def _required_missing_for(code: str, f: dict, motor: str, inc_flag: str) -> list[str]:
        """
        Regras originais de obrigatoriedade para o motor padrão.
        Para códigos em 'codes_extra' usaremos o motor FLUXO_MANUAL, que não precisa desses campos.
        """
        miss = []
        def need(k, is_date=False):
            v = f.get(k)
            if is_date:
                ok = (v is not None) and (not pd.isna(pd.to_datetime(v, errors="coerce")))
            else:
                ok = not _is_empty_like(v)
            if not ok: miss.append(k)

        # Para o motor externo, nada é realmente obrigatório (há fallbacks em _fluxo_codigo_externo).
        # Então só aplicamos a checagem se NÃO estiver em codes_extra.
        if str(code) not in codes_extra:
            need("PU_emissao"); need("alpha"); need("beta")
            need("Data_emissao", True); need("Vencimento", True)
            if motor == MOTOR_PCT_CDI:
                if str(inc_flag).upper() != "SIM":
                    need("Data_prox_juros", True); need("meses")
            else:
                need("Data_prox_juros", True); need("Data_call", True); need("call_step")
        return miss

    for code in cods_fundo:
        fprep = _fields_for_code_with_defaults(code)
        prepared[code] = fprep
        miss = _required_missing_for(code, fprep, fprep["__motor__"], fprep["__inc__"])
        if miss:
            (faltantes_nao_block if str(code) in codes_extra else faltantes_block).append(
                {"Codigo": code, "Campos_faltantes": ", ".join(miss), "Motor": fprep["__motor__"], "IncorporaJuros": fprep["__inc__"]}
            )

    # Avisos
    if faltantes_nao_block:
        st.info(
            "Alguns ativos têm informações faltantes, **mas estão em 'flux_deb.csv'**. "
            "Eles serão calculados pelo motor **FLUXO_MANUAL** usando os dados cadastrados do ativo (PU, alpha, beta, Quantidade). "
            "Preencha depois para auditoria completa."
        )
        st.dataframe(pd.DataFrame(faltantes_nao_block), use_container_width=True, height=240)

    if faltantes_block:
        st.warning("Há informações faltantes para ativos **sem** fluxo manual. Preencha e salve para recalcular.")
        st.dataframe(pd.DataFrame(faltantes_block), use_container_width=True, height=240)
        st.stop()

    # ---------- Motor por ativo ----------
    _flux_cache: dict[tuple[str, float], tuple[pd.DataFrame, float]] = {}
    _accrual_df_base = st.session_state.setdefault("accrual_df_base", {})
    DEBUG  = st.sidebar.checkbox("Ver progresso", value=False)
    def _fluxo_codigo_externo(code: str, fprep: dict, sR: pd.Series, df_extra: pd.DataFrame, bump_bps: float = 0.0
                            ) -> tuple[pd.DataFrame, float]:
        """
        Reproduz a mesma estrutura de retorno de _fluxo_codigo_DU, porém usando um
        calendário manual (df_extra) com:
        Ativo, dt_pagto, du_cum, du_between, amort (R$), vne, alpha (%CDI fator)

        - Juros do período: (1 + alpha * di_dia) ** DU_between_eff - 1
        - Desconto período: (1 +  beta * di_dia) ** DU_between_eff
        (beta = fprep['beta'] se existir; caso contrário, 1.00)

        - Bump +1 bp: mesmo pipeline do motor padrão via _bump_R_pair_direct.
        - Accrual: retornado como 0.0 (DV01 limpo segue idêntico ao resto do app).
        """
        # Filtra o DF externo para o ativo
        sub = df_extra[df_extra["Ativo"].astype(str) == str(code)].copy()
        if sub.empty:
            return pd.DataFrame(), 0.0

        # Parâmetros do ativo
        PU = float(sub["vne"].iloc[0] if pd.notna(sub["vne"].iloc[0]) else (fprep.get("PU_emissao") or 1000.0))
        a  = float(sub["alpha"].iloc[0] if pd.notna(sub["alpha"].iloc[0]) else (fprep.get("alpha") or 1.00))
        a = a/100
        # beta (taxa de desconto) — usa o que vier preparado; se não, 1.00
        try:
            b = float(fprep.get("beta")) if fprep.get("beta") not in [None, "", np.nan] else 1.00
        except Exception:
            b = 1.00

        # DF incremental acumulado e principal em aberto
        DF_running = 1.0
        principal_out = PU

        eventos = []
        # Ordena por horizonte crescente
        sub = sub.sort_values(["dt_pagto", "du_cum"]).reset_index(drop=True)
                # Quantidade no fundo (como no motor padrão)
        try:
            qtd = float(fprep.get("Quantidade")) if fprep.get("Quantidade") not in [None, "", np.nan] else \
              float(out_fundo.loc[out_fundo["Codigo"].astype(str) == str(code), "Quantidade"].iloc[0])
        except Exception:
            qtd = 1.0

        for _, r in sub.iterrows():
            fim_dt   = pd.Timestamp(r["dt_pagto"]).normalize()
            du_cum   = int(r["du_cum"])
            du_prev_raw = du_cum - int(r["du_between"])
            # Efeito "corte na base": se o início do período ficou antes da base, zera
            du_prev_eff  = max(0, du_prev_raw)
            DU_between_eff = max(0, du_cum - du_prev_eff)

            # Taxas implícitas (bump aplicando nos dois pontos)
            R_prev_b, R_cur_b = _bump_R_pair_direct(sR, du_prev_eff, du_cum, bump_bps)
            fator_intervalo, di_per_aa = di_period_from_implied(du_prev_eff, du_cum, R_prev_b, R_cur_b)
            di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0

            # Juros do período sobre o principal em aberto
            taxa_periodo = (1.0 + a * di_dia) ** DU_between_eff - 1.0
            juros  = principal_out * taxa_periodo

            # Amortização absoluta informada
            amort_raw = float(r["amort"] or 0.0)
            amort = amort_raw
            if amort > max(principal_out, PU) * 1.5 and qtd > 1:
                amort = amort_raw / qtd  # normaliza para "por unidade"

            # Sanidade: não amortizar além do saldo
            amort = min(amort, principal_out)
            fluxo  = juros + amort

            # Fator de desconto incremental acumulado com beta
            DF_inc = (1.0 + b * di_dia) ** DU_between_eff
            DF_running *= DF_inc
            DF_base = _df_desconto_desde_base_por_DU(base_dt, fim_dt, b, sR)  # só p/ auditoria
            DF = DF_running

            pv = (fluxo / DF) if DF > 0 else fluxo

            # Monta a linha (formatando como no motor padrão)
            eventos.append({
                "Codigo": str(code),
                "Motor": "FLUXO_MANUAL",
                "IncorporaJuros": "NAO",
                "Data_ini": None,
                "Data_ini_eff": None,
                "Data_fim": fim_dt.date(),
                "DU_prev": du_prev_eff,
                "DU_cum": du_cum,
                "DU_between": DU_between_eff,
                "R_prev": R_implied_at(sR, du_prev_eff),
                "R_cur":  R_implied_at(sR, du_cum),
                "R_prev_b": R_prev_b,
                "R_cur_b":  R_cur_b,
                "fator_intervalo": fator_intervalo,
                "di_per_aa": di_per_aa,
                "di_dia": di_dia,
                "taxa_periodo": taxa_periodo,
                "alpha": a,
                "beta": b,
                "PU_emissao": PU,
                "PU_hoje": np.nan,
                "is_first_period": (du_prev_eff == 0 and DU_between_eff == du_cum),
                "meses": None,
                "call_step": None,
                "bps_aplicado": float(bump_bps or 0.0),
                "DF_base": DF_base,
                "DF": DF,
                "Juros": juros,
                "Amort": amort,
                "Fluxo": fluxo,
                "VP": pv,
                "Tipo": _evt_tipo_from_str(r.get("evt_raw", "")),
            })

            # Atualiza saldo de principal após a amortização
            principal_out -= amort
            if principal_out <= 1e-9:
                principal_out = 0.0
        df_ev = pd.DataFrame(eventos)
        if df_ev.empty:
            return df_ev, 0.0



        df_ev["Quantidade"]  = qtd
        df_ev["Fluxo_total"] = df_ev["Fluxo"] * qtd
        df_ev["VP_total"]    = df_ev["VP"] * qtd

        # Accrual “sujo”: omitido (0.0) — seu DV01 limpo não depende dele
        return df_ev, 0.0


    def _fluxo_codigo_DU(code: str, fprep: dict, sR: pd.Series, bump_bps: float = 0.0
                        ) -> tuple[pd.DataFrame, float]:
        """
        Retorna (df_eventos, accrual_descontado) do ativo 'code' com motor por DU.
        bump_bps aplica +bps a.a. DIRETO em R_prev/R_cur e no DF (via ajuste fechado).
        Agora também devolve, por evento, TODAS as variáveis usadas no cálculo para auditoria.

        >>> ALTERAÇÃO: Reconhece 'AgendaJuros' ('D' para dias, 'M' para meses)
        >>> e usa as novas funções (generate_coupons_lfsc2, next_call_after)
        >>> para replicar a lógica da "Calculadora de Ativos".
        """
        key = (str(code), float(bump_bps or 0.0),)  # cache simples por code+bump
        if key in _flux_cache:
            return _flux_cache[key]

        mot = fprep["__motor__"]; inc_flag = fprep["__inc__"]
        PU = float(fprep.get("PU_emissao")); a = float(fprep.get("alpha")); b = float(fprep.get("beta"))
        em_dt   = _safe_date_from_any(fprep.get("Data_emissao"))
        fcup_dt = _safe_date_from_any(fprep.get("Data_prox_juros"))
        ven_dt  = _safe_date_from_any(fprep.get("Vencimento"))
        

        # --- NOVO: Lógica de agenda por dias ou meses ---
        agenda_juros_val = str((fprep).get("AgendaJuros") or "").strip().upper()
        if agenda_juros_val not in {"D","M"}:
            # Fallback coerente
            agenda_juros_val = "M" if ((fprep).get("meses")) else "D"

        step_days = int(fprep.get("step_days") or fprep.get("call_step") or 180)
        Data_call = fprep.get("Data_call")
        meses = fprep.get("meses")
        call_months_default = 6
        if fprep.get("call_step") == 360 or fprep.get("Ciclo") == "360 DIAS":
            call_months_default = 12

        call_months_default = int(fprep.get("meses") or call_months_default)
        # --- Horizonte do ativo (end_for) ---
        if mot == MOTOR_YTC_CHAMADAS:
            call0_dt   = _naive(pd.to_datetime(Data_call))
            mes_def = 6
            if fprep.get("call_step") == 360 or fprep.get("Ciclo") == "360 DIAS":
                mes_def = 12

            meses_step = int(meses or fprep.get("meses") or mes_def)

            # 1) Próxima CALL de verdade: usa a regra de CALL (LFSC em dias corridos)
            if agenda_juros_val == 'D':
                # LÓGICA PARA DIAS CORRIDOS
                # Usa os dias corridos extraídos do campo "Ciclo" ou o override "call_step"
                call_step_dias = int(fprep.get("step_days") or fprep.get("call_step") or 180)
                next_call_dt = next_call_after(base_dt, call0_dt, step_days=call_step_dias)
                if next_call_dt is pd.NaT:
                    st.error("Não foi possível calcular a próxima CALL pela regra de dias corridos.")
                    st.stop()
            else:
                # LÓGICA PARA MESES (comportamento que já estava)
                # O número de meses entre calls já é derivado de call_step_days ou do input 'call_months'
                next_call_dt = next_call_after_months(base_dt, call0_dt, months_step=int(call_months_default))
                if next_call_dt is pd.NaT:
                    st.error("Não foi possível calcular a próxima CALL pela regra de meses.")
                    st.stop()

            # 2) Primeiro cupom para accrual/PU_hoje (se não informado, inferir por meses_step na malha de cupons)
            if _safe_date_from_any(fcup_dt) is None:
                fc_use = b3_next_session((_naive(em_dt) + pd.DateOffset(months=meses_step)).normalize())
            else:
                fc_use = _naive(fcup_dt)
            ytc_dt  = next_call_dt    
            ytc_ref = _ytc_anchor(call0_dt, em_dt)   # << NOVO
            if ytc_ref == next_call_dt:
                ytc_ref = em_dt
            end_for = next_call_dt
        else:
            end_for = ven_dt
            next_call = None

        if (em_dt is None) or (end_for is None) or (base_dt >= end_for):
            empty = pd.DataFrame(columns=[
                "Codigo","Motor","IncorporaJuros","Data_ini","Data_ini_eff","Data_fim",
                "DU_prev","DU_cum","DU_between","R_prev","R_cur","R_prev_b","R_cur_b",
                "fator_intervalo","di_per_aa","di_dia","taxa_periodo","alpha","beta",
                "PU_emissao","PU_hoje","is_first_period","meses","call_step","bps_aplicado",
                "DF_base","DF","Juros","Amort","Fluxo","VP","Quantidade","Fluxo_total","VP_total","Tipo"
            ])
            _flux_cache[key] = (empty, 0.0)
            return _flux_cache[key]

        curve_acc_full = build_acc_series_for(em_dt, end_for, di_daily_fundo, ref_date=base_dt, use_history=True)
        eventos = []
        primeira_DF = 0.0
        accrued_now = 0.0
        DF_running = 1.0
        MAX_EVENTS = 400
        last_fim_seen = None

        # ---------- MOTOR_PCT_CDI ----------
        if mot == MOTOR_PCT_CDI:
            if inc_flag == "SIM": # bullet capitalizado
                PU_hoje = compute_pu_hoje(PU, a, curve_acc_full, em_dt, em_dt, base_dt, incorpora="SIM")
                du_cum  = int(b3_count_excl(base_dt, end_for))
                R_prev_b, R_cur_b = _bump_R_pair_direct(sR, 0, du_cum, bump_bps)
                fator_intervalo, di_per_aa = di_period_from_implied(0, du_cum, R_prev_b, R_cur_b) # Usa taxas com bump
                di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0
                DU_between = du_cum
                taxa_periodo = (1.0 + a * di_dia) ** DU_between - 1.0
                fluxo  = PU_hoje * (1.0 + taxa_periodo)
                amort  = PU
                juros  = fluxo - amort
                DF = _df_desconto_desde_base_por_DU(base_dt, end_for, b, sR) # Base para accrual
                DF_inc = (1.0 + b * di_dia) ** DU_between
                DF_running *= DF_inc
                DF_final = DF_running
                if primeira_DF == 0.0: primeira_DF = DF_final
                pv = fluxo / DF_final
                eventos.append({
                    "Codigo": code, "Motor": mot, "IncorporaJuros": inc_flag, "Data_ini": None, "Data_ini_eff": None, "Data_fim": end_for.date(),
                    "DU_prev": 0, "DU_cum": du_cum, "DU_between": DU_between, "R_prev": R_implied_at(sR, 0), "R_cur": R_implied_at(sR, du_cum),
                    "R_prev_b": R_prev_b, "R_cur_b": R_cur_b, "fator_intervalo": fator_intervalo, "di_per_aa": di_per_aa, "di_dia": di_dia,
                    "taxa_periodo": taxa_periodo, "alpha": a, "beta": b, "PU_emissao": PU, "PU_hoje": PU_hoje, "is_first_period": True,
                    "meses": None, "call_step": None, "bps_aplicado": float(bump_bps or 0.0), "DF_base": DF, "DF": DF_final, "Juros": juros, "Amort": amort,
                    "Fluxo": fluxo, "VP": pv, "Tipo": "BULLET (capitalizado)"
                })
                fator_DI, n_du_hist, _ = compute_cdi_factor_sgs(em_dt, base_dt, True, False, True)
                fator_alpha_hist = fator_alpha_excel(fator_DI, float(a), n_du_hist) if n_du_hist > 0 else 1.0
                accrued_now = PU * (fator_alpha_hist - 1.0)
            else: # cupons
                if agenda_juros_val == 'D':
                    periods = generate_coupons_lfsc2(em_dt, fcup_dt, end_for, step_days=step_days)
                    last_cpn, next_cpn = last_and_next_coupon2(base_dt, em_dt, fcup_dt, end_for, step_days=step_days)
                    meses = None # Não aplicável
                else:
                    meses = int(fprep.get("meses") or 6)
                    if fcup_dt is None:
                        last_cpn, next_cpn = infer_coupon_anchors_from_emissao(base_dt, em_dt, end_for, months_step=meses)
                        fc_guess = b3_next_session((_naive(em_dt) + pd.DateOffset(months=meses)).normalize())
                        periods = generate_periods_semester_b3(em_dt, fc_guess, end_for, months_step=meses)
                    else:
                        periods = generate_periods_semester_b3(em_dt, fcup_dt, end_for, months_step=meses)
                        last_cpn, next_cpn = last_and_next_coupon(base_dt, em_dt, fcup_dt, end_for, months_step=meses)

                periods = [(ini, fim) for (ini, fim) in periods if (fim > base_dt) and (fim <= end_for)]
                periods = sorted(set(periods))
                PU_hoje = compute_pu_hoje(PU, a, curve_acc_full, em_dt, last_cpn, base_dt, incorpora="NAO")
                fator_DI_last_to_base, n_du_hist, _ = compute_cdi_factor_sgs(last_cpn, base_dt, True, False, True)
                fator_alpha_last_to_base = fator_alpha_excel(fator_DI_last_to_base, float(a), n_du_hist) if n_du_hist > 0 else 1.0
                accrued_now = PU * (fator_alpha_last_to_base - 1.0)
                for k, (ini, fim) in enumerate(periods, start=1):
                    if k > MAX_EVENTS: break
                    if last_fim_seen is not None and fim <= last_fim_seen: continue
                    last_fim_seen = fim
                    ini_eff  = max(ini, base_dt)
                    du_prev  = int(b3_count_excl(base_dt, ini_eff))
                    du_cum   = int(b3_count_excl(base_dt, fim))
                    R_prev_b, R_cur_b = _bump_R_pair_direct(sR, du_prev, du_cum, bump_bps)
                    fator_intervalo, di_per_aa = di_period_from_implied(du_prev, du_cum, R_prev_b, R_cur_b)
                    di_dia = (1.0 + di_per_aa)**(1.0/252.0) - 1.0
                    DU_between = int(b3_count_excl(ini_eff, fim))
                    taxa_periodo = (1.0 + a * di_dia) ** DU_between - 1.0
                    juros = (PU_hoje if ini < base_dt else PU) * taxa_periodo
                    amort = PU if (fim.date() == end_for.date()) else 0.0
                    fluxo = juros + amort
                    DF_base = _df_desconto_desde_base_por_DU(base_dt, fim, b, sR)
                    DF_inc  = (1.0 + b * di_dia) ** DU_between
                    DF_running *= DF_inc
                    DF = DF_running
                    if primeira_DF == 0: primeira_DF = DF
                    pv = fluxo / DF
                    eventos.append({
                        "Codigo": code, "Motor": mot, "IncorporaJuros": inc_flag, "Data_ini": ini.date(), "Data_ini_eff": ini_eff.date(), "Data_fim": fim.date(),
                        "DU_prev": du_prev, "DU_cum": du_cum, "DU_between": DU_between, "R_prev": R_implied_at(sR, du_prev), "R_cur": R_implied_at(sR, du_cum),
                        "R_prev_b": R_prev_b, "R_cur_b": R_cur_b, "fator_intervalo": fator_intervalo, "di_per_aa": di_per_aa, "di_dia": di_dia,
                        "taxa_periodo": taxa_periodo, "alpha": a, "beta": b, "PU_emissao": PU, "PU_hoje": (PU_hoje if ini < base_dt else np.nan),
                        "is_first_period": (ini < base_dt), "meses": meses, "call_step": (step_days if agenda_juros_val == 'D' else None),
                        "bps_aplicado": float(bump_bps or 0.0), "DF_base": DF_base, "DF": DF, "Juros": juros, "Amort": amort,
                        "Fluxo": fluxo, "VP": pv, "Tipo": ("CUPOM+PRINCIPAL" if amort>0 else "CUPOM")
                    })
        # ---------- MOTOR_YTC_CHAMADAS ----------
        else:
            call0_dt = _safe_date_from_any(fprep.get("Data_call"))
            ytc_dt = end_for # já calculado no início
            ytc_ref = _ytc_anchor(_naive(call0_dt), em_dt)
            ytc_dt  = next_call_dt                  
            #st.write(f"YTC Ref: {ytc_ref}, YTC Dt: {ytc_dt}, Emissão: {em_dt}, Call0: {call0_dt}")
            if ytc_ref == next_call_dt:
                ytc_ref = em_dt

            meses_step = int(fprep.get("meses") or (12 if "360" in str(fprep.get("Ciclo","")) else 6))
            fc_use = _naive(fcup_dt) if fcup_dt else b3_next_session((ytc_ref + pd.DateOffset(months=meses_step)).normalize())

            if agenda_juros_val == 'D':
                periods = generate_coupons_lfsc2(em_dt, fc_use, ytc_dt, step_days=step_days)
                last_cpn, _ = last_and_next_coupon2(base_dt, em_dt, fc_use, ytc_dt, step_days=step_days)
            else:
                periods = generate_coupons_semester_until(em_dt, fc_use, ytc_dt, months_step=int(meses_step))
                last_cpn, _ = last_and_next_coupon(base_dt, em_dt, fc_use, ytc_dt, months_step=int(meses_step))
            
            if periods and periods[-1][1] < ytc_dt: periods[-1] = (periods[-1][0], ytc_dt)
            periods = [(ini, fim) for (ini, fim) in periods if (fim > base_dt) and (fim <= ytc_dt)]
            periods = sorted(set(periods))

            if not periods: # se não houver períodos futuros
                return pd.DataFrame(), 0.0

            curve_acc_hist = curve_acc_full.loc[curve_acc_full.index <= base_dt]
            PU_hoje = compute_pu_hoje(PU, a, curve_acc_hist, em_dt, last_cpn, base_dt, incorpora="NAO")
            fator_DI_y, n_du_y, _ = compute_cdi_factor_sgs(last_cpn, base_dt, True, False, True)
            fator_alpha_y = fator_alpha_excel(fator_DI_y, float(a), n_du_y) if n_du_y > 0 else 1.0
            accrued_now = PU * (fator_alpha_y - 1.0)
            
            first_flag = True
            for k, (ini, fim) in enumerate(periods, start=1):
                if k > MAX_EVENTS: break
                if last_fim_seen is not None and fim <= last_fim_seen: continue
                last_fim_seen = fim
                ini_eff = max(ini, base_dt)
                du_prev = int(b3_count_excl(base_dt, ini_eff))
                du_cum  = int(b3_count_excl(base_dt, fim))

                # --- LÓGICA DE BUMP UNIFICADA ---
                R_prev_b, R_cur_b = _bump_R_pair_direct(sR, du_prev, du_cum, bump_bps)
                fator_intervalo, di_per_aa_b = di_period_from_implied(du_prev, du_cum, R_prev_b, R_cur_b)
                daily_b = (1.0 + di_per_aa_b)**(1.0/252.0) - 1.0
                
                DU_between   = int(b3_count_excl(ini_eff, fim))
                taxa_periodo = (1.0 + a * daily_b) ** DU_between - 1.0
                juros        = (PU_hoje if first_flag else PU) * taxa_periodo
                first_flag   = False
                amort = PU if (fim.date() == ytc_dt.date()) else 0.0
                fluxo = juros + amort

                DF_base = _df_desconto_desde_base_por_DU(base_dt, fim, b, sR)
                DF_inc  = (1.0 + b * daily_b) ** DU_between
                DF_running *= DF_inc
                DF = DF_running
                if primeira_DF == 0: primeira_DF = DF
                pv = fluxo / DF
                
                
                eventos.append({
                    "Codigo": code, "Motor": mot, "IncorporaJuros": inc_flag, "Data_ini": ini.date(), "Data_ini_eff": ini_eff.date(), "Data_fim": fim.date(),
                    "DU_prev": du_prev, "DU_cum": du_cum, "DU_between": DU_between, "R_prev": R_implied_at(sR, du_prev), "R_cur": R_implied_at(sR, du_cum),
                    "R_prev_b": R_prev_b, "R_cur_b": R_cur_b, "fator_intervalo": fator_intervalo, "di_per_aa": di_per_aa_b, "di_dia": daily_b,
                    "taxa_periodo": taxa_periodo, "alpha": a, "beta": b, "PU_emissao": PU, "PU_hoje": (PU_hoje if k == 1 else np.nan),
                    "is_first_period": (k == 1), "meses": int(meses_step), "call_step": (step_days if agenda_juros_val == 'D' else None),
                    "bps_aplicado": float(bump_bps or 0.0), "DF_base": DF_base, "DF": DF, "Juros": juros, "Amort": amort, "Fluxo": fluxo, "VP": pv,
                    "Tipo": ("CUPOM+PRINCIPAL@CALL" if amort > 0 else "CUPOM")
                })

        # ---------- Fechamento / accrual PV ----------
        df_ev = pd.DataFrame(eventos)
        if df_ev.empty:
            _flux_cache[key] = (df_ev, 0.0)
            return _flux_cache[key]
        
        # Quantidade do ativo no fundo (puxado da tabela 'out_fundo')
        try:
            qtd = float(out_fundo.loc[out_fundo["Codigo"].astype(str) == str(code), "Quantidade"].iloc[0])
        except Exception:
            qtd = 1.0
        
        df_ev["Quantidade"]  = qtd
        df_ev["Fluxo_total"] = df_ev["Fluxo"] * qtd
        df_ev["VP_total"]    = df_ev["VP"] * qtd

        # --- NOVO: Cálculo do accrual descontado ---
        accr_descont = (accrued_now / primeira_DF) if primeira_DF > 0 else accrued_now
        accr_descont_total = accr_descont * qtd
        #st.write(f"Accrual descontado para {code} (bump {bump_bps} bps): {accr_descont:.2f}")
        #st.write(df_ev)
        # Preciso somar o accr_descont no primeiro VP da primeira linha de eventos. E depois eu preciso fazer as colunas de Fluxo Totak VP_total

        _flux_cache[key] = (df_ev, float(accr_descont_total))
        return _flux_cache[key]

    # ---------- Precificação e DV01 Global ----------
    dfs_base, dfs_bump = [], []
    accr_base_sum, accr_bump_sum = 0.0, 0.0

    # NÃO recarrega df_deb_extra aqui — já foi carregado antes da validação.
    # Apenas garanta que existam:
    if 'df_deb_extra' not in locals() or df_deb_extra is None:
        df_deb_extra = pd.DataFrame()
    if 'codes_extra' not in locals():
        codes_extra = set(df_deb_extra["Ativo"].astype(str)) if not df_deb_extra.empty else set()

    if DEBUG and codes_extra:
        st.write("Códigos com fluxo manual (df_extra):", sorted(list(codes_extra)))

    for code in cods_fundo:

        if DEBUG:
            st.write(f"Calculando fluxo para {code}...")

        fprep = prepared[code]
        print(fprep)

        # Se houver fluxo manual para este código, usa o motor externo;
        # caso contrário, usa seu motor atual por DU
        if code in codes_extra:
            d0, acc0 = _fluxo_codigo_externo(code, fprep, s_R, df_deb_extra, bump_bps=0.0)
            d1, acc1 = _fluxo_codigo_externo(code, fprep, s_R, df_deb_extra, bump_bps=1.0)
        else:
            d0, acc0 = _fluxo_codigo_DU(code, fprep, s_R, bump_bps=0.0)
            d1, acc1 = _fluxo_codigo_DU(code, fprep, s_R, bump_bps=1.0)

        if not d0.empty: dfs_base.append(d0)
        if not d1.empty: dfs_bump.append(d1)
        accr_base_sum += float(acc0 or 0.0)
        accr_bump_sum += float(acc1 or 0.0)
    
    if not dfs_base:
        st.info("Nenhum fluxo calculável para os ativos deste fundo.")
        st.stop()
    
    df_base = pd.concat(dfs_base, ignore_index=True)
    df_bump = pd.concat(dfs_bump, ignore_index=True)

    PU_limpo_base  = float(df_base["VP_total"].sum())
    PU_sujo_base   = PU_limpo_base + float(accr_base_sum)
    PU_limpo_bump  = float(df_bump["VP_total"].sum())
    PU_sujo_bump   = PU_limpo_bump + float(accr_bump_sum)
    DV01_limpo = PU_limpo_bump - PU_limpo_base
    DV01_sujo  = PU_sujo_bump  - PU_sujo_base

    c3, c4 = st.columns(2)
    #c1.metric("PU limpo (base)", _fmt_money(PU_limpo_base))
    #c2.metric("PU sujo (base)",  _fmt_money(PU_sujo_base))
    c3.metric("DV01 fundo (+1 bp)", _fmt_money(DV01_limpo))
    #c4.metric("DV01 sujo (+1 bp)",  _fmt_money(DV01_sujo))
    # ### NOVO: Abas para detalhamento dos fluxos de pagamento ###
    st.markdown("#### Análise Detalhada")
    # ### ALTERAÇÃO: Adicionada a terceira aba "DV01 Total por Ativo" ###
    tab_base, tab_bump, tab_dv01_total = st.tabs(["Fluxos (Cenário Base)", "Fluxos (+1 bp)", "DV01 Total por Ativo"])

    # Define as colunas e a formatação para as tabelas de fluxo
    _fmt_cols = {
        "DU_prev":"{:,.0f}","DU_cum":"{:,.0f}","DU_between":"{:,.0f}",
        "R_prev":"{:,.6f}","R_cur":"{:,.6f}","R_prev_b":"{:,.6f}","R_cur_b":"{:,.6f}",
        "fator_intervalo":"{:,.6f}","di_per_aa":"{:.4%}","di_dia":"{:.6%}",
        "taxa_periodo":"{:.6%}","Taxa de emissão":"{:,.6f}","Taxa de Desconto":"{:,.6f}",
        "PU_emissao":"{:,.2f}","PU_hoje":"{:,.2f}",
        "DF_base":"{:,.6f}","DF":"{:,.6f}",
        "Juros":"{:,.2f}","Amort":"{:,.2f}","Fluxo":"{:,.2f}","VP":"{:,.2f}",
        "Quantidade":"{:,.2f}","Fluxo_total":"{:,.2f}","VP_total":"{:,.2f}",
        "bps_aplicado":"{:,.2f}",
    }
    
    _view_cols = [
        "Codigo","Tipo","Data_fim",
        "DU_between", "taxa_periodo",
        "alpha","beta","PU_emissao","PU_hoje",
        "Juros","Amort","Fluxo","VP",
        "Quantidade","Fluxo_total","VP_total"
    ]
    
    # --- Aba 1: Fluxos (Cenário Base) ---
    with tab_base:
        view_cols_base = [col for col in _view_cols if col in df_base.columns]
        df_display_base = df_base[view_cols_base].copy()
        df_display_base.rename(columns={'alpha': 'Taxa de emissão', 'beta': 'Taxa de Desconto'}, inplace=True)
        
        st.dataframe(
            df_display_base.sort_values(["Data_fim","Codigo"]).style.format(_fmt_cols, na_rep="-"),
            use_container_width=True, height=500
        )
    
    # --- Aba 2: Fluxos (+1 bp) ---
    with tab_bump:
        view_cols_bump = [col for col in _view_cols if col in df_bump.columns]
        df_display_bump = df_bump[view_cols_bump].copy()
        df_display_bump.rename(columns={'alpha': 'Taxa de emissão', 'beta': 'Taxa de Desconto'}, inplace=True)

        st.dataframe(
            df_display_bump.sort_values(["Data_fim","Codigo"]).style.format(_fmt_cols, na_rep="-"),
            use_container_width=True, height=500
        )

    # --- Aba 3: DV01 Total por Ativo (NOVO) ---
    with tab_dv01_total:
        # Agrupa o VP_total por ativo para cada cenário
        pu_base_por_ativo = df_base.groupby("Codigo", as_index=False)["VP_total"].sum().rename(columns={"VP_total":"PU_Base_Total"})
        pu_bump_por_ativo = df_bump.groupby("Codigo", as_index=False)["VP_total"].sum().rename(columns={"VP_total":"PU_+1bp_Total"})
        
        # Junta os dois resultados
        df_dv01_por_ativo = pu_base_por_ativo.merge(pu_bump_por_ativo, on="Codigo", how="outer").fillna(0.0)

        # 1. Pega a quantidade de cada ativo no fundo
        quantidades = df_base[['Codigo', 'Quantidade']].drop_duplicates()
        
        # 2. Junta a quantidade na tabela de DV01
        df_dv01_por_ativo = df_dv01_por_ativo.merge(quantidades, on="Codigo", how='left').fillna({'Quantidade': 0})
        
        # 3. Calcula o DV01 da Posição Total (o que você já tinha)
        df_dv01_por_ativo["DV01 Total (Posição no Fundo)"] = df_dv01_por_ativo["PU_+1bp_Total"] - df_dv01_por_ativo["PU_Base_Total"]

        # 4. Calcula o DV01 Unitário (para 1 qtd)
        # Usamos .replace(0, np.nan) para evitar erros de divisão por zero
        df_dv01_por_ativo['DV01 Unitário (1 qtd)'] = (df_dv01_por_ativo["DV01 Total (Posição no Fundo)"] / df_dv01_por_ativo['Quantidade'].replace(0, np.nan)).fillna(0)
        
        # Seleciona e ordena as colunas para exibição
        colunas_para_ver = [
            'Codigo', 
            'Quantidade', 
            'DV01 Unitário (1 qtd)', 
            'DV01 Total (Posição no Fundo)'
        ]
        
        st.dataframe(
            df_dv01_por_ativo[colunas_para_ver]
            .sort_values("DV01 Total (Posição no Fundo)")
            .style.format({
                'Quantidade': '{:,.2f}',
                'DV01 Unitário (1 qtd)': '{:,.4f}', # Mais precisão para o valor unitário
                'DV01 Total (Posição no Fundo)': '{:,.2f}'
            }),
            use_container_width=True, height=500
        )
    # ---------- Hedge por Vértice (Key-Rate DV01) ----------   
    st.markdown("---")
    st.markdown("### Hedge por Vértice")

    def _bucket_label(dt, use_underscore: bool = False) -> str | None:
        """
        Mapeia a data do fluxo para o bucket do futuro de DI mais próximo:
        - Jan do ano 'yy'  -> DIFyy
        - Jun do ano 'yy'  -> DINyy
        Se a data estiver mais próxima de jan do ano seguinte, cai em DIF(yy+1).
        Observação: acima de 2030 só há contratos 'F' (DIF31, DIF32, DIF33, DIF35, DIF37).
        """
        if pd.isna(dt):
            return None
        d = pd.Timestamp(dt).normalize()
        y = d.year

        jan_y  = pd.Timestamp(year=y, month=1, day=2)   # 1ª sessão aprox.
        jun_y  = pd.Timestamp(year=y, month=6, day=1)

        # Distâncias absolutas
        dist_jan = abs((d - jan_y).days)
        dist_jun = abs((d - jun_y).days)

        if dist_jan <= dist_jun:
            bucket = f"DIF{y % 100:02d}"
        else:
            bucket = f"DIN{y % 100:02d}"

        # Regra das caudas: se depois de out/nov, é mais natural cair em jan do ano+1
        if d.month >= 10:
            bucket = f"DIF{(y + 1) % 100:02d}"

        # Limpeza por disponibilidade: acima de 2030, só DIF (F)
        yy = int(bucket[-2:])
        if yy >= 31:
            bucket = f"DIF{yy:02d}"

        return bucket
    
    def _bucket_norm_one(x: str, default_to_F=True) -> str | None:
        """Normaliza qualquer variação para DIFyy/DINyy.
        - 'DI27' → 'DIF27' (default_to_F=True) ou 'DIN27' se default_to_F=False
        - 'DIF27'/'DIN27' mantidos
        - 'DI_27', 'di 27' → 'DIF27'
        """
        if x is None or (isinstance(x, float) and pd.isna(x)): 
            return None
        s = str(x).strip().upper()
        s = _re.sub(r'\s+', '', s)
        # já correto?
        m = _re.fullmatch(r'DI(F|N)(\d{2})', s)
        if m: 
            return f'DI{m.group(1)}{m.group(2)}'
        # 'DI27', 'DI_27', 'DI 27'
        m = _re.fullmatch(r'DI[_\s]?(\d{2})', s)
        if m:
            yy = m.group(1)
            return (f'DIF{yy}' if default_to_F else f'DIN{yy}')
        # 'ODF26'/'ODN26'
        m = _re.fullmatch(r'OD(F|N)(\d{2})', s)
        if m:
            return f'DI{m.group(1)}{m.group(2)}'
        return None

    def _bucket_norm_series(s: pd.Series, default_to_F=True) -> pd.Series:
        return s.map(lambda z: _bucket_norm_one(z, default_to_F=default_to_F))

    base_g = (df_base.groupby(["Codigo","Data_fim"], as_index=False)["VP_total"].sum().rename(columns={"VP_total":"VP_base"}))
    bump_g = (df_bump.groupby(["Codigo","Data_fim"], as_index=False)["VP_total"].sum().rename(columns={"VP_total":"VP_bump"}))
    delta = (base_g.merge(bump_g, on=["Codigo","Data_fim"], how="outer").fillna(0.0))
    delta["dVP"] = delta["VP_bump"] - delta["VP_base"]
    delta["DI_bucket"] = pd.to_datetime(delta["Data_fim"]).map(lambda x: _bucket_label(x, use_underscore=False))
    dv01_side = (delta.groupby(["DI_bucket","Codigo"], as_index=False)["dVP"].sum().rename(columns={"dVP":"DV01_R$/bp"}))
    
    
    dv01_fundo_vert = (dv01_side.groupby("DI_bucket", as_index=False)["DV01_R$/bp"].sum().rename(columns={"DV01_R$/bp":"DV01_fundo_R$/bp"}))
    
    s_dv01_ctrt = _load_di_fut_dv01_series()

    if s_dv01_ctrt.empty:
        st.warning("DV01 dos contratos de DI não carregado. Coluna de contratos teóricos não será calculada.")
        df_di = pd.DataFrame({"DI_bucket": dv01_fundo_vert["DI_bucket"], "DV01_DI_R$/contrato": np.nan})
    else:
        df_di = s_dv01_ctrt.reset_index()
        df_di.columns = ["DI_bucket", "DV01_DI_R$/contrato"]

    # NORMALIZAÇÃO CONSISTENTE (sempre DIF/DIN)
    dv01_fundo_vert["DI_bucket"] = _bucket_norm_series(dv01_fundo_vert["DI_bucket"], default_to_F=True)
    df_di["DI_bucket"]           = _bucket_norm_series(df_di["DI_bucket"],           default_to_F=True)

    # limpa None
    dv01_fundo_vert = dv01_fundo_vert.dropna(subset=["DI_bucket"])
    df_di           = df_di.dropna(subset=["DI_bucket"])

    res_vert = dv01_fundo_vert.merge(df_di, on="DI_bucket", how="left")
    dv01_contrato = res_vert["DV01_DI_R$/contrato"]
    res_vert["Contratos_teoricos"] = np.where(
        dv01_contrato.notna() & (dv01_contrato != 0),
        (res_vert["DV01_fundo_R$/bp"] / dv01_contrato).round(0),
        0
    ).astype(int)
   
    with st.expander(" Análise de Sensibilidade: DV01 por Ativo x Vértice"):
        if dv01_side.empty:
            st.info("Nenhum dado de sensibilidade para exibir.")
        else:
            # Cria as abas para as duas visualizações
            tab_dv01, tab_contratos = st.tabs(["DV01 (R$ por bp)", "Nº Contratos para Hedge"])
            
            # Transforma a tabela para o formato Ativo x Vértice
            dv01_pivot = dv01_side.pivot_table(
                index="Codigo", 
                columns="DI_bucket", 
                values="DV01_R$/bp",
                fill_value=0.0
            )
            # Adiciona um total por ativo
            dv01_pivot['Total'] = dv01_pivot.sum(axis=1)
            
            # Função de estilo para zerar os zeros (reutilizada em ambas as abas)
            def highlight_zeros(s):
                if s.name == 'Total': return ['' for _ in s]
                return ['background-color: #31333F' if v == 0 else '' for v in s]

            # --- Aba 1: Heatmap de DV01 (em R$) ---
            with tab_dv01:
                styler_dv01 = (dv01_pivot.sort_values("Total")
                               .style
                               .format("{:,.2f}")
                               .background_gradient(cmap='RdYlGn', axis=1, subset=dv01_pivot.columns[:-1])
                               .apply(highlight_zeros, axis=0))

                st.dataframe(
                    styler_dv01,
                    use_container_width=True
                )
            
            # --- Aba 2: Heatmap de Nº de Contratos ---
            with tab_contratos:
                # Verifica se temos os dados de DV01 por contrato para fazer a divisão
                if 'df_di' in locals() and not df_di.empty:
                    # Cria uma série para facilitar a divisão: index=DI_bucket, value=DV01_DI_R$/contrato
                    s_dv01_contrato = df_di.set_index('DI_bucket')['DV01_DI_R$/contrato']
                    
                    # Prepara o dataframe para o cálculo (sem a coluna Total)
                    dv01_para_calc = dv01_pivot.drop(columns='Total')
                    dv01_para_calc.columns = _norm_bucket(pd.Index(dv01_para_calc.columns))
                    s_dv01_contrato = df_di.set_index('DI_bucket')['DV01_DI_R$/contrato']
                    s_dv01_contrato.index = _norm_bucket(s_dv01_contrato.index.to_series())

                    s_dv01_contrato = s_dv01_contrato.reindex(dv01_para_calc.columns)
                    contratos_pivot = dv01_para_calc.div(s_dv01_contrato, axis=1).fillna(0.0)

                    contratos_pivot.fillna(0.0, inplace=True) # Trata casos onde o DV01 do contrato é 0 ou NaN
                    
                    # Adiciona a coluna de Total de contratos por ativo
                    contratos_pivot['Total'] = contratos_pivot.sum(axis=1)

                    # Aplica o mesmo estilo visual, mas com formatação para números inteiros
                    styler_contratos = (contratos_pivot.sort_values("Total")
                                        .style
                                        .format("{:,.0f}") # Formata como inteiro
                                        .background_gradient(cmap='RdYlGn', axis=1, subset=contratos_pivot.columns[:-1])
                                        .apply(highlight_zeros, axis=0))

                    st.dataframe(
                        styler_contratos,
                        use_container_width=True
                    )
                else:
                    st.warning("Dados de DV01 por contrato de DI não estão disponíveis para calcular o hedge em nº de contratos.")


    #st.markdown("#### Posições Atuais em Contratos DI (Estratégia 'Hedge DI')")
    
    try:
        # ### ALTERAÇÃO AQUI ###
        # 1. Filtra o DataFrame `rel_df` pela estratégia E PELO FUNDO SELECIONADO
        filtro_estrategia = rel_df['Estratégia'].str.contains("Hedge DI", na=False)
        filtro_fundo = rel_df['Fundo'] == sel_fundo
        df_di_raw = rel_df[filtro_estrategia & filtro_fundo].copy()

        # 2. Funções de parsing com regex
        def extrair_qtd_hedge(texto: str) -> int:
            """Extrai a quantidade de contratos de 'Hedge DI (XX)'."""
            if not isinstance(texto, str): return 0
            match = re.search(r"Hedge DI\s*\(\s*(-?\d+)\s*\)", texto)
            return int(match.group(1)) if match else 0

        def extrair_info_ativo(ativo: str) -> tuple:
            """
            Extrai Mês/ano e retorna o bucket DIF/DIN.
            Exemplos:
            'DI1JAN27' -> ('JAN', 27, 'DIF27')
            'DI1JUN27' -> ('JUN', 27, 'DIN27')
            'ODF26'    -> ('JAN', 26, 'DIF26')
            'ODN26'    -> ('JUN', 26, 'DIN26')
            """
            if not isinstance(ativo, str):
                return None, None, None
            s = ativo.strip().upper().replace(" ", "")

            m = re.search(r"^DI1(JAN|JUN)(\d{2})$", s)
            if m:
                mon, yy = m.group(1), int(m.group(2))
                bucket = f"DI{'F' if mon=='JAN' else 'N'}{yy:02d}"
                return mon, yy, bucket

            m2 = re.search(r"^OD([FN])(\d{2})$", s)
            if m2:
                fn, yy = m2.group(1), int(m2.group(2))
                bucket = f"DI{'F' if fn=='F' else 'N'}{yy:02d}"
                mon = 'JAN' if fn == 'F' else 'JUN'
                return mon, yy, bucket

            # fallback
            return None, None, None


        if df_di_raw.empty:
            #st.info(f"Nenhuma posição com a estratégia 'Hedge DI' encontrada para o fundo '{sel_fundo}'.")
            # Cria uma coluna vazia se não houver posições para evitar erros no merge
            res_vert['Contratos_atuais'] = 0
        else:
            # 3. Aplica as funções para criar novas colunas
            df_di_raw['Quantidade_Hedge'] = df_di_raw['Estratégia'].apply(extrair_qtd_hedge)
            
            # Extrai Mês, Ano e cria o Bucket para o merge
            ativo_info = df_di_raw['Ativo'].apply(extrair_info_ativo).apply(pd.Series)
            ativo_info.columns = ['Mes', 'Ano', 'DI_bucket']
            df_di_pos = pd.concat([df_di_raw, ativo_info], axis=1)

            # Filtra apenas as linhas onde a extração foi bem-sucedida
            df_di_pos.dropna(subset=['DI_bucket', 'Quantidade_Hedge'], inplace=True)
            
            # Exibe uma tabela de verificação para o usuário
            #st.write("Posições extraídas da base para este fundo:")
            #st.dataframe(
            #    df_di_pos[['Ativo', 'Estratégia', 'Quantidade_Hedge', 'Mes', 'DI_bucket']].style.format({'Quantidade_Hedge': '{:,.0f}'}),
            #    use_container_width=True
            #)

            # 4. Agrega as quantidades por bucket
            s_posicoes_atuais = df_di_pos.groupby('DI_bucket')['Quantidade_Hedge'].sum()

            # 5. Juntar com a tabela de resultados
            res_vert = res_vert.merge(s_posicoes_atuais.rename('Contratos_atuais'), on='DI_bucket', how='left')
            res_vert['Contratos_atuais'].fillna(0, inplace=True)
            res_vert['Contratos_atuais'] = res_vert['Contratos_atuais'].astype(int)

        # 6. Calcula a diferença para o hedge ideal
        res_vert["Hedge_necessario"] = res_vert["Contratos_teoricos"] - res_vert.get("Contratos_atuais", 0)

    except NameError:
        st.warning("A variável `rel_df` não foi encontrada. Não foi possível carregar as posições atuais de DI.")
        res_vert["Contratos_atuais"] = 0
        res_vert["Hedge_necessario"] = res_vert["Contratos_teoricos"]
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar `rel_df`: {e}")
        res_vert["Contratos_atuais"] = "Erro"
        res_vert["Hedge_necessario"] = "Erro"



    st.markdown("### Contratos de DI por Vértice")
    # Garante que as colunas existam antes de formatar
    colunas_finais = ["DI_bucket", "DV01_fundo_R$/bp", "DV01_DI_R$/contrato", "Contratos_teoricos", "Contratos_atuais", "Hedge_necessario"]
    for col in colunas_finais:
        if col not in res_vert.columns:
            res_vert[col] = 0 # ou np.nan

    st.dataframe(
        res_vert[colunas_finais].sort_values("DI_bucket").style.format({
            "DV01_fundo_R$/bp": "{:,.2f}",
            "DV01_DI_R$/contrato": "{:,.2f}",
            "Contratos_teoricos": "{:,.0f}",
            "Contratos_atuais": "{:,.0f}",
            "Hedge_necessario": "{:,.0f}",
        }),
        use_container_width=True
    )

    # ### NOVO: Expander com hedge por "Vértice Consecutivo" e por "Duration" ###
    with st.expander(" Análise Alternativa: Hedge por Ano de Referência (Consecutivo vs Duration)"):
        # 'delta' já foi calculado mais acima: contém dVP (DV01) por fluxo e Data_fim
        if 'delta' not in locals() or delta.empty:
            st.info("Dados de fluxo insuficientes para calcular o hedge por referência.")
        else:
            # ---------------------------
            # Funções auxiliares
            # ---------------------------
            import re as _re

            def _norm_bucket(s: pd.Series) -> pd.Series:
                # força DIF/DIN (default F quando vier 'DIyy' puro)
                return _bucket_norm_series(s, default_to_F=True)

            def _yy_from_bucket(b: str) -> int | None:
                m = _re.search(r'(\d{2})$', str(b) if b is not None else '')
                return int(m.group(1)) if m else None

            def _year_to_bucket(year: int, prefer='F') -> str:
                yy = int(year) % 100
                return f'DI{"F" if prefer=="F" else "N"}{yy:02d}'

            def _expand_df_di_to_cover(df_di: pd.DataFrame, needed_buckets: list[str]) -> pd.DataFrame:
                """
                Recebe df_di = ['DI_bucket','DV01_DI_R$/contrato'] com DI26..DI35 e
                expande/interpola para cobrir todos os buckets de `needed_buckets`.
                Faz forward/backward fill nas pontas.
                """
                if df_di is None or df_di.empty:
                    return pd.DataFrame(columns=["DI_bucket","DV01_DI_R$/contrato"])

                df = df_di.copy()
                df['DI_bucket'] = _norm_bucket(df['DI_bucket'])
                df['YY'] = df['DI_bucket'].map(_yy_from_bucket)
                df = df.dropna(subset=['YY'])
                s = df.groupby('YY')['DV01_DI_R$/contrato'].mean().sort_index()

                # range necessário
                needed = pd.Series(needed_buckets).pipe(_norm_bucket)
                yy_needed = [y for y in (needed.map(_yy_from_bucket).dropna().astype(int).tolist())]
                if not yy_needed:
                    return df[['DI_bucket','DV01_DI_R$/contrato']].drop_duplicates()

                # Queremos dois conjuntos: F e N (quando existir)
                df = df.copy()
                df['IS_F'] = df['DI_bucket'].str.contains(r"DIF")

                # Interpola separadamente por F e N para evitar "contaminar" um semestre com o outro
                out_parts = []
                for is_f in [True, False]:
                    sub = df[df['IS_F'] == is_f]
                    if sub.empty:
                        continue
                    s_sub = (sub.set_index('YY')['DV01_DI_R$/contrato']
                                .sort_index())
                    idx_full = pd.Index(range(min(yy_needed), max(yy_needed)+1), name='YY')
                    s_full = (s_sub.reindex(idx_full)
                                    .interpolate(method='index', limit_direction='both')
                                    .ffill().bfill())
                    tag = 'F' if is_f else 'N'
                    tmp = s_full.reset_index()
                    tmp['DI_bucket'] = tmp['YY'].map(lambda y: f"DI{tag}{int(y):02d}")
                    tmp.rename(columns={0:'DV01_DI_R$/contrato'}, inplace=True)
                    tmp['DV01_DI_R$/contrato'] = tmp['DV01_DI_R$/contrato'].values
                    out_parts.append(tmp[['DI_bucket','DV01_DI_R$/contrato']])

                out = pd.concat(out_parts, ignore_index=True) if out_parts else df[['DI_bucket','DV01_DI_R$/contrato']]
                # Pós-regra: para YY >= 31, manter apenas DIF
                out = out[~(out['DI_bucket'].str.match(r"^DIN(\d{2})$") & (out['DI_bucket'].str[-2:].astype(int) >= 31))]
                return out[['DI_bucket','DV01_DI_R$/contrato']]

            # ---------------------------
            # Base comum a ambos métodos
            # ---------------------------
            df_ref = delta.copy()
            df_ref['Data_fim'] = pd.to_datetime(df_ref['Data_fim'])
            df_ref['Ano_Pgto'] = df_ref['Data_fim'].dt.year
            df_ref['abs_dVP']  = df_ref['dVP'].abs()

            # DV01 total por ativo
            df_total_dv01 = (df_ref.groupby('Codigo', as_index=False)['dVP']
                            .sum().rename(columns={'dVP': 'DV01_Total_Ativo'}))

            # Data base para duration
            try:
                _base_dt_for_dur = base_dt.normalize()
            except Exception:
                _base_dt_for_dur = pd.to_datetime(REF_DATE_CURVA).normalize()

            # =========================
            # Método A) Vértice consecutivo
            # =========================
            dv01_por_ano = (df_ref.groupby(['Codigo','Ano_Pgto'], as_index=False)['abs_dVP'].sum())
            idx_max = dv01_por_ano.groupby('Codigo')['abs_dVP'].idxmax()
            df_ano_ref = (dv01_por_ano.loc[idx_max, ['Codigo','Ano_Pgto']]
                        .rename(columns={'Ano_Pgto':'Ano_Referencia'}))

            df_consec = df_ano_ref.merge(df_total_dv01, on='Codigo', how='left')
            df_consec['Hedge_Bucket_Consecutivo'] = df_consec['Ano_Referencia'].apply(
                lambda y: _year_to_bucket(int(y) + 1, prefer='F')  # sempre janeiro do ano seguinte
            )

            st.markdown("##### Mapeamento (Consecutivo): Ativo → Vértice")
            st.dataframe(
                df_consec[['Codigo','Ano_Referencia','DV01_Total_Ativo','Hedge_Bucket_Consecutivo']]
                .sort_values(['Hedge_Bucket_Consecutivo','Codigo'])
                .style.format({'Ano_Referencia': '{:,.0f}', 'DV01_Total_Ativo': '{:,.2f}'}),
                use_container_width=True
            )

            agg_consec = (df_consec.groupby('Hedge_Bucket_Consecutivo', as_index=False)['DV01_Total_Ativo']
                        .sum().rename(columns={'Hedge_Bucket_Consecutivo':'DI_bucket',
                                                'DV01_Total_Ativo':'DV01_Agregado_R$/bp'}))

            # =========================
            # Método B) Por Duration
            # =========================
            
            def _nearest_bucket_from_target(base_date: pd.Timestamp, dur_years: float) -> tuple[pd.Timestamp, str]:
                """
                Converte duration em anos -> data alvo (base + round(anos*365) dias) e
                escolhe o bucket mais próximo entre:
                - DIF(y): 01/y
                - DIN(y): 06/y (apenas para yy<31)
                - DIF(y+1): 01/(y+1)
                Retorna (data_âncora_escolhida, 'DIFyy'/'DINyy').
                """
                try:
                    x = float(dur_years)
                except Exception:
                    x = 0.0
                if not np.isfinite(x) or x < 0:
                    x = 0.0

                # Data-alvo pela “regra simples”
                days = int(round(x * 365.0))
                target = (pd.Timestamp(base_date) + pd.Timedelta(days=days)).normalize()
                y = int(target.year)

                # Âncoras: Jan/y (DIFy), Jun/y (DINy se yy<31), Jan/(y+1) (DIFy+1)
                jan_y  = pd.Timestamp(year=y, month=1, day=2)   # ~primeira sessão útil de jan
                jun_y  = pd.Timestamp(year=y, month=6, day=1)   # ~início de jun
                jan_y1 = pd.Timestamp(year=y+1, month=1, day=2)

                candidates = [
                    (jan_y,  f"DIF{y % 100:02d}", abs((target - jan_y).days)),
                    (jan_y1, f"DIF{(y+1) % 100:02d}", abs((target - jan_y1).days)),
                ]
                if (y % 100) < 31:  # DIN só até 2030
                    candidates.append((jun_y, f"DIN{y % 100:02d}", abs((target - jun_y).days)))

                # Escolhe a âncora mais próxima
                anchor_date, bucket, _ = min(candidates, key=lambda t: t[2])
                return anchor_date, bucket


            # 1) Base para duration: usa os fluxos do CENÁRIO BASE (df_base)
            _df_dur_base = df_base.copy()
            _df_dur_base['Data_fim'] = pd.to_datetime(_df_dur_base['Data_fim'])
            _df_dur_base = _df_dur_base[_df_dur_base['Data_fim'] > base_dt]  # só fluxos futuros

            if _df_dur_base.empty:
                df_dur = pd.DataFrame(columns=[
                    'Codigo','Duration_anos','Data_Alvo','Ano_Referencia_DUR','DV01_Total_Ativo','Hedge_Bucket_Duration'
                ])
                agg_dur = pd.DataFrame(columns=['DI_bucket','DV01_Agregado_R$/bp'])
            else:
                # 2) Tempo em anos a partir da base
                _df_dur_base['t_anos'] = (_df_dur_base['Data_fim'] - base_dt).dt.days / 365.25
                _df_dur_base = _df_dur_base[_df_dur_base['t_anos'] > 0]

                # 3) Duration (Macaulay “no dia”): sum(PV * t) / sum(PV), por ativo
                def _dur_macaulay(g: pd.DataFrame) -> float:
                    pv = g['VP_total'].to_numpy(dtype=float)
                    t  = g['t_anos'].to_numpy(dtype=float)
                    den = float(np.sum(pv))
                    if den <= 0 or not np.isfinite(den):
                        return 0.0
                    return float(np.sum(pv * t) / den)

                dur_por_ativo = (
                    _df_dur_base.groupby('Codigo')
                    .apply(_dur_macaulay)
                    .rename('Duration_anos')
                    .reset_index()
                )

                # 4) Data alvo e bucket (regra simples das âncoras)
                tmp = dur_por_ativo['Duration_anos'].apply(lambda z: _nearest_bucket_from_target(base_dt, z))
                dur_por_ativo['Data_Alvo'] = tmp.map(lambda t: t[0])
                dur_por_ativo['Hedge_Bucket_Duration'] = tmp.map(lambda t: t[1])
                dur_por_ativo['Ano_Referencia_DUR'] = pd.to_datetime(dur_por_ativo['Data_Alvo']).dt.year

                # 5) DV01 total por ativo (do delta já calculado)
                df_total_dv01 = (
                    df_ref.groupby('Codigo', as_index=False)['dVP']
                    .sum()
                    .rename(columns={'dVP': 'DV01_Total_Ativo'})
                )

                df_dur = dur_por_ativo.merge(df_total_dv01, on='Codigo', how='left')

                st.markdown("##### Mapeamento (Duration): Ativo → Vértice (âncoras 01/yy, 06/yy, 01/(yy+1))")
                st.dataframe(
                    df_dur[['Codigo','Duration_anos','Data_Alvo','Ano_Referencia_DUR','DV01_Total_Ativo','Hedge_Bucket_Duration']]
                    .sort_values(['Hedge_Bucket_Duration','Codigo'])
                    .style.format({
                        'Duration_anos':'{:,.4f}',
                        'DV01_Total_Ativo':'{:,.2f}'
                    }),
                    use_container_width=True
                )

                # 6) Agregado por bucket
                agg_dur = (
                    df_dur.groupby('Hedge_Bucket_Duration', as_index=False)['DV01_Total_Ativo']
                    .sum()
                    .rename(columns={'Hedge_Bucket_Duration':'DI_bucket',
                                    'DV01_Total_Ativo':'DV01_Agregado_R$/bp'})
    )
            # --------------------------
            # Normaliza e expande df_di
            # --------------------------
            agg_consec['DI_bucket'] = _norm_bucket(agg_consec['DI_bucket'])
            agg_dur['DI_bucket']    = _norm_bucket(agg_dur['DI_bucket'])

            if 'df_di' in locals() and isinstance(df_di, pd.DataFrame) and not df_di.empty:
                # df_di já normalizado no passo 2; expanda cobrindo YY necessários
                needed = pd.unique(pd.concat([agg_consec['DI_bucket'], agg_dur['DI_bucket']], ignore_index=True))
                df_di_use = _expand_df_di_to_cover(df_di, needed.tolist())
                df_di_use['DI_bucket'] = _norm_bucket(df_di_use['DI_bucket'])
            else:
                df_di_use = pd.DataFrame(columns=["DI_bucket","DV01_DI_R$/contrato"])

            # -----------------------------------
            # Função utilitária: completar tabela
            # -----------------------------------
            def _finaliza_agregado(agg_df: pd.DataFrame, df_di_local: pd.DataFrame | None, s_pos: pd.Series | None) -> pd.DataFrame:
                out_df = agg_df.copy()
                out_df['DI_bucket'] = _norm_bucket(out_df['DI_bucket'])
                if isinstance(df_di_local, pd.DataFrame) and not df_di_local.empty:
                    out_df = out_df.merge(df_di_local, on='DI_bucket', how='left')
                else:
                    out_df['DV01_DI_R$/contrato'] = np.nan

                dv01_ctrt = out_df['DV01_DI_R$/contrato']
                out_df['Contratos_teoricos'] = np.where(
                    dv01_ctrt.notna() & (dv01_ctrt != 0),
                    (out_df['DV01_Agregado_R$/bp'] / dv01_ctrt).round(0),
                    0
                ).astype(int)

                if s_pos is not None and not s_pos.empty:
                    out_df = out_df.merge(s_pos.rename('Contratos_atuais'), on='DI_bucket', how='left')
                else:
                    out_df['Contratos_atuais'] = 0

                out_df['Contratos_atuais'] = out_df['Contratos_atuais'].fillna(0).astype(int)
                out_df['Hedge_necessario'] = out_df['Contratos_teoricos'] - out_df['Contratos_atuais']

                for c in ["DV01_Agregado_R$/bp","DV01_DI_R$/contrato","Contratos_teoricos","Contratos_atuais","Hedge_necessario"]:
                    if c not in out_df.columns: out_df[c] = 0
                return out_df

            # Aplica
            resultado_consec = _finaliza_agregado(agg_consec, df_di_use,
                                                s_posicoes_atuais if 's_posicoes_atuais' in locals() else None)
            resultado_dur    = _finaliza_agregado(agg_dur, df_di_use,
                                                s_posicoes_atuais if 's_posicoes_atuais' in locals() else None)

            # Exibe tabelas
            st.markdown("##### Resultado Agregado — Método Consecutivo")
            cols_fmt = ['DI_bucket','DV01_Agregado_R$/bp','DV01_DI_R$/contrato','Contratos_teoricos','Contratos_atuais','Hedge_necessario']
            st.dataframe(
                resultado_consec[cols_fmt].sort_values('DI_bucket').style.format({
                    'DV01_Agregado_R$/bp':'{:,.2f}','DV01_DI_R$/contrato':'{:,.2f}',
                    'Contratos_teoricos':'{:,.0f}','Contratos_atuais':'{:,.0f}','Hedge_necessario':'{:,.0f}'
                }),
                use_container_width=True
            )

            st.markdown("##### Resultado Agregado — Método por Duration")
            st.dataframe(
                resultado_dur[cols_fmt].sort_values('DI_bucket').style.format({
                    'DV01_Agregado_R$/bp':'{:,.2f}','DV01_DI_R$/contrato':'{:,.2f}',
                    'Contratos_teoricos':'{:,.0f}','Contratos_atuais':'{:,.0f}','Hedge_necessario':'{:,.0f}'
                }),
                use_container_width=True
            )


            # -----------------------------
            # Comparação lado a lado
            # -----------------------------
            comp = (resultado_consec[['DI_bucket','Contratos_teoricos']]
                    .rename(columns={'Contratos_teoricos':'Contratos_teoricos_CONSEC'})
                    ).merge(
                        resultado_dur[['DI_bucket','Contratos_teoricos']]
                        .rename(columns={'Contratos_teoricos':'Contratos_teoricos_DUR'}),
                        on='DI_bucket', how='outer'
                    ).merge(
                        df_di_use[['DI_bucket','DV01_DI_R$/contrato']],
                        on='DI_bucket', how='left'
                    )

            comp['Contratos_teoricos_CONSEC'] = comp['Contratos_teoricos_CONSEC'].fillna(0).astype(int)
            comp['Contratos_teoricos_DUR']    = comp['Contratos_teoricos_DUR'].fillna(0).astype(int)
            comp['Delta_(DUR-CONSEC)']        = (comp['Contratos_teoricos_DUR'] - comp['Contratos_teoricos_CONSEC']).astype(int)

            st.markdown("##### Comparação de Contratos Teóricos (Duration vs Consecutivo)")
            st.dataframe(
                comp.sort_values('DI_bucket').style.format({
                    'Contratos_teoricos_CONSEC':'{:,.0f}',
                    'Contratos_teoricos_DUR':'{:,.0f}',
                    'Delta_(DUR-CONSEC)':'{:,.0f}',
                    'DV01_DI_R$/contrato':'{:,.2f}'
                }),
                use_container_width=True
            )

            # Debug
            #st.caption("Debug DV01 contratos")
            #st.write("df_di_use.head():", df_di_use.head())
            #st.write("Buckets em df_di_use:", sorted(df_di_use['DI_bucket'].astype(str).unique()))
            #_bucks_needed = sorted(pd.unique(pd.concat([agg_consec['DI_bucket'], agg_dur['DI_bucket']], ignore_index=True)))
            #st.write("Buckets necessários (resultado atual):", _bucks_needed)
