# fusao_taxas.py
# -*- coding: utf-8 -*-
"""
Fluxo unificado:
1) Lê Dados/ativos_mapeados_para_controle.xlsx (colunas: Sub Classe, Emissor, Fundo, Ativo, ISIN, COD_XP, Vencimento_final)
2) Para fundos BTG: tenta obter TAXA_ISIN (último dia disponível do fundo nas carteiras).
3) Para todos: calcula Taxa_matriz (matriz de curvas; rating/YMF) como fallback.
4) Regra de prioridade: TAXA_EFETIVA = TAXA_ISIN (se existir) senão Taxa_matriz.
5) Propaga taxa entre fundos para o mesmo ativo (mesma chave sem “Fundo”), marcando sanity_propagado=True.
6) Avisa se restarem ativos sem taxa.
7) Grava: Dados/ativos_mapeados_com_taxa_efetiva.xlsx (com TAXA_EFETIVA única), e logs auxiliares.

Dependências:
    pip install pandas numpy openpyxl pandas-market-calendars Unidecode
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
import re
import unicodedata

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from unidecode import unidecode

# ========================= Paths / Config =========================
CONTROLE_XLSX = Path("Dados/ativos_mapeados_para_controle.xlsx")   # entrada
MATRIZ_XLSX   = Path("Dados/Matriz de Curvas 31102025.xlsx")             # matriz de curvas
OUT_FINAL_XLSX = Path("Dados/ativos_mapeados_com_taxa_efetiva.xlsx")
OUT_SEM_TAXA_XLSX = Path("Dados/ativos_sem_taxa.xlsx")

# Diretório da árvore de carteiras diárias (BTG)
BASE_DIR = Path(r"Z:\Asset Management")  # ajuste se necessário
SPECIFIC_DATE = pd.Timestamp("2025-10-31")            # <- Data que será inserida no campo #Data quando o toggle estiver ligado

# DEBUG
DEBUG = True
def dbg(*a, **k):
    if DEBUG: print(*a, **k)

# ========================= Helpers texto/num =========================
def _norm(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    return s.upper().strip()

def strip_accents(s: str) -> str:
    if s is None: return ""
    return unidecode(str(s))

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", strip_accents(s or "")).strip().upper()

def _num(val):
    if val is None: return np.nan
    if isinstance(val, (int, float, np.number)): return float(val)
    s = str(val).strip().replace(" ", "")
    if s == "": return np.nan
    s = re.sub(r"[^\d,.\-+]", "", s)
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def _infer_subclasse(row: pd.Series) -> str:
    sc = norm_text(row.get("Sub Classe", ""))
    codxp = str(row.get("COD_XP", "")).upper()
    ativo = str(row.get("Ativo", "")).upper()

    # 1) Se já for específico, mantém
    if any(tag in sc for tag in ["CDB","LFSN","LFSC","LF","LC","CRI","CRA","DEB"]):
        return row.get("Sub Classe","")

    # 2) Derivar pelo COD_XP/Ativo
    if "CDB" in codxp or " CDB" in f" {ativo} ":
        return "CDB"
    if re.search(r"\bLFSN\b", ativo): return "LFSN"
    if re.search(r"\bLFSC\b", ativo): return "LFSC"
    if re.search(r"\bLF\b",   ativo): return "LF"
    if "LC" in ativo: return "LC"

    # 3) Se vier "TÍTULOS PRIVADOS", troca por algo plausível (ex.: CDB)
    if "TITUL" in sc and "PRIV" in sc:
        return "CDB"

    return row.get("Sub Classe","")



# ========================= Mapeamento de Fundos (nomes de pasta/arquivo) =========================
# Mapa fornecido: chaves = como vem no CONTROLE, valores = nome usado no arquivo/pasta
FUND_NAME_MAP_RAW = {
    "BBRASIL FIM CP RESP": "BBRASIL FIM CP RESP",
    "BH FIRF INFRA":       "BH INFRA",
    "BMG SEG":             "BMG SEG",
    "BORDEAUX INFRA":      "BORDEAUX INFRA",
    "FIRF GERAES":         "GERAES",
    "FIRF GERAES 30":      "GERAES 30",
    "HORIZONTE":           "AF HORIZONTE",
    "JERA2026":            "JERA2026",
    "MANACA INFRA FIRF":   "MANACA INFRA",
    "REAL FIM":            "REAL FIM",
    "TOPAZIO INFRA":       "TOPAZIO INFRA",
}
# versão normalizada para lookup case/acentos-insensitive
FUND_NAME_MAP = {norm_text(k): v for k, v in FUND_NAME_MAP_RAW.items()}

def canon_fund_name(nome: str) -> str:
    """
    Retorna o nome canônico do fundo conforme usado nos arquivos/pastas.
    Se não estiver no mapa, retorna versão normalizada (UPPER sem acento).
    """
    n = norm_text(nome)
    if not n:
        return ""
    return FUND_NAME_MAP.get(n, n)

def candidate_fund_names_for_files(nome: str) -> list[str]:
    """
    Para robustez, testa (nesta ordem):
      1) nome canônico mapeado
      2) nome original "como veio"
      3) nome normalizado (UPPER sem acentos)
    """
    raw = str(nome or "").strip()
    can = canon_fund_name(raw)
    nrm = norm_text(raw)
    seq = [can, raw, nrm]
    out = []
    for s in seq:
        if s and s not in out:
            out.append(s)
    return out

# ========================= Definições BTG/XP =========================
# exceções: estes são XP (não procurar ISIN em árvore BTG)
XP_EXCEPTIONS = {norm_text("JERA2026"), norm_text("REAL FIM")}

BTG_FUNDS_WHITELIST = set()   # opcional: {'AF HORIZONTE', 'GERAES', ...}
XP_FUNDS_WHITELIST  = set(XP_EXCEPTIONS)  # marca XP pelas exceções informadas

def is_xp_fund(nome: str) -> bool:
    n = norm_text(nome)
    if n in XP_FUNDS_WHITELIST: 
        return True
    # heurística simples: contém 'XP' no nome → trata como XP
    return " XP" in f" {n} " or n.startswith("XP ") or n.endswith(" XP")

def is_btg_fund(nome: str) -> bool:
    n = norm_text(nome)
    if n in BTG_FUNDS_WHITELIST: 
        return True
    # default: tudo que NÃO for XP consideramos BTG (para tentar TAXA_ISIN)
    return not is_xp_fund(n)

# ========================= Parte A — TAXA_ISIN (último dia da carteira) =========================
CAL_B3 = mcal.get_calendar("B3")
MESES_PT = {"01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril","05":"Maio","06":"Junho",
            "07":"Julho","08":"Agosto","09":"Setembro","10":"Outubro","11":"Novembro","12":"Dezembro"}

def taxas_isin_no_ultimo_dia(fundo: str,
                              d_ini: str,
                              d_fim: str) -> tuple[pd.Timestamp | None, pd.Series]:
    """
    Retorna (data_ultimo_dia_com_isin, mapa_taxas_por_isin)
    O 'último dia' aqui é o último DIA ÚTIL <= d_fim que realmente contenha
    a seção com ISIN e Coupon (não basta existir o arquivo).
    """
    # Garante limites coerentes
    d_ini = pd.to_datetime(d_ini).date().isoformat()
    d_fim = pd.to_datetime(d_fim).date().isoformat()

    # Anda para trás no calendário da B3
    for dt in CAL_B3.schedule(d_ini, d_fim).index[::-1]:
        dt = pd.Timestamp(dt)
        p = _path_fund(fundo, dt)
        if p is None:
            continue

        # Tenta extrair ISIN↔cupom desse dia
        df_tax = parse_tp_taxas_isin(p, fundo, dt)
        if not df_tax.empty:
            # Mapa {ISIN: taxa_frac}
            mapa = df_tax.set_index("isin")["tax"].astype(float)
            if mapa.notna().any():
                return dt.normalize(), mapa

    # Nada encontrado no intervalo
    return None, pd.Series(dtype=float)


def _path_fund(fund: str, dt: pd.Timestamp) -> Path | None:
    """
    Tenta montar o caminho do arquivo da carteira diária para um fundo em dt,
    testando várias variantes de nome (mapeada, original, normalizada).
    """
    base  = BASE_DIR / "FUNDOS e CLUBES" / "Carteira diária"
    ano   = str(dt.year)
    mes   = dt.strftime("%m"); nome = MESES_PT.get(mes, mes)
    dia   = dt.strftime("%d"); ddmm  = dt.strftime("%d%m")
    pastas_mes    = [f"{mes} - {nome}", f"{mes}- {nome}"]

    for pasta in pastas_mes:
        dir_dia = base / ano / pasta / dia
        if not dir_dia.exists():
            continue
        for fnd in candidate_fund_names_for_files(fund):
            nomes_arquivo = [f"{ddmm}_{fnd}.xlsx", f"{fnd}.xlsx"]
            for nome_arq in nomes_arquivo:
                caminho = dir_dia / nome_arq
                if caminho.exists():
                    return caminho
    return None

HEADER_TP_RE = re.compile(r"titulos[\s_]*publicos", flags=re.I)

def _find_tp_header_row(raw: pd.DataFrame) -> int | None:
    col0 = raw.iloc[:, 0].astype(str).map(unidecode).str.strip()
    for i, v in enumerate(col0):
        if HEADER_TP_RE.fullmatch(v): return i
    return None

def _le_cupom(v, titulo: str) -> float | np.nan:
    if pd.isna(v): return np.nan
    try: x = float(str(v).replace(",", "."))
    except Exception: return np.nan
    # regra simples: trate como % a.a. → fração
    return x / 100.0

def parse_tp_taxas_isin(path: Path, fund: str, dt_ref: pd.Timestamp) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    lin_tp = _find_tp_header_row(raw)
    if lin_tp is None:
        return pd.DataFrame(columns=["isin","tax","data","fundo"])
    header = raw.iloc[lin_tp + 1].astype(str).str.strip()
    tp = raw.iloc[lin_tp + 2:].copy(); tp.columns = header
    col_isin = next((c for c in tp.columns if "isin" in str(c).lower()), None)
    if col_isin is None:
        return pd.DataFrame(columns=["isin","tax","data","fundo"])
    col_titulo = next((c for c in tp.columns if "titul" in str(c).lower()), None)
    col_coupon = next((c for c in tp.columns if "coupon" in str(c).lower()), None)
    tp = tp.dropna(subset=[col_isin]).copy()
    tp[col_isin] = tp[col_isin].astype(str).str.strip().str.upper()
    if col_coupon is not None:
        if col_titulo is None:
            col_titulo = "__TituloTemp__"; tp[col_titulo] = ""
        tp["tax"] = tp.apply(lambda r: _le_cupom(r[col_coupon], str(r[col_titulo])), axis=1)
    else:
        tp["tax"] = np.nan
    out = (tp.loc[:, [col_isin, "tax"]]
             .rename(columns={col_isin: "isin"})
             .dropna(subset=["isin","tax"])
             .drop_duplicates(subset=["isin"], keep="last")
             .assign(data=pd.to_datetime(dt_ref).normalize(), fundo=fund)
             .loc[:, ["isin","tax","data","fundo"]])
    return out

def ultimo_dia_disponivel(fundo: str,
                          d_ini: str = "2024-01-01",
                          d_fim: str | None = None) -> tuple[pd.Timestamp | None, Path | None]:
    if d_fim is None: d_fim = date.today().isoformat()
    sched = CAL_B3.schedule(d_ini, d_fim).index[::-1]
    for dt in sched:
        p = _path_fund(fundo, dt)
        if p is not None: return pd.Timestamp(dt), p
    return None, None

def taxas_isin_no_ultimo_dia(fundo: str,
                             d_ini: str = "2024-01-01",
                             d_fim: str | None = None) -> tuple[pd.Timestamp | None, pd.Series]:
    dt_ult, path = ultimo_dia_disponivel(fundo, d_ini, d_fim)
    if path is None: return None, pd.Series(dtype=float)
    df = parse_tp_taxas_isin(path, fundo, dt_ult)
    if df.empty: return dt_ult, pd.Series(dtype=float)
    return dt_ult, df.set_index("isin")["tax"]

# ========================= Parte B — Matriz (rating / YMF) =========================
def carrega_mapa_bancos(arquivo: Path) -> pd.DataFrame:
    base = pd.read_excel(arquivo, sheet_name=0)
    ren = {}
    for c in base.columns:
        cn = _norm(c)
        if "BANC" in cn:   ren[c] = "Banco"
        elif "YMF" in cn:  ren[c] = "YMF"
        elif "RATING" in cn: ren[c] = "Rating"
    base = base.rename(columns=ren)
    if "Banco" not in base.columns:
        raise ValueError("Sheet 0 precisa ter coluna de Banco.")
    base["Banco"] = base["Banco"].astype(str).str.strip()
    base["Banco_norm"] = base["Banco"].map(_norm)
    base["Rating"] = pd.to_numeric(base.get("Rating"), errors="coerce")
    base["YMF"] = base.get("YMF", np.nan).astype(str).str.strip()
    base["YMF_norm"] = base["YMF"].map(_norm)
    return base[["Banco","Banco_norm","Rating","YMF","YMF_norm"]].dropna(subset=["Banco"])

KNOWN_MAP = {
    "BANCO ABC BRASIL S.A.": "Banco ABC",
    "BANCO BRADESCO S.A.": "Bradesco",
    "BANCO INTER SA": "Banco Intermedium",
    "BANCO MERCANTIL DO BRASIL S.A.": "Banco Mercantil do Brasil",
    "BANCO BMG S.A.": "Banco BMG",
    "BANCO DO NORDESTE DO BRASIL S.A.": "Banco do Nordeste do Brasil",
    "BANCO BTG PACTUAL S.A.": "Banco BTG Pactual",
    "BANCO SAFRA S.A.": "Banco Safra",
    "BANCO VOLKSWAGEN S/A": "Banco Volkswagen",
    "PARANA BANCO S.A.": "Paraná Banco",
    "BANCO RANDON": "Banco Randon",
    "BANCO XP" : "Xp investimentos"
}
STOPWORDS = {"S","SA","S.A","BANCO","DO","DA","DE","DEL","BRASIL"}

def emissor_para_banco(emissor: str, mapa_bancos: pd.DataFrame) -> str | None:
    if not isinstance(emissor, str) or not emissor.strip(): return None
    if emissor in KNOWN_MAP: return KNOWN_MAP[emissor]
    em_norm = _norm(emissor)
    cand = mapa_bancos.loc[mapa_bancos["Banco_norm"].eq(em_norm), "Banco"]
    if not cand.empty: return cand.iloc[0]
    toks = [t for t in re.sub(r"[^\w\s]", " ", em_norm).split() if t and t not in STOPWORDS]
    if toks:
        mask = mapa_bancos["Banco_norm"].apply(lambda s: all(tok in s for tok in toks))
        cand = mapa_bancos.loc[mask, "Banco"]
        if not cand.empty: return cand.iloc[0]
    return None

def escolhe_sheet(sub_classe: str) -> str | None:
    s = _norm(sub_classe)
    if not s: return None
    if "DEBENT" in s: return None              # sem matriz
    if "TITULO PRIVADO" in s or "TITULOS PRIVADOS" in s: return "LFSC PERCENTUAL"
    if "LFSC" in s:  return "LFSC PERCENTUAL"
    if "LFSN" in s:  return "LFSN PERCENTUAL"
    if re.search(r"\bLFS\b", s): return "LFS PERCENTUAL"
    if "LETRA FINANCEIRA" in s or re.search(r"\bLF\b", s): return "LF PERCENTUAL"
    if "CDB" in s: return "CDB PERCENTUAL"
    if re.search(r"\bLC\b", s): return "LC PERCENTUAL"
    return None

def _detectar_du_col(raw: pd.DataFrame) -> tuple[int, int]:
    max_check_cols = min(5, raw.shape[1])
    conv = raw.iloc[:, :max_check_cols].applymap(_num)
    melhor_col = 0; melhor_score = -1.0; melhor_start = 0
    for j in range(conv.shape[1]):
        col = conv.iloc[:, j]; notna = col.notna()
        if not notna.any(): continue
        vals = col[notna]
        ints = np.isclose(vals.values, np.round(vals.values), atol=1e-6)
        share_ints = float(ints.mean()) if len(vals) else 0.0
        score = share_ints * (len(vals) / len(col))
        start_idx = int(vals.index.min())
        if score > melhor_score:
            melhor_score = score; melhor_col = j; melhor_start = start_idx
    header_row = max(0, melhor_start - 1)
    return melhor_col, header_row

def carrega_matriz_produto(arquivo: Path, sheet: str) -> tuple[pd.DataFrame, str]:
    raw = pd.read_excel(arquivo, sheet_name=sheet, header=None, dtype=str)
    raw = raw.dropna(how="all").reset_index(drop=True)
    try:
        du_col_idx, header_row = _detectar_du_col(raw)
    except Exception:
        header_row = 0; du_col_idx = 0
    cols = raw.iloc[header_row].fillna("").astype(str).str.strip().tolist()
    body = raw.iloc[header_row+1:].copy(); body.columns = cols
    du_col = body.columns[du_col_idx if du_col_idx < len(body.columns) else 0]
    body = body.dropna(how="all")
    if str(du_col).strip() != "": body = body.dropna(subset=[du_col])
    body[du_col] = body[du_col].map(_num)
    body = body.dropna(subset=[du_col]); body[du_col] = body[du_col].astype(int)
    val_cols = [c for c in body.columns if c != du_col and str(c).strip() != ""]
    for c in val_cols: body[c] = body[c].map(_num)
    mat = body.set_index(du_col)

    modo = "rating"
    new_cols = {}; ints_ok = []
    for c in mat.columns:
        try:
            new_cols[c] = int(float(str(c).replace(",", "."))); ints_ok.append(True)
        except Exception:
            new_cols[c] = c; ints_ok.append(False)
    if all(ints_ok) and len(ints_ok) > 0:
        mat = mat.rename(columns=new_cols)
        int_cols = [c for c in mat.columns if isinstance(c, int)]
        mat = mat[int_cols].sort_index(axis=1)
        modo = "rating"
    else:
        mat.columns = [_norm(c) for c in mat.columns]; modo = "ymf"

    mat = mat[~mat.index.duplicated(keep="first")].sort_index()
    return mat, modo

def taxa_por_du_rating(matriz: pd.DataFrame, du: int, rating: int):
    if matriz.empty or pd.isna(du) or pd.isna(rating):
        return None, None, None, None
    du = int(max(0, du)); rating = int(rating)
    dus = matriz.index.to_numpy(); ge = dus[dus >= du]
    du_escolhido = int(ge.min()) if ge.size else int(dus.max())
    cols = [c for c in matriz.columns if isinstance(c, int)]
    col_escolhida = rating if rating in cols else min(cols, key=lambda x: abs(x - rating))
    raw = matriz.at[du_escolhido, col_escolhida]
    if pd.isna(raw): return None, None, du_escolhido, col_escolhida
    raw = float(raw); dec = raw / 100.0
    return raw, dec, du_escolhido, col_escolhida

def taxa_por_du_ymf(matriz: pd.DataFrame, du: int, ymf_col: str):
    if matriz.empty or pd.isna(du) or not ymf_col:
        return None, None, None, None
    du = int(max(0, du))
    dus = matriz.index.to_numpy(); ge = dus[dus >= du]
    du_escolhido = int(ge.min()) if ge.size else int(dus.max())
    ymf_norm = _norm(ymf_col); headers = list(matriz.columns)
    if ymf_norm not in headers:
        return None, None, du_escolhido, None
    raw = matriz.at[du_escolhido, ymf_norm]
    if pd.isna(raw): return None, None, du_escolhido, ymf_norm
    raw = float(raw); dec = raw / 100.0
    return raw, dec, du_escolhido, ymf_norm

def escolher_ymf_para_banco(banco: str | None, mapa_bancos: pd.DataFrame, cols_ymf_sheet: list[str]) -> str | None:
    if not banco or not cols_ymf_sheet: return None
    banco_norm = _norm(banco)
    ymfs_do_banco = mapa_bancos.loc[mapa_bancos["Banco_norm"] == banco_norm, "YMF_norm"].dropna().unique().tolist()
    ymfs_do_banco = [y for y in ymfs_do_banco if y]
    headers_norm = [_norm(c) for c in cols_ymf_sheet]
    for y in ymfs_do_banco:
        if y in headers_norm: return y
    return None

def du_b3(inicio: pd.Timestamp, fim: pd.Timestamp) -> int | float:
    if pd.isna(fim): return np.nan
    inicio = pd.Timestamp(inicio).normalize()
    fim    = pd.Timestamp(fim).normalize()
    if fim <= inicio: return 0
    sched = CAL_B3.schedule(start_date=inicio, end_date=fim)
    dias = pd.DatetimeIndex(sched.index).normalize()
    return int(((dias > inicio) & (dias <= fim)).sum())

# ========================= Chave p/ propagar taxa =========================
def parse_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns: return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[col], errors="coerce", dayfirst=True)

def build_key(df: pd.DataFrame) -> pd.Series:
    sc  = df.get("Sub Classe","").map(norm_text)
    emi = df.get("Emissor","").map(norm_text)
    ati = df.get("Ativo","").map(norm_text)
    ven = parse_date_col(df, "Vencimento_final").dt.date.astype(str)
    return sc + " | " + emi + " | " + ati + " | " + ven

# ========================= MAIN =========================
def main():
    if not CONTROLE_XLSX.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CONTROLE_XLSX}")
    base = pd.read_excel(CONTROLE_XLSX, dtype=str).fillna("")
    exigidas = ["Sub Classe","Emissor","Fundo","Ativo","ISIN","COD_XP","Vencimento_final"]
    fal = [c for c in exigidas if c not in base.columns]
    if fal: raise ValueError(f"Colunas ausentes no controle: {fal}")

    # --- A) TAXA_ISIN (somente fundos BTG) ---
    dados = base.copy()
    # aplica mapeamento para nome canônico de arquivo/pasta
    dados["_Fundo_can"] = dados["Fundo"].map(canon_fund_name)
    dados["_ISIN_up"]   = dados["ISIN"].astype(str).str.strip().str.upper()

    # filtra apenas fundos BTG (XP ficam de fora)
    pares_btg = (dados.loc[dados["Fundo"].map(is_btg_fund), ["_Fundo_can","_ISIN_up"]]
                      .rename(columns={"_Fundo_can":"Fundo_can","_ISIN_up":"ISIN_up"}))
    pares_btg = pares_btg[(pares_btg["Fundo_can"]!="") & (pares_btg["ISIN_up"]!="")].drop_duplicates()

    if DEBUG:
        dbg(f"[MAPA FUNDOS] exemplos de mapeamento:")
        ex = (dados.loc[:, ["Fundo","_Fundo_can"]].drop_duplicates().head(10))
        try:
            print(ex.to_string(index=False))
        except Exception:
            pass

    taxa_isin_rows = []
    if not pares_btg.empty:
        ano = date.today().year; d_ini = f"{ano}-01-01"; d_fim = SPECIFIC_DATE.isoformat()
        for fundo in sorted(pares_btg["Fundo_can"].unique()):
            dbg(f"\n[BTG] Procurando último dia de '{fundo}'...")
            try:
                dt_ult, mapa = taxas_isin_no_ultimo_dia(fundo, d_ini, d_fim)
                if dt_ult is None:
                    dbg(f"  ✗ Nenhum arquivo encontrado p/ {fundo}.")
                    subset = pares_btg[pares_btg["Fundo_can"]==fundo]
                    for isin in subset["ISIN_up"]:
                        taxa_isin_rows.append({"Fundo_can":fundo,"ISIN_up":isin,"TAXA_ISIN":np.nan})
                    continue
                dbg(f"  ✓ {dt_ult:%Y-%m-%d} | ISINs com taxa: {mapa.notna().sum()}")
                subset = pares_btg[pares_btg["Fundo_can"]==fundo]
                for isin in subset["ISIN_up"]:
                    taxa = float(mapa.get(isin, np.nan)) if not mapa.empty else np.nan
                    taxa_isin_rows.append({"Fundo_can":fundo,"ISIN_up":isin,"TAXA_ISIN":taxa})
            except Exception as e:
                dbg(f"  ✗ Erro em {fundo}: {e}")
    df_taxa_isin = pd.DataFrame(taxa_isin_rows) if taxa_isin_rows else pd.DataFrame(columns=["Fundo_can","ISIN_up","TAXA_ISIN"])

    if not df_taxa_isin.empty:
        dados = dados.merge(df_taxa_isin, left_on=["_Fundo_can","_ISIN_up"], right_on=["Fundo_can","ISIN_up"], how="left")
        dados.drop(columns=["Fundo_can","ISIN_up"], inplace=True, errors="ignore")
    else:
        dados["TAXA_ISIN"] = np.nan

    # --- B) Taxa_matriz (rating/YMF) ---
    dados = dados.copy()
    dados["Vencimento do ativo"] = dados["Vencimento_final"]  # renome interno
    mapa_bancos = carrega_mapa_bancos(MATRIZ_XLSX)

    cache_matrizes: dict[str, dict] = {}
    hoje = pd.Timestamp(date.today())

    dados["Banco_matriz"]  = None
    dados["Rating"]        = np.nan
    dados["DU_B3"]         = np.nan
    dados["Produto_sheet"] = None
    dados["Taxa_matriz"]   = np.nan
    dados["Modo_matriz"]   = None
    dados["Coluna_usada"]  = None

    dados["Sub Classe"] = dados.apply(_infer_subclasse, axis=1)


    for idx, row in dados.iterrows():
        subc = row.get("Sub Classe","")
        emissor = row.get("Emissor","")
        venc = pd.to_datetime(row.get("Vencimento do ativo"), dayfirst=True, errors="coerce")
        du = du_b3(hoje, venc) if not pd.isna(venc) else np.nan
        sheet = escolhe_sheet(subc)
        if sheet is None:
            # tenta uma escolha básica por palavra-chave
            s = norm_text(subc)
            if "CDB" in s: sheet = "CDB PERCENTUAL"
            elif "LFSC" in s: sheet = "LFSC PERCENTUAL"
            elif "LFSN" in s: sheet = "LFSN PERCENTUAL"
            elif re.search(r"\bLF\b", s): sheet = "LF PERCENTUAL"
        banco = emissor_para_banco(emissor, mapa_bancos)

        rating = np.nan
        if banco:
            banco_norm = _norm(banco)
            r = mapa_bancos.loc[mapa_bancos["Banco_norm"]==banco_norm, "Rating"]
            rating = float(r.iloc[0]) if not r.empty and pd.notna(r.iloc[0]) else np.nan

        dados.at[idx,"DU_B3"] = du
        dados.at[idx,"Banco_matriz"] = banco
        dados.at[idx,"Rating"] = rating
        dados.at[idx,"Produto_sheet"] = sheet

        raw_tax = dec_tax = np.nan; col_usada = None; modo_mat = None
        if sheet and pd.notna(du):
            if sheet not in cache_matrizes:
                try:
                    mat, modo = carrega_matriz_produto(MATRIZ_XLSX, sheet)
                except Exception as e:
                    dbg(f"[WARN] lendo sheet '{sheet}': {e}")
                    mat, modo = pd.DataFrame(), "rating"
                cache_matrizes[sheet] = {"mat":mat, "modo":modo}
            mat = cache_matrizes[sheet]["mat"]; modo = cache_matrizes[sheet]["modo"]; modo_mat = modo

            if modo == "rating":
                if pd.notna(rating):
                    raw_tax, dec_tax, du_sel, col_sel = taxa_por_du_rating(mat, int(du), int(rating))
                    col_usada = f"RATING={col_sel}"
            else:
                ymf = escolher_ymf_para_banco(banco, mapa_bancos, list(mat.columns))
                if ymf:
                    raw_tax, dec_tax, du_sel, ymf_sel = taxa_por_du_ymf(mat, int(du), ymf)
                    col_usada = f"YMF={ymf_sel}"

        dados.at[idx,"Taxa_matriz"]  = dec_tax
        dados.at[idx,"Coluna_usada"] = col_usada
        dados.at[idx,"Modo_matriz"]  = modo_mat

    # --- C) Prioridade e Propagação ---
    dados["TAXA_ISIN"]   = pd.to_numeric(dados.get("TAXA_ISIN"), errors="coerce")
    dados["Taxa_matriz"] = pd.to_numeric(dados.get("Taxa_matriz"), errors="coerce")

    # prioridade: TAXA_ISIN > Taxa_matriz
    dados["TAXA_EFETIVA"] = dados["TAXA_ISIN"].where(dados["TAXA_ISIN"].notna(), dados["Taxa_matriz"])
    dados["origem_taxa"] = np.where(dados["TAXA_ISIN"].notna(), "ISIN",
                              np.where(dados["Taxa_matriz"].notna(), "MATRIZ", "NA"))

    # Propagar taxa entre fundos para o mesmo ativo (mesma chave sem "Fundo")
    dados["KEY"] = build_key(dados)
    grp = dados.groupby("KEY", dropna=False)["TAXA_EFETIVA"].apply(lambda s: s.dropna().iloc[0] if s.dropna().size else np.nan)
    dados["TAXA_PROPAGADA"] = dados["KEY"].map(grp)
    precisa_propag = dados["TAXA_EFETIVA"].isna() & dados["TAXA_PROPAGADA"].notna()
    dados.loc[precisa_propag, "TAXA_EFETIVA"] = dados.loc[precisa_propag, "TAXA_PROPAGADA"]
    dados["sanity_propagado"] = precisa_propag

    # --- D) Avisos e saídas ---
    sem_taxa = dados[dados["TAXA_EFETIVA"].isna()].copy()

    # salvar arquivo final (uma coluna de taxa efetiva + colunas úteis)
    cols_out = [c for c in [
        "Sub Classe","Emissor","Fundo","Ativo","ISIN","COD_XP","Vencimento_final",
        "TAXA_EFETIVA","origem_taxa","sanity_propagado"
    ] if c in dados.columns]
    OUT_FINAL_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_FINAL_XLSX, engine="openpyxl") as w:
        dados[cols_out].to_excel(w, index=False, sheet_name="taxa_efetiva")

    if not sem_taxa.empty:
        sem_cols = [c for c in ["Sub Classe","Emissor","Fundo","Ativo","ISIN","COD_XP","Vencimento_final"] if c in sem_taxa.columns]
        sem_taxa[sem_cols].to_excel(OUT_SEM_TAXA_XLSX, index=False)
        print("\n⚠️ ATENÇÃO: Existem ativos SEM TAXA após todas as tentativas.")
        print(f"  → Listagem salva em: {OUT_SEM_TAXA_XLSX.resolve()}")
        try:
            print(sem_taxa[sem_cols].head(15).to_string(index=False))
        except Exception:
            pass
    else:
        print("\n✅ Todos os ativos ficaram com TAXA_EFETIVA.")

    # mini-log
    total = len(dados)
    n_isin   = int(dados["origem_taxa"].eq("ISIN").sum())
    n_matriz = int(dados["origem_taxa"].eq("MATRIZ").sum())
    n_prop   = int(dados["sanity_propagado"].sum())
    n_sem    = int(dados["TAXA_EFETIVA"].isna().sum())
    print("\n[RESUMO]")
    print(f" - Linhas totais:          {total:,}")
    print(f" - Por ISIN (BTG):         {n_isin:,}")
    print(f" - Por Matriz (fallback):  {n_matriz:,}")
    print(f" - Propagadas entre fundos:{n_prop:,}")
    print(f" - Sem taxa:               {n_sem:,}")
    print(f"\n📄 Arquivo final: {OUT_FINAL_XLSX.resolve()}")

if __name__ == "__main__":
    main()
