# main_df_filtrado.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import os
import re
import unicodedata
import pandas as pd
import numpy as np

# ---------------- CONFIG ----------------
PDF_DIR   = Path("Dados/saida_pdf_cetip_consolidado")
PDF_CSV   = PDF_DIR / "consolidado_pdfs_codativos.csv"
PDF_XLSX  = PDF_DIR / "consolidado_pdfs_codativos.xlsx"
REL_GLOB  = "Dados/Relatório de Posição 2025-10-31.xlsx"
EXC_PATTS = ["Dados/Tratamento Exceções.xlsx"]

OUT_DIR   = Path("Dados")
OUT_DIR.mkdir(exist_ok=True)
OUT_MAIN          = OUT_DIR / "main_df_filtrado.xlsx"
OUT_CONT_NOVO     = OUT_DIR / "Ativos_Sem_Codigo.xlsx"   # apenas ENTRADAS sem ISIN/COD_XP
OUT_DEPURACAO_XLS = OUT_DIR / "depuracao_entradas.xlsx"

# planilha antiga (com ISIN e COD_XP já preenchidos manualmente)
MAP_ANTIGO = OUT_DIR / "ativos_mapeados_para_controle.xlsx"

# ---------------- DEBUG ----------------
DEBUG = os.getenv("HEDGE_DEBUG", "1").strip().lower() not in {"0","false","no"}
DEBUG_DIR = OUT_DIR / "debug"
if DEBUG:
    DEBUG_DIR.mkdir(exist_ok=True)

def _dbg(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")

def _strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out

def _dump_csv(df: pd.DataFrame, name: str, cols: Optional[List[str]] = None):
    if not DEBUG: return
    try:
        path = DEBUG_DIR / f"{name}.csv"
        if cols:
            cols = [c for c in cols if c in df.columns]
            df.loc[:, cols].to_csv(path, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(path, index=False, encoding="utf-8-sig")
        _dbg(f"DUMP salvo: {path.as_posix()} (linhas={len(df):,})")
    except Exception as e:
        _dbg(f"Falha ao salvar dump {name}.csv: {e}")

def _dump_excel_multi(sheets: Dict[str, pd.DataFrame], name: str):
    if not DEBUG: return
    path = DEBUG_DIR / name
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            for sheet, df in sheets.items():
                df = df if df is not None and not df.empty else pd.DataFrame({"info": ["(vazio)"]})
                df.to_excel(w, index=False, sheet_name=sheet[:31] or "Sheet")
        _dbg(f"Excel de diagnóstico salvo: {path.as_posix()}")
    except Exception as e:
        _dbg(f"Falha ao salvar {path.name}: {e}")

# -------------- utils básicos --------------
def strip_accents(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_code(s: str) -> str:
    s = strip_accents(str(s)).upper()
    return re.sub(r"[^A-Z0-9]", "", s or "")

def code_root(c: str) -> str:
    if not c: return ""
    return c[:-1] if c[-1].isalpha() else c

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", strip_accents(s or "")).strip().upper()

def read_any(path: Path) -> pd.DataFrame:
    if path is None or not path.exists(): return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    return pd.read_excel(path, dtype=str)

def find_relatorio_path() -> Optional[Path]:
    files = sorted(Path(".").glob(REL_GLOB))
    return files[0] if files else None

def find_exceptions_path() -> Optional[Path]:
    for pat in EXC_PATTS:
        files = sorted(Path(".").glob(pat))
        if files:
            return files[0]
    return None

def _parse_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[col], errors="coerce", dayfirst=True)

# ----------- extra: extrator de código para qualquer DF -----------
CODE_REGEXES = [re.compile(r"[A-Z]{2,6}\d{3,12}[A-Z0-9]{0,4}")]
def extract_code_from_free_text(s: str) -> Optional[str]:
    if not s: return None
    t = strip_accents(str(s)).upper()
    for rgx in CODE_REGEXES:
        m = rgx.search(t)
        if m: return m.group(0)
    cand = norm_code(t)
    return cand if 6 <= len(cand) <= 20 else None

# ----------- KEYS -----------
def _build_key_strict(df: pd.DataFrame) -> pd.Series:
    """
    Chave ESTRITA: Sub Classe + Ativo + Emissor + Fundo + Vencimento_final (data)
    """
    sc   = df.get("Sub Classe", "").map(norm_text)
    ati  = df.get("Ativo", "").map(norm_text)
    emi  = df.get("Emissor", "").map(norm_text)
    fun  = df.get("Fundo", "").map(norm_text)
    venD = _parse_date_col(df, "Vencimento_final").dt.date.astype(str)
    return sc + " | " + ati + " | " + emi + " | " + fun + " | " + venD

# -------------- parsing bases --------------
def prepare_pdf_base(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.copy()
    df = _strip_cols(df)

    ren = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl == "cod_ativo": ren[c] = "cod_Ativo"
        if cl == "data emissao": ren[c] = "Data Emissão"
        if cl == "data proximo juros": ren[c] = "Data Proximo Juros"
    if ren: df = df.rename(columns=ren)

    need_cols = [
        "cod_Ativo","FormaCDI","pct_flutuante","spread",
        "Data Emissão","vencimento","TipoJuros","CicloJuros",
        "taxa_emissão","Data Proximo Juros","Emissor","TIPO","PU_emissão",
        "leitura_ok","Data Call Inicial"
    ]
    for c in need_cols:
        if c not in df.columns: df[c] = None

    df["cod_Ativo_norm"] = df["cod_Ativo"].map(norm_code)
    df["cod_root"] = df["cod_Ativo_norm"].map(code_root)

    for dc in ["Data Emissão","vencimento","Data Proximo Juros","DataRef","Data Call Inicial"]:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce", dayfirst=True)

    def _formacdi(x: str) -> str:
        t = (x or "").strip().upper().replace(" ", "")
        if "CDI+" in t: return "CDI+"
        if "%CDI" in t or t == "PCTCDI": return "%CDI"
        if "IPCA+" in t: return "IPCA+"
        if t in {"IPCA","IPC A"}: return "IPCA"
        if t in {"%IPCA","PCTIPCA"}: return "%IPCA"
        if t in {"PREFIXADO","PRE","PREFIXADA"}: return "PREFIXADO"
        return t or "UNKNOWN"
    df["FormaCDI"] = df["FormaCDI"].map(_formacdi)

    for c in ["pct_flutuante","spread","taxa_emissão","PU_emissão"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["leitura_ok"] = df["leitura_ok"].astype(str).str.strip().str.lower().isin(["true","1","yes","y"])

    if DEBUG:
        _dbg("prepare_pdf_base(): colunas → " + ", ".join(df.columns))
        _dump_csv(df.head(50), "pdf_base_head")

    return df

def prepare_carteira(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.copy()
    df = _strip_cols(df)

    mapping = {
        "Data": ["Data","data","DataRef","Data Ref"],
        "Sub Classe": ["Sub Classe","Sub-Classe","Classe","SubClasse"],
        "Emissor": ["Emissor","Nome do Emissor"],
        "Fundo": ["Fundo","Nome do Fundo"],
        "Ativo": ["Ativo","Código Ativo","Codigo Ativo"],
        "Estratégia": ["Estratégia","Estrategia","Estrategia/Indexador"],
        "Vencimento do ativo": ["Vencimento do ativo","Vencimento","Vencimento Ativo"],
        "Quantidade": ["Quantidade","Qtde","Qtd"],
        "Pu Posição": ["Pu Posição","PU Posição","PU Posicao","PU Posição","PU"],
        "Valor": ["Valor","Valor Posição","ValorPosicao"],
        "% PL": ["% PL","%PL","Percentual PL"],
    }
    ren = {}
    for tgt, aliases in mapping.items():
        for a in aliases:
            if a in df.columns:
                ren[a] = tgt
                break
    if ren: df = df.rename(columns=ren)

    df["cod_Ativo_guess"] = df.get("Ativo","").map(extract_code_from_free_text)
    df["cod_Ativo_guess_norm"] = df["cod_Ativo_guess"].map(lambda x: norm_code(x) if pd.notna(x) else "")
    df["cod_root_guess"] = df["cod_Ativo_guess_norm"].map(code_root)

    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    if "Vencimento do ativo" in df.columns:
        df["Vencimento do ativo"] = pd.to_datetime(df["Vencimento do ativo"], errors="coerce", dayfirst=True)

    df["estrategia_percent_cdi"] = df.get("Estratégia","").astype(str).str.contains("%CDI", case=False, na=False)

    if DEBUG:
        n_total = len(df)
        n_fundo = df.get("Fundo","").astype(str).str.strip().ne("").sum()
        _dbg(f"prepare_carteira(): linhas={n_total:,}, com Fundo não-vazio={n_fundo:,}")
        _dump_csv(df[["Ativo","cod_Ativo_guess","cod_Ativo_guess_norm","Fundo"]], "carteira_fundos")

    return df

# -------------- exceções (opcional) --------------
def overlay_exceptions_into_pdf_base(pdf_base: pd.DataFrame, exc: pd.DataFrame) -> pd.DataFrame:
    if pdf_base.empty or exc.empty: return pdf_base
    pb = pdf_base.copy()
    if "cod_Ativo_norm" not in pb.columns:
        pb["cod_Ativo_norm"] = pb["cod_Ativo"].map(norm_code)

    exc = _strip_cols(exc.copy())
    ren = {
        "Código":"cod_Ativo",
        "pct_flutuante_final":"pct_flutuante",
        "CicloJuros_final":"CicloJuros",
        "Data_Prox_Juros":"Data Proximo Juros",
        "Data_Emissao":"Data Emissão",
        "Vencimento_final":"vencimento",
        "Data Call Inicial":"Data Call Inicial",
        "TIPO":"FormaCDI",
    }
    for k,v in list(ren.items()):
        if k in exc.columns: exc = exc.rename(columns={k:v})
    if "cod_Ativo" not in exc.columns: exc["cod_Ativo"] = None
    exc["cod_Ativo_norm"] = exc["cod_Ativo"].map(norm_code)
    for dc in ["Data Emissão","vencimento","Data Proximo Juros","Data Call Inicial"]:
        if dc in exc.columns:
            exc[dc] = pd.to_datetime(exc[dc], errors="coerce", dayfirst=True)
    for c in ["pct_flutuante","taxa_emissão","PU_emissão","spread"]:
        if c in exc.columns:
            exc[c] = pd.to_numeric(exc[c], errors="coerce")

    pb_indexed  = pb.set_index("cod_Ativo_norm", drop=False)
    exc_indexed = exc.set_index("cod_Ativo_norm", drop=False)
    overlay_cols = ["FormaCDI","pct_flutuante","CicloJuros","Data Proximo Juros",
                    "Data Emissão","vencimento","Data Call Inicial","TipoJuros","PU_emissão","spread","taxa_emissão"]
    for code, row in exc_indexed.iterrows():
        if code in pb_indexed.index:
            for col in overlay_cols:
                if col in row.index and pd.notna(row[col]) and str(row[col]) != "":
                    pb_indexed.loc[code, col] = row[col]
            if pd.notna(row.get("cod_Ativo","")) and str(row["cod_Ativo"]).strip():
                pb_indexed.loc[code, "cod_Ativo"] = row["cod_Ativo"]
        else:
            newrow = {c: None for c in pb.columns}
            newrow["cod_Ativo"] = row.get("cod_Ativo")
            newrow["cod_Ativo_norm"] = code
            for col in overlay_cols:
                if col in row.index:
                    newrow[col] = row[col]
            newrow["leitura_ok"] = False if "leitura_ok" in pb.columns else None
            pb_indexed = pd.concat([pb_indexed,
                                    pd.DataFrame([newrow]).set_index("cod_Ativo_norm", drop=False)])
    out = pb_indexed.reset_index(drop=True)

    if DEBUG:
        _dbg("overlay_exceptions_into_pdf_base(): exceções aplicadas.")
        _dump_csv(exc, "excecoes_normalizadas")
        _dump_csv(out.head(50), "pdf_base_pos_excecoes_head")

    return out

# -------------- agregação por código + matching --------------
def build_pdf_code_index(pdf: pd.DataFrame):
    if pdf.empty: return pd.DataFrame(), {}
    def first_valid(s: pd.Series):
        x = s.dropna()
        return x.iloc[0] if not x.empty else None

    PRIO = ["%CDI","CDI+","IPCA+","IPCA","%IPCA","PREFIXADO"]
    def pick_forma(series: pd.Series) -> str:
        vals = [v for v in series.dropna().astype(str) if v]
        if not vals: return "UNKNOWN"
        s = set(vals)
        for k in PRIO:
            if k in s: return k
        return list(s)[0]

    g = pdf.groupby("cod_Ativo_norm", dropna=False)
    out = g.apply(lambda d: pd.Series({
        "cod_root": first_valid(d["cod_root"]),
        "FormaCDI_final": pick_forma(d["FormaCDI"]),
        "pct_flutuante_final": first_valid(d["pct_flutuante"]),
        "spread_final": first_valid(d["spread"]),
        "TipoJuros_final": first_valid(d["TipoJuros"]),
        "CicloJuros_final": first_valid(d["CicloJuros"]),
        "Data_Prox_Juros_final": first_valid(d["Data Proximo Juros"]),
        "Data_Emissao_final": first_valid(d["Data Emissão"]),
        "Vencimento_final": first_valid(d["vencimento"]),
        "taxa_emissao_final": first_valid(d["taxa_emissão"]),
        "PU_emissao_final": first_valid(d["PU_emissão"]),
        "TIPO_final": first_valid(d["TIPO"]),
        "Emissor_pdf_ref": first_valid(d["Emissor"]),
        "Data_Call_Inicial_final": (
            d["Data Call Inicial"].dropna().min() if "Data Call Inicial" in d else pd.NaT
        )
    })).reset_index(names="cod_Ativo_norm")

    root_best = out.dropna(subset=["cod_root"]).drop_duplicates("cod_root")
    root_map = dict(zip(root_best["cod_root"], root_best["cod_Ativo_norm"]))

    if DEBUG:
        _dbg(f"build_pdf_code_index(): códigos agregados={len(out):,}")
        _dump_csv(out.head(100), "pdf_idx_head")

    return out, root_map

def best_code_match(code: str, pdf_codes: List[str], root_map: Dict[str,str]) -> Tuple[Optional[str], str]:
    if not code: return None, "empty_code"
    if code in pdf_codes: return code, "exact"
    r = code_root(code)
    if r and r in root_map: return root_map[r], "root"
    cand = [c for c in pdf_codes if (c.startswith(code) or code.startswith(c)) and abs(len(c)-len(code))<=1]
    cand = list(dict.fromkeys(cand))
    if len(cand) == 1: return cand[0], "prefix/suffix±1"
    if len(cand) > 1: return max(cand, key=len), "prefix/suffix±1_longest"
    return None, "no_match"

def cruzar(carteira: pd.DataFrame, pdf_base: pd.DataFrame) -> pd.DataFrame:
    if carteira.empty or pdf_base.empty: return pd.DataFrame()

    pdf_idx, root_map = build_pdf_code_index(pdf_base)
    pdf_codes = pdf_idx["cod_Ativo_norm"].tolist()

    match_code, match_how = [], []
    for c in carteira["cod_Ativo_guess_norm"].fillna(""):
        m, how = best_code_match(c, pdf_codes, root_map)
        match_code.append(m); match_how.append(how)

    left = carteira.copy()
    left["cod_Ativo_norm_match"] = match_code
    left["match_method"] = match_how

    merged = left.merge(
        pdf_idx, how="left",
        left_on="cod_Ativo_norm_match", right_on="cod_Ativo_norm",
        suffixes=("", "_pdf"),
    )

    # ====== FUNDO: mantém o do Relatório; se vazio, tenta completar com o próprio relatório ======
    fmap_guess = (carteira.loc[:, ["cod_Ativo_guess_norm","Fundo"]].copy())
    fmap_guess["Fundo"] = fmap_guess["Fundo"].astype(str).str.strip()
    fmap_guess = fmap_guess[(fmap_guess["Fundo"].ne("")) & (fmap_guess["cod_Ativo_guess_norm"].ne(""))]
    map_guess = (fmap_guess
                 .drop_duplicates(subset=["cod_Ativo_guess_norm"])
                 .set_index("cod_Ativo_guess_norm")["Fundo"]
                 .to_dict())

    merged["Fundo_final"] = merged.get("Fundo", "").astype(str).str.strip()
    merged["Fundo_final"].replace({"": np.nan}, inplace=True)

    idx_empty1 = merged["Fundo_final"].isna()
    merged.loc[idx_empty1, "Fundo_final"] = merged.loc[idx_empty1, "cod_Ativo_guess_norm"].map(map_guess)

    map_match = (merged.loc[
                    merged["Fundo"].astype(str).str.strip().ne("") & merged["cod_Ativo_norm_match"].notna(),
                    ["cod_Ativo_norm_match","Fundo"]
                 ]
                 .drop_duplicates(subset=["cod_Ativo_norm_match"])
                 .set_index("cod_Ativo_norm_match")["Fundo"]
                 .to_dict())
    idx_empty2 = merged["Fundo_final"].isna()
    merged.loc[idx_empty2, "Fundo_final"] = merged.loc[idx_empty2, "cod_Ativo_norm_match"].map(map_match)

    merged["Fundo_final"] = merged["Fundo_final"].fillna("").astype(str)

    if DEBUG:
        n_lin = len(merged)
        n_ini = merged.get("Fundo","").astype(str).str.strip().ne("").sum()
        n_after_guess = merged.loc[idx_empty1, "Fundo_final"].astype(str).str.strip().ne("").sum()
        n_after_match = merged.loc[idx_empty2, "Fundo_final"].astype(str).str.strip().ne("").sum()
        n_final = merged["Fundo_final"].astype(str).str.strip().ne("").sum()
        _dbg(f"cruzar(): linhas={n_lin:,} | Fundo.ini={n_ini,} | "
             f"preenchidos_guess={n_after_guess:,} | preenchidos_match_extra={n_after_match:,} | "
             f"final_nao_vazio={n_final:,}")
        _dump_csv(merged[["Ativo","cod_Ativo_guess_norm","cod_Ativo_norm_match","Fundo","Fundo_final"]]
                  .head(200), "merged_fundo_cols_head")

    merged["Emissor_final"] = merged.get("Emissor", merged.get("Emissor_pdf_ref", ""))
    merged["match_pdf"] = merged["cod_Ativo_norm_match"].notna()
    return merged

# -------------- MAIN --------------
def main():
    # 1) base PDF consolidada
    if PDF_CSV.exists():
        base_pdf_raw = read_any(PDF_CSV)
    elif PDF_XLSX.exists():
        base_pdf_raw = read_any(PDF_XLSX)
    else:
        print("❌ Não encontrei a base consolidada em ./saida_pdf_cetip_consolidado/")
        return
    pdf_base = prepare_pdf_base(base_pdf_raw)

    # 2) exceções (opcional)
    exc_path = find_exceptions_path()
    if exc_path:
        exc_df = read_any(exc_path)
        pdf_base = overlay_exceptions_into_pdf_base(pdf_base, exc_df)

    # 3) relatório de posição
    rel_path = find_relatorio_path()
    if not rel_path:
        print("❌ Não encontrei `Relatório de Posição*.xlsx` no diretório atual.")
        return
    carteira_raw = read_any(rel_path)
    carteira = prepare_carteira(carteira_raw)

    # 4) cruzamento
    merged = cruzar(carteira, pdf_base)
    if merged.empty:
        print("❌ Cruzamento vazio (verifique entradas).")
        return

    # 5) FILTRO main_df (mesma lógica do dashboard)
    df = merged.copy()
    pct = pd.to_numeric(df.get("pct_flutuante_final"), errors="coerce")
    has_pct = pct.notna()
    keep_pct = has_pct & (~pct.isin([100.0, 1.0]))

    df["Data_Call_Inicial_final"] = _parse_date_col(df, "Data_Call_Inicial_final")
    df["Vencimento_final"]        = _parse_date_col(df, "Vencimento_final")
    df["Vencimento_final"] = df["Vencimento_final"].fillna(df["Data_Call_Inicial_final"])

    v_ok = df["Vencimento_final"].notna()
    c_ok = df["Data_Call_Inicial_final"].notna()
    has_dates = v_ok | c_ok

    main_df = df[keep_pct & has_dates].copy()

    # coluna “Código” (melhor disponível) — e GARANTE 'Fundo'
    code_col = (
        "cod_Ativo_norm_match" if "cod_Ativo_norm_match" in main_df.columns and main_df["cod_Ativo_norm_match"].notna().any()
        else ("cod_Ativo_norm" if "cod_Ativo_norm" in main_df.columns and main_df["cod_Ativo_norm"].notna().any()
              else ("cod_Ativo_guess_norm" if "cod_Ativo_guess_norm" in main_df.columns and main_df["cod_Ativo_guess_norm"].notna().any()
                    else "cod_Ativo_guess"))
    )
    if code_col in main_df.columns:
        main_df = main_df.rename(columns={code_col: "Código"})
    if "Fundo_final" in main_df.columns:
        main_df["Fundo"] = main_df["Fundo_final"].fillna("").astype(str).str.strip()

    # DEBUG principal
    if DEBUG:
        n_main = len(main_df)
        n_fundo_main = main_df.get("Fundo","").astype(str).str.strip().ne("").sum()
        _dbg(f"main_df: linhas={n_main:,}, Fundo não-vazio={n_fundo_main:,}")
        _dump_csv(main_df[["Ativo","Código","Fundo","Vencimento_final","Sub Classe","Emissor"]], "main_df_preview")

    # salva snapshot do main_df
    cols_show = [c for c in [
        "Data","Fundo","Emissor","Ativo","Código",
        "pct_flutuante_final","CicloJuros_final",
        "Data_Prox_Juros_final","Data_Emissao_final",
        "Vencimento_final","Sub Classe"
    ] if c in main_df.columns]
    #with pd.ExcelWriter(OUT_MAIN, engine="openpyxl") as w:
    #    main_df[cols_show].to_excel(w, index=False, sheet_name="main_df_filtrado")

    # ---------------- CHAVE ESTRITA para comparar com MAP_ANTIGO ----------------
    # recorte do main_df com as 5 colunas-chave (agora com Emissor no lugar de Código)
    strict_cols = ["Sub Classe","Ativo","Emissor","Fundo","Vencimento_final"]
    md_strict = main_df.loc[:, [c for c in strict_cols if c in main_df.columns]].drop_duplicates().copy()
    md_strict["Vencimento_final"] = _parse_date_col(md_strict, "Vencimento_final")
    md_strict["KEY_STRICT"] = _build_key_strict(md_strict)

    if DEBUG:
        _dbg(f"md_strict: chaves únicas={md_strict['KEY_STRICT'].nunique():,}")
        _dump_csv(md_strict, "md_strict_keys")

    # --------- carrega/normaliza o MAP_ANTIGO ----------
    antigo = read_any(MAP_ANTIGO)
    antigo = _strip_cols(antigo)
    if antigo.empty:
        base_cols = ["Sub Classe","Emissor","Ativo","Vencimento_final","Fundo","ISIN","COD_XP"]
        antigo = pd.DataFrame(columns=base_cols)

    # normaliza nomes
    ren_old = {}
    for c in antigo.columns:
        cl = c.strip().lower()
        if cl in {"vencimento","vencimento_final","vencimento do ativo"}: ren_old[c] = "Vencimento_final"
        if cl in {"sub classe","sub-classe","subclasse"}: ren_old[c] = "Sub Classe"
        if cl == "emissor": ren_old[c] = "Emissor"
        if cl == "ativo": ren_old[c] = "Ativo"
        if cl in {"cod_xp","código xp","codigo xp"}: ren_old[c] = "COD_XP"
        if cl in {"isin","codigo isin","código isin"}: ren_old[c] = "ISIN"
        if cl == "fundo": ren_old[c] = "Fundo"
    if ren_old: antigo = antigo.rename(columns=ren_old)

    for col in ["Sub Classe","Emissor","Ativo","Vencimento_final","Fundo","ISIN","COD_XP"]:
        if col not in antigo.columns: antigo[col] = None

    antigo["Vencimento_final"] = _parse_date_col(antigo, "Vencimento_final")
    antigo["KEY_STRICT"] = _build_key_strict(antigo)

    if DEBUG:
        n_antigo_total = len(antigo)
        n_antigo_fundo = antigo.get("Fundo","").astype(str).str.strip().ne("").sum()
        _dbg(f"MAP_ANTIGO: linhas={n_antigo_total:,}, Fundo não-vazio={n_antigo_fundo:,}, chaves={antigo['KEY_STRICT'].nunique():,}")
        _dump_csv(antigo, "mapa_antigo_normalizado")

    # ---------------- ENTRADAS novas (pela CHAVE ESTRITA) ----------------
    entrou_mask = ~md_strict["KEY_STRICT"].isin(antigo["KEY_STRICT"])
    entradas = md_strict[entrou_mask].copy()

    # linhas a adicionar ao mapa (mantendo Fundo do main_df)
    add_cols = ["Sub Classe","Emissor","Ativo","Vencimento_final","Fundo","ISIN","COD_XP"]
    to_add = entradas.loc[:, [c for c in add_cols if c in entradas.columns]].copy()
    for c in ["ISIN","COD_XP"]:
        if c not in to_add.columns:
            to_add[c] = None
    to_add = to_add[add_cols]
    to_add["KEY_STRICT"] = entradas["KEY_STRICT"]

    if DEBUG:
        _dbg(f"Novas entradas por KEY_STRICT: {len(to_add):,}")
        _dump_csv(to_add, "to_add_strict")

    # -------------------- incrementa o MAP_ANTIGO (SEM agrupar por chave que ignore FUNDO) --------------------
    if not to_add.empty:
        antigo_min = antigo.loc[:, [c for c in add_cols + ["KEY_STRICT"] if c in antigo.columns or c == "KEY_STRICT"]].copy()
        if antigo_min.empty:
            atual = to_add.copy()
        else:
            # mantém originais e acrescenta somente as novas chaves estritas
            atual = pd.concat([antigo_min, to_add], ignore_index=True)

        # Consolida por KEY_STRICT preservando primeiro preenchido (não derruba Fundo)
        agg_merge = {
            "Sub Classe":"first",
            "Emissor":"first",
            "Ativo":"first",
            "Vencimento_final":"first",
            "Fundo": lambda s: next((x for x in s if pd.notna(x) and str(x).strip()), None),
            "ISIN":  lambda s: next((x for x in s if pd.notna(x) and str(x).strip()), None),
            "COD_XP":lambda s: next((x for x in s if pd.notna(x) and str(x).strip()), None),
        }
        
        atual = (atual
                 .sort_values(["Sub Classe","Emissor","Ativo","Vencimento_final","Fundo"], na_position="last")
                 .groupby("KEY_STRICT", dropna=False)
                 .agg(agg_merge)
                 .reset_index(drop=True))
        atualizado = atual.loc[:, add_cols].copy()
    else:
        atualizado = antigo.loc[:, [c for c in add_cols if c in antigo.columns]].copy()
        for c in add_cols:
            if c not in atualizado.columns:
                atualizado[c] = None
        atualizado = atualizado[add_cols]

    # grava o arquivo INCREMENTADO (com a coluna 'Fundo' preservada)
    with pd.ExcelWriter(MAP_ANTIGO, engine="openpyxl") as w:
        atualizado.to_excel(w, index=False, sheet_name="mapa")

    # -------------- arquivos auxiliares (novos sem código + depuração) --------------
    # somente os novos (pela chave estrita) sem ISIN/COD_XP
    novos_sem_codigo = to_add.loc[to_add["ISIN"].isna() & to_add["COD_XP"].isna(),
                                  [c for c in add_cols if c != "Emissor"]]
    if not novos_sem_codigo.empty:
        novos_sem_codigo.to_excel(OUT_CONT_NOVO, index=False)
    #else:
    #    pd.DataFrame(columns=[c for c in add_cols if c != "Emissor"]).to_excel(OUT_CONT_NOVO, index=False)

    #with pd.ExcelWriter(OUT_DEPURACAO_XLS, engine="openpyxl") as w:
    #    if not novos_sem_codigo.empty:
    #        novos_sem_codigo.to_excel(w, index=False, sheet_name="novos_sem_codigo")
    #    else:
    #        pd.DataFrame({"info":["Nenhuma entrada sem códigos pela chave estrita."]}).to_excel(w, index=False, sheet_name="novos_sem_codigo")
    #
    #    ja_existiam = md_strict[~entrou_mask].copy()
    #    if not ja_existiam.empty:
    #        ja_existiam.to_excel(w, index=False, sheet_name="ja_existiam_strict")
    #    else:
    #        pd.DataFrame({"info":["Nenhuma chave estrita antiga encontrada; tudo foi tratado como novo."]}).to_excel(w, index=False, sheet_name="ja_existiam_strict")

    # ---------------- logs no console ----------------
    print("\n[DEPURAÇÃO — CHAVE ESTRITA (Sub Classe | Ativo | Emissor | Fundo | Vencimento_final)]")
    print(f" - Chaves únicas no main_df: {md_strict['KEY_STRICT'].nunique():,}")
    print(f" - Já existentes no arquivo antigo: {(~entrou_mask).sum():,}")
    print(f" - ENTRARAM (novos pela chave estrita): {entrou_mask.sum():,}")

    #print(f"\n✅ main_df filtrado salvo em: {OUT_MAIN.as_posix()}")
    print(f"✅ ENTRADAS sem código (chave estrita) salvo em: {OUT_CONT_NOVO.as_posix()}")
    #print(f"✅ Depuração detalhada salvo em: {OUT_DEPURACAO_XLS.as_posix()}")
    print(f"✅ Mapa de códigos (preserva 'Fundo' e usa chave estrita) em: {MAP_ANTIGO.as_posix()}")
    if DEBUG:
        print(f"🧪 Artefatos de debug em: {DEBUG_DIR.as_posix()} (CSV/Excel com *strict keys*)")

if __name__ == "__main__":
    main()
