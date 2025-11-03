# -*- coding: utf-8 -*-
"""
Consolidador CETIP (LF/LFSC/LFSN/CDB/DEB) + Data da Call (LFSC):

- Varre ROOT_DIRS por PDFs e consolida num DataFrame.
- Extração robusta: PyMuPDF -> pdfplumber -> PyPDF2 -> pdfminer -> OCR (opcional).
- Regex tolerantes (com e sem “:”).

Regras principais:
- `Data Proximo Juros`: sempre na mesma coluna para todos os ativos.
- `Data Call Proxima`: unificada na coluna de sempre:
    * Preferência 1: data textual explícita “Próxima Recompra (Call): dd/mm/aaaa”.
    * Preferência 2: se existirem “Data(s) programada(s) exercício opção recompra: <lista>” → pega a primeira **futura**.
    * Preferência 3: se nada textual, projeta a partir da periodicidade e âncora (“a partir do Xº ano”, etc.).

Coberturas extras:
- Periodicidade de CALL aceita DIA/DIAS, MES/MESES e ANO/ANOS (inclui padrão “A CADA N <UNIDADE>”).
- Reconhecimento de “LFSC COM OPÇÃO DE RECOMPRA TOTAL A CADA 5 ANOS.” (mesmo fora do bloco formal).
"""

import re
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd

# ========= CONFIG =========
ROOT_DIRS = [
    r"Z:\Asset Management\FUNDOS e CLUBES\CETIP - Instrumentos Financeiros (TELAS)\lf, lfsc, lfsn",
    r"Z:\Asset Management\FUNDOS e CLUBES\CETIP - Instrumentos Financeiros (TELAS)\cdb",
    r"Z:\Asset Management\FUNDOS e CLUBES\CETIP - Instrumentos Financeiros (TELAS)"
]
OUT_DIR  = Path("Dados/saida_pdf_cetip_consolidado")
OUT_DIR.mkdir(parents=True, exist_ok=True)
SAVE_XLSX = True
SAVE_CSV  = True

# OCR quando nada extrai texto (requer poppler + Tesseract no PATH)
ENABLE_OCR_WHEN_EMPTY = True

# Debug de texto
DEBUG_DUMP_TEXT = True
DEBUG_DUMP_DIR  = OUT_DIR / "debug_text"
USE_DEBUG_IF_EXISTS = True  # se existir debug_text/<cod>.txt, usa o dump (não relê o PDF)
if DEBUG_DUMP_TEXT:
    DEBUG_DUMP_DIR.mkdir(parents=True, exist_ok=True)

# ---- Extratores ----
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    import PyPDF2
except Exception:
    PyPDF2 = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    try:
        from pdfminer.high_level import extract_text as _tmp
        pdfminer_extract_text = _tmp
    except Exception:
        pdfminer_extract_text = None

# OCR deps (opcionais)
if ENABLE_OCR_WHEN_EMPTY:
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except Exception:
        ENABLE_OCR_WHEN_EMPTY = False


# ========= utils =========
def today_date() -> pd.Timestamp:
    return pd.to_datetime(datetime.today().date())

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = strip_accents(s)
    return re.sub(r"\s+", " ", s).strip()

def parse_date_pt(s: str) -> Optional[pd.Timestamp]:
    if not s:
        return pd.NaT
    m = re.search(r"(\d{2}/\d{2}/\d{4})", s)
    return pd.to_datetime(m.group(1), dayfirst=True, errors="coerce") if m else pd.NaT

def to_float_br(s: str) -> Optional[float]:
    if not s:
        return None
    t = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        mm = re.findall(r"[\d\.,]+", s)
        if mm:
            tt = mm[0].replace(".", "").replace(",", ".")
            try:
                return float(tt)
            except Exception:
                return None
        return None

def nearest_cycle(days: int) -> str:
    if days <= 0:
        return "BULLET"
    allowed = [15, 30, 45, 60, 180, 360, 720]
    return f"{min(allowed, key=lambda d: abs(d - days))} DIAS"

def clean_emissor(txt: str) -> str:
    """Remove sufixos como 'Codigo IF/ISIN ...' que colam ao emissor."""
    if not txt:
        return ""
    cut_tokens = [
        r"\bCodigo\s+IF\b",
        r"\bCodigo\s+ISIN\b",
        r"\bCodigo\s+ISIN/CODIF\b",
        r"\bISIN\b",
        r"\bCodigo\b",
        r"\bCod\s*IF\b",
    ]
    pattern = re.compile(r"(.*?)\s*(?:%s)\b.*" % "|".join(cut_tokens), re.IGNORECASE)
    m = pattern.match(txt.strip())
    if m:
        return m.group(1).strip()
    return txt.strip()


# ========= extração por página =========
def extract_pages_pymupdf(pdf_path: str) -> List[str]:
    out = []
    if fitz is None:
        return out
    try:
        doc = fitz.open(pdf_path)
        if doc.needs_pass:
            try:
                doc.authenticate("")
            except Exception:
                pass
        for page in doc:
            try:
                out.append(page.get_text("text") or "")
            except Exception:
                out.append("")
        doc.close()
    except Exception:
        pass
    return out

def extract_pages_pdfplumber(pdf_path: str) -> List[str]:
    out = []
    if pdfplumber is None:
        return out
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pg in pdf.pages:
                try:
                    out.append(pg.extract_text() or "")
                except Exception:
                    out.append("")
    except Exception:
        pass
    return out

def extract_pages_pypdf2(pdf_path: str) -> List[str]:
    out = []
    if PyPDF2 is None:
        return out
    try:
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            if getattr(r, "is_encrypted", False):
                try:
                    r.decrypt("")
                except Exception:
                    pass
            for pg in r.pages:
                try:
                    out.append(pg.extract_text() or "")
                except Exception:
                    out.append("")
    except Exception:
        pass
    return out

def extract_pages_pdfminer(pdf_path: str) -> List[str]:
    if pdfminer_extract_text is None:
        return []
    try:
        whole = pdfminer_extract_text(pdf_path) or ""
        if not whole:
            return []
        parts = [p for p in re.split(r"\f+|\n{3,}", whole) if p.strip()]
        return parts if parts else [whole]
    except Exception:
        return []

def extract_pages_ocr(pdf_path: str) -> List[str]:
    if not ENABLE_OCR_WHEN_EMPTY:
        return []
    try:
        images = convert_from_path(pdf_path, dpi=200)
        outs = []
        for img in images:
            try:
                txt = pytesseract.image_to_string(img, lang="por+eng") or ""
            except Exception:
                txt = ""
            outs.append(txt)
        return outs
    except Exception:
        return []

def get_pages_text(pdf_path: str) -> Tuple[List[str], str]:
    """
    Tenta TODOS os extratores e escolhe o que mais texto normalizado retornar.
    Se houver dump debug <cod>.txt e USE_DEBUG_IF_EXISTS=True, usa o dump.
    """
    stem = Path(pdf_path).stem
    dbg_file = DEBUG_DUMP_DIR / f"{stem}.txt"
    if USE_DEBUG_IF_EXISTS and dbg_file.exists():
        try:
            txt = dbg_file.read_text(encoding="utf-8")
            if norm_text(txt):
                return [txt], "debug_text"
        except Exception:
            pass

    candidates: List[Tuple[str, List[str]]] = []
    for fn, label in [
        (extract_pages_pymupdf,   "pymupdf"),
        (extract_pages_pdfplumber,"pdfplumber"),
        (extract_pages_pypdf2,    "pypdf2"),
        (extract_pages_pdfminer,  "pdfminer"),
    ]:
        pages = fn(pdf_path)
        if pages:
            score = len(norm_text(" ".join(pages)))
            candidates.append((label, pages if score else []))

    best_label, best_pages = ("none", [])
    best_score = 0
    for label, pages in candidates:
        txt = norm_text(" ".join(pages))
        sc = len(txt)
        if sc > best_score:
            best_label, best_pages, best_score = label, pages, sc

    if best_score > 0:
        return best_pages, best_label

    # fallback: OCR
    pages = extract_pages_ocr(pdf_path)
    if any(norm_text(p) for p in pages):
        return pages, "ocr"
    return [], "none"

def _nz(x: Optional[float], tol: float = 1e-9) -> bool:
    """True se x não-nulo e |x| > tol."""
    return x is not None and abs(float(x)) > tol

# ========= blocos (juros/call) =========
def cut_block(text: str, header_patterns: list[str], tail_stop_patterns: list[str], window: int = 1200) -> str:
    """
    Recorta um bloco começando no primeiro header encontrado e terminando:
    - no próximo header 'competidor' OU
    - no primeiro 'tail_stop' (ex.: próxima seção) OU
    - após 'window' chars de segurança.
    """
    T = text
    start = None
    for hp in header_patterns:
        m = re.search(hp, T, flags=re.IGNORECASE)
        if m:
            start = m.end()
            break
    if start is None:
        return ""
    end = min(len(T), start + window)
    for tp in tail_stop_patterns:
        mm = re.search(tp, T[start:end], flags=re.IGNORECASE)
        if mm:
            end = start + mm.start()
            break
    return T[start:end].strip()

JR_HEADERS = [
    r"Fluxo\s+de\s+Pagamento\s+de\s+Juros",
    r"Periodicidade\s+de\s+Juros",
    r"Forma\s+de\s+Pagamento"
]
JR_TAILS = [
    r"Dados\s+do\s+Evento",
    r"Registro\s+de\s+Eventos",
    r"Recompra|\bCALL\b|Opc[aã]o\s+de\s+Recompra|Resgate",
    r"Valores\s+Atualizados", r"Complemento", r"Observac",
]
CALL_HEADERS = [
    r"Recompra\s*\(Call\)", r"Opc[aã]o\s*de\s*Recompra",
    r"datas\s*programadas?.*exercicio\s*op[cç]ao\s*recompra",
    r"Periodicidade\s*Recompra\s*\(Call\)"
]

# ========= regex =========
def any_match(text: str, patterns: List[re.Pattern]) -> Optional[re.Match]:
    for p in patterns:
        m = p.search(text)
        if m:
            return m
    return None

# TIPO
RE_TIPO = [re.compile(r"Tipo\s*:?\s*([A-Za-z]{2,12})", re.IGNORECASE)]
RE_DEB_HINT = re.compile(r"\bdebentur", re.IGNORECASE)  # Heurística Debênture

# Cod IF / ISIN
RE_COD_IF = [
    re.compile(r"Codigo\s*IF\s*:?\s*([A-Za-z0-9\-\.\s]+)", re.IGNORECASE),
    re.compile(r"Cod\s*IF\s*:?\s*([A-Za-z0-9\-\.\s]+)", re.IGNORECASE),
]
RE_ISIN = [
    re.compile(r"Codigo\s*ISIN\s*:?\s*([A-Za-z0-9\-\.\s]+)", re.IGNORECASE),
    re.compile(r"ISIN\s*:?\s*([A-Za-z0-9\-\.\s]+)", re.IGNORECASE),
]

# Emissor
RE_EMISSOR = [
    re.compile(r"Nome\s+Simplificado\s+do\s+Emissor\s*:?\s*([A-Za-z0-9\-\.\s]+(?:\sCodigo\b.*)?)", re.IGNORECASE),
    re.compile(r"Emissor\s*:?\s*([A-Za-z0-9\-\.\s]+(?:\sCodigo\b.*)?)", re.IGNORECASE),
    re.compile(r"Instituicao\s+(?:Financeira|Emissora)\s*:?\s*([A-Za-z0-9\-\.\s]+(?:\sCodigo\b.*)?)", re.IGNORECASE),
    re.compile(r"Banco\s*:?\s*([A-Za-z0-9\-\.\s]+(?:\sCodigo\b.*)?)", re.IGNORECASE),
]

# Datas
RE_DATA_EMISSAO = [
    re.compile(r"Data\s+de\s+Emissao\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Emissao\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
]
RE_VENCIMENTO = [
    re.compile(r"Vencimento(?:\s*Final)?\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Data\s+de\s+Vencimento\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
]

# PU emissão
RE_PU_EMISSAO = [
    re.compile(r"Valor\s+Unitario\s+de\s+Emissao\s*:?\s*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Valor\s+Nominal\s+Unitario.*Emissao\s*:?\s*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"PU\s*na\s*Emissao\s*:?\s*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
]

# Tipo de juros / indexador
RE_TIPO_JUROS = [
    re.compile(r"Rentabilidade/Indexador/Taxa\s+Flutuante\s*:?\s*([A-Za-z\-\s]+)", re.IGNORECASE),
    re.compile(r"(?:Indexador|Indicador|Indice)\s*:?\s*([A-Za-z\-\s]+)", re.IGNORECASE),
    re.compile(r"Indexador\s*:?\s*(CDI|DI|IPCA(?:-E)?|IPC-?A|PRE|PREFIXADO|PREFIXADA)", re.IGNORECASE),
]

# % da flutuante / % do indexador
RE_TAXA_FLUTUANTE_PCT = [
    re.compile(r"%\s*da\s*Taxa\s*Flutuante\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Percentual\s+da\s+Taxa\s+Flutuante\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"%\s*do\s*CDI\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Percentual\s+do\s*CDI\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"%\s*do\s*Indexador\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Percentual\s+do\s*Indexador\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"%\s*Indice/Taxa\s*Flutuante\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Rentabilidade/Multiplicador\s*\(%\)\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
]

# Spread / taxa de emissão (cobre várias formas)
RE_TAXA_EMISSAO_SPREAD = [
    re.compile(r"Taxa\s+de\s+Juros/Spread\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Spread\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Taxa\s*de\s*Juros\s*:?\s*([\d\.\,]+)", re.IGNORECASE),
]

# Ciclo e “a partir” (APENAS BLOCO DE JUROS)
# >>>>> MODIFICADO para capturar "DIA CORRIDO" também <<<<<
RE_CICLO_DIAS_APARTIR = [
    re.compile(r"Juros\s*a\s*cada\s*:?\s*(\d+)\s*(MES(?:ES)?|DIA(?:S)?(?:\s+CORRIDO)?)"
               r"(?:.*?\ba\s*partir\s*(?:de|:)?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}))?", re.IGNORECASE),
    re.compile(r"Periodicidade\s*(?:de\s*Juros)?\s*:?\s*(\d+)\s*(MES(?:ES)?|DIA(?:S)?(?:\s+CORRIDO)?)"
               r"(?:.*?\ba\s*partir\s*(?:de|:)?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}))?", re.IGNORECASE),
]

# Debênture: “A cada: N … Unidade: …”
RE_ACADA_SIMPLE = re.compile(r"A\s*cada\s*:\s*(\d+)", re.IGNORECASE)
RE_UNIDADE = re.compile(r"Unidade\s*:\s*(MES|MESES|DIA|DIAS)", re.IGNORECASE)

# Datas Próximo/Último Juros
RE_DATA_PROX_JUROS = [
    re.compile(r"Data\s+do\s+Proximo\s+Pagamento\s+de\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Proximo\s+Pagamento\s+de\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Data\s+do\s+Proximo\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Proximo\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
]

RE_DATA_ULT_JUROS = [
    re.compile(r"Data\s+do\s+Ultimo\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Ultimo\s+Juros\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
]

# ===== CALL =====
# Cabeçalho com "Data(s) programada(s) ..."
RE_CALL_HEADER = re.compile(r"data(?:\(s\))?\s*programada(?:\(s\))?.*exercicio\s*op[cç]ao\s*recompra\s*:?", re.IGNORECASE)

# Periodicidade (agora aceita ANO/ANOS)
RE_CALL_PERIODICIDADE = [
    re.compile(r"Periodicidade\s+Recompra\s*\(Call\)\s*:?\s*A\s*cada\s*(\d+)\s*(ANO|ANOS|MES|MESES|DIA|DIAS)", re.IGNORECASE),
    re.compile(r"Recompra\s*\(Call\)\s*:?\s*A\s*cada\s*(\d+)\s*(ANO|ANOS|MES|MESES|DIA|DIAS)", re.IGNORECASE),
]
# Periodicidade genérica (ex.: "A CADA 5 ANOS" fora do label)
RE_CALL_PERIOD_ANY = re.compile(r"\bA\s*CADA\s*(\d+)\s*(ANO|ANOS|MES|MESES|DIA|DIAS)\b", re.IGNORECASE)

# "a partir do Xº ano"
RE_CALL_INICIO_ANO = re.compile(r"a\s*partir\s*do\s*(\d+)[ºo]?\s*ano", re.IGNORECASE)

# "Próxima Recompra (Call): dd/mm/aaaa"
RE_CALL_PROXIMA = [
    re.compile(r"Proxima\s+Recompra\s*\(Call\)\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Proxima\s+Call\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
    re.compile(r"Recompra\s*\(Call\)\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.IGNORECASE),
]

# Lista de datas após o cabeçalho "Data(s) programada(s)..."
RE_ANY_DATE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

# freq textual com "a partir de <data>"
RE_CALL_FREQ_APARTIR = re.compile(
    r"\b(anual(?:mente)?|semestral|trimestral|bimestral|mensal)\b\s*a\s*partir\s*de\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
    re.IGNORECASE
)
RE_CALL_APARTIR_FREQ = re.compile(
    r"a\s*partir\s*de\s*([0-9]{2}/[0-9]{2}/[0-9]{4}).{0,80}\b(anual(?:mente)?|semestral|trimestral|bimestral|mensal)\b",
    re.IGNORECASE
)
RE_CALL_APARTIR_CTX1 = re.compile(
    r"(?:opcao\s*(?:de)?\s*(?:recompra|resgate)|recompra|resgate|call).{0,120}?a\s*partir\s*de\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
    re.IGNORECASE
)
RE_CALL_APARTIR_CTX2 = re.compile(
    r"a\s*partir\s*de\s*([0-9]{2}/[0-9]{2}/[0-9]{4}).{0,120}?(?:opcao\s*(?:de)?\s*(?:recompra|resgate)|recompra|resgate|call)",
    re.IGNORECASE
)

FREQ_TO_MONTHS = {"ANUAL": 12, "ANUALMENTE": 12, "SEMESTRAL": 6, "TRIMESTRAL": 3, "BIMESTRAL": 2, "MENSAL": 1}

# Presença de fluxo/periodicidade de juros
RE_PAGAMENTO_PERIODICO_JUROS = re.compile(r"Pagamento\s+periodico\s+de\s+juros", re.IGNORECASE)
RE_PERIODICIDADE_VARIAVEL = re.compile(r"Periodicidade(?:\s+de\s+(?:Eventos|Juros))?\s*:?\s*VARI[ÁA]VEL", re.IGNORECASE)

# Incorpora Juros: SIM/NAO
RE_INCORPORA_SIM = re.compile(r"\bincorpora\s+juros\s*:?\s*sim\b", re.IGNORECASE)
RE_INCORPORA_NAO = re.compile(r"\bincorpora\s+juros\s*:?\s*n[aã]o\b", re.IGNORECASE)


def _call_offset(n: int, unit: str):
    """Converte 'n' + unidade em offset de data (ACEITA ANO/ANOS, MES/MESES, DIA/DIAS)."""
    unit = (unit or "").upper()
    if "ANO" in unit:
        return pd.DateOffset(years=int(n))
    if "MES" in unit:
        return pd.DateOffset(months=int(n))
    if "DIA" in unit:
        n = int(n)
        if n % 30 == 0:
            return pd.DateOffset(months=n // 30)  # 180 dias -> 6 meses
        return pd.Timedelta(days=n)
    # fallback
    return pd.Timedelta(days=int(n))


def _roll_forward_until_after(start_dt: pd.Timestamp, offset) -> pd.Timestamp:
    """Soma offset em loop até passar de 'hoje'. Retorna NaT se sem base/offset."""
    if pd.isna(start_dt) or offset is None:
        return pd.NaT
    today = pd.Timestamp.today().normalize()
    cur = pd.to_datetime(start_dt)
    for _ in range(400):
        if cur > today:
            return cur
        cur = cur + offset
    return cur


# ========= parser de 1 PDF =========
def parse_pdf_fields(pdf_path: str) -> Dict[str, Any]:
    pages, extractor = get_pages_text(pdf_path)
    norm_pages = [norm_text(p) for p in pages]
    text = " ".join(p for p in norm_pages if p)
    leitura_ok = bool(norm_text(text))

    # dump para debug (até 2000 chars)
    if DEBUG_DUMP_TEXT and extractor != "debug_text":
        try:
            (DEBUG_DUMP_DIR / (Path(pdf_path).stem + ".txt")).write_text(text[:2000], encoding="utf-8")
        except Exception:
            pass

    tipo = emissor = tipo_juros = codigo_if = isin = ""
    data_emissao = vencimento = pd.NaT
    pu_emissao = None
    pct_flutuante = None
    spread = None
    forma_cdi = ""
    tem_fluxo = "BULLET"
    ciclo = "BULLET"
    agenda_juros = ""  # NOVA COLUNA: 'D' (dias corridos), 'M' (mês), ou ''
    data_prox_juros = pd.NaT
    data_call_inicial = pd.NaT
    sanity_check = "OK"
    sanity_notes = []
    incorp = "DESCONHECIDO"

    # ======= Campos básicos =======
    m = any_match(text, RE_TIPO);           tipo = m.group(1).upper() if m else ("DEB" if RE_DEB_HINT.search(text) else re.search(r"(LFSC|LFSN|LF|DEB|CRI|CRA|CDB)", Path(pdf_path).stem.upper()).group(1) if re.search(r"(LFSC|LFSN|LF|DEB|CRI|CRA|CDB)", Path(pdf_path).stem.upper()) else "")
    m = any_match(text, RE_COD_IF);       codigo_if = m.group(1).strip() if m else ""
    m = any_match(text, RE_ISIN);         isin = m.group(1).strip() if m else ""
    m = any_match(text, RE_EMISSOR);      emissor = clean_emissor(m.group(1)) if m else ""
    m = any_match(text, RE_DATA_EMISSAO); data_emissao = parse_date_pt(m.group(1)) if m else pd.NaT
    m = any_match(text, RE_VENCIMENTO);   vencimento = parse_date_pt(m.group(1)) if m else pd.NaT
    m = any_match(text, RE_PU_EMISSAO);   pu_emissao = to_float_br(m.group(1)) if m else None
    m = any_match(text, RE_TIPO_JUROS);   tipo_juros = (m.group(1) or "").upper() if m else ""

    # % flutuante / spread
    m = any_match(text, RE_TAXA_FLUTUANTE_PCT); pct_flutuante = to_float_br(m.group(1)) if m else None
    m = any_match(text, RE_TAXA_EMISSAO_SPREAD); spread = to_float_br(m.group(1)) if m else None

    # FormaCDI / IPCA
    tj = (tipo_juros or "").replace("-", "").replace(" ", "").upper()
    pct, sp = pct_flutuante, spread
    if tj in {"PRE", "PREFIXADO", "PREFIXADA"}:
        forma_cdi, taxa_emissao = "PREFIXADO", sp
    elif tj in {"CDI", "DI", "D.I.", "DI."}:
        if sp is not None and abs(sp) > 1e-9: forma_cdi, taxa_emissao = "CDI+", sp
        elif pct is not None:                 forma_cdi, taxa_emissao = "%CDI", pct
        else:                                 forma_cdi, taxa_emissao = "", None
    elif "IPCA" in tj or re.search(r"\bIPC-?A\b", (tipo_juros or "").upper()):
        if sp is not None and abs(sp) > 1e-9: forma_cdi, taxa_emissao = "IPCA+", sp
        elif pct is not None:                 forma_cdi, taxa_emissao = "%IPCA", pct
        else:                                 forma_cdi, taxa_emissao = "IPCA", None
    else:
        forma_cdi, taxa_emissao = "", (pct if pct is not None else sp)

    # ======== Blocos ========
    juros_block = cut_block(text, JR_HEADERS, JR_TAILS, window=2000)
    call_block  = cut_block(text, CALL_HEADERS, JR_TAILS, window=2000)

    # Incorpora Juros
    if RE_INCORPORA_NAO.search(text): incorp = "NAO"
    elif RE_INCORPORA_SIM.search(text): incorp = "SIM"


    ### ALTERAÇÃO INÍCIO ###
    # ======== Ciclo / Juros (LÓGICA CORRIGIDA) ========

    # 1. PRIMEIRO: Tenta encontrar a data EXPLICITA no texto completo, pois é a fonte mais confiável.
    m_prox_juros_explicito = any_match(text, RE_DATA_PROX_JUROS)
    if m_prox_juros_explicito:
        data_prox_juros = parse_date_pt(m_prox_juros_explicito.group(1))

    # 2. SEGUNDO: Processa o bloco de juros para definir o ciclo e,
    #    APENAS SE a data explícita não foi encontrada, tenta inferir a data a partir da periodicidade.
    if RE_PAGAMENTO_PERIODICO_JUROS.search(juros_block):
        tem_fluxo = "Juros"

    m = any_match(juros_block, RE_CICLO_DIAS_APARTIR)
    if m:
        n_val = int(m.group(1))
        unit = (m.group(2) or "").upper()
        if "MES" in unit:
            agenda_juros = "M"
            dias = 30 * n_val
        elif "DIA" in unit:
            agenda_juros = "D"
            dias = n_val
        else:
            dias = n_val # fallback

        ciclo = nearest_cycle(dias)
        tem_fluxo = "Juros"

        # Tenta preencher a data a partir de "a partir de", mas só se ainda estiver vazia.
        if pd.isna(data_prox_juros) and m.lastindex and m.lastindex >= 3 and m.group(3):
            data_prox_juros = parse_date_pt(m.group(3))

    if ciclo == "BULLET":
        mm = RE_ACADA_SIMPLE.search(juros_block)
        if mm:
            n = int(mm.group(1))
            tail = juros_block[mm.end(): mm.end()+120]
            mu = RE_UNIDADE.search(tail)
            if mu:
                unit = mu.group(1).upper()
                if "MES" in unit:
                    agenda_juros = "M"
                    dias = 30 * n
                elif "DIA" in unit:
                    agenda_juros = "D"
                    dias = n
                else:
                    dias = n # fallback
                ciclo = nearest_cycle(dias)
                tem_fluxo = "Juros"

    if ciclo == "BULLET" and RE_PERIODICIDADE_VARIAVEL.search(juros_block):
        ciclo = "VARIAVEL"; tem_fluxo = "Juros"

    # O antigo bloco de fallback foi removido daqui, pois a lógica agora está no início desta seção.
    ### ALTERAÇÃO FIM ###


    # ======== CALL ========
    call_period_n = None
    call_period_unit = ""
    call_inicio_anos = None

    # (A) Periodicidade com label
    m = any_match(text, RE_CALL_PERIODICIDADE)
    if m:
        call_period_n = int(m.group(1))
        call_period_unit = (m.group(2) or "").upper()

    # (A2) Periodicidade genérica "A CADA N <UNIDADE>" (cobre "A CADA 5 ANOS")
    if call_period_n is None:
        m = RE_CALL_PERIOD_ANY.search(call_block or text)
        if m:
            call_period_n = int(m.group(1))
            call_period_unit = (m.group(2) or "").upper()

    # (B) "a partir do Xº ano"
    m = RE_CALL_INICIO_ANO.search(text)
    if m:
        call_inicio_anos = int(m.group(1))

    # (C1) Próxima call textual direta
    data_call_proxima_txt = pd.NaT
    m = any_match(text, RE_CALL_PROXIMA)
    if m:
        data_call_proxima_txt = parse_date_pt(m.group(1))

    # (C2) Lista de "Data(s) programada(s) ..." → pegar primeira futura
    listed_call_dates: List[pd.Timestamp] = []
    for mm in RE_CALL_HEADER.finditer(text):
        tail = text[mm.end(): mm.end() + 400]
        for d in RE_ANY_DATE.findall(tail):
            dt = parse_date_pt(d)
            if pd.notna(dt):
                listed_call_dates.append(dt)
    listed_call_dates = sorted(set(listed_call_dates))
    today = pd.Timestamp.today().normalize()
    first_future_from_list = next((d for d in listed_call_dates if d > today), pd.NaT)

    # (D) freq textual com "a partir de <data>"
    anchor_from_freq = pd.NaT
    freq_months = None
    m = RE_CALL_FREQ_APARTIR.search(text)
    if m:
        freq = strip_accents((m.group(1) or "").upper())
        dt = parse_date_pt(m.group(2))
        if pd.notna(dt):
            anchor_from_freq = dt
            freq_months = FREQ_TO_MONTHS.get(freq, None)

    if pd.isna(anchor_from_freq):
        m = RE_CALL_APARTIR_FREQ.search(text)
        if m:
            dt = parse_date_pt(m.group(1))
            freq = strip_accents((m.group(2) or "").upper())
            if pd.notna(dt):
                anchor_from_freq = dt
                freq_months = FREQ_TO_MONTHS.get(freq, None)

    anchor_from_ctx = pd.NaT
    for pat in (RE_CALL_APARTIR_CTX1, RE_CALL_APARTIR_CTX2):
        mm = pat.search(text)
        if mm:
            dt = parse_date_pt(mm.group(1))
            if pd.notna(dt):
                anchor_from_ctx = dt
                break

    # inferir periodicidade em meses a partir do texto (caso "anual", etc.)
    if (freq_months is not None) and (call_period_n is None):
        call_period_n = int(freq_months)
        call_period_unit = "MES"

    # Definir âncora base
    call_anchor = data_call_proxima_txt
    if pd.isna(call_anchor) and pd.notna(first_future_from_list):
        call_anchor = first_future_from_list
    if pd.isna(call_anchor) and pd.notna(anchor_from_freq):
        call_anchor = anchor_from_freq
    if pd.isna(call_anchor) and pd.notna(anchor_from_ctx):
        call_anchor = anchor_from_ctx
    if pd.isna(call_anchor) and (call_inicio_anos is not None) and pd.notna(data_emissao):
        call_anchor = pd.to_datetime(data_emissao) + pd.DateOffset(years=int(call_inicio_anos))

    # Data Call Inicial (primeira listada ou âncora)
    data_call_inicial = listed_call_dates[0] if listed_call_dates else (call_anchor if pd.notna(call_anchor) else pd.NaT)

    # === Escolha FINAL da Data Call Proxima (unificada) ===
    # 1) Se houver lista de programadas → pega a 1ª futura
    # 2) Senão, se houver "Próxima Recompra" textual → usa
    # 3) Senão, projeta pela periodicidade + âncora
    data_call_proxima_final = pd.NaT
    if pd.notna(first_future_from_list):
        data_call_proxima_final = first_future_from_list
    elif pd.notna(data_call_proxima_txt):
        data_call_proxima_final = data_call_proxima_txt
    else:
        # Projeção
        if (call_period_n is not None) and pd.notna(call_anchor):
            off = _call_offset(call_period_n, call_period_unit or "")
            data_call_proxima_final = _roll_forward_until_after(call_anchor, off)
        # sem periodicidade: se houver âncora futura, usa
        if pd.isna(data_call_proxima_final) and pd.notna(call_anchor) and call_anchor > today:
            data_call_proxima_final = call_anchor

    # ===== sanity =====
    if not text:
        sanity_check = "FAIL"; sanity_notes.append("no_text_extracted")
    missing = []
    if not tipo: missing.append("tipo")
    if not emissor: missing.append("emissor")
    if not (pd.notna(data_emissao) or pd.notna(vencimento)): missing.append("datas")
    if missing:
        if sanity_check != "FAIL":
            sanity_check = "PARTIAL" if len(missing) < 3 else "FAIL"
        sanity_notes.append("missing: " + ",".join(missing))

    return {
        "TIPO": tipo,
        "Tem_fluxo": tem_fluxo,
        "CicloJuros": ciclo,
        "AgendaJuros": agenda_juros,      # <<<<< COLUNA NOVA/RENOMEADA
        "IncorporaJuros": incorp,
        "DataRef": today_date(),
        "Emissor": emissor,
        "cod_Ativo": Path(pdf_path).stem,
        "Codigo_IF": codigo_if,
        "ISIN": isin,
        "Data Emissão": data_emissao,
        "vencimento": vencimento,
        "PU_emissão": pu_emissao,
        "TipoJuros": tipo_juros,
        "pct_flutuante": pct_flutuante,
        "spread": spread,
        "FormaCDI": forma_cdi,
        "taxa_emissão": taxa_emissao,
        "Data Proximo Juros": data_prox_juros,
        "Data_prox_juros": data_prox_juros,
        "Data Call Inicial": data_call_inicial,
        "Call_Periodicidade": (f"{call_period_n} {call_period_unit}".strip() if call_period_n else ""),
        "Call_Inicio_Apos_anos": call_inicio_anos,
        "Data Call Proxima": data_call_proxima_final,
        "source_file": str(pdf_path),
        "pages_total": len(pages),
        "pages_blank": sum(1 for p in norm_pages if not p),
        "pages_used": len(pages),
        "extractor_used": extractor,
        "leitura_ok": leitura_ok,
        "sanity_check": sanity_check,
        "sanity_notes": "; ".join(sanity_notes),
        # Auditoria
        "debug_bloco_juros": juros_block[:250],
        "debug_bloco_call":  call_block[:250],
    }


# ========= varre diretórios e monta DF =========
def list_all_pdfs_from_dirs(root_dirs: List[str]) -> List[Path]:
    files: List[Path] = []
    for rd in root_dirs:
        root = Path(rd)
        if root.is_file() and root.suffix.lower() == ".pdf":
            files.append(root); continue
        for pat in ["*.pdf", "*.PDF", "*.[pP][dD][fF]"]:
            files.extend(root.rglob(pat))
    return sorted(set(files), key=lambda p: (p.parent.as_posix(), p.name.lower()))

try:
    NULL_INT_DTYPE = pd.Int64Dtype()
except Exception:
    NULL_INT_DTYPE = None

def to_nullable_int(s) -> pd.Series:
    ser = pd.to_numeric(s, errors="coerce")
    if NULL_INT_DTYPE is not None:
        try:
            return ser.astype(NULL_INT_DTYPE)
        except Exception:
            return ser
    return ser

def build_dataframe(root_dirs: List[str]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    pdfs = list_all_pdfs_from_dirs(root_dirs)

    for pdf in pdfs:
        try:
            rows.append(parse_pdf_fields(str(pdf)))
        except Exception as e:
            rows.append({
                "TIPO": "", "Tem_fluxo": "BULLET", "CicloJuros": "BULLET",
                "AgendaJuros": "",
                "IncorporaJuros": "DESCONHECIDO",
                "DataRef": today_date(),
                "Emissor": "", "cod_Ativo": Path(pdf).stem, "Codigo_IF": "", "ISIN": "",
                "Data Emissão": pd.NaT, "vencimento": pd.NaT, "PU_emissão": None,
                "TipoJuros": "", "pct_flutuante": None, "spread": None,
                "FormaCDI": "", "taxa_emissão": None,
                "Data Proximo Juros": pd.NaT,
                "Data Call Inicial": pd.NaT,
                "Call_Periodicidade": "",
                "Call_Inicio_Apos_anos": None,
                "Data Call Proxima": pd.NaT,
                "source_file": str(pdf),
                "pages_total": 0, "pages_blank": 0, "pages_used": 0,
                "extractor_used": "exception",
                "leitura_ok": False,
                "sanity_check": "FAIL",
                "sanity_notes": f"exception: {e}",
                "debug_bloco_juros": "", "debug_bloco_call": ""
            })

    cols = [
        "TIPO","Tem_fluxo","CicloJuros","AgendaJuros","IncorporaJuros","DataRef","Emissor","cod_Ativo","Codigo_IF","ISIN", # <<<<< COLUNA NOVA/RENOMEADA
        "Data Emissão","vencimento","PU_emissão","TipoJuros",
        "pct_flutuante","spread","FormaCDI","taxa_emissão",
        "Data_prox_juros",
        "Data Call Inicial","Call_Periodicidade","Call_Inicio_Apos_anos","Data Call Proxima",
        "source_file",
        "pages_total","pages_blank","pages_used","extractor_used",
        "leitura_ok","sanity_check","sanity_notes",
        "debug_bloco_juros","debug_bloco_call"
    ]
    df = pd.DataFrame(rows, columns=cols)

    if not df.empty:
        counts = df["cod_Ativo"].value_counts(dropna=False)
        df["cod_Ativo_count"] = to_nullable_int(df["cod_Ativo"].map(counts))
        df["is_unico"] = df["cod_Ativo_count"] == 1
        grp_idx, uniques = pd.factorize(df["cod_Ativo"], sort=True)
        df["cod_Ativo_group_index"] = to_nullable_int(pd.Series(grp_idx + 1))
        unicos = [u for u in uniques.tolist() if counts.get(u, 0) == 1]
        unico_map = {code: i+1 for i, code in enumerate(sorted(unicos))}
        df["unico_index"] = to_nullable_int(df["cod_Ativo"].map(unico_map))
        df = df.sort_values(
            ["sanity_check","TIPO","cod_Ativo","source_file"],
            kind="stable"
        ).reset_index(drop=True)
    return df


# ========= main =========
def main():
    df = build_dataframe(ROOT_DIRS)

    if SAVE_XLSX:
        xlsx_path = OUT_DIR / "consolidado_pdfs_codativos.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as w:
            df.to_excel(w, index=False, sheet_name="tabela")

            # sanity issues
            df[df["sanity_check"] != "OK"].to_excel(w, index=False, sheet_name="sanity_issues")

            # únicos (um por cod_Ativo)
            cols_unicos = [
                "unico_index","cod_Ativo","TIPO","Codigo_IF","ISIN","Emissor",
                "Data Emissão","vencimento","PU_emissão","TipoJuros",
                "pct_flutuante","spread","FormaCDI","taxa_emissão",
                "Tem_fluxo","CicloJuros","AgendaJuros","IncorporaJuros", # <<<<< COLUNA NOVA/RENOMEADA
                "Data_prox_juros",
                "Data Call Inicial","Call_Periodicidade","Call_Inicio_Apos_anos","Data Call Proxima",
                "leitura_ok"
            ]
            keep = [c for c in cols_unicos if c in df.columns]
            df[df["is_unico"]].drop_duplicates("cod_Ativo")[keep].to_excel(
                w, index=False, sheet_name="cods_unicos"
            )

            # amostra de auditoria dos blocos (até 250 chars já vem cortado)
            aud_cols = ["cod_Ativo","debug_bloco_juros","debug_bloco_call","sanity_notes"]
            df[aud_cols].head(2000).to_excel(w, index=False, sheet_name="auditoria_blocos")

        print("XLSX salvo em:", xlsx_path)

    if SAVE_CSV:
        csv_path = OUT_DIR / "consolidado_pdfs_codativos.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print("CSV  salvo em:", csv_path)

    # resumo
    try:
        print("\nExtratores usados:")
        print(df["extractor_used"].value_counts(dropna=False).to_string())
        print("\nPor TIPO:")
        print(df["TIPO"].value_counts(dropna=False).to_string())
        print("\nFormaCDI:")
        print(df["FormaCDI"].value_counts(dropna=False).to_string())
        print("\nIncorporaJuros:")
        print(df["IncorporaJuros"].value_counts(dropna=False).to_string())
        print("\nCicloJuros:")
        print(df["CicloJuros"].value_counts(dropna=False).to_string())
        print("\nAgendaJuros:") # NOVO
        print(df["AgendaJuros"].value_counts(dropna=False).to_string())
        print("\nLeitura OK:")
        print(df["leitura_ok"].value_counts(dropna=False).to_string())
        print("\nSanity check:")
        print(df["sanity_check"].value_counts(dropna=False).to_string())
    except Exception:
        pass

    return df


if __name__ == "__main__":
    main()