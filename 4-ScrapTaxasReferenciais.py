# -*- coding: utf-8 -*-
# Scraper B3 (Taxas referenciais DI x Pré) + Interpolação por DU (log-DF, PCHIP)
# Agora com toggle para DATA ESPECÍFICA:
# - Preenche <input id="Data" ...> com dd/mm/aaaa
# - Clica no botão <button class="button expand">OK</button>
# - Espera 5 segundos antes de extrair a tabela

import re
import time
from pathlib import Path
from functools import lru_cache

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ===================== CONFIG =====================
B3_URL = "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-de-derivativos/precos-referenciais/taxas-referenciais-bm-fbovespa/"
IFRAME_URL = "https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp"

# --- NOVOS TOGGLES ---
USE_SPECIFIC_DATE = True                             # <- Toggle nova data específica (default False)
SPECIFIC_DATE = pd.Timestamp("2025-10-17")            # <- Data que será inserida no campo #Data quando o toggle estiver ligado

# Toggle de intervalo
BUILD_INTERVAL = False                                # <- Toggle de intervalo (default False)
START_DATE = pd.Timestamp("2025-08-01")
END_DATE   = pd.Timestamp("2025-10-07")

# Caminhos
DATA_DIR = Path("Dados"); DATA_DIR.mkdir(exist_ok=True)
FERIADOS_PATH = Path("feriados_nacionais.xls")
PARQUET_RAW   = DATA_DIR / "b3_taxas_ref_di_raw.parquet"             # base longa (ref_date x dias_corridos)
PARQUET_INTERP = DATA_DIR / "curva_di_interpolada_por_DU.parquet"    # última curva interpolada do dia

# Scraper
REQ_TIMEOUT = (10, 30)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Selenium
USE_SELENIUM_FALLBACK = True
SELENIUM_HEADLESS = True
SELENIUM_WAIT = 25  # segundos máximos para aguardar elementos


# ===================== PARSERS / UTILS =====================
def parse_num_br_smart(x):
    if pd.isna(x): return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)): return float(x)
    s = str(x).strip().replace('%','').replace('\u00A0','').replace(' ', '')
    if ('.' in s) and (',' in s): s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return np.nan

@lru_cache(maxsize=1)
def load_feriados_set() -> set:
    """Lê 'feriados_nacionais.xls' e retorna set(date)."""
    if not FERIADOS_PATH.exists():
        return set()
    try:
        df = pd.read_excel(FERIADOS_PATH)
    except Exception:
        return set()
    if df.empty or len(df.columns) == 0:
        return set()
    df = df.dropna()
    df = df[[df.columns[0]]]  # só a coluna de datas
    feriados = (
        pd.to_datetime(df[df.columns[0]], errors="coerce", dayfirst=True)
          .dropna()
          .dt.normalize()
          .dt.date
          .unique()
    )
    print(feriados.tolist())
    return set(feriados.tolist())


def _holidays_np(feriados_set: set) -> np.ndarray:
    return np.array(list(feriados_set), dtype='datetime64[D]') if feriados_set else np.array([], dtype='datetime64[D]')

def business_days_between(start_dt: pd.Timestamp, end_dt: pd.Timestamp, feriados_np: np.ndarray) -> np.ndarray:
    start_np = np.datetime64(start_dt.normalize().date(), 'D')
    end_np   = np.datetime64(end_dt.normalize().date(), 'D')
    return np.busday_range(start_np, end_np + np.timedelta64(1, 'D'), holidays=feriados_np)

def dias_corridos_para_du(ref_dt: pd.Timestamp, dias_corridos: np.ndarray, feriados_np: np.ndarray, include_end=False) -> np.ndarray:
    ref_np = np.datetime64(ref_dt.normalize().date(), 'D')
    dias_corridos = np.asarray(dias_corridos, dtype=int)
    end_np = ref_np + dias_corridos.astype('timedelta64[D]')
    du = np.busday_count(ref_np, end_np, holidays=feriados_np)
    if include_end: du += np.is_busday(end_np, holidays=feriados_np).astype(int)
    return du.astype(int)

def _extract_ref_date_from_text(text: str) -> pd.Timestamp | None:
    m = list(re.finditer(r"\b(\d{2})/(\d{2})/(\d{4})\b", text))
    if not m: return None
    dd, mm, yyyy = m[-1].groups()
    try: return pd.Timestamp(day=int(dd), month=int(mm), year=int(yyyy))
    except: return None

def _parse_table_tb_principal1(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "tb_principal1"})
    if table is None: raise ValueError("Tabela #tb_principal1 não encontrada.")
    rows = []
    for tr in table.select("tbody > tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) >= 3:
            rows.append((tds[0], tds[1], tds[2]))
    if not rows: raise ValueError("Tabela sem linhas (#tb_principal1).")
    df = pd.DataFrame(rows, columns=["dias_corridos","di_aa_252","pre_aa_360"])
    df["dias_corridos"] = pd.to_numeric(df["dias_corridos"], errors="coerce").astype("Int64")
    df["di_aa_252"]     = df["di_aa_252"].map(parse_num_br_smart)
    df["pre_aa_360"]    = df["pre_aa_360"].map(parse_num_br_smart)
    df = df.dropna(subset=["dias_corridos","di_aa_252"]).copy()
    df["dias_corridos"] = df["dias_corridos"].astype(int)
    df = df.sort_values("dias_corridos").drop_duplicates("dias_corridos")
    assert df["di_aa_252"].between(0, 100).all(), "Parsing estranho: di_aa_252 fora de 0–100%."
    return df.reset_index(drop=True)

def _switch_to_bmf_iframe(driver):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # Espera qualquer iframe aparecer
    WebDriverWait(driver, SELENIUM_WAIT).until(
        EC.presence_of_element_located((By.TAG_NAME, "iframe"))
    )

    # Tenta o iframe cujo src contém o endpoint LUMIS
    iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")
    target = None
    for fr in iframes:
        try:
            src = fr.get_attribute("src") or ""
            if "lum-taxas-referenciais-bmf-ptBR.asp" in src:
                target = fr
                break
        except Exception:
            pass

    # Se não achou por src, tenta apenas o primeiro (página costuma ter só 1)
    if target is None:
        target = iframes[0]

    driver.switch_to.frame(target)

# ===================== SCRAPER (requests) =====================
def scrape_b3_table(for_date: pd.Timestamp | None = None) -> tuple[pd.DataFrame, pd.Timestamp]:
    """
    Baixa a página LUMIS (onde a tabela de fato existe). Se for_date=None,
    baixa a data mais recente disponível.
    """
    session = requests.Session()
    session.headers.update({
        **HEADERS,
        "Referer": B3_URL,
        "Origin": "https://www.b3.com.br",
    })

    params = {}
    if for_date is not None:
        dmy = for_date.strftime("%d/%m/%Y")
        ymd = for_date.strftime("%Y%m%d")
        params = {"Data": dmy, "Data1": ymd, "slcTaxa": "PRE"}

    r = session.get(IFRAME_URL, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    html = r.text

    df = _parse_table_tb_principal1(html)

    # tenta detectar "Atualizado em: dd/mm/aaaa" no HTML; se não achar, usa for_date ou hoje
    ref_detected = _extract_ref_date_from_text(html) or (for_date or pd.Timestamp.today())
    return df, ref_detected.normalize()


# ===================== SCRAPER (Selenium geral) =====================
def _get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    # fallback: usa Chrome no PATH se não houver webdriver_manager
    try:
        from selenium.webdriver.chrome.service import Service
        service = Service()
        use_service = True
    except Exception:
        from selenium.webdriver.chrome.service import Service
        service = Service()  # exige chromedriver no PATH
        use_service = True

    opts = Options()
    if SELENIUM_HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")

    if use_service:
        driver = webdriver.Chrome(service=service, options=opts)
    else:
        driver = webdriver.Chrome(options=opts)

    driver.set_page_load_timeout(60)
    return driver

# ====== AJUSTE 2: helpers para cookies e iframes ======
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def try_accept_cookies(driver, timeout=8):
    # OneTrust (varia por A/B test)
    try:
        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#onetrust-accept-btn-handler, button#onetrust-accept-btn-handler"))
        )
        driver.execute_script("arguments[0].click()", btn)
        return True
    except Exception:
        # fallback por texto “OK” / “Aceitar”
        try:
            btns = driver.find_elements(By.XPATH, "//button[contains(translate(., 'ACEITAROK', 'aceitarok'),'aceitar') or normalize-space(.)='OK']")
            if btns:
                driver.execute_script("arguments[0].click()", btns[0])
                return True
        except Exception:
            pass
    return False

def switch_to_frame_containing(driver, by, locator, max_depth=4):
    """
    Entra recursivamente no primeiro iframe que contenha (by, locator).
    Se achar, sai já DENTRO do frame correto e retorna True. Senão, False.
    """
    driver.switch_to.default_content()

    def _search(level=0):
        try:
            if driver.find_elements(by, locator):
                return True
        except Exception:
            pass
        if level >= max_depth:
            return False
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        for fr in frames:
            try:
                driver.switch_to.frame(fr)
                if _search(level + 1):
                    return True
            finally:
                driver.switch_to.parent_frame()
        return False

    return _search(0)

# --------- NOVO: Selenium que seta #Data, clica OK e espera 5s ---------
# ====== AJUSTE 3: Selenium p/ DATA ESPECÍFICA (preenche #Data, clica OK, espera 5s) ======
def scrape_b3_specific_date_via_selenium(date_target: pd.Timestamp) -> tuple[pd.DataFrame, pd.Timestamp]:
    # Encaminha para a função acima (mantida por compatibilidade com o seu fluxo)
    return scrape_b3_table_selenium(for_date=date_target)

# ====== AJUSTE 4: Selenium p/ "última data" (sem setar #Data; só acha o iframe e lê a tabela) ======

def scrape_b3_table_selenium(for_date: pd.Timestamp | None = None) -> tuple[pd.DataFrame, pd.Timestamp]:
    """
    Abre a página container da B3, entra no iframe LUMIS e lê a tabela (dia atual se for_date=None).
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver = _get_driver()
    try:
        driver.get(B3_URL)
        _switch_to_bmf_iframe(driver)

        # Se nenhuma data foi solicitada, a tabela já está renderizada
        if for_date is None:
            WebDriverWait(driver, SELENIUM_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table#tb_principal1"))
            )
            html = driver.page_source
            df = _parse_table_tb_principal1(html)
            ref_dt = _extract_ref_date_from_text(html) or pd.Timestamp.today()
            return df, ref_dt.normalize()

        # Caso contrário, setamos a data e apertamos OK com a exigência de aguardar 5s
        dmy = for_date.strftime("%d/%m/%Y")
        ymd = for_date.strftime("%Y%m%d")

        # Preenche #Data e #Data1 via JS para garantir disparo de eventos
        driver.execute_script("""
            const d = arguments[0], d1 = arguments[1];
            const inData = document.querySelector('input#Data');
            const inData1 = document.querySelector('input#Data1');
            if (inData) {
                inData.value = d;
                inData.dispatchEvent(new Event('input', {bubbles:true}));
                inData.dispatchEvent(new Event('change', {bubbles:true}));
            }
            if (inData1) { inData1.value = d1; }
        """, dmy, ymd)

        # Guarda referência da tabela antiga (se já existir) para esperar "staleness"
        old_table = None
        try:
            old_table = driver.find_element(By.CSS_SELECTOR, "table#tb_principal1")
        except Exception:
            pass

        # Clica em OK (submit do form)
        WebDriverWait(driver, SELENIUM_WAIT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.button.expand"))
        ).click()

        # Espera 5s, conforme solicitado
        time.sleep(5)

        # Se havia tabela, espera ela "sumir" (página recarrega dentro do iframe)
        if old_table is not None:
            try:
                WebDriverWait(driver, SELENIUM_WAIT).until(EC.staleness_of(old_table))
            except Exception:
                pass

        # Aguarda a tabela nova
        WebDriverWait(driver, SELENIUM_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table#tb_principal1"))
        )

        # Opcional: valida “Atualizado em: dd/mm/aaaa”
        alvo = dmy
        try:
            WebDriverWait(driver, SELENIUM_WAIT).until(
                EC.text_to_be_present_in_element((By.CSS_SELECTOR, "p.legenda"), alvo)
            )
        except Exception:
            pass

        html = driver.page_source
        df = _parse_table_tb_principal1(html)
        return df, for_date.normalize()
    finally:
        driver.quit()


# ===================== INTERPOLAÇÃO (log-DF sobre DU) =====================
try:
    from scipy.interpolate import PchipInterpolator
    _HAVE_SCIPY = True
except Exception:
    PchipInterpolator = None
    _HAVE_SCIPY = False

def construir_interpolador_di_por_DU(df_taxas: pd.DataFrame, ref_dt: pd.Timestamp, feriados_np: np.ndarray, include_end=False):
    dias = df_taxas['dias_corridos'].to_numpy(dtype=int)
    r_aa = df_taxas['di_aa_252'].to_numpy(dtype=float) / 100.0
    du_nodes = dias_corridos_para_du(ref_dt, dias, feriados_np, include_end=include_end)
    df_nodes = pd.DataFrame({'du': du_nodes, 'r_aa': r_aa}).drop_duplicates('du', keep='last').sort_values('du')
    du = df_nodes['du'].to_numpy(dtype=float); r = df_nodes['r_aa'].to_numpy(dtype=float)
    if len(du) == 0 or du[0] > 0:
        du = np.insert(du, 0, 0.0); r = np.insert(r, 0, r[0] if len(r) else 0.14)
    u = du/252.0; DF_nodes = (1.0 + r)**(-u); y = np.log(DF_nodes)
    if _HAVE_SCIPY: f_logdf = PchipInterpolator(u, y, extrapolate=True)
    else:
        def f_logdf(x): x = np.asarray(x, float); return np.interp(x, u, y, left=y[0], right=y[-1])
    def _to_u(du_val): return np.asarray(du_val, float)/252.0
    def df_at_du(du_val): return np.exp(f_logdf(_to_u(du_val)))
    def di_aa_at_du(du_val):
        uu = _to_u(du_val); DF = np.exp(f_logdf(uu)); out = np.empty_like(uu)
        near0 = np.isclose(uu, 0.0); not0 = ~near0
        out[near0] = r[0] if len(r) else 0.0
        out[not0] = np.power(DF[not0], -1.0/uu[not0]) - 1.0
        return out * 100.0
    def di_daily_at_du(du_val): rr = di_aa_at_du(du_val)/100.0; return np.power(1.0 + rr, 1.0/252.0) - 1.0
    return {"di_aa_at_du": di_aa_at_du, "di_daily_at_du": di_daily_at_du, "df_at_du": df_at_du, "du_max": int(np.nanmax(du))}

def gerar_curva_diaria_por_DU(interp, ref_dt: pd.Timestamp, feriados_np: np.ndarray) -> pd.DataFrame:
    du_grid = np.arange(0, interp["du_max"] + 1, dtype=int)
    ref_np = np.datetime64(ref_dt.normalize().date(), 'D')
    dates_np = np.busday_offset(ref_np, du_grid, roll='forward', holidays=feriados_np)
    dates_pd = pd.to_datetime(dates_np.astype('datetime64[D]'))
    di_aa = interp["di_aa_at_du"](du_grid); di_d = interp["di_daily_at_du"](du_grid)
    return pd.DataFrame({"ref_date": ref_dt.normalize(), "data": dates_pd, "DU": du_grid,
                         "di_aa_252_interp_pct": di_aa, "di_diaria_interp": di_d})

# ===================== BUILD INTERVAL =====================
def build_parquet_interval(start_date: pd.Timestamp, end_date: pd.Timestamp, parquet_path: Path):
    fer_np = _holidays_np(load_feriados_set())
    bdays = business_days_between(start_date, end_date, fer_np)
    rows = []
    for dt64 in bdays:
        target_dt = pd.to_datetime(dt64).normalize()
        print(f"[{target_dt.date()}] coletando...", end=" ")
        try:
            df, ref_dt = scrape_b3_table(for_date=target_dt)
            print("ok (requests).")
        except Exception as e1:
            if not USE_SELENIUM_FALLBACK:
                print(f"falhou (requests): {e1}"); continue
            try:
                df, ref_dt = scrape_b3_specific_date_via_selenium(target_dt)
                print("ok (selenium setando #Data).")
            except Exception as e2:
                print(f"falhou (requests/selenium): {e2}"); continue
        tmp = df.copy(); tmp["ref_date"] = ref_dt; tmp["source"] = "b3"
        rows.append(tmp[["ref_date","dias_corridos","di_aa_252","pre_aa_360","source"]])
        time.sleep(0.6)  # gentileza com o servidor
    if not rows: raise RuntimeError("Nenhuma data coletada no intervalo.")
    base_long = pd.concat(rows, ignore_index=True).sort_values(["ref_date","dias_corridos"])
    base_long.to_parquet(parquet_path, index=False)
    print(f"\nBase construída: {base_long['ref_date'].nunique()} datas | {len(base_long)} linhas")
    print(f"Arquivo salvo: {parquet_path}")

# ===================== LATEST & INTERPOLATE =====================
def run_latest_and_interpolate():
    try:
        df_today, ref_dt = scrape_b3_table(for_date=None)
    except Exception as e_req:
        if USE_SELENIUM_FALLBACK:
            # antes chamava scrape_b3_specific_date_via_selenium(pd.Timestamp.today())
            # agora: só entra no iframe e lê a tabela do dia
            df_today, ref_dt = scrape_b3_table_selenium(for_date=None)
        else:
            raise RuntimeError(f"Falha ao extrair última data: {e_req}")
    # salva/atualiza RAW
    if PARQUET_RAW.exists():
        base = pd.read_parquet(PARQUET_RAW)
        base = base[base["ref_date"] != ref_dt]
        add  = df_today.assign(ref_date=ref_dt, source="b3")[["ref_date","dias_corridos","di_aa_252","pre_aa_360","source"]]
        base = pd.concat([base, add], ignore_index=True)
    else:
        base = df_today.assign(ref_date=ref_dt, source="b3")[["ref_date","dias_corridos","di_aa_252","pre_aa_360","source"]]
    base.sort_values(["ref_date","dias_corridos"]).to_parquet(PARQUET_RAW, index=False)

    fer_np = _holidays_np(load_feriados_set())
    interp = construir_interpolador_di_por_DU(df_today, ref_dt, fer_np, include_end=False)
    curva_du = gerar_curva_diaria_por_DU(interp, ref_dt, fer_np)
    curva_du.to_parquet(PARQUET_INTERP, index=False)
    print(f"Última data: {ref_dt.date()} | Pontos: {len(df_today)} | RAW: {PARQUET_RAW.name} | INTERP: {PARQUET_INTERP.name}")
    return ref_dt, df_today, curva_du

# ===================== NOVO: ESPECÍFICA & INTERPOLATE =====================
def run_specific_date_and_interpolate(spec_date: pd.Timestamp):
    # Sempre via Selenium para obedecer: setar #Data, clicar OK, esperar 5s
    df_day, ref_dt = scrape_b3_specific_date_via_selenium(spec_date)

    # Atualiza RAW (evita duplicar a mesma ref_date)
    if PARQUET_RAW.exists():
        base = pd.read_parquet(PARQUET_RAW)
        base = base[base["ref_date"] != ref_dt]
        add  = df_day.assign(ref_date=ref_dt, source="b3")[["ref_date","dias_corridos","di_aa_252","pre_aa_360","source"]]
        base = pd.concat([base, add], ignore_index=True)
    else:
        base = df_day.assign(ref_date=ref_dt, source="b3")[["ref_date","dias_corridos","di_aa_252","pre_aa_360","source"]]
    base.sort_values(["ref_date","dias_corridos"]).to_parquet(PARQUET_RAW, index=False)

    # Interpola por DU usando a própria SPECIFIC_DATE como ref_date
    fer_np = _holidays_np(load_feriados_set())
    interp = construir_interpolador_di_por_DU(df_day, ref_dt, fer_np, include_end=False)
    curva_du = gerar_curva_diaria_por_DU(interp, ref_dt, fer_np)
    curva_du.to_parquet(PARQUET_INTERP, index=False)

    print(f"Data específica: {ref_dt.date()} | Pontos: {len(df_day)} | RAW: {PARQUET_RAW.name} | INTERP: {PARQUET_INTERP.name}")
    return ref_dt, df_day, curva_du


# ===================== MAIN =====================
if USE_SPECIFIC_DATE:
    ref_dt, df_today, curva_du = run_specific_date_and_interpolate(SPECIFIC_DATE)
elif BUILD_INTERVAL:
    build_parquet_interval(START_DATE, END_DATE, PARQUET_RAW)
    # Opcional: interpola a última página atual depois:
    ref_dt, df_today, curva_du = run_latest_and_interpolate()
else:
    ref_dt, df_today, curva_du = run_latest_and_interpolate()


# Visual quick checks (Jupyter)
print("\n--- Resumo ---")
print("ref_date:", ref_dt.date())
print(df_today.head())
print(curva_du.head())
