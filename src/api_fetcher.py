import requests
from requests.adapters import HTTPAdapter     # For connection pooling + retries
from urllib3.util.retry import Retry          # Retry strategy for robustness

import pandas as pd
#import time
import logging
from pathlib import Path
from unit_types import add_unit_types

# --- Directories ---
Path("logs").mkdir(exist_ok=True)  # ensure a logs folder exists
Path("data").mkdir(exist_ok=True)


# --- Config ---

BASE_URL = "https://api.worldbank.org/v2/country/{countries}/indicator/{indicator}"
country_codes = ["EGY", "MAR", "SAU", "JOR", "TUN", "IRQ", "YEM", "OMN", "QAT", "BHR", "KWT", "DZA", "LBY"]
countries_str = ";".join(country_codes)

selected_indicators ={
    "SG.APL.PSPT.EQ":"A woman can apply for a passport in the same way as a man (1=yes; 0=no)",
    "SG.LOC.LIVE.EQ":"A woman can choose where to live in the same way as a man (1=yes; 0=no)",
    "SG.LAW.INDX":"Women Business and the Law Index Score (scale 1-100)",
    "SH.STA.MMRT":"Maternal mortality ratio (modeled estimate, per 100,000 live births)",
    "SE.ADT.1524.LT.FE.ZS":"Literacy rate, youth female (% of females ages 15-24)",
    "SE.SEC.CUAT.UP.FE.ZS":"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)",
    "SG.NGT.WORK.EQ":"A woman can work at night in the same way as a man (1=yes; 0=no)",
    "SG.OBT.DVRC.EQ":"A woman can obtain a judgment of divorce in the same way as a man (1=yes; 0=no)",
    "SL.SRV.EMPL.MA.ZS":"Employment in services, male (% of male employment) (modeled ILO estimate)",
    "SG.IND.WORK.EQ":"A woman can work in an industrial job in the same way as a man (1=yes; 0=no)",
    "SP.DYN.CBRT.IN":"Birth rate, crude (per 1,000 people)",
    "FP.CPI.TOTL.ZG":"Inflation, consumer prices (annual %)",
    "SL.UEM.TOTL.MA.ZS":"Unemployment, male (% of male labor force) (modeled ILO estimate)",
    "SL.TLF.CACT.FE.ZS":"Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate)",
    "NY.GDP.PCAP.CD":"GDP per capita (current US$)",
    "SE.ADT.LITR.MA.ZS":"Literacy rate, adult male (% of males ages 15 and above)",
    "SP.DYN.LE00.MA.IN":"Life expectancy at birth, male (years)",
    "SG.CNT.SIGN.EQ":"A woman can sign a contract in the same way as a man (1=yes; 0=no)",
    "SH.HIV.1524.MA.ZS":"Prevalence of HIV, male (% ages 15-24)",
    "SG.DNG.WORK.DN.EQ":"A woman can work in a job deemed dangerous in the same way as a man (1=yes; 0=no)",
    "SG.OPN.BANK.EQ":"A woman can open a bank account in the same way as a man (1=yes; 0=no)",
    "SG.CTR.TRVL.EQ":"A woman can travel outside the country in the same way as a man (1=yes; 0=no)",
    "SG.HME.TRVL.EQ":"A woman can travel outside her home in the same way as a man (1=yes; 0=no)",
    "SL.SRV.EMPL.FE.ZS":"Employment in services, female (% of female employment) (modeled ILO estimate)",
    "SP.ADO.TFRT":"Adolescent fertility rate (births per 1,000 women ages 15-19)",
    "SE.ADT.LITR.ZS":"Literacy rate, adult total (% of people ages 15 and above)",
    "SE.SEC.CUAT.UP.MA.ZS":"Educational attainment, at least completed upper secondary, population 25+, male (%) (cumulative)",
    "SL.UEM.TOTL.FE.ZS":"Unemployment, female (% of female labor force) (modeled ILO estimate)",
    "SG.GET.JOBS.EQ":"A woman can get a job in the same way as a man (1=yes; 0=no)",
    "SG.BUS.REGT.EQ":"A woman can register a business in the same way as a man (1=yes; 0=no)",
    "SH.HIV.1524.FE.ZS":"Prevalence of HIV, female (% ages 15-24)",
    "SE.ADT.1524.LT.MA.ZS":"Literacy rate, youth male (% of males ages 15-24)",
    "SG.HLD.HEAD.EQ":'A woman can be "head of household" in the same way as a man (1=yes; 0=no)',
    "SG.PEN.SXHR.EM":"Criminal penalties or civil remedies exist for sexual harassment in employment (1=yes; 0=no)",
    "SG.REM.RIGT.EQ":"A woman has the same rights to remarry as a man (1=yes; 0=no)",
    "SG.GEN.MNST.ZS":"Proportion of women in ministerial level positions (%)",
    "SP.DYN.LE00.FE.IN":"Life expectancy at birth, female (years)",
    "SE.ADT.LITR.FE.ZS":"Literacy rate, adult female (% of females ages 15 and above)",
    "SL.TLF.CACT.MA.ZS":"Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)",
    "SG.GEN.PARL.ZS":"Proportion of seats held by women in national parliaments (%)",
    "SE.ENR.SECO.FM.ZS":"School enrollment, secondary (gross), gender parity index (GPI)",
    "SP.M15.2024.FE.ZS":"Women who were first married by age 15 (% of women ages 20-24)",
    "SP.M18.2024.FE.ZS":"Women who were first married by age 18 (% of women ages 20-24)",
    "SP.DYN.CONU.ZS":"Contraceptive prevalence, any method (% of married women ages 15-49)",
    "SP.DYN.CONM.ZS":"Contraceptive prevalence, any modern method (% of married women ages 15-49)"
    
}

params = {"format": "json", "per_page": 1000, "date":"2000:2024"}


# ----------------- Session with retries (fast + robust) -----------------
session = requests.Session()

retry = Retry(
    total=3,                      # try up to 3 times
    backoff_factor=0.5,           # wait 0.5s, then 1s, then 2s between retries
    status_forcelist=[429, 500, 502, 503, 504],  # retry on these HTTP codes
    allowed_methods=["GET"]       # only retry GET requests
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("https://", adapter)
session.headers.update({"User-Agent": "MENA-Tableau-Fetcher/1.0"})




# ============================================================
# Helper function
# ============================================================

def fetch_one_page(url:str, params:dict) -> dict | None:
    
    """
    Fetch one page of results with retry logic and polite delay.
    """

    try:
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status() #fail if not HTTP error
        return r.json() #top level is a dict for this API
    except requests.exceptions.RequestException as e:
        logging.warning(f"Fetch failed: {url} | {e}")
        return None

# ============================================================
# Main extraction loop
# ============================================================
if __name__ == "__main__":


    for code in country_codes:
        # --------------------------------------------------------
        # Set up a logger unique to this country
        # --------------------------------------------------------
        
        logging.basicConfig(
            level = logging.INFO,
            format = "%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(f"logs/fetch.log", mode = "w"),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger("WorldBankFetcher")
        
        
        # --------------------------------------------------------
        # Begin fetching
        # --------------------------------------------------------
        
        all_indicators_df = []
        
        N = len(selected_indicators)
        logger.info(f"Fetching {N} indicators for {len(country_codes)} countries ...")
        

        
        # ⭐︎ fetch exactly the indicators you care about (fast, clean, non-empty).
        for i, (ind_code, ind_name) in enumerate(selected_indicators.items(), start=1):
            logger.info(f"[{i}/{N}] Fetching {ind_name} ({ind_code}) ...")
            url = BASE_URL.format(countries= countries_str, indicator=ind_code)
            page =1
            pages = []
            
            while True:
                query = {**params, "page": page}
                payload = fetch_one_page(url, query)
                
                # Stop if no data array or empty data
                if not payload or len(payload) < 2 or not payload[1]:
                    logger.warning(f"⚠️ No data for {ind_code} (page {page})")
                    break
                
                df = pd.DataFrame(payload[1])[["country","indicator","date","value"]]
            
                df["country_id"] = df["country"].apply(lambda x: x.get("id") if isinstance(x,dict) else None)
                df["country_name"] = df["country"].apply(lambda x: x.get("value") if isinstance(x,dict) else None)
                df["indicator_id"] = df["indicator"].apply(lambda x: x.get("id") if isinstance(x,dict) else None)
                df["indicator_name"] = df["indicator"].apply(lambda x: x.get("value") if isinstance(x,dict) else None)
            
                df = df.drop(columns=["country","indicator"]).rename(columns={"date":"year"})
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
            
                pages.append(df)
                page +=1
            
                if pages:
                    df_indicator = pd.concat(pages, ignore_index=True)
                    df_indicator.to_csv(f"data/IND_{ind_code}.csv", index=False)
                    logger.info(f"Saved {ind_code} ({len(df_indicator)} rows)")
                    all_indicators_df.append(df_indicator)
                else:
                    logger.warning(f"⚠️ No data fetched for {ind_code}")

        
        # ----------------- Combine all + (optional) split per country -----------------
        
        if all_indicators_df:
            df_all = pd.concat(all_indicators_df, ignore_index=True)
            df_all = add_unit_types(df_all, column="indicator_name")
            df_all.to_csv("data/all_countries_selected.csv", index=False)
            
            # Optional per-country files for Tableau
            for code in country_codes:
                df_c = df_all[df_all["country_id"] == code]
                if not df_c.empty:
                    df_c.to_csv(f"data/{code}_selected.csv", index=False)
            
            logger.info(f"Finished! Combined dataset saved → data/all_countries_selected.csv")
        else:
            logger.warning("\n⚠️ No data fetched for any indicator.")
        

        
    
        

    
