#import os
#import sys
#from pathlib import Path
#import re

#PROJECT_ROOT = Path(os.getcwd()).resolve()
#if str(PROJECT_ROOT) not in sys.path:
  #  sys.path.append(str(PROJECT_ROOT))
#os.chdir(PROJECT_ROOT)


#import requests
import pandas as pd
from pathlib import Path


# --- Directories ---
Path("logs").mkdir(exist_ok=True)  # ensure a logs folder exists
Path("data").mkdir(exist_ok=True)


"""
Unit Type Inference Utility
---------------------------
Infers a clean 'unit_type' label from indicator names (no API calls).

Usage:
    from unit_types import add_unit_types
    df = add_unit_types(df, column="indicator_name")
"""



# --------- Core rule-based inference ----------
def infer_unit_from_name(name: str) -> str:
    """
    Infer a unit type (e.g., Percent, Index, Currency) based on keywords in the indicator name.
    Returns a short, consistent label suitable for visualization and reporting.
    """
    if not isinstance(name, str) or not name.strip():
        return "Unknown"

    s = name.lower()

    # Binary or yes/no
    if "(1=yes" in s or "1=yes" in s or "0=no" in s:
        return "Binary (1/0)"

    # Percentages and proportions
    if "%" in s or "percent" in s or "share of" in s:
        return "Percent (%)"

    # Rates per population
    if "per 100,000" in s or "per 100000" in s:
        return "Rate per 100,000"
    if "per 1,000" in s or "per 1000" in s:
        return "Rate per 1,000"

    # Currency
    if any(tok in s for tok in ["us$", "usd", "current us$", "current $", "dollar", "current prices"]):
        return "Currency (US$)"

    # Index / score / scale
    if any(tok in s for tok in ["index", "score", "scale"]):
        return "Index / Score"

    # Ratios or parity
    if "ratio" in s or "parity" in s or "gpi" in s:
        return "Ratio"

    # Years or ages
    if "year" in s or "years" in s:
        return "Years"

    # Counts / population
    if any(tok in s for tok in ["number", "population", "people", "count"]):
        return "Count"

    # Default fallback
    return "Unknown"


# --------- Helper to apply inference to a DataFrame ----------
def add_unit_types(df: pd.DataFrame, column: str = "indicator_name") -> pd.DataFrame:
    """
    Add a 'unit_type' column to a DataFrame based on indicator names.

    Args:
        df: DataFrame containing an indicator name column.
        column: Column name with indicator names (default = 'indicator_name').

    Returns:
        DataFrame with an additional 'unit_type' column.
    """
    df = df.copy()
    df["unit_type"] = df[column].apply(infer_unit_from_name)
    return df



