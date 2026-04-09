from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
from .pp_config import OUTPUT_ROOT
from .tech_groups import TECH_GROUPS

COL_TECH = ["Technology","TECHNOLOGY","tech","Tech"]
COL_YEAR = ["Year","YEAR","Period","PERIOD"]
COL_VAL  = ["Value","VALUE","val","Amount","AMOUNT"]
COL_VAR  = ["Variable","VAR","Result","Name"]

def _find_col(df: pd.DataFrame, cands) -> Optional[str]:
    low = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in df.columns: return c
        if c.lower() in low: return low[c.lower()]
    return None

def _norm(df: pd.DataFrame, want_var: Optional[str]=None, tech_req=True, year_opt=True) -> pd.DataFrame:
    df = df.copy()
    vcol = _find_col(df, COL_VAR)
    if want_var and vcol:
        df = df[df[vcol].astype(str).str.lower()==want_var.lower()]
    tcol = _find_col(df, COL_TECH)
    ycol = _find_col(df, COL_YEAR) if year_opt else None
    vcol2 = _find_col(df, COL_VAL)
    if tech_req and not tcol: raise ValueError("No Technology column")
    if not vcol2: raise ValueError("No Value column")
    out = pd.DataFrame()
    out["Technology"] = df[tcol].astype(str) if tcol else "ALL"
    out["Year"] = pd.to_numeric(df[ycol], errors="coerce") if ycol else np.nan
    out["Value"] = pd.to_numeric(df[vcol2], errors="coerce").fillna(0.0)
    return out

def _rev_map() -> Dict[str,str]:
    rev={}
    for g, codes in TECH_GROUPS.items():
        for c in codes:
            rev[str(c)] = g
    return rev

def sum_by_group(df_norm: pd.DataFrame) -> pd.Series:
    rev=_rev_map()
    tmp=df_norm.copy()
    tmp["group"]=tmp["Technology"].map(lambda t: rev.get(str(t),"Other"))
    return tmp.groupby("group")["Value"].sum()

def list_scenarios(output_root: Path = OUTPUT_ROOT) -> list[Path]:
    root = output_root.resolve()
    return sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("LTLE_scenario_")])

def read_csv_if_exists(p: Path) -> pd.DataFrame:
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

def load_invest_cost_emis_cap(sdir: Path) -> Tuple[pd.Series, pd.Series, Optional[float], Optional[float]]:
    inv = pd.Series(dtype=float)
    nc = read_csv_if_exists(sdir / "NewCapacity.csv")
    if not nc.empty:
        inv = sum_by_group(_norm(nc, want_var=None))
    cap = pd.Series(dtype=float)
    tc = read_csv_if_exists(sdir / "TotCapacityAnn.csv")
    if not tc.empty:
        cap = sum_by_group(_norm(tc, want_var=None))
    cost = None
    c1 = read_csv_if_exists(sdir / "TotDiscCostByTech.csv")
    if not c1.empty:
        cost = float(_norm(c1)["Value"].sum())
    emis = None
    e1 = read_csv_if_exists(sdir / "AnnTechEmission.csv")
    if not e1.empty:
        vcol = _find_col(e1, COL_VAL)
        if vcol:
            emis = float(pd.to_numeric(e1[vcol], errors="coerce").sum())
    return inv, cap, cost, emis
