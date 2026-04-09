from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from utils.tech_groups import TECH_GROUPS, NON_COMBUSTION_BUCKETS, COMBUSTION_BUCKETS

HEAT_FUEL_TOKEN = "CONVHEAT"
EXCLUDE_TECH_SUBSTRINGS = {"DHN"}
TO_GWH = 0.27777778

def _rev_map() -> dict[str,str]:
    rev={}
    for g, codes in TECH_GROUPS.items():
        for c in codes: rev[str(c)] = g
    return rev

def compute_heat_shares_for_scenario(sdir: Path):
    src = sdir / "ProdByTechAnn.csv"
    if not src.exists(): return pd.DataFrame(), pd.DataFrame()
    df = pd.read_csv(src)

    need = {"FUEL","TECHNOLOGY","YEAR","VALUE"}
    if not need.issubset(df.columns): raise ValueError(f"{src} missing {need - set(df.columns)}")

    df = df[df["FUEL"].astype(str).str.contains(HEAT_FUEL_TOKEN, case=False, na=False)].copy()
    tlow = df["TECHNOLOGY"].astype(str).str.lower()
    for token in EXCLUDE_TECH_SUBSTRINGS:
        df = df[~tlow.str.contains(token.lower(), na=False)]

    df["GWh"] = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0.0) * TO_GWH
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce")
    rev = _rev_map()
    df["group"] = df["TECHNOLOGY"].astype(str).map(lambda t: rev.get(t,"Other"))

    grouped = df.groupby(["YEAR","group"], as_index=False)["GWh"].sum()
    wide = grouped.pivot_table(index="YEAR", columns="group", values="GWh", aggfunc="sum", fill_value=0.0).reset_index()

    wide["non_combustion_GWh"] = wide.reindex(columns=[c for c in wide.columns if c in NON_COMBUSTION_BUCKETS], fill_value=0.0).sum(axis=1)
    wide["combustion_GWh"]     = wide.reindex(columns=[c for c in wide.columns if c in COMBUSTION_BUCKETS], fill_value=0.0).sum(axis=1)
    group_cols = [c for c in wide.columns if c not in {"YEAR","non_combustion_GWh","combustion_GWh"}]
    wide["total_heat_GWh"] = wide[group_cols].sum(axis=1)

    shares = wide[["YEAR","non_combustion_GWh","combustion_GWh","total_heat_GWh"]].copy()
    denom = shares["total_heat_GWh"].replace(0, np.nan)
    shares["share_non_combustion"] = (shares["non_combustion_GWh"]/denom).fillna(0.0)
    shares["share_combustion"]     = (shares["combustion_GWh"]/denom).fillna(0.0)
    return wide, shares

def compute_all_heat_shares(output_root: str | Path = "Output_Data", summary_year: int | None = None):
    out_root = Path(output_root)
    scenarios = sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("LTLE_scenario_")])
    rows_shares, rows_break = [], []
    for sdir in scenarios:
        breakdown, shares = compute_heat_shares_for_scenario(sdir)
        if breakdown.empty and shares.empty: continue
        breakdown.insert(0, "scenario", sdir.name)
        shares.insert(0, "scenario", sdir.name)
        rows_break.append(breakdown.rename(columns={"YEAR":"year"}))
        rows_shares.append(shares.rename(columns={"YEAR":"year"}))
    if not rows_shares:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    shares_all = pd.concat(rows_shares, ignore_index=True)
    breakdown_all = pd.concat(rows_break, ignore_index=True)

    summaries=[]
    for stem, sub in shares_all.groupby("scenario"):
        sub=sub.sort_values("year")
        if summary_year is not None and summary_year in set(sub["year"]):
            row=sub[sub["year"]==summary_year].iloc[-1]
        else:
            row=sub.iloc[-1]
        summaries.append(row)
    summary_df = pd.DataFrame(summaries).reset_index(drop=True)
    return shares_all, summary_df, breakdown_all
