from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Tuple
from .pp_config import AN_DIR, OUTPUT_ROOT, INV_PATH, META_PATH
from .results_loader import list_scenarios, load_invest_cost_emis_cap

def build_features_from_output(output_root: Path = OUTPUT_ROOT) -> Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    inv_rows, cap_rows, meta_rows = [], [], []
    for sdir in list_scenarios(output_root):
        stem = sdir.name
        inv, cap, cost, emis = load_invest_cost_emis_cap(sdir)
        inv.name = stem; cap.name = stem
        if not inv.empty: inv_rows.append(inv)
        if not cap.empty: cap_rows.append(cap)
        meta_rows.append({"scenario": stem, "system_cost": cost, "emissions": emis})
    M_inv = pd.DataFrame(inv_rows).fillna(0.0).sort_index() if inv_rows else pd.DataFrame()
    M_cap = pd.DataFrame(cap_rows).fillna(0.0).sort_index() if cap_rows else pd.DataFrame()
    meta  = pd.DataFrame(meta_rows).set_index("scenario").sort_index()
    return M_inv, meta, M_cap

def write_feature_tables(M_inv: pd.DataFrame, meta: pd.DataFrame, M_cap: pd.DataFrame):
    AN_DIR.mkdir(parents=True, exist_ok=True)
    if not M_inv.empty:
        M_inv.to_csv(INV_PATH, index_label="scenario")
    meta.to_csv(META_PATH)
    if not M_cap.empty:
        M_cap.to_csv(AN_DIR / "features_capacity.csv", index_label="scenario")
