from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple, Optional
import pandas as pd
import numpy as np
import re

try:
    from utils.tech_groups import TECH_GROUPS  # type: ignore
except Exception:
    TECH_GROUPS = {}

# Your exact technology codes (case-insensitive matching)
CUSTOM_TECH_SETS = {
    "EXCESS_HEAT": {  # Share of Excess heat
        "DCACC", "HMRBYSEWHP", "SEWACC", "SUPACC"  # include SEWACC if present
    },
    "HP_SW": {        # Seawater heat pumps (used for hp_share)
        "VVNHP",
    },
    "BIOMASS": {      # Everything with Bio* and Wood Chips*
        "KVV1",                  # Bio Oil CHP
        "DECFOHOB", "CSBOHOB",   # Bio Oil HOBs
        "HSLBYP1TOP3",           # Bio Pellets CHP
        "BRISTAB1", "KVV8",      # Wood Chips CHP
    },
    "ELECTRIC_BOILER": {
        "VVHVELHOB",
    },
    "MSW": {          # Waste Incineration CHP
        "BRISTAB2", "HGLDP3468", "LSVTACHP",
    },
}

# If your CSV has slightly different spellings/cases for the same codes,
# add them to the sets above (they are matched in upper-case).


# ---------- NEW: robust scenario-id extractor ----------
def _scenario_id_from_path(p: Path):
    """
    Extract trailing digits from a scenario folder name, e.g. 'LTLE_scenario_051' -> 51.
    If no digits are found, fall back to the folder name string.
    """
    m = re.search(r"(\d+)$", p.name)
    return int(m.group(1)) if m else p.name
# -------------------------------------------------------

_PRODUCTION_CANDIDATES = [
    "AnnualTechnologyProduction.csv",
    "ProdByTechAnn.csv",
    "ProductionByTechnology.csv",
    "AnnualTechProduction.csv",
]

# --- Python 3.9-friendly Optional typing ---
def _read_production(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        cols_lower = {c.lower(): c for c in df.columns}
        tech_col = cols_lower.get("technology") or next(c for c in df.columns if c.lower().startswith("tech"))
        val_col  = cols_lower.get("value")      or next(c for c in df.columns if c.lower().startswith("val"))
        out = df[[tech_col, val_col]].copy()
        out.columns = ["TECHNOLOGY", "VALUE"]
        # Optional: commodity/output marker
        for k in ("commodity", "outputcommodity", "fuel", "carrier"):
            if k in cols_lower:
                out["FUEL"] = df[cols_lower[k]].astype(str)
                break
        # Optional: year
        for k in ("year", "YYYY", "yr"):
            lk = k.lower()
            if lk in cols_lower:
                out["__YEAR__"] = pd.to_numeric(df[cols_lower[lk]], errors="coerce")
                break
        return out
    except Exception:
        return None

def _load_any_production(sdir: Path) -> Optional[pd.DataFrame]:
    for fname in _PRODUCTION_CANDIDATES:
        df = _read_production(sdir / fname)
        if df is not None:
            return df
    return None

def _build_group_resolver(tech_groups: Dict[str, Any]):
    """Support either group->prefixes or tech->group maps."""
    is_prefix_map = False
    if tech_groups:
        first_val = next(iter(tech_groups.values()))
        is_prefix_map = isinstance(first_val, (list, tuple, set))
    if is_prefix_map:
        rules = []
        for grp, pfxs in tech_groups.items():
            for p in pfxs:
                rules.append((str(grp), str(p)))
        def resolve(tech: str) -> str:
            for grp, p in rules:
                if str(tech).startswith(p):
                    return grp
            return "OTHER"
        return resolve
    else:
        tg = {str(k): str(v) for k, v in tech_groups.items()}
        def resolve(tech: str) -> str:
            return tg.get(str(tech), "OTHER")
        return resolve

# --- Python 3.9-friendly return type annotation ---
def _shares_by_group(prod_heat: pd.DataFrame, resolve_group) -> Tuple[pd.DataFrame, float]:
    g = prod_heat.copy()
    g["GROUP"] = g["TECHNOLOGY"].astype(str).map(resolve_group)
    by_group = g.groupby("GROUP", as_index=False)["VALUE"].sum()
    total = by_group["VALUE"].sum()
    if total == 0:
        by_group["SHARE"] = 0.0
    else:
        by_group["SHARE"] = by_group["VALUE"] / total
    return by_group, float(total)

def _hhi_from_shares(shares) -> Tuple[float, float, float]:
    shares = np.array([s for s in shares if s > 0], dtype=float)
    if shares.size == 0:
        return 0.0, 0.0, 0.0
    hhi = float(np.sum(shares ** 2))
    en = (1.0 / hhi) if hhi > 0 else 0.0
    N = shares.size
    if N > 1:
        hhi_norm = float((hhi - 1.0 / N) / (1.0 - 1.0 / N))
    else:
        hhi_norm = 1.0  # single-source
    return hhi, hhi_norm, en

def compute_heat_kpis(raw_root: Path,
                      aggregate: str = "total",   # "total" (sum of all years) or "final"
                      final_year: int | None = None) -> pd.DataFrame:
    """
    EXACT implementation:

    - Load production-by-technology (first file found in _PRODUCTION_CANDIDATES, e.g. ProdByTechAnn.csv)
    - Filter: keep only rows where FUEL contains 'CONVHEAT' (case-insensitive)
    - Exclude: any TECHNOLOGY containing 'DHN' (case-insensitive)
    - Denominator (total_heat): sum of VALUE across all remaining technologies (after the filter/exclusion)
    - Shares:
        share_excess  = (DCACC + HmrbySEWHP + SEWACC + SUPACC) / total_heat
        hp_share      = (VVNHP) / total_heat
        biomass_share = (KVV1 + DECFOHOB + CSBOHOB + HslbyP1toP3 + BristaB1 + KVV8) / total_heat
        share_electric_boiler = (VVHVELHOB) / total_heat
        share_msw     = (BristaB2 + HgldP3468 + LSVTACHP) / total_heat
    - Diversity (HHI / normalized HHI / EN) computed over the buckets
      {EXCESS_HEAT, HP_SW, BIOMASS, ELECTRIC_BOILER, MSW, OTHER}.
    - aggregate="total": sum over all available years; aggregate="final": use only final year
      (either max YEAR found or user-specified final_year).

    Returns: DataFrame with columns
      scenario, total_heat, share_excess, hp_share, biomass_share, share_electric_boiler, share_msw, hhi, hhi_norm, hhi_en
    """
    raw_root = Path(raw_root)

    # exact code sets (case-insensitive)
    EXCESS_SET   = {"DCACC", "HMRBYSEWHP", "SEWACC", "SUPACC"}
    HP_SW_SET    = {"VVNHP"}
    BIOMASS_SET  = {"KVV1", "DECFOHOB", "CSBOHOB", "HSLBYP1TOP3", "BRISTAB1", "KVV8"}
    EBOILER_SET  = {"VVHVELHOB"}
    MSW_SET      = {"BRISTAB2", "HGLDP3468", "LSVTACHP"}

    rows = []

    for sdir in sorted([p for p in raw_root.iterdir() if p.is_dir()]):
        sid = _scenario_id_from_path(sdir)

        prod = _load_any_production(sdir)
        if prod is None or prod.empty:
            continue

        # normalize columns to ensure we have TECHNOLOGY, VALUE, FUEL, YEAR if present
        cols_lower = {c.lower(): c for c in prod.columns}
        tech_col = cols_lower.get("technology") or next(c for c in prod.columns if c.lower().startswith("tech"))
        val_col  = cols_lower.get("value")      or next(c for c in prod.columns if c.lower().startswith("val"))
        fuel_col = cols_lower.get("fuel")       # may be None
        year_col = None
        for k in ("year","yyyy","yr","__year__"):
            if k in cols_lower:
                year_col = cols_lower[k]
                break
        # if sid == 1:  # <-- change this to the scenario ID you want to check
        #     debug_path = Path("analysis") / f"debug_prod_heat1_scen{sid}.csv"
        #     debug_path.parent.mkdir(parents=True, exist_ok=True)
        #     prod.to_csv(debug_path, index=False)
        #     print(f"[DEBUG] Saved filtered production for scenario {sid} → {debug_path}")

        df = prod[[tech_col, val_col] + ([fuel_col] if fuel_col else []) + ([year_col] if year_col else [])].copy()
        df.columns = ["TECHNOLOGY","VALUE"] + (["FUEL"] if fuel_col else []) + (["YEAR"] if year_col else [])
        df["TECH_UP"] = df["TECHNOLOGY"].astype(str).str.upper()



        # 1) Filter FUEL contains 'CONVHEAT' (if FUEL exists)
        if "FUEL" in df.columns:
            df = df[df["FUEL"].astype(str).str.contains("CONVHEAT", case=False, na=False)]

        # 2) Exclude any TECHNOLOGY containing 'DHN'
        df = df[~df["TECH_UP"].str.contains("DHN", case=False, na=False)]

        if df.empty:
            rows.append({
                "scenario": sid,
                "total_heat": 0.0,
                "share_excess": 0.0,
                "hp_share": 0.0,
                "biomass_share": 0.0,
                "share_electric_boiler": 0.0,
                "share_msw": 0.0,
                "hhi": 0.0,
                "hhi_norm": 0.0,
                "hhi_en": 0.0,
            })
            continue

        # 3) Choose years to include
        if aggregate.lower() == "final" and "YEAR" in df.columns:
            y_fin = final_year if final_year is not None else int(df["YEAR"].max())
            df = df[df["YEAR"] == y_fin]

        # DEBUG: save filtered production for one scenario to inspect
        # if sid == 1:  # <-- change this to the scenario ID you want to check
        #     debug_path = Path("analysis") / f"debug_prod_heat_scen{sid}.csv"
        #     debug_path.parent.mkdir(parents=True, exist_ok=True)
        #     df.to_csv(debug_path, index=False)
        #     print(f"[DEBUG] Saved filtered production for scenario {sid} → {debug_path}")

        # 4) Sum production by code over chosen years
        tech_sum = df.groupby("TECH_UP", as_index=False)["VALUE"].sum()
        total_heat = float(tech_sum["VALUE"].sum())
        if total_heat <= 0:
            rows.append({
                "scenario": sid,
                "total_heat": 0.0,
                "share_excess": 0.0,
                "hp_share": 0.0,
                "biomass_share": 0.0,
                "share_electric_boiler": 0.0,
                "share_msw": 0.0,
                "hhi": 0.0,
                "hhi_norm": 0.0,
                "hhi_en": 0.0,
            })
            continue

        def sum_codes(codes: set[str]) -> float:
            if not codes:
                return 0.0
            return float(tech_sum.loc[tech_sum["TECH_UP"].isin(codes), "VALUE"].sum())

        val_excess  = sum_codes(EXCESS_SET)
        val_hp      = sum_codes(HP_SW_SET)
        val_bio     = sum_codes(BIOMASS_SET)
        val_eboiler = sum_codes(EBOILER_SET)
        val_msw     = sum_codes(MSW_SET)

        share_excess            = val_excess  / total_heat
        hp_share                = val_hp      / total_heat
        biomass_share           = val_bio     / total_heat
        share_electric_boiler   = val_eboiler / total_heat
        share_msw               = val_msw     / total_heat

        # diversity over the 5 buckets + OTHER
        known_total = val_excess + val_hp + val_bio + val_eboiler + val_msw
        val_other   = max(0.0, total_heat - known_total)
        parts = [val_excess, val_hp, val_bio, val_eboiler, val_msw, val_other]
        shares = np.array([v/total_heat for v in parts if v > 0], dtype=float)
        if shares.size == 0:
            hhi = hhi_norm = hhi_en = 0.0
        else:
            hhi = float(np.sum(shares**2))
            N = shares.size
            hhi_norm = float((hhi - 1.0/N) / (1.0 - 1.0/N)) if N > 1 else 1.0
            hhi_en = float(1.0 / hhi) if hhi > 0 else 0.0

        rows.append({
            "scenario": sid,
            "total_heat": total_heat,
            "share_excess": share_excess,
            "hp_share": hp_share,
            "biomass_share": biomass_share,
            "share_electric_boiler": share_electric_boiler,
            "share_msw": share_msw,
            "hhi": hhi,
            "hhi_norm": hhi_norm,
            "hhi_en": hhi_en,
        })

    # Safe return
    if not rows:
        cols = ["scenario","total_heat","share_excess","hp_share","biomass_share",
                "share_electric_boiler","share_msw","hhi","hhi_norm","hhi_en"]
        return pd.DataFrame(columns=cols)

    df_out = pd.DataFrame(rows)
    if "scenario" in df_out.columns and not df_out.empty:
        df_out = df_out.sort_values("scenario").reset_index(drop=True)
    return df_out


# ---- LCOH helpers ----
def _df_discount_from_year_column(year_series, r: float = 0.0):
    years = pd.to_numeric(year_series, errors="coerce").values
    if len(years) == 0 or np.all(np.isnan(years)):
        return np.ones_like(years, dtype=float), None
    base_year = int(np.nanmin(years))
    dfs = np.array([1.0 / ((1.0 + r) ** (y - base_year)) if not np.isnan(y) else 1.0 for y in years], dtype=float)
    return dfs, base_year

def compute_lcoh_dhn(raw_root: Path, discount_rate: float = 0.0,
                     dhn_names: Iterable[str] = ("DHNNV", "DHNCS")) -> pd.DataFrame:
    """
    LCOH (per user spec):
      - Numerator: sum of discounted total costs by technology from TotDiscCostByTech.csv,
                   IGNORING technologies with negative costs (drop VALUE < 0).
      - Denominator: total heat through DHN technologies (DHNNV, DHNCS).
                     Sum annual production for DHN techs; if YEAR exists, discount annual production.
    Returns: scenario, lcoh, discounted_heat_dhn, total_pos_disc_cost, denom_discounted (True/False)
    """
    raw_root = Path(raw_root)
    rows = []

    dhn_target = {s.upper() for s in dhn_names}  # exact, case-insensitive match set

    for sdir in sorted([p for p in raw_root.iterdir() if p.is_dir()]):
        # ---------- use robust scenario id ----------
        sid = _scenario_id_from_path(sdir)

        # Numerator: TotDiscCostByTech (keep VALUE > 0)
        cost_file = sdir / "TotDiscCostByTech.csv"
        if not cost_file.exists():
            continue
        cdf = pd.read_csv(cost_file)
        cl = {c.lower(): c for c in cdf.columns}
        tech_col = cl.get("technology") or next(c for c in cdf.columns if c.lower().startswith("tech"))
        val_col  = cl.get("value")      or next(c for c in cdf.columns if c.lower().startswith("val"))
        costs = cdf[[tech_col, val_col]].copy()
        costs.columns = ["TECHNOLOGY", "VALUE"]
        n_neg = int((costs["VALUE"] < 0).sum())
        costs_pos = costs.loc[costs["VALUE"] > 0.0].copy()
        total_pos_disc_cost = float(costs_pos["VALUE"].sum())

        # Denominator: DHN production
        prod = _load_any_production(sdir)
        if prod is None:
            continue

        tech_upper = prod["TECHNOLOGY"].astype(str).str.upper()
        prod_dhn = prod.loc[tech_upper.isin(dhn_target)].copy()
        if prod_dhn.empty:
            mask_fb = prod["TECHNOLOGY"].astype(str).str.contains("|".join(dhn_names), case=False, regex=True)
            prod_dhn = prod.loc[mask_fb].copy()

        if prod_dhn.empty:
            rows.append({
                "scenario": sid,
                "total_pos_disc_cost": total_pos_disc_cost,
                "discounted_heat_dhn": np.nan,
                "lcoh": np.nan,
                "denom_discounted": False,
                "note": "No DHNNV/DHNCS production rows found"
            })
            continue

        # Discount annual production if YEAR available
        year_col = "__YEAR__" if "__YEAR__" in prod_dhn.columns else None
        denom_discounted = False
        if year_col is not None:
            dfs, _ = _df_discount_from_year_column(prod_dhn[year_col], r=discount_rate)
            prod_dhn["DISCOUNT"] = dfs
            denom_discounted = True
        else:
            prod_dhn["DISCOUNT"] = 1.0

        prod_dhn["VAL_DISC"] = prod_dhn["VALUE"] * prod_dhn["DISCOUNT"]
        discounted_heat_dhn = float(prod_dhn["VAL_DISC"].sum())
        lcoh_val = (total_pos_disc_cost / discounted_heat_dhn) if discounted_heat_dhn > 0 else np.nan

        # NEW: convert MSEK/TJ -> SEK/MWh  (factor = 1e6 / 277.777... = 3600)
        lcoh_sek_mwh = lcoh_val * 3600.0 if pd.notna(lcoh_val) else np.nan

        rows.append({
            "scenario": sid,
            "total_pos_disc_cost": total_pos_disc_cost,
            "discounted_heat_dhn": discounted_heat_dhn,
             "lcoh_sek_per_mwh": lcoh_sek_mwh,
            "denom_discounted": bool(denom_discounted),
            "note": f"ignored_negative_cost_rows={n_neg}"
        })

    # Safe return if no scenarios produced rows
    if not rows:
        cols = ["scenario","total_pos_disc_cost","discounted_heat_dhn","lcoh","denom_discounted","note"]
        return pd.DataFrame(columns=cols)

    df_out = pd.DataFrame(rows)
    if "scenario" in df_out.columns and not df_out.empty:
        df_out = df_out.sort_values("scenario").reset_index(drop=True)
    return df_out

