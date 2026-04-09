#!/usr/bin/env python3
"""
Scenario generator for LTLE.xlsx
--------------------------------
Creates modified scenario input files based on uncertainties.

Usage:
    python create_scenario.py --n 1
    python create_scenario.py --n 200
"""

import argparse
import os
import json
import numpy as np
import pandas as pd


def run_scenarios(input_file="HTHE.xlsx", n_scenarios=1, output_dir="Scenarios", seed_base=123):

    # Make output directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "scenario_details"), exist_ok=True)

    # Load full workbook (all sheets)
    xl = pd.ExcelFile(input_file)
    sheets = {name: xl.parse(name) for name in xl.sheet_names}
    df_base = sheets["PARAMETERS"]

    # Scenario logs for CSV
    logs = []

    for s in range(1, n_scenarios + 1):
        rng = np.random.default_rng(seed_base + s)  # unique seed per scenario

        # Copy base PARAMETERS
        df = df_base.copy()

        # -----------------
        # 1) Biomass fuel price
        biomass_techs = ["MINSOLIDBIOP", "MINBIOOIL", "MINFINBIOOIL"]
        multiplier_bio = rng.uniform(0.5, 1.5)
        mask = (df["PARAM"] == "VariableCost") & (df["TECHNOLOGY"].isin(biomass_techs))
        df.loc[mask, "VALUE"] *= multiplier_bio

        # 2) MSW availability
        waste_techs = ["MINWASTE", "IMPWASTE"]
        multiplier_msw = rng.uniform(0.5, 1.0)
        mask = (df["PARAM"] == "TotalTechnologyAnnualActivityUpperLimit") & (df["TECHNOLOGY"].isin(waste_techs))
        df.loc[mask, "VALUE"] *= multiplier_msw

        # 3) Heat pump investment cost
        hp_techs = ["HmrbySEWHP", "SUPACC", "SEWACC", "DCACC", "VVNHP"]
        multiplier_hp = rng.uniform(0.7, 1.4)
        mask = (df["PARAM"] == "CapitalCost") & (df["TECHNOLOGY"].isin(hp_techs))
        df.loc[mask, "VALUE"] *= multiplier_hp

        # 4) Heat demand trajectory
        g = rng.uniform(-0.01, 0.005)
        demand_fuels = ["DEMANDCS", "DEMANDNV"]
        for fuel in demand_fuels:
            mask_fuel = (df["PARAM"] == "SpecifiedAnnualDemand") & (df["FUEL"] == fuel)
            df_fuel = df[mask_fuel].sort_values("YEAR")
            base_year = int(df_fuel["YEAR"].min())
            base_val = df_fuel.loc[df_fuel["YEAR"] == base_year, "VALUE"].values[0]
            for idx, row in df_fuel.iterrows():
                year = int(row["YEAR"])
                df.at[idx, "VALUE"] = base_val * ((1 + g) ** (year - base_year))

        # 5) MSW calorific value
        m_cv = rng.uniform(0.8, 1.0)
        R2050 = rng.uniform(50, 80)
        k = rng.uniform(0.5, 1.5)
        years = np.arange(2023, 2051)
        R = [39 + (R2050 - 39) * ((y - 2023) / (2050 - 2023)) ** k for y in years]
        for i in range(1, len(R)):
            R[i] = max(R[i], R[i - 1])
        AF = {y: (R[i] / 39) * (1 / m_cv) for i, y in enumerate(years)}
        mask = (df["PARAM"] == "InputActivityRatio") & (df["FUEL"] == "WASTE")
        for idx, row in df[mask].iterrows():
            df.at[idx, "VALUE"] = row["VALUE"] * AF[int(row["YEAR"])]

        # 6) Industrial shutdown probability (data centres)
        dc_techs = [f"S{i}" for i in range(213, 245)] + [f"S{i}" for i in range(281, 285)]
        lam = rng.uniform(0.3, 0.6)
        years = np.arange(2031, 2051)
        K = int(rng.poisson(lam * len(years)))
        K = min(K, min(30, len(dc_techs)))  # always ≥20 and ≤30 (or tech limit)
        shutdowns = []
        if K > 0:
            chosen = rng.choice(dc_techs, size=K, replace=False)
            shut_years = rng.integers(2031, 2051, size=K)
            rows = []
            for tech, y in zip(chosen, shut_years):
                for yr in range(y + 1, 2051):
                    rows.append({
                        "PARAM": "TotalTechnologyAnnualActivityUpperLimit",
                        "VALUE": 0.0,
                        "REGION": "STOCKHOLM",
                        "TECHNOLOGY": tech,
                        "YEAR": float(yr)
                    })
                shutdowns.append({"tech": tech, "shutdown_year": int(y)})
            df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

        # 7) Excess heat outages (CapacityFactor)
        years = np.arange(2030, 2051)
        ts = np.arange(1, 289)
        exheat_techs = ["SUPACC", "DCACC", "SEWACC"]

        lam_ex = rng.uniform(0.4, 0.8)  # controls stochastic variability
        expected_outages = int(rng.poisson(lam_ex * len(years) * len(exheat_techs)))

        max_outages = 100 * len(years)

        # Enforce only upper bound
        K = min(expected_outages, max_outages)

        outages = []
        if K > 0:
            chosen_t = rng.choice(exheat_techs, size=K, replace=True)
            chosen_y = rng.choice(years, size=K, replace=True)
            chosen_ts = rng.choice(ts, size=K, replace=True)
            for tech, y, t0 in zip(chosen_t, chosen_y, chosen_ts):
                rec_len = int(rng.integers(3, 5))
                rec_fac = np.linspace(0.25, 1.0, rec_len)
                event_slices = [t0, t0 + 1] + list(range(t0 + 2, t0 + 2 + rec_len))
                event_vals = [0.0, 0.0] + rec_fac.tolist()
                applied = []
                for s_, v in zip(event_slices, event_vals):
                    if s_ <= 288:
                        mask = (df["PARAM"] == "CapacityFactor") & (df["TECHNOLOGY"] == tech) & \
                            (df["YEAR"] == y) & (df["TIMESLICE"] == s_)
                        df.loc[mask, "VALUE"] = v
                        applied.append([int(s_), float(v)])
                outages.append({"tech": tech, "year": int(y), "start_ts": int(t0), "slices": applied})


        # 8) Electricity price spikes (ELGRID VariableCost)
        lam_spike = rng.uniform(0.4, 0.8)
        years = np.arange(2030, 2051)
        ts_allowed = list(range(1, 73)) + list(range(240, 289))

        expected_spikes = int(rng.poisson(lam_spike * len(years)))
        min_spikes = 0
        max_spikes = 60

        # Enforce lower and upper bounds
        K = max(min_spikes, min(expected_spikes, max_spikes))

        spikes = []
        if K > 0:
            chosen_y = rng.choice(years, size=K, replace=True)
            chosen_ts = rng.choice(ts_allowed, size=K, replace=True)
            for y, t0 in zip(chosen_y, chosen_ts):
                L = int(rng.integers(2, 6))
                m0 = rng.uniform(1.5, 3.0)
                rho = rng.uniform(0.6, 0.9)
                applied = []
                for j in range(L):
                    tsj = t0 + j
                    if tsj in ts_allowed:
                        f = m0 * (rho ** j)
                        mask = (df["PARAM"] == "VariableCost") & (df["TECHNOLOGY"] == "ELGRID") & \
                               (df["YEAR"] == y) & (df["TIMESLICE"] == tsj)
                        df.loc[mask, "VALUE"] = df.loc[mask, "VALUE"] * f
                        applied.append([int(tsj), float(f)])
                spikes.append({"year": int(y), "start_ts": int(t0), "length": L,
                               "m0": float(m0), "rho": float(rho), "factors": applied})

        # -----------------
        # Save scenario Excel
        scenario_file = os.path.join(output_dir, f"LTLE_scenario_{330 + s:03d}.xlsx")
        with pd.ExcelWriter(scenario_file, engine="openpyxl") as writer:
            for sheet, data in sheets.items():
                if sheet == "PARAMETERS":
                    df.to_excel(writer, sheet_name="PARAMETERS", index=False)
                else:
                    data.to_excel(writer, sheet_name=sheet, index=False)

        # Log summary row for CSV
        logs.append({
            "scenario": s,
            "seed": seed_base + s,
            "biomass_multiplier": multiplier_bio,
            "msw_multiplier": multiplier_msw,
            "hp_multiplier": multiplier_hp,
            "demand_growth": g,
            "msw_mcv": m_cv,
            "msw_R2050": R2050,
            "msw_k": k,
            "n_shutdowns": len(shutdowns),
            "n_outages": len(outages),
            "n_spikes": len(spikes)
        })

        # Build full JSON log
        scenario_json = {
            "scenario": s,
            "seed": seed_base + s,
            "parameters": {
                "biomass_multiplier": float(multiplier_bio),
                "msw_multiplier": float(multiplier_msw),
                "hp_multiplier": float(multiplier_hp),
                "demand_growth": float(g),
                "msw_mcv": float(m_cv),
                "msw_R2050": float(R2050),
                "msw_k": float(k)
            },
            "events": {
                "dc_shutdowns": shutdowns,
                "exheat_outages": outages,
                "elgrid_spikes": spikes
            }
        }

        details_dir = os.path.join(output_dir, "scenario_details_300")
        os.makedirs(details_dir, exist_ok=True)
        json_file = os.path.join(details_dir, f"scenario_{300 + s:03d}.json")
        with open(json_file, "w") as f:
            json.dump(scenario_json, f, indent=2)

    # Save scenario log CSV
    pd.DataFrame(logs).to_csv(os.path.join(output_dir, "scenario_log.csv"), index=False)
    print(f"Finished {n_scenarios} scenarios. Files saved in {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate LTLE scenarios")
    parser.add_argument("--n", type=int, required=True, help="Number of scenarios to generate")
    args = parser.parse_args()

    run_scenarios(n_scenarios=args.n)
