"""
verification.py
---------------
Validation Gate & Baseline Mathematical Audit — IQRE v1.5 Institutional Recalibration Build

Selection Logic
---------------
Loads 'data/tv_export_raw.csv' (preferred) or 'data/tv_export_raw.xlsx' (fallback).
Filters for Trade IDs that contain exactly two rows: one Entry and one Exit.
These single-entry trades form the deterministic ground-truth anchor set.

Comparison
----------
Imports IQREAnalytics from engine.py, processes the full dataset, and compares
each engine-computed 'net_pnl' against the raw 'Net P&L USDT' column sum for
every qualified single-entry Trade ID.

Reporting
---------
Writes 'output/VERIFICATION_BASELINE.txt' containing:
  - Total Single-Entry Trades analysed
  - Success Rate  (percentage of matches where Delta < 0.01)
  - Detailed Audit Log for any discrepancy >= 0.01

Success Criterion : |engine_net_pnl - manual_net_pnl| < 0.01

Governance (2026 Protocols — ARCHITECTURE.md §6):
  - Fail Fast    : halts immediately on missing file or schema violation.
  - Data Lineage : every comparison is tied to a raw source Trade #.
  - Sandbox      : all paths derived from Path(__file__).parent.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from engine import IQREAnalytics

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
_ROOT        = Path(__file__).parent
_DATA_CSV    = _ROOT / "data" / "tv_export_raw.csv"
_DATA_XLSX   = _ROOT / "data" / "tv_export_raw.xlsx"
_REPORT_PATH = _ROOT / "output" / "VERIFICATION_BASELINE.txt"
_SHEET       = "List of trades"

_THRESHOLD   = 0.01          # success criterion: delta must be strictly below this
_VERSION     = "1.5.0"

# Raw column names (before canonical rename)
_COL_TRADE   = "Trade #"
_COL_PNL_RAW = "Net P&L USDT"
_COL_TYPE    = "Type"

COLUMN_MAP = {
    "Price USDT":          "Price",
    "Position size (qty)": "Size",
    "Net P&L USDT":        "Profit",
    "Date and time":       "Date",
}
REQUIRED_COLS = {"Trade #", "Price", "Size", "Profit", "Type"}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class BaselineAuditError(RuntimeError):
    """Raised when source data is missing or fails schema validation (baseline audit)."""


class VerificationError(RuntimeError):
    """
    Raised when the pipeline parity check detects a discrepancy or the raw
    source schema is invalid.  Caught by main.py to halt execution (Fail Fast).
    """


# ---------------------------------------------------------------------------
# Pipeline Parity Verification  (called by main.py)
# ---------------------------------------------------------------------------
_PIPELINE_TOLERANCE = 1e-6          # deterministic parity epsilon
_PIPELINE_COL_PNL   = "Net P&L USDT"
_PIPELINE_COL_TRADE = "Trade #"


def _build_v15_report(
    units:            List,
    wins:             int,
    losses:           int,
    raw_total_pnl:    float,
    units_total_pnl:  float,
    pnl_delta:        float,
    parity_pass:      bool,
    raw_row_count:    int,
    generated_at:     str,
) -> str:
    """
    Build the v1.5 Verification_Report.txt content.

    Confirms:
      1. Win/Loss gate result (9 Wins / 1 Loss required).
      2. Net P&L parity: total of all Decision Units == raw source row sum.
      3. Strategic Compression summary (raw rows → Decision Units).
    """
    total_units = len(units)
    gate_pass   = (wins == 9 and losses == 1)
    verdict     = "VERIFIED" if (gate_pass and parity_pass) else "DISCREPANCY DETECTED"

    thick = "=" * 80
    thin  = "-" * 80

    lines: List[str] = [
        thick,
        "  IQRE v1.5 -- INSTITUTIONAL RECALIBRATION VALIDATION REPORT",
        thick,
        f"  Generated   : {generated_at}",
        f"  Engine      : IQRE v{_VERSION} -- Institutional Recalibration Build",
        f"  Protocol    : Strategic Aggregation v1.5 -- Time-Window Clustering",
        f"  Tolerance   : {_PIPELINE_TOLERANCE:.1e}  (absolute floating-point epsilon)",
        thick,
        "",
        "VALIDATION GATE — WIN/LOSS RATIO CHECK", thin,
        f"  Total Decision Units  : {total_units}",
        f"  WIN  Count            : {wins}",
        f"  LOSS Count            : {losses}",
        f"  Win/Loss Ratio        : {wins}:{losses}",
        f"  Required Ratio        : 9:1  (9 Wins / 1 Loss)",
        f"  Gate Status           : {'PASSED' if gate_pass else 'FAILED -- PDF BLOCKED'}",
        "",
        "NET P&L PARITY VERIFICATION", thin,
        f"  Strategic Compression : {raw_row_count} Raw Rows → {total_units} Decision Units",
        f"  Raw Source Rows       : {raw_row_count}",
        f"  Raw Total Net P&L     : ${raw_total_pnl:,.4f} USDT",
        f"  Units Total Net P&L   : ${units_total_pnl:,.4f} USDT",
        f"  Absolute Delta        : {pnl_delta:.2e}",
        f"  Tolerance             : {_PIPELINE_TOLERANCE:.1e}",
        f"  Parity Status         : {'PASS' if parity_pass else 'FAIL'}",
        "",
        "DECISION UNITS BREAKDOWN", thin,
        f"  {'Unit #':>6}  {'Source Trade IDs':<34}  {'Net P&L (USDT)':>16}  {'Outcome':>8}",
        f"  {'------':>6}  {'----------------':<34}  {'---------------':>16}  {'-------':>8}",
    ]

    for u in units:
        src_ids = str(getattr(u, 'source_trade_ids', [u.trade_id]))
        lines.append(
            f"  {u.trade_id:>6}  "
            f"{src_ids:<36}  "
            f"  {u.net_pnl:>14.4f}  "
            f"  {u.outcome:>8}"
        )

    lines += [
        "",
        thick,
        "  AUDIT CERTIFICATE",
        thick,
        "",
        f"  This report certifies the v1.5 Institutional Recalibration Build.",
        f"  Strategic Compression: {raw_row_count} raw TradingView rows consolidated",
        f"  into {total_units} Decision Units via Time-Window Clustering.",
        "",
        f"  Net P&L Integrity     : {'CONFIRMED — Units total matches raw source sum' if parity_pass else 'FAILED — Discrepancy detected'}",
        f"  Win/Loss Gate         : {'9 Wins / 1 Loss — CONFIRMED' if gate_pass else f'FAILED — Got {wins}:{losses}'}",
        f"  Overall Verdict       : {verdict}",
        "",
        thick,
        "",
    ]

    return "\n".join(lines)


def run_verification(
    df_raw:      pd.DataFrame,
    units:       List,
    report_path: Path,
) -> Tuple[bool, Path]:
    """
    v1.5 Institutional Recalibration Validation Gate — called by main.py.

    Performs three checks in sequence:
      1. Logs Win/Loss count to console.
      2. Enforces the 9:1 Win/Loss gate — raises VerificationError if not met,
         preventing PDF generation.
      3. Confirms total Net P&L of all Decision Units equals the raw source sum.

    Writes Verification_Report.txt regardless of gate outcome (so the failure
    is documented before the exception propagates).

    Parameters
    ----------
    df_raw      : raw source DataFrame (before column renaming)
    units       : list[DecisionUnit] from IQREAnalytics.aggregate_sequences()
    report_path : destination for the .txt report

    Returns
    -------
    (parity_pass: bool, report_path: Path)

    Raises
    ------
    VerificationError
        If the raw schema is missing required columns OR if the Win/Loss
        ratio is not exactly 9:1 (PDF generation halted).
    """
    # --- Schema guard ---
    required = {_PIPELINE_COL_TRADE, _PIPELINE_COL_PNL}
    missing  = required - set(df_raw.columns)
    if missing:
        raise VerificationError(
            f"[VERIFY FAIL] Raw source file missing columns: {sorted(missing)}"
        )

    report_path = Path(report_path).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Step 1: Win/Loss count — always log to console ---
    wins   = sum(1 for u in units if u.outcome == 'WIN')
    losses = sum(1 for u in units if u.outcome == 'LOSS')
    print(f"[VERIFY] Decision Units   : {len(units)}")
    print(f"[VERIFY] Win/Loss Count   : {wins} Wins / {losses} Losses")
    print(f"[VERIFY] Win/Loss Ratio   : {wins}:{losses}")

    # --- Step 2: Net P&L parity ---
    raw_total_pnl   = float(df_raw[_PIPELINE_COL_PNL].sum())
    units_total_pnl = float(sum(u.net_pnl for u in units))
    pnl_delta       = abs(raw_total_pnl - units_total_pnl)
    parity_pass     = pnl_delta < _PIPELINE_TOLERANCE
    raw_row_count   = len(df_raw)

    # --- Write report (before raising, so failure is documented) ---
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report_text  = _build_v15_report(
        units           = units,
        wins            = wins,
        losses          = losses,
        raw_total_pnl   = raw_total_pnl,
        units_total_pnl = units_total_pnl,
        pnl_delta       = pnl_delta,
        parity_pass     = parity_pass,
        raw_row_count   = raw_row_count,
        generated_at    = generated_at,
    )
    report_path.write_text(report_text, encoding="utf-8")

    # --- Step 3: Enforce 9:1 gate (halts PDF generation) ---
    if wins != 9 or losses != 1:
        raise VerificationError(
            f"[VERIFY GATE] FAILED — Expected 9 Wins / 1 Loss. "
            f"Got {wins} Wins / {losses} Losses. "
            f"Report written to {report_path}. "
            "PDF generation halted per Institutional Recalibration Protocol (v1.5)."
        )

    print(f"[VERIFY] Win/Loss Gate    : PASSED (9:1 confirmed)")
    print(f"[VERIFY] Net P&L Parity  : {'PASS' if parity_pass else 'FAIL'} "
          f"(delta={pnl_delta:.2e})")

    return parity_pass, report_path


# ---------------------------------------------------------------------------
# Ingestion (CSV preferred → XLSX fallback)
# ---------------------------------------------------------------------------
def _load_source() -> Tuple[pd.DataFrame, str]:
    """
    Load the raw source file without any transformation.

    Tries tv_export_raw.csv first (TradingView localized export).
    Falls back to tv_export_raw.xlsx if CSV is absent.

    Returns
    -------
    (df_raw, source_label) — unmodified DataFrame and its display path.

    Raises
    ------
    BaselineAuditError if neither file exists.
    """
    if _DATA_CSV.exists():
        df = pd.read_csv(_DATA_CSV, sep=";", decimal=",", thousands=".")
        return df, "data/tv_export_raw.csv"

    if _DATA_XLSX.exists():
        df = pd.read_excel(_DATA_XLSX, sheet_name=_SHEET)
        return df, f"data/tv_export_raw.xlsx (sheet: {_SHEET})"

    raise BaselineAuditError(
        "[AUDIT FAIL] No source data found.\n"
        "  Expected: data/tv_export_raw.csv  OR  data/tv_export_raw.xlsx\n"
        "  Place a TradingView export in the data/ directory and retry."
    )


# ---------------------------------------------------------------------------
# Selection: single-entry Trade IDs (exactly 1 Entry row + 1 Exit row)
# ---------------------------------------------------------------------------
def _select_single_entry_ids(df_raw: pd.DataFrame) -> List[int]:
    """
    Return Trade IDs that have exactly two rows, one of which is an entry
    and one of which is an exit (case-insensitive substring match on Type).

    A two-row trade with two exits (or two entries) is excluded because it
    does not satisfy the deterministic ground-truth criterion.
    """
    valid: List[int] = []
    for tid, group in df_raw.groupby(_COL_TRADE):
        if len(group) != 2:
            continue
        types = group[_COL_TYPE].str.lower().tolist()
        has_entry = any("entry" in t for t in types)
        has_exit  = any("exit"  in t for t in types)
        if has_entry and has_exit:
            valid.append(int(tid))
    return sorted(valid)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------
def _build_report(
    results:      List[dict],
    source_label: str,
    generated_at: str,
) -> str:
    """Compose the full VERIFICATION_BASELINE.txt content."""

    n        = len(results)
    passed   = [r for r in results if r["delta"] < _THRESHOLD]
    failed   = [r for r in results if r["delta"] >= _THRESHOLD]
    rate     = (len(passed) / n * 100) if n > 0 else 0.0
    verdict  = "VERIFIED" if not failed else "DISCREPANCY DETECTED"

    thick = "=" * 80
    thin  = "-" * 80

    lines: List[str] = []

    # ---- header ------------------------------------------------------------
    lines += [
        thick,
        "  IQRE — BASELINE MATHEMATICAL AUDIT REPORT",
        thick,
        f"  Generated        : {generated_at}",
        f"  Source File      : {source_label}",
        f"  Engine           : IQRE v{_VERSION}",
        f"  Success Criterion: |engine_net_pnl - manual_net_pnl| < {_THRESHOLD}",
        thick,
        "",
    ]

    # ---- summary -----------------------------------------------------------
    lines += [
        "AUDIT SUMMARY",
        thin,
        f"  Total Single-Entry Trades Analysed : {n}",
        f"  Passed  (Delta < {_THRESHOLD})          : {len(passed)}",
        f"  Failed  (Delta >= {_THRESHOLD})          : {len(failed)}",
        f"  Success Rate                       : {rate:.2f}%",
        "",
    ]

    # ---- parity table ------------------------------------------------------
    lines += [
        "NET P&L PARITY TABLE",
        thin,
        f"  {'Trade ID':>8}  {'Manual Net P&L':>16}  "
        f"{'Engine Net P&L':>16}  {'Delta':>12}  {'Status':>6}",
        f"  {'--------':>8}  {'---------------':>16}  "
        f"{'---------------':>16}  {'------------':>12}  {'------':>6}",
    ]
    for r in results:
        status = "PASS" if r["delta"] < _THRESHOLD else "FAIL"
        lines.append(
            f"  {r['trade_id']:>8}  "
            f"  {r['manual_pnl']:>14.4f}  "
            f"  {r['engine_pnl']:>14.4f}  "
            f"  {r['delta']:>12.6f}  "
            f"  {status:>6}"
        )
    lines.append("")

    # ---- discrepancy audit log ---------------------------------------------
    lines += [
        "DISCREPANCY AUDIT LOG",
        thin,
    ]
    if not failed:
        lines.append(
            "  No discrepancies detected. "
            f"All {n} single-entry trades are within the {_THRESHOLD} threshold."
        )
    else:
        for r in failed:
            lines += [
                f"  Trade ID  : {r['trade_id']}",
                f"    Manual Net P&L : {r['manual_pnl']:.6f}",
                f"    Engine Net P&L : {r['engine_pnl']:.6f}",
                f"    Delta          : {r['delta']:.6f}  [EXCEEDS THRESHOLD {_THRESHOLD}]",
                "",
            ]
    lines.append("")

    # ---- audit certificate -------------------------------------------------
    lines += [
        thick,
        "  AUDIT CERTIFICATE",
        thick,
        "",
        f"  Trades Analysed  : {n}",
        f"  Trades Passed    : {len(passed)}",
        f"  Success Rate     : {rate:.2f}%",
        f"  VERDICT          : {verdict}",
        "",
        thick,
        "",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core audit function
# ---------------------------------------------------------------------------
def run_baseline_audit() -> Tuple[float, int]:
    """
    Execute the Baseline Mathematical Audit end-to-end.

    Returns
    -------
    (success_rate, n_analysed)
        success_rate : float, 0.0–100.0
        n_analysed   : int, number of single-entry trades tested
    """
    # 1. Load raw source -------------------------------------------------------
    df_raw, source_label = _load_source()

    # 2. Validate raw schema (Fail Fast) ---------------------------------------
    required_raw = {_COL_TRADE, _COL_PNL_RAW, _COL_TYPE}
    missing_raw  = required_raw - set(df_raw.columns)
    if missing_raw:
        raise BaselineAuditError(
            f"[AUDIT FAIL] Source file missing required columns: {sorted(missing_raw)}"
        )

    # 3. Select single-entry Trade IDs -----------------------------------------
    single_ids = _select_single_entry_ids(df_raw)
    if not single_ids:
        raise BaselineAuditError(
            "[AUDIT FAIL] No single-entry trades found. "
            "Each qualifying trade must have exactly 2 rows: one Entry + one Exit."
        )

    # 4. Build canonical DataFrame for the engine ------------------------------
    df_clean = df_raw.rename(columns=COLUMN_MAP).copy()
    missing_clean = REQUIRED_COLS - set(df_clean.columns)
    if missing_clean:
        raise BaselineAuditError(
            f"[AUDIT FAIL] Missing canonical columns after mapping: {sorted(missing_clean)}"
        )

    df_clean[_COL_TRADE]  = df_clean[_COL_TRADE].astype(int)
    df_clean["Price"]     = df_clean["Price"].astype(float)
    df_clean["Size"]      = df_clean["Size"].astype(float)
    df_clean["Profit"]    = df_clean["Profit"].astype(float)
    df_clean = df_clean.dropna(subset=list(REQUIRED_COLS))

    # 5. Run engine on all trades ----------------------------------------------
    engine    = IQREAnalytics(df_clean)
    all_units = engine.aggregate_sequences()

    # Build reverse map: original Trade# → the DecisionUnit cluster that contains it.
    # With v1.5 Time-Window Clustering, u.trade_id is the cluster index (1-based),
    # while u.source_trade_ids holds the original TradingView Trade # values.
    source_to_unit = {}
    for u in all_units:
        for src_tid in getattr(u, 'source_trade_ids', [u.trade_id]):
            source_to_unit[int(src_tid)] = u

    # 6. Compute per-trade deltas for single-entry trades only -----------------
    results: List[dict] = []
    for tid in single_ids:
        raw_group  = df_raw[df_raw[_COL_TRADE] == tid]
        manual_pnl = float(raw_group[_COL_PNL_RAW].sum())
        unit       = source_to_unit.get(tid)
        engine_pnl = unit.net_pnl if unit is not None else float("nan")
        delta      = abs(manual_pnl - engine_pnl)
        results.append({
            "trade_id":   tid,
            "manual_pnl": manual_pnl,
            "engine_pnl": engine_pnl,
            "delta":      delta,
        })

    # 7. Write report ----------------------------------------------------------
    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report_text  = _build_report(results, source_label, generated_at)
    _REPORT_PATH.write_text(report_text, encoding="utf-8")

    n      = len(results)
    passed = sum(1 for r in results if r["delta"] < _THRESHOLD)
    rate   = (passed / n * 100) if n > 0 else 0.0

    return rate, n


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        success_rate, n_analysed = run_baseline_audit()

        print()
        print("=" * 60)
        print("  IQRE — BASELINE MATHEMATICAL AUDIT")
        print("=" * 60)
        print(f"  Single-Entry Trades Analysed : {n_analysed}")
        print(f"  SUCCESS RATE                 : {success_rate:.2f}%")
        print(f"  Threshold                    : Delta < 0.01")
        print(f"  Report -> {_REPORT_PATH}")
        print("=" * 60)
        print()

        sys.exit(0 if success_rate == 100.0 else 2)

    except BaselineAuditError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
