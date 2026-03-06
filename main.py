"""
main.py
-------
Entry Point -- Ingestion, Validation, and Full-Pipeline Orchestration
IQRE (Institutional Quantitative Reporting Engine) | Fase 1 de 3

Pipeline: Ingest -> Aggregate -> Verify -> Export (PDF & XLSX)
    main.py (ingestion) -> engine.py (aggregation)
    -> verification.py (parity audit) -> tear_sheet_gen.py / audit_workbook.py

Supported source formats:
    .xlsx : TradingView multi-sheet workbook  (primary)
    .csv  : TradingView localized CSV export  (semicolon delimiter,
                                               comma decimal separator)

Governance (2026 Protocols -- ARCHITECTURE.md §6):
    - Architecture as Authority : only logic defined in ARCHITECTURE.md is implemented.
    - Data Lineage              : every metric is traceable to its source row.
    - Security Sandbox          : all paths are relative to this file's directory.
    - Fail Fast                 : schema mismatches raise AuditInconsistencyError.
    - PII Redaction             : no account IDs or sensitive fields are surfaced.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from tabulate import tabulate

from engine import IQREAnalytics
from tear_sheet_gen import generate_report
from audit_workbook import generate_audit_workbook
from verification import run_verification, VerificationError
from visual_generator import VisualAnalytics, VisualAnalyticsError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ROOT            = Path(__file__).parent          # portable: all paths relative to here
DATA_PATH        = _ROOT / "data" / "tv_export_raw.xlsx"
SHEET_NAME       = "List of trades"
OUTPUT_PATH      = _ROOT / "output" / "IQRE_Institutional_Audit_FINAL_v1.5.pdf"
AUDIT_PATH       = _ROOT / "output" / "institutional_audit_data.xlsx"
VERIFY_PATH      = _ROOT / "output" / "Verification_Report.txt"
AUDIT_LOG_PATH   = _ROOT / "output" / "audit_log.txt"
PLOTS_DIR        = _ROOT / "output" / "temp_plots"

# Canonical schema required by engine.py (ARCHITECTURE.md §5)
REQUIRED_COLUMNS = {"Trade #", "Price", "Size", "Profit", "Type"}

# Mapping from raw TradingView export headers → canonical schema
COLUMN_MAP = {
    "Price USDT": "Price",
    "Position size (qty)": "Size",
    "Net P&L USDT": "Profit",
    "Date and time": "Date",
}


# ---------------------------------------------------------------------------
# Audit guard (Fail Fast — ARCHITECTURE.md §6)
# ---------------------------------------------------------------------------
class AuditInconsistencyError(RuntimeError):
    """Raised when the input data violates the expected schema."""


def _validate_schema(df: pd.DataFrame) -> None:
    """Ensure all required canonical columns are present; abort otherwise."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise AuditInconsistencyError(
            f"[AUDIT FAIL] Missing required columns after mapping: {sorted(missing)}. "
            "Execution halted — no data is guessed."
        )


# ---------------------------------------------------------------------------
# Ingestion & cleaning
# ---------------------------------------------------------------------------
def _read_raw(path: Path, sheet: str) -> pd.DataFrame:
    """
    Read the source file without any transformation, preserving original
    column names.  Supports .xlsx and .csv (localized European format).
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        # Localized TradingView CSV: semicolon delimiter, comma decimal separator
        return pd.read_csv(path, sep=";", decimal=",", thousands=".")
    # Default: Excel multi-sheet workbook
    return pd.read_excel(path, sheet_name=sheet)


def load_and_clean(path: Path, sheet: str) -> pd.DataFrame:
    """
    Read the TradingView export (Excel or localized CSV), rename columns to
    the canonical schema, validate, clean, and return a DataFrame ready for
    the analytics engine.

    Supported formats
    -----------------
    .xlsx  : multi-sheet TradingView workbook (reads the specified sheet)
    .csv   : TradingView localized export     (sep=';', decimal=',')
    """
    if not path.exists():
        raise AuditInconsistencyError(
            f"[AUDIT FAIL] Data file not found: {path}"
        )

    df = _read_raw(path, sheet)

    # Rename raw headers to canonical schema
    df = df.rename(columns=COLUMN_MAP)

    # Validate schema before any computation (Fail Fast)
    _validate_schema(df)

    # Drop rows with nulls in critical columns (Data Lineage principle)
    critical = list(REQUIRED_COLUMNS)
    before = len(df)
    df = df.dropna(subset=critical)
    dropped = before - len(df)
    if dropped:
        print(f"[INFO] Dropped {dropped} row(s) with nulls in critical columns.")

    # Enforce correct dtypes
    df["Trade #"] = df["Trade #"].astype(int)
    df["Price"] = df["Price"].astype(float)
    df["Size"] = df["Size"].astype(float)
    df["Profit"] = df["Profit"].astype(float)

    return df


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
def _print_units_table(units) -> None:
    """Print the aggregated Decision Units as a formatted table."""
    rows = [
        [
            u.trade_id,
            u.entries,
            f"${u.net_pnl:,.2f}",
            f"${u.pne:,.2f}",
            f"${u.mae:,.2f}",
            f"{u.mae_pct:.2f}%",
            u.outcome,
        ]
        for u in units
    ]
    headers = [
        "Trade #", "Entries", "Net P&L", "PNE", "MAE", "MAE %", "Outcome"
    ]
    print("\n" + "=" * 70)
    print("  IQRE — Decision Units (Aggregated Sequences)")
    print("=" * 70)
    print(tabulate(rows, headers=headers, tablefmt="grid"))


def _print_summary_table(summary: dict) -> None:
    """Print the institutional summary metrics as a formatted table."""
    rows = [[k, v] for k, v in summary.items()]
    print("\n" + "=" * 70)
    print("  IQRE — Institutional Summary (GIPS-Aligned)")
    print("=" * 70)
    print(tabulate(rows, headers=["Metric", "Value"], tablefmt="grid"))
    print()


# ---------------------------------------------------------------------------
# Operational Audit Log  (Institutional Hardening)
# ---------------------------------------------------------------------------
def _write_audit_log(
    rows_processed:  int,
    n_sequences:     int,
    n_single_entry:  int,
    n_dca:           int,
    verify_result:   str,
    elapsed_sec:     float,
    utc_ts:          str,
) -> None:
    """
    Write output/audit_log.txt recording pipeline metrics, artefact inventory,
    and governance status for every production run.
    """
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    thick = "=" * 80
    thin  = "-" * 80

    lines = [
        thick,
        "  IQRE v1.5 \u2014 Institutional Recalibration Build",
        "  OPERATIONAL AUDIT LOG",
        thick,
        f"  Generated (UTC) : {utc_ts}",
        f"  Source File     : {DATA_PATH.relative_to(_ROOT)}",
        f"  Sandbox         : {_ROOT / 'output'}",
        thick,
        "",
        "PIPELINE EXECUTION METRICS",
        thin,
        f"  Rows Processed              : {rows_processed:>6}",
        f"  Total Decision Units        : {n_sequences:>6}",
        f"    Single-Entry Sequences    : {n_single_entry:>6}  "
        f"(2 rows: 1 entry + 1 exit)",
        f"    Multi-Entry DCA Sequences : {n_dca:>6}  "
        f"(3+ rows: multiple DCA legs)",
        f"  Verification Result         :  {verify_result}",
        f"  Total Execution Time        :  {elapsed_sec:.3f} s",
        "",
        "OUTPUT ARTEFACTS  (Sandbox Governance \u2014 output/)",
        thin,
        f"  [OK]  {VERIFY_PATH.name}",
        f"  [OK]  equity_curve.png         (output/temp_plots/)",
        f"  [OK]  monthly_heatmap.png      (output/temp_plots/)",
        f"  [OK]  win_rate_comparison.png  (output/temp_plots/)",
        f"  [OK]  {OUTPUT_PATH.name}",
        f"  [OK]  {AUDIT_PATH.name}",
        f"  [OK]  {AUDIT_LOG_PATH.name}",
        "",
        "GOVERNANCE STATUS",
        thin,
        "  Governance Protocol     :  GIPS 2026 \u2014 Deterministic Validation",
        "  Build Version           :  IQRE v1.5 \u2014 Institutional Recalibration Build",
        "  Classification          :  Proprietary \u2014 Production Build",
        "  Fail-Fast               :  Active",
        "  Data Lineage            :  Verified",
        "  PII Redaction           :  Active",
        "  DataIntegrityError      :  Active  (engine.py schema guard)",
        "",
        thick,
        "",
    ]

    AUDIT_LOG_PATH.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    _start = time.perf_counter()
    _utc_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("[IQRE] Institutional Quantitative Reporting Engine -- v1.5 Institutional Recalibration Build")
    print(f"[IQRE] Ingesting: {DATA_PATH}")

    # -- Phase 1: Ingestion & validation --------------------------------------
    df_raw = _read_raw(DATA_PATH, SHEET_NAME)   # preserved for verification
    df     = load_and_clean(DATA_PATH, SHEET_NAME)
    print(f"[IQRE] Loaded {len(df)} rows across {df['Trade #'].nunique()} trade IDs.")

    # -- Phase 2: DCA aggregation ---------------------------------------------
    engine = IQREAnalytics(df)
    units  = engine.aggregate_sequences()
    n_single_entry = sum(1 for u in units if u.entries == 2)
    n_dca          = len(units) - n_single_entry
    print(f"[IQRE] Aggregated into {len(units)} Decision Unit(s)  "
          f"({n_single_entry} single-entry, {n_dca} multi-entry DCA).")

    # -- Phase 2.5: Deterministic verification --------------------------------
    print("[IQRE] Running mathematical parity verification ...")
    try:
        ok, verify_path = run_verification(df_raw, units, VERIFY_PATH)
    except VerificationError as exc:
        raise AuditInconsistencyError(str(exc)) from exc

    verdict = "VERIFIED (100% parity)" if ok else "DISCREPANCY DETECTED"
    print(f"[IQRE] Verification {verdict} -> {verify_path}")
    if not ok:
        raise AuditInconsistencyError(
            "[AUDIT FAIL] Parity verification failed. "
            f"Review {verify_path} for details."
        )

    # -- Phase 3: CLI output --------------------------------------------------
    summary = engine.get_summary()
    _print_units_table(units)
    _print_summary_table(summary)

    # -- Phase 3.5: Visual Analytics ------------------------------------------
    print("[IQRE] Generating visual analytics charts ...")
    va         = VisualAnalytics(units, engine.df, engine.equity_curve)
    plot_paths = va.generate_plots(PLOTS_DIR)
    print(f"[IQRE] Equity curve      -> {plot_paths['equity_curve']}")
    print(f"[IQRE] Monthly heatmap   -> {plot_paths['monthly_heatmap']}")
    print(f"[IQRE] Win-rate chart    -> {plot_paths['win_rate_comparison']}")

    # -- Phase 4: B2B PDF generation ------------------------------------------
    print("[IQRE] Generating institutional PDF report ...")
    pdf_path = generate_report(summary, units, OUTPUT_PATH, plot_paths=plot_paths)
    print(f"[IQRE] PDF saved -> {pdf_path}")

    # -- Phase 3: GIPS audit workbook -----------------------------------------
    print("[IQRE] Generating GIPS audit workbook ...")
    wb_path = generate_audit_workbook(units, engine.df, AUDIT_PATH)
    print(f"[IQRE] Audit workbook saved -> {wb_path}")

    # -- Phase 4: Operational Audit Log ---------------------------------------
    elapsed = time.perf_counter() - _start
    _write_audit_log(
        rows_processed = len(df),
        n_sequences    = len(units),
        n_single_entry = n_single_entry,
        n_dca          = n_dca,
        verify_result  = verdict,
        elapsed_sec    = elapsed,
        utc_ts         = _utc_ts,
    )
    print(f"[IQRE] Audit log saved -> {AUDIT_LOG_PATH}")


if __name__ == "__main__":
    try:
        main()
    except (AuditInconsistencyError, VerificationError, VisualAnalyticsError) as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
