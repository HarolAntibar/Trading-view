# IQRE — Institutional Quantitative Reporting Engine

**Version:** 1.0.0 | **Status:** Production Release | **Classification:** Proprietary

---

## Executive Overview

The Institutional Quantitative Reporting Engine (IQRE) is a Python-based
analytical infrastructure designed to bridge the gap between retail trading
exports and the rigorous auditing standards employed by institutional investment
funds. Rather than evaluating individual trade rows in isolation, IQRE
consolidates fragmented TradingView data into coherent **Decision Units** —
each representing a complete Dollar-Cost Averaging (DCA) sequence — and applies
GIPS-aligned performance and risk metrics to each consolidated unit.

By prioritising **Peak Notional Exposure (PNE)** and **Maximum Adverse
Excursion (MAE)** over superficial win-rate statistics, IQRE provides
stakeholders and compliance officers with a scientifically grounded view of
capital efficiency and true risk per strategic decision. Every metric is fully
traceable to its originating row in the source file, satisfying the Data
Lineage requirements of the 2026 Governance Protocol.

---

## What IQRE Produces

Upon a single invocation of `python main.py`, the pipeline generates three
audit-ready artefacts in the `output/` directory:

| Artefact | File | Description |
|---|---|---|
| Institutional Tear Sheet | `institutional_report.pdf` | Three-page PDF with KPI dashboard, Decision Units table, and GIPS Methodology & Governance disclosure |
| Audit Ledger | `institutional_audit_data.xlsx` | Two-sheet Excel workbook: `Decision_Units` (aggregated metrics) and `Data_Traceability` (raw-row lineage mapping) |
| Verification Certificate | `Verification_Report.txt` | Plain-text parity audit confirming 100% mathematical consistency between engine output and raw source data |

---

## System Requirements

| Component | Minimum Version |
|---|---|
| Python | 3.12 or later |
| Operating System | Windows 10/11, macOS 13+, or Ubuntu 22.04+ |
| Disk Space | 50 MB (excluding virtual environment) |

---

## Installation

**Step 1 — Clone or extract the repository**

Ensure all source files are present in a single directory. The expected
structure is described in the *Repository Layout* section below.

**Step 2 — Create an isolated virtual environment**

```bash
python -m venv .venv
```

**Step 3 — Activate the environment**

```bash
# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

**Step 4 — Install all dependencies**

```bash
pip install -r requirements.txt
```

No further configuration is required. All file paths within the system are
computed relative to the repository root, ensuring complete OS portability.

---

## Running the Pipeline

Place your TradingView export in the `data/` directory and execute:

```bash
python main.py
```

The system will proceed through four phases and print progress to the console:

```
[IQRE] Institutional Quantitative Reporting Engine -- v1.0.0
[IQRE] Ingesting: data/tv_export_raw.xlsx
[IQRE] Loaded 56 rows across 28 trade IDs.
[IQRE] Aggregated into 28 Decision Unit(s).
[IQRE] Running mathematical parity verification ...
[IQRE] Verification VERIFIED (100% parity) -> output/Verification_Report.txt
[IQRE] Generating institutional PDF report ...
[IQRE] PDF saved -> output/institutional_report.pdf
[IQRE] Generating GIPS audit workbook ...
[IQRE] Audit workbook saved -> output/institutional_audit_data.xlsx
```

### Supported Input Formats

| Format | Extension | Notes |
|---|---|---|
| TradingView Excel Workbook | `.xlsx` | Reads the *List of trades* sheet automatically |
| TradingView Localized CSV | `.csv` | Semicolon-delimited; comma decimal separator (European format) |

To use a localized CSV export, rename your file to `tv_export_raw.csv` (or
update `DATA_PATH` in `main.py`) and the ingestion layer will detect the
extension and parse accordingly.

### Error Handling

IQRE implements **Fail-Fast** logic throughout. If the input file is missing,
contains unexpected column headers, or if a mathematical discrepancy is
detected during verification, execution halts immediately with a descriptive
error message and exits with a non-zero status code. No partial outputs are
written. This behaviour is intentional: in institutional contexts, a silent
failure is far more damaging than a controlled halt.

---

## Data Requirements

Your TradingView export must contain the following columns in the
*List of trades* sheet (exact names as exported by TradingView):

| TradingView Column | Canonical Name | Type | Role |
|---|---|---|---|
| `Trade #` | `Trade #` | Integer | Aggregation key — groups DCA rows into Decision Units |
| `Price USDT` | `Price` | Float | Execution price for notional exposure calculation |
| `Position size (qty)` | `Size` | Float | Position size in contracts or lots |
| `Net P&L USDT` | `Profit` | Float | Realised P&L per row; used for MAE and Net P&L |
| `Type` | `Type` | String | Distinguishes entry rows from exit rows |
| `Date and time` | `Date` | Datetime | Enables chronological sorting before aggregation |

If any of the first four columns is absent, the system raises an
`AuditInconsistencyError` and halts immediately.

---

## Output Reference

### institutional_report.pdf

A three-page institutional tear sheet structured as follows:

- **Page 1** — Executive cover with seven KPI tiles (Decision Units, Win Rate,
  Profit Factor, Average PNE, Maximum MAE, Ulcer Index, MWRR) followed by the
  full GIPS-aligned summary table.
- **Page 1–2** — Complete Decision Units table, paginated automatically.
- **Page 3** — Methodology & Governance disclosure covering MWRR, Ulcer Index,
  Adjusted Profit Factor, DCA aggregation logic, and all five 2026 Governance
  Protocols.

### institutional_audit_data.xlsx

A dual-sheet Excel workbook:

- **Decision_Units** — One row per aggregated trade sequence with hardcoded
  computed values, Excel `SUM`/`AVERAGE`/`MAX` formulas in the totals row,
  frozen header, auto-filter, and USD currency formatting on all monetary
  columns.
- **Data_Traceability** — One row per cleaned source row, sorted by Trade #.
  The *Source Row (Excel)* column maps each row back to its exact position in
  the source workbook, fulfilling the GIPS Data Lineage requirement.

### Verification_Report.txt

A plain-text certificate containing:

1. Verification scope and methodology
2. Net P&L parity table (manual computation vs. engine output, per trade)
3. MAE parity table
4. Summary pass/fail counts
5. Audit certificate with final verdict

---

## Repository Layout

```
traiding-view/
│
├── ARCHITECTURE.md              # Authoritative engineering specification
├── README.md                    # This document
├── requirements.txt             # Pinned production dependencies
│
├── data/
│   └── tv_export_raw.xlsx       # TradingView source export (user-supplied)
│
├── engine.py                    # DCA aggregation; PNE, MAE, MWRR, Ulcer Index
├── main.py                      # Pipeline orchestrator: Ingest->Aggregate->Verify->Export
├── tear_sheet_gen.py            # PDF tear sheet generator (fpdf2)
├── audit_workbook.py            # Excel audit ledger generator (openpyxl)
├── verification.py              # Mathematical parity verification engine
│
└── output/                      # All generated artefacts (created automatically)
    ├── institutional_report.pdf
    ├── institutional_audit_data.xlsx
    └── Verification_Report.txt
```

---

## Architecture

The system operates as a strictly unidirectional data pipeline:

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│    DATA      │     │   DCA ENGINE     │     │  VERIFICATION    │
│  INGESTION   │────▶│  (AGGREGATION)   │────▶│  (PARITY AUDIT)  │
│  (main.py)   │     │   (engine.py)    │     │(verification.py) │
└──────────────┘     └──────────────────┘     └────────┬─────────┘
                                                        │
                              ┌─────────────────────────┤
                              ▼                         ▼
                    ┌──────────────────┐     ┌──────────────────┐
                    │  PDF TEAR SHEET  │     │  EXCEL WORKBOOK  │
                    │(tear_sheet_gen)  │     │(audit_workbook)  │
                    └──────────────────┘     └──────────────────┘
```

### Key Metrics

| Metric | Definition |
|---|---|
| **PNE** (Peak Notional Exposure) | Sum of `Price × Size` across all rows of a Decision Unit. Represents the total capital deployed in the sequence. |
| **MAE** (Max Adverse Excursion) | Absolute value of the most negative individual row P&L within a sequence, normalised against PNE to produce MAE%. |
| **MWRR** (Money-Weighted Rate of Return) | Total Net P&L divided by Average PNE, expressed as a percentage. Weights performance by capital deployed. |
| **Ulcer Index** | Root mean square of all percentage drawdowns from running equity peaks. Measures the depth and duration of adverse periods. |
| **Adjusted Profit Factor** | Sum of winning unit P&Ls divided by the absolute sum of losing unit P&Ls, calculated on consolidated Decision Units only. |

---

## GIPS Compliance Framework

IQRE is designed in alignment with the Global Investment Performance Standards
(GIPS). The following table maps each system component to its corresponding
GIPS principle:

| GIPS Principle | IQRE Implementation |
|---|---|
| **Composite Definition** | All trades aggregated into Decision Units; no cherry-picking of individual rows |
| **Data Input** | Raw TradingView export used without modification; source file preserved |
| **Calculation Methodology** | MWRR and Adjusted Profit Factor computed on consolidated units, not on individual rows |
| **Disclosures** | Full methodology disclosure printed on page 3 of every tear sheet |
| **Recordkeeping** | `Data_Traceability` sheet maps every output metric to its originating source row |

---

## 2026 Governance Protocols

All five protocols defined in `ARCHITECTURE.md §6` are enforced at runtime:

| Protocol | Enforcement Mechanism |
|---|---|
| **Architecture as Authority** | No logic is implemented unless defined in `ARCHITECTURE.md` |
| **Data Lineage** | Every output metric carries a `Source Row (Excel)` reference traceable to the raw file |
| **Security Sandbox** | All paths computed with `Path(__file__).parent`; no absolute OS paths |
| **PII Redaction** | Account identifiers and sensitive signal labels excluded from all outputs |
| **Fail Fast** | `AuditInconsistencyError` or `VerificationError` halts execution with `sys.exit(1)` |

---

## Support & Engagement

This system is maintained under active development. For technical inquiries, institutional integration requests, or audit support, please contact the **Lead Decision Systems Architect**:

* **Architect:** Harol Danilo Antibar Latorre
* **Professional Profile:** [LinkedIn Portfolio](https://www.linkedin.com/in/harol-antibar/)
* **Contact:** harolantibar100@gmail.com

*ARCHITECTURE.md is the single source of truth for all engineering decisions. Last revised: February 2026.*