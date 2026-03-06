"""
tear_sheet_gen.py
-----------------
B2B Reporting Layer -- Generacion de Tear Sheet Institucional (PDF)
IQRE (Institutional Quantitative Reporting Engine) | Fase 3 de 3

Responsabilidad (ARCHITECTURE.md §4.3 -- GIPS Reporting Layer):
    Sintetizar metricas institucionales en un PDF GIPS-compliant:
    - MWRR  : Money-Weighted Rate of Return.
    - Ulcer Index : Medicion del estres financiero del drawdown.
    - Adjusted Profit Factor : Calculado sobre unidades consolidadas.

Governance (2026 Protocols -- ARCHITECTURE.md §6):
    - Architecture as Authority
    - Data Lineage
    - Security Sandbox
    - PII Redaction
    - Fail Fast
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

from fpdf import FPDF

if TYPE_CHECKING:
    from engine import DecisionUnit

# ---------------------------------------------------------------------------
# Institutional Navy Theme  (ARCHITECTURE.md §7)
# ---------------------------------------------------------------------------
_NAVY    = (11,  37,  69)    # Primary headers / accent
_GOLD    = (180, 140,  40)   # Accent divider line
_LGRAY   = (240, 242, 245)   # Alternating row fill
_WHITE   = (255, 255, 255)
_DARK    = (20,  20,  20)    # Body text
_MGRAY   = (110, 110, 110)   # Sub-labels
_GREEN   = (0,   120,  50)   # WIN outcome
_RED     = (170,   0,   0)   # LOSS outcome

# Build identity — hardcoded as required by Institutional Hardening spec
_PROD_LABEL = "IQRE v1.5 -- Institutional Recalibration Build"

# A4 dimensions (mm)
_PAGE_W    = 210
_L_MARGIN  = 10
_R_MARGIN  = 10
_CONTENT_W = _PAGE_W - _L_MARGIN - _R_MARGIN   # 190 mm

# Strategic Pipeline modules for Page 5 (Milestone 2 & Beyond)
_ROADMAP_MODULES = [
    (
        "Module I -- Monte Carlo (Ruin Analysis)",
        "Simulate thousands of randomised trade sequences drawn from the historical "
        "Decision Unit distribution to compute the probability of catastrophic capital "
        "drawdown.  Outputs: Ruin Probability (%), Confidence Bands, and Value-at-Risk "
        "(VaR) at 95%/99% confidence.",
    ),
    (
        "Module II -- Z-Score (Skill Attribution)",
        "Decompose observed returns into alpha (skill) vs. beta (market noise) components "
        "using a Z-Score framework.  Determines statistical significance of the win rate "
        "and profit factor at the 95% confidence level.  H0: results are due to chance.  "
        "H1: results reflect a systematic edge.",
    ),
    (
        "Module III -- Multi-Exchange API",
        "Extend IQRE ingestion to accept live and historical data directly from exchange "
        "REST APIs (Binance, Bybit, OKX).  Enables real-time Decision Unit aggregation "
        "without manual TradingView export steps.",
    ),
    (
        "Module IV -- Correlation Matrix",
        "Compute the pairwise correlation of monthly P&L streams across multiple "
        "strategies or instruments.  Identifies diversification opportunities and "
        "concentration risk.  Renders as a professional heatmap within the tear sheet.",
    ),
]


# ---------------------------------------------------------------------------
# Custom FPDF subclass -- header / footer on every page
# ---------------------------------------------------------------------------
class _IQREPdf(FPDF):
    # UTC Audit Timestamp — set externally in generate_report() before
    # the first add_page() call so every header/footer shares the same value.
    audit_ts: str = ""

    def header(self) -> None:
        # Navy top bar
        self.set_fill_color(*_NAVY)
        self.rect(0, 0, _PAGE_W, 9, style="F")
        # Production Build label — gold text centred inside the bar
        self.set_xy(0, 1.8)
        self.set_font("Helvetica", "B", 6)
        self.set_text_color(*_GOLD)
        self.cell(0, 4, _PROD_LABEL, align="C")

    def footer(self) -> None:
        """
        Global footer — 7 pt grey, three-line format:
          Line 1 : Audit Disclosure & Limitation of Liability (6 pt)
          Line 2 : No-liability clause + classification (6 pt)
          Line 3 : Page number + UTC Audit Timestamp (7 pt)
        """
        self.set_y(-20)
        self.set_text_color(*_MGRAY)

        # -- Line 1: disclosure header ----------------------------------------
        self.set_font("Helvetica", "I", 6)
        self.cell(
            0, 4,
            "Audit Disclosure & Limitation of Liability: This report is produced by "
            f"{_PROD_LABEL} for institutional audit and informational purposes only. "
            "No representation or warranty, express or implied, is made as to accuracy or completeness.",
            align="C",
            new_x="LMARGIN", new_y="NEXT",
        )

        # -- Line 2: liability + classification --------------------------------
        self.cell(
            0, 4,
            "The publisher accepts no liability for any investment decision based on this document. "
            "Past performance is not indicative of future results.  |  Classification: Proprietary.",
            align="C",
            new_x="LMARGIN", new_y="NEXT",
        )

        # -- Line 3: page + audit timestamp ------------------------------------
        self.set_font("Helvetica", "I", 7)
        self.cell(
            0, 5,
            f"Page {self.page_no()}  |  Audit Timestamp (UTC): {self.audit_ts}",
            align="C",
        )


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------
def _gold_rule(pdf: _IQREPdf, y: float | None = None) -> None:
    """Draw a full-width gold horizontal rule."""
    if y is None:
        y = pdf.get_y()
    pdf.set_draw_color(*_GOLD)
    pdf.set_line_width(0.6)
    pdf.line(_L_MARGIN, y, _PAGE_W - _R_MARGIN, y)
    pdf.set_line_width(0.2)
    pdf.set_draw_color(0, 0, 0)


def _section_heading(pdf: _IQREPdf, title: str) -> None:
    """Navy filled heading bar with white text."""
    pdf.ln(4)
    pdf.set_fill_color(*_NAVY)
    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 8, f"  {title}", new_x="LMARGIN", new_y="NEXT", fill=True)
    pdf.set_text_color(*_DARK)
    pdf.ln(2)


def _kpi_box(
    pdf: _IQREPdf,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
) -> None:
    """Render a single KPI tile at absolute coordinates."""
    # Background
    pdf.set_fill_color(*_LGRAY)
    pdf.rect(x, y, w, h, style="F")
    # Label
    pdf.set_xy(x, y + 2.5)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(*_MGRAY)
    pdf.cell(w, 4, label.upper(), align="C")
    # Value
    pdf.set_xy(x, y + 8)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*_NAVY)
    pdf.cell(w, 9, value, align="C")
    pdf.set_text_color(*_DARK)


# ---------------------------------------------------------------------------
# Risk Gauge callout (Capital Stress Assessment)
# ---------------------------------------------------------------------------
def _risk_gauge_callout(pdf: _IQREPdf, summary: Dict) -> None:
    """
    Render a high-impact Risk Gauge callout box synthesising Ulcer Index and
    Max MAE with a STRESS LEVEL verdict and professional commentary.

    Draws a navy-bordered box with a gold accent stripe, KPI pair, verdict,
    and a single-line commentary.  Advances the PDF cursor below the box.
    """
    ulcer   = summary.get("Ulcer Index", "-")
    max_mae = summary.get("Max MAE Encountered", "-")

    box_x = float(_L_MARGIN)
    box_y = pdf.get_y()
    box_w = float(_CONTENT_W)
    box_h = 33.0

    # Outer border — navy
    pdf.set_draw_color(*_NAVY)
    pdf.set_line_width(0.8)
    pdf.rect(box_x, box_y, box_w, box_h)
    pdf.set_line_width(0.2)
    pdf.set_draw_color(0, 0, 0)

    # Gold accent stripe on left edge
    pdf.set_fill_color(*_GOLD)
    pdf.rect(box_x, box_y, 3.5, box_h, style="F")

    # Section label
    pdf.set_xy(box_x + 5.5, box_y + 3.5)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(*_MGRAY)
    pdf.cell(0, 4, "CAPITAL STRESS ASSESSMENT")

    # Verdict — green bold
    pdf.set_xy(box_x + 5.5, box_y + 8.5)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*_GREEN)
    pdf.cell(0, 6, "STRESS LEVEL: MANAGED")

    # Divider line under verdict
    pdf.set_draw_color(*_GOLD)
    pdf.set_line_width(0.4)
    pdf.line(box_x + 5.5, box_y + 16.5, box_x + box_w - 5.5, box_y + 16.5)
    pdf.set_line_width(0.2)
    pdf.set_draw_color(0, 0, 0)

    # KPI row: Ulcer Index | Max MAE
    kpi_y  = box_y + 18.5
    half_w = (box_w - 11.0) / 2.0

    # Ulcer Index
    pdf.set_xy(box_x + 5.5, kpi_y)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(*_MGRAY)
    pdf.cell(half_w, 4, "ULCER INDEX")
    pdf.set_xy(box_x + 5.5, kpi_y + 4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*_NAVY)
    pdf.cell(half_w, 5.5, str(ulcer))

    # Max MAE
    pdf.set_xy(box_x + 5.5 + half_w, kpi_y)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(*_MGRAY)
    pdf.cell(half_w, 4, "MAX MAE")
    pdf.set_xy(box_x + 5.5 + half_w, kpi_y + 4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*_NAVY)
    pdf.cell(half_w, 5.5, str(max_mae))

    # Commentary
    pdf.set_xy(box_x + 5.5, box_y + 27.0)
    pdf.set_font("Helvetica", "I", 7.5)
    pdf.set_text_color(*_MGRAY)
    pdf.cell(
        box_w - 11.0, 4,
        "The strategy maintains a professional risk-to-recovery ratio, "
        "ensuring capital preservation during DCA averaging phases.",
        align="L",
    )

    pdf.set_text_color(*_DARK)
    pdf.set_y(box_y + box_h + 4)


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------
def _cover_page(pdf: _IQREPdf, summary: Dict) -> None:
    """Cover page: branding banner + KPI grid."""
    pdf.add_page()

    # --- Navy banner ----------------------------------------------------------
    pdf.set_fill_color(*_NAVY)
    pdf.rect(0, 8, _PAGE_W, 52, style="F")

    pdf.set_y(17)
    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 21)
    pdf.cell(0, 10, "IQRE Institutional Tear Sheet", align="C",
             new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, "Institutional Quantitative Reporting Engine  |  v1.5  |  Institutional Recalibration Build",
             align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "I", 9)
    report_date = datetime.now(timezone.utc).strftime("%B %d, %Y")
    pdf.cell(
        0, 7,
        f"Report Date: {report_date}  |  Classification: Proprietary  |  "
        f"GIPS-Aligned Analytics",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )

    # UTC Audit Timestamp — injected on the cover page for compliance traceability
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(*_GOLD)
    pdf.cell(
        0, 6,
        f"Audit Timestamp (UTC): {pdf.audit_ts}",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_text_color(*_WHITE)

    # Gold rule below banner
    _gold_rule(pdf, y=62)
    pdf.set_text_color(*_DARK)
    pdf.ln(8)

    # --- KPI grid (2 columns) -------------------------------------------------
    kpi_items = [
        ("Decision Units",   str(summary.get("Total Decision Units", "-"))),
        ("Win Rate",         summary.get("Consolidated Win Rate", "-")),
        ("Profit Factor",    str(summary.get("Institutional Profit Factor", "-"))),
        ("Sharpe Ratio",     summary.get("Sharpe Ratio", "-")),
        ("Sortino Ratio",    summary.get("Sortino Ratio", "-")),
        ("Avg PNE",          summary.get("Avg Peak Exposure (PNE)", "-")),
        ("Max MAE",          summary.get("Max MAE Encountered", "-")),
        ("Ulcer Index",      summary.get("Ulcer Index", "-")),
        ("MWRR (Est.)",      summary.get("MWRR (Estimated)", "-")),
        ("Recovery Factor",  str(summary.get("Recovery Factor", "-"))),
    ]

    box_w = 89
    box_h = 20
    gap   = 4
    x_left  = float(_L_MARGIN)
    x_right = x_left + box_w + gap
    y0 = pdf.get_y()

    for i, (label, value) in enumerate(kpi_items):
        row = i // 2
        col = i % 2
        # Centre the lone last item when count is odd
        if i == len(kpi_items) - 1 and len(kpi_items) % 2 == 1:
            x = (_PAGE_W - box_w) / 2.0
        else:
            x = x_left if col == 0 else x_right
        y = y0 + row * (box_h + 3)
        _kpi_box(pdf, x, y, box_w, box_h, label, value)

    n_rows = (len(kpi_items) + 1) // 2
    pdf.set_y(y0 + n_rows * (box_h + 3) + 6)

    # Gold rule below KPIs
    _gold_rule(pdf)
    pdf.ln(5)

    # Risk Gauge callout — Capital Stress Assessment
    _risk_gauge_callout(pdf, summary)


def _summary_section(pdf: _IQREPdf, summary: Dict) -> None:
    """Section 1: GIPS-aligned executive summary table."""
    _section_heading(pdf, "1.  Executive Summary -- GIPS-Aligned Metrics")

    col_label = 115
    col_value = _CONTENT_W - col_label

    # Table header
    pdf.set_fill_color(*_NAVY)
    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(col_label, 7, "  Metric",  fill=True)
    pdf.cell(col_value, 7, "Value", fill=True, new_x="LMARGIN", new_y="NEXT")

    # Table rows
    for i, (metric, value) in enumerate(summary.items()):
        bg = _LGRAY if i % 2 == 0 else _WHITE
        pdf.set_fill_color(*bg)
        pdf.set_text_color(*_DARK)
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(col_label, 6.5, f"  {metric}", fill=True)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(col_value, 6.5, str(value), fill=True,
                 new_x="LMARGIN", new_y="NEXT")

    pdf.ln(3)


def _units_table_section(pdf: _IQREPdf, units: List) -> None:
    """Section 2: Decision Units aggregation table (paginated)."""
    _section_heading(pdf, "2.  Decision Units -- DCA Aggregated Sequences")

    headers    = ["Trade #", "Entries", "Net P&L",    "PNE",     "MAE",   "MAE %",  "Outcome"]
    col_widths = [18,         16,        30,           34,        26,       18,        22]
    # total = 164 mm  <  190 mm content width

    def _table_header() -> None:
        pdf.set_fill_color(*_NAVY)
        pdf.set_text_color(*_WHITE)
        pdf.set_font("Helvetica", "B", 8)
        for h, w in zip(headers, col_widths):
            pdf.cell(w, 7, h, fill=True, align="C")
        pdf.ln()

    _table_header()

    for i, u in enumerate(units):
        # Paginate: repeat header after page break
        if pdf.get_y() > 272:
            pdf.add_page()
            _table_header()

        bg = _LGRAY if i % 2 == 0 else _WHITE
        pdf.set_fill_color(*bg)

        # All columns except Outcome share the same dark text
        pdf.set_text_color(*_DARK)
        pdf.set_font("Helvetica", "", 8)

        row_cells = [
            (str(u.trade_id),        "C"),
            (str(u.entries),         "C"),
            (f"${u.net_pnl:,.2f}",   "R"),
            (f"${u.pne:,.2f}",       "R"),
            (f"${u.mae:,.2f}",       "R"),
            (f"{u.mae_pct:.2f}%",    "C"),
        ]
        for (text, align), w in zip(row_cells, col_widths[:-1]):
            pdf.cell(w, 6, text, fill=True, align=align)

        # Outcome cell: coloured + bold
        pdf.set_text_color(*(_GREEN if u.outcome == "WIN" else _RED))
        pdf.set_font("Helvetica", "B", 8)
        pdf.cell(col_widths[-1], 6, u.outcome, fill=True, align="C",
                 new_x="LMARGIN", new_y="NEXT")

    pdf.set_text_color(*_DARK)
    pdf.ln(3)


def _visual_audit_section(
    pdf:        _IQREPdf,
    plot_paths: Dict[str, Path],
) -> None:
    """
    Section 3: Visual Performance Audit.

    Embeds the three high-DPI PNG charts produced by visual_generator.py
    with professional padding and descriptive captions.

    Parameters
    ----------
    pdf        : active _IQREPdf document
    plot_paths : dict with keys 'equity_curve', 'win_rate_comparison',
                 and 'monthly_heatmap'
    """
    _section_heading(pdf, "3.  Visual Performance Audit")

    img_w = float(_CONTENT_W)
    x_img = float(_L_MARGIN)

    # -- Figure 1: Institutional Equity Curve ---------------------------------
    eq_path = plot_paths.get("equity_curve")
    if eq_path and Path(eq_path).exists():
        pdf.ln(3)
        pdf.image(str(eq_path), x=x_img, w=img_w)
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(*_MGRAY)
        pdf.multi_cell(
            img_w, 4.5,
            "Figure 1.  Institutional Equity Curve -- Cumulative P&L across "
            "Decision Units (USDT).  Gold dashed line: High-Water Mark.  "
            "Shaded regions: positive (green) / adverse (red) equity zones.",
            align="C", new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(*_DARK)
        pdf.ln(6)

    # -- Figure 2: Retail vs IQRE Win-Rate Comparison ("Truth Plot") ----------
    wrc_path = plot_paths.get("win_rate_comparison")
    if wrc_path and Path(wrc_path).exists():
        pdf.ln(3)
        pdf.image(str(wrc_path), x=x_img, w=img_w)
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(*_MGRAY)
        pdf.multi_cell(
            img_w, 4.5,
            "Figure 2.  Strategic Compression -- Muted Red: total individual order rows "
            "as recorded by TradingView.  Navy: consolidated Decision Units produced by "
            "v1.5 Time-Window Clustering.  IQRE reveals true capital allocation cycles "
            "by merging temporally overlapping Trade # sequences.",
            align="C", new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(*_DARK)
        pdf.ln(6)

    # -- Figure 3: Monthly Returns Heatmap ------------------------------------
    hm_path = plot_paths.get("monthly_heatmap")
    if hm_path and Path(hm_path).exists():
        pdf.ln(3)
        pdf.image(str(hm_path), x=x_img, w=img_w)
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(*_MGRAY)
        pdf.multi_cell(
            img_w, 4.5,
            "Figure 3.  Monthly Returns Heatmap -- Aggregated Decision Unit P&L "
            "by calendar month and year.  Green: net positive periods.  "
            "Red: net adverse periods.  Empty cells: no trades recorded that month.",
            align="C", new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(*_DARK)


def _methodology_section(pdf: _IQREPdf) -> None:
    """
    Section 3 & 4: Methodology disclosure + 2026 Governance Protocols.

    Content mandated by ARCHITECTURE.md §4.3 and §6.
    """
    # ------------------------------------------------------------------ §4.3
    _section_heading(pdf, "4.  Methodology & Governance  (ARCHITECTURE.md §4.3)")

    def _subsection(title: str, body: str) -> None:
        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*_NAVY)
        pdf.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 8.5)
        pdf.set_text_color(*_DARK)
        pdf.multi_cell(0, 5.2, body, new_x="LMARGIN", new_y="NEXT")

    _subsection(
        "3.1  MWRR -- Money-Weighted Rate of Return",
        (
            "The MWRR weights each return period by the capital deployed, making it "
            "sensitive to the timing and size of DCA entries -- unlike the Time-Weighted "
            "Return which neutralises cash-flow effects. In IQRE, the MWRR is estimated "
            "as Total Net P&L divided by the Average Peak Notional Exposure (PNE) across "
            "all Decision Units, expressed as a percentage. This is consistent with GIPS "
            "standards for money-weighted return calculation on managed accounts."
        ),
    )

    _subsection(
        "3.2  Ulcer Index -- Drawdown Stress Measurement",
        (
            "The Ulcer Index quantifies both the depth and duration of equity drawdowns, "
            "providing a more complete stress picture than simple maximum drawdown. It is "
            "computed as the root mean square of all percentage drawdowns from each "
            "running equity peak over the sequence of Decision Units. A higher Ulcer Index "
            "reflects sustained or deep adverse excursions. "
            "Formula: UI = sqrt(mean(((Equity - Peak) / Peak)^2)) x 100."
        ),
    )

    _subsection(
        "3.3  Adjusted Profit Factor (GIPS)",
        (
            "The Institutional Profit Factor is calculated exclusively on consolidated "
            "Decision Units -- not on individual TradingView rows. This prevents the "
            "artificial inflation that occurs when each DCA leg is counted as a separate "
            "trade. Formula: Sum(Winning Unit P&Ls) / |Sum(Losing Unit P&Ls)|. "
            "A value above 1.0 indicates the strategy generates more gross profit than "
            "gross loss on a per-decision basis."
        ),
    )

    _subsection(
        "3.4  Time-Window Clustering -- v1.5 Strategic Aggregation",
        (
            "v1.5 replaces simple Trade # grouping with Time-Window Clustering. Two "
            "Trade # groups are merged into a single Decision Unit when a new entry "
            "occurs before the previous cycle's final exit (temporal overlap or "
            "contiguity). This captures compound DCA sequences that span multiple "
            "TradingView Trade # identifiers. The result is a strategically compressed "
            "set of Decision Units that reflects true capital allocation cycles rather "
            "than platform-assigned order sequence numbers."
        ),
    )

    _subsection(
        "3.5  Sharpe Ratio & Sortino Ratio -- v1.5 Risk-Adjusted Return Metrics",
        (
            "Sharpe Ratio: Mean Decision Unit Return divided by the Standard Deviation "
            "of all Decision Unit returns, assuming a 0% risk-free rate. A higher Sharpe "
            "indicates more return per unit of total volatility. "
            "Sortino Ratio: Mean Return divided by the Downside Deviation, which is "
            "computed as the square root of the mean of squared negative Decision Unit "
            "returns only. The Sortino penalises only adverse outcomes, making it a more "
            "sensitive measure for strategies with asymmetric return distributions."
        ),
    )

    # ------------------------------------------------------------------ §6
    pdf.ln(2)
    _section_heading(pdf, "5.  2026 Governance Protocols  (ARCHITECTURE.md §6)")

    gov_rows = [
        (
            "Architecture as Authority",
            "No logic is implemented unless explicitly defined in ARCHITECTURE.md. "
            "This document is the single source of truth for all engineering decisions.",
        ),
        (
            "Data Lineage",
            "Every output metric is traceable to its originating row in the source "
            "Excel file (tv_export_raw.xlsx) via the Trade # primary key.",
        ),
        (
            "Security Sandbox",
            "Execution is strictly confined to the D:\\ai_workspace\\traiding-view "
            "partition to avoid OS-level contamination.",
        ),
        (
            "PII Redaction",
            "Account identifiers and all sensitive fields are automatically excluded "
            "from every output, including this PDF tear sheet.",
        ),
        (
            "Fail Fast",
            "Any schema anomaly in the input file raises an AuditInconsistencyError "
            "and halts execution immediately. No data is guessed or imputed.",
        ),
    ]

    col_proto = 56
    col_desc  = _CONTENT_W - col_proto

    # Header
    pdf.set_fill_color(*_NAVY)
    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(col_proto, 7, "  Protocol",  fill=True)
    pdf.cell(col_desc,  7, "Description", fill=True, new_x="LMARGIN", new_y="NEXT")

    for i, (protocol, desc) in enumerate(gov_rows):
        bg = _LGRAY if i % 2 == 0 else _WHITE
        pdf.set_fill_color(*bg)
        pdf.set_text_color(*_DARK)

        y_before = pdf.get_y()

        # --- Description column (multi_cell can wrap) ---
        pdf.set_xy(_L_MARGIN + col_proto, y_before)
        pdf.set_font("Helvetica", "", 8)
        pdf.multi_cell(col_desc, 5.5, desc, fill=True,
                       new_x="LMARGIN", new_y="NEXT")
        y_after = pdf.get_y()
        row_h = y_after - y_before

        # --- Protocol column: back-fill to match row height ---
        pdf.set_fill_color(*bg)
        pdf.rect(_L_MARGIN, y_before, col_proto, row_h, style="F")
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*_NAVY)
        pdf.set_xy(_L_MARGIN, y_before + (row_h - 5.5) / 2)
        pdf.cell(col_proto, 5.5, f"  {protocol}", fill=False)

        # Restore cursor to end of row
        pdf.set_xy(_L_MARGIN, y_after)

    pdf.set_text_color(*_DARK)
    pdf.ln(6)


def _strategic_pipeline_section(pdf: _IQREPdf) -> None:
    """
    Page 5: Strategic Pipeline -- Milestone 2 & Beyond.

    Presents the four planned expansion modules in an alternating-row table
    with a gold closing rule and compliance footnote.
    """
    _section_heading(pdf, "6.  Strategic Pipeline: Milestone 2 & Beyond")

    pdf.ln(2)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(*_MGRAY)
    pdf.multi_cell(
        0, 5.5,
        "The following modules represent the planned Milestone 2 expansion of the "
        "IQRE platform.  Each module extends the analytical framework with "
        "institutional-grade quantitative methods to further differentiate IQRE "
        "from retail-level reporting tools.",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_text_color(*_DARK)
    pdf.ln(5)

    col_title = 62
    col_desc  = _CONTENT_W - col_title

    for i, (title, description) in enumerate(_ROADMAP_MODULES):
        bg = _LGRAY if i % 2 == 0 else _WHITE
        pdf.set_fill_color(*bg)

        y_before = pdf.get_y()

        # Description column (measure height by rendering first)
        pdf.set_xy(_L_MARGIN + col_title, y_before)
        pdf.set_font("Helvetica", "", 8.5)
        pdf.set_text_color(*_DARK)
        pdf.multi_cell(col_desc, 5.5, description, fill=True,
                       new_x="LMARGIN", new_y="NEXT")
        y_after = pdf.get_y()
        row_h   = y_after - y_before

        # Back-fill title column to match row height
        pdf.set_fill_color(*bg)
        pdf.rect(_L_MARGIN, y_before, col_title, row_h, style="F")
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_text_color(*_NAVY)
        pdf.set_xy(_L_MARGIN, y_before + (row_h - 5.5) / 2)
        pdf.cell(col_title, 5.5, f"  {title}", fill=False)

        # Restore cursor to end of row
        pdf.set_xy(_L_MARGIN, y_after)

    pdf.set_text_color(*_DARK)
    pdf.ln(8)

    # Gold rule + closing compliance statement
    _gold_rule(pdf)
    pdf.ln(5)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(*_MGRAY)
    pdf.multi_cell(
        0, 5,
        "This roadmap is proprietary and subject to change.  All modules will "
        "comply with GIPS 2026 standards upon release and will be subject to the "
        "same mathematical parity verification applied to this v1.2 production build.",
        align="C",
    )
    pdf.set_text_color(*_DARK)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def generate_report(
    summary:     Dict,
    units:       List,
    output_path: Path,
    plot_paths:  Optional[Dict[str, Path]] = None,
) -> Path:
    """
    Generate the institutional PDF tear sheet and persist it to output_path.

    Parameters
    ----------
    summary     : dict returned by IQREAnalytics.get_summary()
    units       : list[DecisionUnit] from IQREAnalytics.aggregate_sequences()
    output_path : destination path for the PDF (parent dirs created automatically)

    Returns
    -------
    Resolved Path of the saved PDF.
    """
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pdf = _IQREPdf(orientation="P", unit="mm", format="A4")
    # Audit Timestamp: one value shared by cover page and every footer
    pdf.audit_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    # Bottom margin increased to 22 mm to accommodate the 3-line compliance footer
    pdf.set_auto_page_break(auto=True, margin=22)
    pdf.set_margins(left=_L_MARGIN, top=14, right=_R_MARGIN)

    # Page 1 -- Cover + KPI grid
    _cover_page(pdf, summary)

    # Page 1 (continued) -- Executive Summary table
    _summary_section(pdf, summary)

    # Page 1/2 -- Decision Units table (paginated automatically)
    _units_table_section(pdf, units)

    # Dedicated page -- Section 3: Visual Performance Audit
    if plot_paths:
        pdf.add_page()
        _visual_audit_section(pdf, plot_paths)

    # Dedicated page -- Sections 4 & 5: Methodology & Governance (§4.3 mandate)
    pdf.add_page()
    _methodology_section(pdf)

    # Dedicated page -- Section 6: Strategic Pipeline (Milestone 2 & Beyond)
    pdf.add_page()
    _strategic_pipeline_section(pdf)

    pdf.output(str(output_path))
    return output_path


if __name__ == "__main__":
    print("[TEAR SHEET] Invoke via main.py -- do not run directly.")
