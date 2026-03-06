"""
visual_generator.py
-------------------
Visual Analytics Layer -- IQRE v1.5 Institutional Recalibration Build

Generates three institutional-grade, high-DPI (300 DPI) PNG charts:

  Plot 1 -- Institutional Equity Curve
      Cumulative P&L across Decision Units with high-water-mark overlay,
      positive/negative fill zones, and USD currency axis formatting.

  Plot 2 -- Monthly Returns Heatmap
      Decision Unit P&L aggregated by calendar month and year, rendered as
      a diverging RdYlGn heatmap to surface performance seasonality.

  Plot 3 -- Retail vs IQRE Win-Rate Comparison ("Truth Plot")
      Dual-bar chart exposing the win-rate inflation produced by retail
      platforms that count each DCA entry row as a separate trade.

Output
------
  output/temp_plots/equity_curve.png
  output/temp_plots/monthly_heatmap.png
  output/temp_plots/win_rate_comparison.png

Aesthetics
----------
  - Sans-Serif typography stack: DejaVu Sans / Helvetica / Arial
  - Institutional Navy (#0B2545) primary colour; Gold (#B48C28) accent
  - Anti-aliased rendering; light grid (#E0E4EC)
  - 300 DPI PNG exports (> 300 DPI minimum per specification)

Governance (2026 Protocols -- ARCHITECTURE.md §6):
  - Security Sandbox : all paths relative to Path(__file__).parent.
  - PII Redaction    : no account identifiers in chart labels or titles.
  - Fail Fast        : VisualAnalyticsError raised on missing Date column.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")          # non-interactive backend — safe on all OS
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------
class VisualAnalyticsError(RuntimeError):
    """Raised when chart generation cannot proceed due to missing data."""


# ---------------------------------------------------------------------------
# Institutional theme constants (mirror tear_sheet_gen.py palette)
# ---------------------------------------------------------------------------
_NAVY    = "#0B2545"
_GOLD    = "#B48C28"
_BGLIGHT = "#F7F9FC"   # axes face colour
_GREEN   = "#006B2B"
_RED     = "#9E0000"
_MGRAY   = "#6E6E6E"
_WHITE   = "#FFFFFF"
_LGRAY      = "#F0F2F5"
_RETAIL_RED     = "#E63946"   # Muted Red -- Retail benchmark bar (Truth Plot)
_IQRE_NAVY_DARK = "#0A2342"   # Deep Institutional Navy -- IQRE bar (Truth Plot)

# Sans-Serif typeface stack (Inter / Helvetica style, DejaVu bundled fallback)
_FONT_STACK = [
    "DejaVu Sans", "Helvetica Neue", "Helvetica", "Arial", "sans-serif"
]

# Shared rcParams injected via plt.rc_context inside each plot function
_BASE_RC: Dict = {
    "font.family":        "sans-serif",
    "font.sans-serif":    _FONT_STACK,
    "text.antialiased":   True,
    "figure.facecolor":   _WHITE,
    "axes.facecolor":     _BGLIGHT,
    "axes.edgecolor":     "#C8C8C8",
    "axes.linewidth":     0.8,
    "axes.grid":          True,
    "grid.color":         "#E0E4EC",
    "grid.linewidth":     0.6,
    "grid.alpha":         0.8,
    "xtick.color":        "#666666",
    "ytick.color":        "#666666",
    "xtick.labelsize":    9,
    "ytick.labelsize":    9,
    "axes.labelcolor":    "#444444",
    "axes.labelsize":     10,
    "axes.titlesize":     13,
    "axes.titleweight":   "bold",
    "axes.titlepad":      14,
    "legend.frameon":     True,
    "legend.framealpha":  0.88,
    "legend.edgecolor":   "#DDDDDD",
    "legend.fontsize":    8.5,
}

_DPI = 300


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _clean_spines(ax: plt.Axes) -> None:
    """Remove top/right spines; mute remaining spine colour."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")


def _usd_formatter(value: float, _pos) -> str:
    """Format y-axis tick as abbreviated USD currency."""
    if abs(value) >= 1_000:
        return f"${value:,.0f}"
    return f"${value:.2f}"


def _subtitle(fig: plt.Figure, text: str) -> None:
    """Add a small grey subtitle strip at the bottom of a figure."""
    fig.text(0.5, 0.005, text, ha="center", fontsize=7, color="#AAAAAA")


# ---------------------------------------------------------------------------
# Plot 1 — Institutional Equity Curve
# ---------------------------------------------------------------------------
def _plot_equity_curve(
    equity_curve: List[float],
    units:        List,
    output_dir:   Path,
) -> Path:
    """
    Render Plot 1: Institutional Equity Curve.

    Parameters
    ----------
    equity_curve : cumulative P&L list from IQREAnalytics (len = n_units + 1,
                   starts at 0.0)
    units        : list[DecisionUnit] — supplies Trade # labels for x-axis
    output_dir   : destination directory for the PNG

    Returns
    -------
    Resolved Path of the saved equity_curve.png.
    """
    with plt.rc_context(_BASE_RC):
        fig, ax = plt.subplots(figsize=(12, 5))

        x     = list(range(len(equity_curve)))
        y_arr = np.array(equity_curve, dtype=float)

        # -- Zero baseline ------------------------------------------------
        ax.axhline(
            y=0, color="#AAAAAA", linewidth=0.9,
            linestyle="--", zorder=2, label="_nolegend_",
        )

        # -- Fill: positive / adverse equity zones ------------------------
        ax.fill_between(
            x, y_arr, 0,
            where=(y_arr >= 0),
            alpha=0.13, color=_GREEN, interpolate=True, zorder=2,
        )
        ax.fill_between(
            x, y_arr, 0,
            where=(y_arr <  0),
            alpha=0.16, color=_RED,   interpolate=True, zorder=2,
        )

        # -- High-water mark overlay (gold dashed) ------------------------
        hwm = np.maximum.accumulate(y_arr)
        ax.plot(
            x, hwm,
            color=_GOLD, linewidth=1.3, linestyle="--",
            alpha=0.80, label="High-Water Mark", zorder=3,
        )

        # -- Primary equity curve -----------------------------------------
        ax.plot(
            x, y_arr,
            color=_NAVY, linewidth=2.3,
            antialiased=True, zorder=4, label="Cumulative P&L",
        )

        # -- Endpoint annotation ------------------------------------------
        final      = float(y_arr[-1])
        sign       = "+" if final >= 0 else ""
        anno_color = _GREEN if final >= 0 else _RED
        ax.annotate(
            f"  {sign}{final:,.2f} USDT",
            xy=(x[-1], final),
            fontsize=9.5, color=anno_color,
            fontweight="bold", va="center",
        )

        # -- X-axis: Trade # labels (max ~14 visible ticks) ---------------
        trade_ids = [u.trade_id for u in units]
        step      = max(1, len(trade_ids) // 14)
        positions = [0] + [i + 1 for i in range(0, len(trade_ids), step)]
        labels    = ["0"] + [str(trade_ids[i]) for i in range(0, len(trade_ids), step)]
        ax.set_xticks(positions)
        ax.set_xticklabels(labels, fontsize=8.5)

        # -- Axis formatting ----------------------------------------------
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_usd_formatter))
        ax.set_xlabel("Trade #  (Decision Unit Sequence)", labelpad=8)
        ax.set_ylabel("Cumulative P&L  (USDT)",            labelpad=8)
        ax.set_title("Institutional Equity Curve")
        ax.set_xlim(0, len(x) - 1)
        ax.legend(loc="upper left", handlelength=1.8, handletextpad=0.6)
        _clean_spines(ax)

        _subtitle(
            fig,
            "IQRE v1.5 -- Institutional Recalibration Build  |  Anti-aliased  |  "
            "300 DPI  |  Theme: Institutional Navy (#0B2545)",
        )

        plt.tight_layout(rect=(0, 0.025, 1, 1))

        out = output_dir / "equity_curve.png"
        fig.savefig(
            str(out), dpi=_DPI,
            bbox_inches="tight", facecolor=fig.get_facecolor(),
        )
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# Plot 2 — Monthly Returns Heatmap
# ---------------------------------------------------------------------------
_MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def _build_monthly_pivot(units: List, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate Decision Unit P&L into a (year x month) pivot table.

    For each Trade #, the exit date (last row with 'exit' in Type) is used
    to assign the P&L to a calendar month.  Months with no trades are NaN.

    Raises
    ------
    VisualAnalyticsError
        If no valid dates can be extracted from the DataFrame.
    """
    unit_pnl: Dict[int, float] = {u.trade_id: u.net_pnl for u in units}
    monthly:  Dict             = {}   # (year, month) -> cumulative P&L

    for trade_id, group in df.groupby("Trade #"):
        # Prefer exit row; fall back to the last row in the group
        if "Type" in df.columns:
            exit_mask = group["Type"].str.lower().str.contains("exit", na=False)
            ref_row   = group[exit_mask].iloc[-1] if exit_mask.any() else group.iloc[-1]
        else:
            ref_row = group.iloc[-1]

        try:
            ts = pd.Timestamp(ref_row["Date"])
        except Exception:
            continue

        if pd.isna(ts):
            continue

        key = (ts.year, ts.month)
        monthly[key] = monthly.get(key, 0.0) + unit_pnl.get(int(trade_id), 0.0)

    if not monthly:
        raise VisualAnalyticsError(
            "[VISUAL FAIL] No valid dates found in the DataFrame. "
            "Ensure the 'Date' column is present and parseable as timestamps."
        )

    years = sorted({k[0] for k in monthly})
    data  = np.full((len(years), 12), np.nan)
    for (yr, mo), pnl in monthly.items():
        data[years.index(yr), mo - 1] = pnl

    return pd.DataFrame(data, index=years, columns=_MONTH_LABELS)


def _plot_monthly_heatmap(
    units:      List,
    df:         pd.DataFrame,
    output_dir: Path,
) -> Path:
    """
    Render Plot 2: Monthly Returns Heatmap.

    Parameters
    ----------
    units      : list[DecisionUnit] — P&L values for each trade
    df         : canonical DataFrame from IQREAnalytics (must include 'Date')
    output_dir : destination directory for the PNG

    Returns
    -------
    Resolved Path of the saved monthly_heatmap.png.
    """
    pivot   = _build_monthly_pivot(units, df)
    n_years = len(pivot)
    fig_h   = max(2.8, n_years * 1.7)   # grow gracefully with more years

    with plt.rc_context(_BASE_RC):
        fig, ax = plt.subplots(figsize=(14, fig_h))

        # Symmetric colour scale centred at zero
        vals    = pivot.values[~np.isnan(pivot.values)]
        abs_max = max(float(np.abs(vals).max()), 1.0) if len(vals) else 1.0

        # Boolean mask: True → cell is empty (no trades that month)
        mask = pivot.isna()

        # Pre-formatted annotation strings (pandas-version-safe)
        annot_df = pivot.copy()
        for col in annot_df.columns:
            annot_df[col] = annot_df[col].apply(
                lambda v: f"${v:.0f}" if pd.notna(v) else ""
            )

        sns.heatmap(
            pivot,
            ax=ax,
            cmap="RdYlGn",
            center=0,
            vmin=-abs_max,
            vmax=abs_max,
            annot=annot_df,
            fmt="",                     # strings are pre-formatted
            linewidths=0.6,
            linecolor=_WHITE,
            mask=mask,
            cbar_kws={"label": "Net P&L (USDT)", "shrink": 0.75, "pad": 0.02},
            annot_kws={"size": 9.5, "weight": "bold"},
        )

        # Style corrections after seaborn builds the axes
        ax.set_facecolor(_BGLIGHT)
        ax.set_title("Monthly Returns Heatmap")
        ax.set_xlabel("Month", labelpad=8)
        ax.set_ylabel("Year",  labelpad=8)
        ax.tick_params(axis="both", which="both", length=0)
        ax.yaxis.set_tick_params(rotation=0)
        ax.xaxis.set_tick_params(rotation=0)

        # Colour bar styling
        cb_collection = ax.collections[0] if ax.collections else None
        if cb_collection is not None and hasattr(cb_collection, "colorbar"):
            cbar = cb_collection.colorbar
            if cbar is not None:
                cbar.ax.yaxis.label.set_fontsize(9)
                cbar.ax.tick_params(labelsize=8)

        _subtitle(
            fig,
            "IQRE v1.5 -- Institutional Recalibration Build  |  "
            "Aggregated Decision Unit P&L  |  "
            "Green: Positive  |  Red: Adverse  |  Empty: No Trades",
        )

        plt.tight_layout(rect=(0, 0.03, 1, 1))

        out = output_dir / "monthly_heatmap.png"
        fig.savefig(
            str(out), dpi=_DPI,
            bbox_inches="tight", facecolor=fig.get_facecolor(),
        )
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# Plot 3 — Retail vs IQRE Win-Rate Comparison ("Truth Plot")
# ---------------------------------------------------------------------------
def _plot_win_rate_comparison(
    units:      List,
    df:         pd.DataFrame,
    output_dir: Path,
) -> Path:
    """
    Render Plot 3: Raw Row Count vs IQRE Decision Units (Consolidation Chart).

    Demonstrates the DCA aggregation benefit by contrasting the number of
    individual order rows visible on a retail platform against the number of
    consolidated Decision Units produced by IQRE.  The reduction in event
    count directly represents the elimination of execution noise.

    Bar A (Retail) : total raw rows in the source DataFrame   [Muted Red]
    Bar B (IQRE)   : consolidated Decision Units              [Deep Navy]

    Parameters
    ----------
    units      : list[DecisionUnit] from IQREAnalytics.aggregate_sequences()
    df         : canonical DataFrame from IQREAnalytics
    output_dir : destination directory for the PNG

    Returns
    -------
    Resolved Path of the saved win_rate_comparison.png.
    """
    raw_count  = len(df)          # retail view: every order row
    iqre_count = len(units)       # IQRE view: consolidated Decision Units
    reduction  = (raw_count - iqre_count) / raw_count * 100 if raw_count else 0.0

    with plt.rc_context(_BASE_RC):
        fig, ax = plt.subplots(figsize=(12, 4.5))
        fig.patch.set_facecolor(_WHITE)

        bars_x   = [0, 1]
        bars_h   = [float(raw_count), float(iqre_count)]
        bars_clr = [_RETAIL_RED, _IQRE_NAVY_DARK]
        bars_lbl = [
            f"Retail Raw Events\n({raw_count} order rows)",
            f"IQRE Decision Units\n({iqre_count} consolidated units)",
        ]

        bar_container = ax.bar(
            bars_x, bars_h,
            color=bars_clr,
            width=0.45,
            zorder=3,
            edgecolor=_WHITE,
            linewidth=0.8,
        )

        # Value annotations centred on each bar
        for bar, count in zip(bar_container, bars_h):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                count + 0.6,
                str(int(count)),
                ha="center", va="bottom",
                fontsize=16, fontweight="bold",
                color="#2D2D2D",
            )

        # Strategic Compression callout annotation
        mid_y = (bars_h[0] + bars_h[1]) / 2
        ax.annotate(
            f"{reduction:.0f}% strategic\ncompression",
            xy=(1, iqre_count),
            xytext=(1.30, mid_y),
            fontsize=8.5, color=_IQRE_NAVY_DARK, fontweight="bold",
            arrowprops=dict(
                arrowstyle="-|>", color=_IQRE_NAVY_DARK,
                lw=1.2, connectionstyle="arc3,rad=-0.15",
            ),
            va="center", ha="left",
        )

        ax.set_xticks(bars_x)
        ax.set_xticklabels(bars_lbl, fontsize=11)
        ax.set_ylabel("Count", labelpad=8)
        ax.set_ylim(0, raw_count + 14)
        ax.set_xlim(-0.55, 1.70)
        ax.set_title(
            f"Strategic Compression: {raw_count} Raw Rows \u2192 {iqre_count} Decision Units"
        )
        ax.tick_params(axis="x", length=0)
        _clean_spines(ax)

        # Colour-code x-tick labels to match bar colours
        for tick_label, colour in zip(ax.get_xticklabels(), bars_clr):
            tick_label.set_color(colour)
            tick_label.set_fontweight("bold")

        # Audit insight caption
        fig.text(
            0.5, 0.072,
            "Audit Insight: Retail platforms count each order row as an independent "
            "event. IQRE consolidates all DCA entries per trade into a single Decision "
            "Unit, eliminating execution noise and revealing true strategic performance.",
            ha="center", fontsize=8, color="#444444", style="italic",
        )

        _subtitle(
            fig,
            "IQRE v1.5 -- Institutional Recalibration Build  |  "
            "Retail: individual order rows in source file  |  "
            "IQRE: Decision Units via Time-Window Clustering",
        )

        plt.tight_layout(rect=(0, 0.12, 1, 1))

        out = output_dir / "win_rate_comparison.png"
        fig.savefig(
            str(out), dpi=_DPI,
            bbox_inches="tight", facecolor=fig.get_facecolor(),
        )
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
class VisualAnalytics:
    """
    Visual Analytics Layer for IQRE v1.5 Institutional Recalibration Build.

    Encapsulates all three chart generators.  Call generate_plots() after
    IQREAnalytics.aggregate_sequences() to produce high-DPI PNG assets
    for embedding in the institutional PDF tear sheet.

    Parameters
    ----------
    units        : list[DecisionUnit] from IQREAnalytics.aggregate_sequences()
    df           : cleaned source DataFrame (engine.df after aggregation)
    equity_curve : cumulative P&L list from IQREAnalytics.equity_curve
    """

    def __init__(
        self,
        units:        List,
        df:           pd.DataFrame,
        equity_curve: List[float],
    ) -> None:
        self.units        = units
        self.df           = df
        self.equity_curve = equity_curve

    def generate_plots(self, output_dir: Path) -> Dict[str, Path]:
        """
        Generate all visual analytics charts and persist as 300 DPI PNGs.

        Parameters
        ----------
        output_dir : destination directory (created if absent)

        Returns
        -------
        dict with keys:
          'equity_curve'        -> Path to equity_curve.png
          'monthly_heatmap'     -> Path to monthly_heatmap.png
          'win_rate_comparison' -> Path to win_rate_comparison.png
        """
        output_dir = Path(output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        return {
            "equity_curve": _plot_equity_curve(
                self.equity_curve, self.units, output_dir
            ),
            "monthly_heatmap": _plot_monthly_heatmap(
                self.units, self.df, output_dir
            ),
            "win_rate_comparison": _plot_win_rate_comparison(
                self.units, self.df, output_dir
            ),
        }


# ---------------------------------------------------------------------------
# Standalone entry point (diagnostic use)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("[VISUAL] Invoke via main.py -- do not run directly.")
