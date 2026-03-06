"""
engine.py
---------
Motor Analítico Core — Time-Window Clustering y Métricas de Riesgo Avanzadas
IQRE (Institutional Quantitative Reporting Engine) | v1.5 | Institutional Recalibration Build

Responsabilidades:
    1. Agrupar filas de TradingView en "Unidades de Decisión" usando Time-Window Clustering.
    2. Calcular el PNE (Peak Notional Exposure) para cada secuencia.
    3. Determinar el MAE (Max Adverse Excursion) relativo al PNE.
    4. Implementar métricas avanzadas: MWRR, Ulcer Index, Sharpe Ratio, Sortino Ratio.

Strategic Aggregation Logic (v1.5 — Time-Window Clustering):
    Two Trade # groups are merged into a single Decision Unit when a new entry
    occurs before the previous cycle's final exit (temporal overlap / contiguity).
    This replaces the simple Trade # grouping of earlier builds.

Data Integrity (ARCHITECTURE.md §6 — Fail Fast):
    DataIntegrityError is raised immediately if the DataFrame schema is
    incompatible, before any computation is attempted.
"""

import pandas as pd
import numpy as np
from pydantic import BaseModel, Field
from typing import List, Dict


# ---------------------------------------------------------------------------
# Data Integrity Guard  (Governance Layer — Institutional Hardening)
# ---------------------------------------------------------------------------
class DataIntegrityError(RuntimeError):
    """
    Raised when the input DataFrame fails the mandatory schema validation.

    This error is non-recoverable. Execution is halted immediately to prevent
    silent data corruption consistent with the Fail Fast governance protocol.
    """


_MANDATORY_COLS: frozenset = frozenset({
    "Trade #",
    "Price",
    "Size",
    "Profit",
    "Type",
})


def _validate_schema(df: pd.DataFrame) -> None:
    """
    Strict schema validator executed before any analytical computation.

    Checks that all mandatory canonical columns are present in the DataFrame.
    Raises DataIntegrityError immediately on the first missing column.

    Parameters
    ----------
    df : pd.DataFrame
        Canonical DataFrame (post column-rename, pre-aggregation).

    Raises
    ------
    DataIntegrityError
        If one or more mandatory columns are absent.
    """
    missing = _MANDATORY_COLS - set(df.columns)
    if missing:
        raise DataIntegrityError(
            "Incompatible CSV Format detected. Please contact your Architect "
            "(Harol Antibar) for a version update."
        )


class DecisionUnit(BaseModel):
    trade_id: int                      # Sequential cluster index (1-based)
    source_trade_ids: List[int]        # Original TradingView Trade # values in this cluster
    entries: int
    net_pnl: float
    pne: float  # Peak Notional Exposure (Exposición Notacional Máxima)
    mae: float  # Max Adverse Excursion (Absoluto)
    mae_pct: float  # MAE como % del PNE
    outcome: str


class IQREAnalytics:
    def __init__(self, df: pd.DataFrame):
        _validate_schema(df)          # Fail Fast — halts on incompatible schema
        self.df = df
        self.units: List[DecisionUnit] = []
        self.equity_curve: List[float] = [0.0]

    def _cluster_by_time_window(self) -> List[List[int]]:
        """
        Group Trade IDs into temporal clusters using time-window overlap detection.

        Two trades are merged into the same Decision Unit when a new entry
        occurs before the previous cycle's final exit (temporal overlap or
        contiguity).  This implements the v1.5 Strategic Aggregation Logic.

        Algorithm
        ---------
        1. For each Trade #, compute: start = first timestamp, end = last exit timestamp.
        2. Sort groups by start time (chronological order).
        3. Merge adjacent groups where next_start <= current_cluster_end.

        Returns
        -------
        List[List[int]]
            Each inner list is a cluster of Trade # IDs forming one Decision Unit.
        """
        if 'Date' not in self.df.columns:
            # Fallback: each Trade # is its own cluster (original behaviour)
            return [[int(tid)] for tid in sorted(self.df['Trade #'].unique())]

        # --- Step 1: compute time window per Trade # ---
        trade_windows: Dict[int, tuple] = {}
        for trade_id, group in self.df.groupby('Trade #', sort=False):
            start = group['Date'].min()
            # Final exit = latest timestamp of any exit row; fall back to row max
            if 'Type' in self.df.columns:
                exit_rows = group[
                    group['Type'].str.lower().str.contains('exit', na=False)
                ]
                end = exit_rows['Date'].max() if len(exit_rows) > 0 else group['Date'].max()
            else:
                end = group['Date'].max()
            trade_windows[int(trade_id)] = (start, end)

        if not trade_windows:
            return []

        # --- Step 2: sort by start time ---
        sorted_ids = sorted(trade_windows.keys(), key=lambda tid: trade_windows[tid][0])

        # --- Step 3: merge overlapping / contiguous intervals ---
        clusters: List[List[int]] = []
        current_cluster = [sorted_ids[0]]
        current_end = trade_windows[sorted_ids[0]][1]

        for trade_id in sorted_ids[1:]:
            start, end = trade_windows[trade_id]
            if start <= current_end:
                # New entry before previous cycle's final exit → same Decision Unit
                current_cluster.append(trade_id)
                current_end = max(current_end, end)
            else:
                clusters.append(current_cluster)
                current_cluster = [trade_id]
                current_end = end

        clusters.append(current_cluster)
        return clusters

    def aggregate_sequences(self) -> List[DecisionUnit]:
        """
        Time-Window Clustering: consolidate raw TradingView rows into Decision Units.

        Replaces the simple Trade # grouping of earlier builds.  Two Trade #
        groups are merged when a new entry occurs before the previous cycle's
        final exit (temporal overlap / contiguity), per v1.5 Strategic
        Aggregation Logic.
        """
        # Ensure chronological ordering before clustering
        if 'Date' in self.df.columns:
            self.df = self.df.sort_values('Date')

        clusters = self._cluster_by_time_window()

        current_equity = 0.0
        for cluster_idx, trade_ids in enumerate(clusters, start=1):
            # All rows belonging to this temporal cluster
            group = self.df[self.df['Trade #'].isin(trade_ids)]

            # 1. PNE: total capital committed across all entries in the cluster
            pne = (group['Price'] * group['Size']).sum()

            # 2. Net PnL: sum of all row profits in the cluster
            net_pnl = group['Profit'].sum()
            current_equity += net_pnl
            self.equity_curve.append(current_equity)

            # 3. MAE: worst adverse excursion across all cluster rows
            raw_mae = group['Profit'].min()
            mae = abs(raw_mae) if raw_mae < 0 else 0.0

            # 4. MAE %: relative to PNE
            mae_pct = (mae / pne * 100) if pne > 0 else 0.0

            unit = DecisionUnit(
                trade_id=cluster_idx,
                source_trade_ids=list(trade_ids),
                entries=len(group),
                net_pnl=net_pnl,
                pne=pne,
                mae=mae,
                mae_pct=mae_pct,
                outcome='WIN' if net_pnl > 0 else 'LOSS',
            )
            self.units.append(unit)

        return self.units

    def _calculate_ulcer_index(self) -> float:
        """
        Calcula el Ulcer Index basado en la curva de equidad consolidada.
        Mide la profundidad y duración de los drawdowns.
        """
        if len(self.equity_curve) < 2:
            return 0.0

        series = pd.Series(self.equity_curve)
        peaks = series.cummax()
        drawdowns = (series - peaks) / peaks.replace(0, 1)
        squared_drawdowns = drawdowns ** 2
        return np.sqrt(squared_drawdowns.mean()) * 100

    def _calculate_recovery_factor(self) -> float:
        """
        Recovery Factor = Total Net Profit / Max Peak-to-Trough Drawdown.

        Measures how efficiently the strategy recovers from its worst
        absolute drawdown.  A value >= 1 means cumulative profit exceeds
        the worst drawdown; a value < 1 means the strategy has not yet
        recovered fully.  Returns float('inf') when no drawdown exists.
        """
        if len(self.equity_curve) < 2:
            return 0.0

        series       = pd.Series(self.equity_curve)
        peaks        = series.cummax()
        max_drawdown = float((peaks - series).max())
        total_profit = float(series.iloc[-1])

        if max_drawdown <= 0:
            return float("inf")
        return total_profit / max_drawdown

    def _calculate_mwrr(self) -> float:
        """
        Cálculo simplificado del Money-Weighted Rate of Return (MWRR).
        Ajustado por el tamaño de las entradas DCA y el PNE.
        """
        if not self.units:
            return 0.0
        df_u = pd.DataFrame([u.model_dump() for u in self.units])
        total_pnl = df_u['net_pnl'].sum()
        avg_pne = df_u['pne'].mean()
        return (total_pnl / avg_pne * 100) if avg_pne > 0 else 0.0

    def _calculate_sharpe_ratio(self) -> float:
        """
        Sharpe Ratio: Mean Return / Standard Deviation of returns.

        Risk-free rate = 0% (per institutional specification).
        Returns are Decision Unit net P&L values (USDT).
        Requires at least 2 Decision Units; returns 0.0 otherwise.

        Formula: Sharpe = mean(R) / std(R, ddof=1)
        """
        if len(self.units) < 2:
            return 0.0
        returns = [u.net_pnl for u in self.units]
        mean_r = float(np.mean(returns))
        std_r = float(np.std(returns, ddof=1))
        return mean_r / std_r if std_r > 0 else 0.0

    def _calculate_sortino_ratio(self) -> float:
        """
        Sortino Ratio: Mean Return / Downside Deviation.

        Downside Deviation is computed using only the negative Decision Unit
        returns: sqrt(mean(R_i^2)) for all R_i < 0.

        A result of float('inf') is returned when there are no losing units
        (no downside to measure).

        Formula: Sortino = mean(R) / sqrt(mean(R_neg^2))
        """
        if not self.units:
            return 0.0
        returns = [u.net_pnl for u in self.units]
        mean_r = float(np.mean(returns))
        neg_returns = [r for r in returns if r < 0]
        if not neg_returns:
            return float('inf')
        downside_dev = float(np.sqrt(np.mean([r ** 2 for r in neg_returns])))
        return mean_r / downside_dev if downside_dev > 0 else 0.0

    def get_summary(self) -> Dict:
        """Genera el resumen ejecutivo para el Auditor Institucional (v1.5)."""
        if not self.units:
            return {}

        df_u = pd.DataFrame([u.model_dump() for u in self.units])
        win_rate = (df_u['outcome'] == 'WIN').mean()

        pos_pnl = df_u[df_u['net_pnl'] > 0]['net_pnl'].sum()
        neg_pnl = abs(df_u[df_u['net_pnl'] < 0]['net_pnl'].sum())
        adj_profit_factor = pos_pnl / neg_pnl if neg_pnl > 0 else np.inf

        rf       = self._calculate_recovery_factor()
        rf_str   = "N/A (No Drawdown)" if not np.isfinite(rf) else f"{rf:.2f}"

        sortino  = self._calculate_sortino_ratio()
        sor_str  = "N/A (No Losses)" if not np.isfinite(sortino) else f"{sortino:.2f}"

        return {
            "Total Decision Units":        len(df_u),
            "Consolidated Win Rate":       f"{win_rate:.2%}",
            "Institutional Profit Factor": round(adj_profit_factor, 2),
            "Sharpe Ratio":                f"{self._calculate_sharpe_ratio():.2f}",
            "Sortino Ratio":               sor_str,
            "Avg Peak Exposure (PNE)":     f"${df_u['pne'].mean():,.2f}",
            "Max MAE Encountered":         f"{df_u['mae_pct'].max():.2f}%",
            "Ulcer Index":                 f"{self._calculate_ulcer_index():.2f}",
            "MWRR (Estimated)":            f"{self._calculate_mwrr():.2f}%",
            "Recovery Factor":             rf_str,
        }


if __name__ == "__main__":
    print("[ENGINE] IQRE v1.5 -- Institutional Recalibration Build.")
    print("[ENGINE] Motor cargado: Time-Window Clustering | Sharpe | Sortino | MWRR | Ulcer.")
    print("[ENGINE] Ejecute 'python main.py' para procesar su base de datos.")
