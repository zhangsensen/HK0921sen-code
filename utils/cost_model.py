"""Hong Kong trading cost model."""
from __future__ import annotations


class HongKongTradingCosts:
    """Estimate HK equity trading costs."""

    def __init__(self) -> None:
        self.stamp_duty = 0.0013
        self.commission = 0.001
        self.slippage = 0.0008
        self.min_commission = 50.0

    def calculate_total_cost(self, trade_value: float) -> float:
        stamp_duty_cost = trade_value * self.stamp_duty
        commission_cost = max(trade_value * self.commission, self.min_commission)
        slippage_cost = trade_value * self.slippage
        return stamp_duty_cost + commission_cost + slippage_cost
