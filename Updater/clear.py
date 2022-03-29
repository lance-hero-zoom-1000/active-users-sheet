from datetime import datetime

from Helper.MetricCalculator import MetricCalculator
from Helper.MetricCalculatorCity import MetricCalculatorCity


def clear(period, city=False):
    # MC IS METRIC CALCULATOR

    if city:
        mc = MetricCalculatorCity(datetime.today(), period=period)
        li = [
            mc.active_users_dict,
            mc.dp_active_users_dict,
            mc.loggedin_active_users_dict,
            mc.non_loggedin_active_users_dict,
            mc.rdp_viewed_users_dict,
        ]
    else:
        mc = MetricCalculator(datetime.today(), period=period)
        li = [
            mc.active_users_dict,
            mc.rdp_viewed_users_dict,
            mc.dopay_users_dict,
            mc.dopay_transactions_dict,
            mc.dp_users_in_db_dict,
            mc.dp_active_users_dict,
            mc.dp_redemption_users_dict,
            mc.dp_redemptions_dict,
        ]

    for r in li:
        mc.clearCustomRange(r)
