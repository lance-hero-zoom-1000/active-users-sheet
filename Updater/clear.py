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
            mc.transactions_dict,
            mc.gmv_dict,
            mc.transacted_users_dict,
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
            mc.total_rdp_views_dict,
            mc.total_discovery_restaurant_views_dict,
            mc.total_id_restaurant_views_dict,
            mc.total_app_launches_dict,
            mc.total_ff_restaurant_views_dict,
            mc.total_dp_restaurant_views_dict,
            mc.total_reserve_restaurant_views_dict,
        ]

    for r in li:
        mc.clearCustomRange(r)
