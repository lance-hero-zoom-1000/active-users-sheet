from Helper.MetricCalculatorCity import MetricCalculatorCity
from Helper.DataFetcher import DataFetcher
from datetime import datetime
from dateutil.relativedelta import relativedelta


def update_citywise(drange, period):
    f = len(drange)

    for i, d in enumerate(drange):
        i = i + 1

        if i == f:
            format = True
        else:
            format = False

        print(f"Updating for {d}, period: {period}")

        df = DataFetcher(d, period=period)
        updater = MetricCalculatorCity(d, period=period)

        mau_data = df.get_mau_data()

        # calculate different tables
        a = updater.calculate_active_users(mau_data=mau_data)
        b = updater.calculate_dp_active_users(mau_data=mau_data)
        c = updater.calculate_loggedin_active_users(mau_data=mau_data)
        d = updater.calculate_non_loggedin_active_users(mau_data=mau_data)
        e = updater.calculate_rdp_views_overall(mau_data=mau_data)
        f = updater.calculate_transaction(mau_data=mau_data)
        g = updater.calculate_gmv(mau_data=mau_data)
        h = updater.calculate_app_launch_to_transaction_percentage(mau_data=mau_data)
        i = updater.calculate_rdp_view_to_transaction_percentage(mau_data=mau_data)

        # update sheet
        updater.update_sheet(a, MetricCalculatorCity.active_users_dict, format=format)
        updater.update_sheet(
            b, MetricCalculatorCity.dp_active_users_dict, format=format
        )
        updater.update_sheet(
            c, MetricCalculatorCity.loggedin_active_users_dict, format=format
        )
        updater.update_sheet(
            d, MetricCalculatorCity.non_loggedin_active_users_dict, format=format
        )
        updater.update_sheet(
            e, MetricCalculatorCity.rdp_viewed_users_dict, format=format
        )
        updater.update_sheet(f, MetricCalculatorCity.transactions_dict, format=format)
        updater.update_sheet(g, MetricCalculatorCity.gmv_dict, format=format)
        updater.update_sheet(
            h, MetricCalculatorCity.app_launch_to_transaction_dict, format=format
        )
        updater.update_sheet(
            i, MetricCalculatorCity.rdp_view_to_transaction_dict, format=format
        )

        del df, updater, mau_data


if __name__ == "__main__":
    yesterday = datetime.today() - relativedelta(days=1)
    update_citywise([yesterday], period="day")
