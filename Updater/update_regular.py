from Helper.DataFetcher import DataFetcher
from Helper.MetricCalculator import MetricCalculator
from datetime import datetime
from dateutil.relativedelta import relativedelta


def update_regular(drange, period):
    f = len(drange)

    for i, custom_date in enumerate(drange):
        i = i + 1

        if i == f:
            format = True
        else:
            format = False

        print(f"Updating for {custom_date}, period: {period}")

        data_fetcher = DataFetcher(custom_date, period=period)
        metric_calculator = MetricCalculator(custom_date, period=period)

        # get data
        mau_data = data_fetcher.get_mau_data()
        dp_membership = data_fetcher.get_memberships_data()

        # calculate different tables
        active_users_df = metric_calculator.active_users(mau_data=mau_data)
        rdp_views_df = metric_calculator.rdp_viewed_users(mau_data=mau_data)
        dopay_users_df = metric_calculator.dopay_users(mau_data=mau_data)
        dopay_trans_df = metric_calculator.dopay_transactions(mau_data=mau_data)
        dp_members_df = metric_calculator.dp_users_in_db(membership=dp_membership)
        dp_active_df = metric_calculator.dp_active_users(mau_data=mau_data)
        dp_redeemed_df = metric_calculator.dp_redemption_users(mau_data=mau_data)
        no_redemptions_df = metric_calculator.dp_redemptions(mau_data=mau_data)

        # update tables on google sheets
        metric_calculator.update_sheet(
            active_users_df, MetricCalculator.active_users_dict, format=format
        )
        metric_calculator.update_sheet(
            rdp_views_df,
            bounds_dict=MetricCalculator.rdp_viewed_users_dict,
            format=format,
        )
        metric_calculator.update_sheet(
            dopay_users_df, bounds_dict=MetricCalculator.dopay_users_dict, format=format
        )
        metric_calculator.update_sheet(
            dopay_trans_df,
            bounds_dict=MetricCalculator.dopay_transactions_dict,
            format=format,
        )
        metric_calculator.update_sheet(
            dp_members_df,
            bounds_dict=MetricCalculator.dp_users_in_db_dict,
            format=format,
        )
        metric_calculator.update_sheet(
            dp_active_df,
            bounds_dict=MetricCalculator.dp_active_users_dict,
            format=format,
        )
        metric_calculator.update_sheet(
            dp_redeemed_df,
            bounds_dict=MetricCalculator.dp_redemption_users_dict,
            format=format,
        )
        metric_calculator.update_sheet(
            no_redemptions_df,
            bounds_dict=MetricCalculator.dp_redemptions_dict,
            format=format,
        )

        del data_fetcher
        del metric_calculator
        del mau_data
        del dp_membership


if __name__ == "__main__":
    yesterday = datetime.today() - relativedelta(days=1)

    update_regular([yesterday], period="day")
