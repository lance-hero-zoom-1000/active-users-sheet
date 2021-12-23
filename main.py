#! /usr/local/bin/python3.9

from MetricCalculator import MetricCalculator
from DataFetcher import DataFetcher
from FormatHelper import FormatHelper
from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta
import logging
import numpy as np
import pandas as pd
import argparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(funcName)s:%(name)s:%(message)s"
)

stream = logging.StreamHandler()
stream.setFormatter(formatter)
logger.addHandler(stream)


def calculate_date_range(start_date, end_date, period="month"):
    drange = pd.date_range(start=start_date, end=end_date)

    date_range = []
    if period == "month":
        for date in drange:
            last_date = calendar.monthrange(date.year, date.month)[1]

            last_date = datetime(date.year, date.month, last_date)
            # check that date created is greater than and start date and less than end date
            if (last_date >= start_date) & (last_date <= end_date):
                date_range.append(last_date)

    elif period == "week":
        for date in drange:
            date = date.to_pydatetime()
            week_day = date - timedelta(days=date.weekday() + 1)

            if (week_day >= start_date) & (week_day <= end_date):
                date_range.append(week_day)

    elif period == "day":
        for date in drange:
            date_to_append = date.to_pydatetime()

            date_range.append(date_to_append)

    date_range = np.unique(np.array(date_range))
    return date_range


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--test", action="store_true")
    parser.add_argument("-p", "--period", default="day")
    parser.add_argument(
        "-cd",
        "--custom_date",
        default=datetime.combine(
            datetime.today() - relativedelta(days=1), datetime.min.time()
        ),
    )
    parser.add_argument("-cr", "--custom_run", nargs=2, default=None)

    args = parser.parse_args()

    logger.info(f"Arguments passed: {args}")

    # handle custom date
    if isinstance(args.custom_date, str):
        args.custom_date = datetime.strptime(args.custom_date, "%Y-%m-%d")

    # handle custom run
    if args.custom_run == None:
        date_range = [args.custom_date]
    else:
        date_range = tuple(args.custom_run)
        start_date = datetime.strptime(date_range[0], "%Y-%m-%d")
        end_date = datetime.strptime(date_range[1], "%Y-%m-%d")
        date_range = calculate_date_range(start_date, end_date, period=args.period)

    logger.info("Creating instances of helpers")

    for custom_date in date_range:
        # custom_date = d
        logger.info(custom_date)
        data_fetcher = DataFetcher(custom_date, period=args.period)

        if args.period == "day":
            format_helper = FormatHelper("Daily Summary")
        elif args.period == "week":
            format_helper = FormatHelper("Week Summary")
        else:
            format_helper = FormatHelper("Month Summary")

        metric_calculator = MetricCalculator(custom_date, period=args.period)

        mau_data = data_fetcher.get_mau_data()
        dp_membership = data_fetcher.get_memberships_data()
        MetricCalculator.get_column_name(mau_data)

        print(
            metric_calculator.update_sheet(
                metric_calculator.active_users(mau_data=mau_data),
                bounds_dict=MetricCalculator.active_users_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.rdp_viewed_users(mau_data=mau_data),
                bounds_dict=MetricCalculator.rdp_viewed_users_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dopay_users(mau_data=mau_data),
                bounds_dict=MetricCalculator.dopay_users_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dopay_transactions(mau_data=mau_data),
                bounds_dict=MetricCalculator.dopay_transactions_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dp_users_in_db(membership=dp_membership),
                bounds_dict=MetricCalculator.dp_users_in_db_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dp_active_users(mau_data=mau_data),
                bounds_dict=MetricCalculator.dp_active_users_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dp_redemption_users(mau_data=mau_data),
                bounds_dict=MetricCalculator.dp_redemption_users_dict,
                formatter=format_helper,
            )
        )

        print(
            metric_calculator.update_sheet(
                metric_calculator.dp_redemptions(mau_data=mau_data),
                bounds_dict=MetricCalculator.dp_redemptions_dict,
                formatter=format_helper,
            )
        )

        del data_fetcher
        del format_helper
        del metric_calculator
