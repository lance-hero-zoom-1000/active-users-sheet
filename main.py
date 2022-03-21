#! /usr/local/bin/python3.9

import os
from dotenv import load_dotenv

if os.path.exists("/home/lawrence.veigas/projects/creds/.env"):
    ok = load_dotenv("/home/lawrence.veigas/projects/creds/.env")
else:
    ok = load_dotenv("/users/lawrence.veigas/downloads/projects/creds/.env")
print("Environment Variables Loaded: ", ok)

from Updater.update_regular import update_regular
from Updater.update_citywise import update_citywise
from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta
import logging
import json
import numpy as np
import pandas as pd
import argparse
import traceback
from dineout_common.dineout_funcs import send_email

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

    elif period == "dod":
        for date in drange:
            date = date.to_pydatetime()
            today_weekday = datetime.today().weekday()
            if (date.weekday() + 1) % 7 == today_weekday:
                date_range.append(date)

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

    try:
        if args.test:
            print(date_range)
        else:
            update_regular(date_range, period=args.period)
            update_citywise(date_range, period=args.period)
    except:
        with open("error.txt", "w") as f:
            traceback.print_exc(file=f)
        traceback.print_exc()

        with open(os.environ["DINEOUT_DB_CREDENTIALS"], "r") as f:
            creds = json.load(f)
        send_email(
            creds.get("app_pass"),
            subject="GA Sheet Automation Failed",
            body="Monday Meeting Data Sheet has failed to update<br>Please look into it",
            error=True,
            cc=[
                "priyanka.grover@dineout.co.in",
                "vandit.gupta@dineout.co.in",
                "jithin.haridas@dineout.co.in",
            ],
        )
