#! /usr/local/bin/python3.9

import os
from dotenv import load_dotenv

if os.path.exists("/home/lawrence.veigas/projects/creds/.env"):
    ok = load_dotenv("/home/lawrence.veigas/projects/creds/.env")
elif os.path.exists("C:\Projects\creds\.env"):
    ok = load_dotenv("C:\Projects\creds\.env")
elif os.path.exists("/users/lawrence.veigas/downloads/projects/creds/.env"):
    ok = load_dotenv("/users/lawrence.veigas/downloads/projects/creds/.env")
print("Environment Variables Loaded: ", ok)

from Updater.update_regular import update_regular
from Updater.update_citywise import update_citywise
from Updater.clear import clear
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import json
import argparse
import traceback
from dineout_common.dineout_funcs import send_email
from Helper.meta import calculate_date_range

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(funcName)s:%(name)s:%(message)s"
)

stream = logging.StreamHandler()
stream.setFormatter(formatter)
logger.addHandler(stream)


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
        if args.period != "dod":
            date_range = [args.custom_date]
        else:
            date_range = calculate_date_range(
                args.custom_date - relativedelta(weeks=6),
                args.custom_date,
                period=args.period,
            )
            # CLEAR EXISTING VALUES
            if not args.test:
                clear(period=args.period)
                clear(period=args.period, city=True)

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
