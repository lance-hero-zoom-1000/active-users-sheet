import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
import logging
import pandas as pd
from dineout_common.dineout_funcs import get_data_from_server
import Helper.queries as queries


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(funcName)s:%(name)s:%(message)s"
)

stream = logging.StreamHandler()
stream.setFormatter(formatter)
logger.addHandler(stream)


with open(os.environ["DINEOUT_DB_CREDENTIALS"], "r") as f:
    creds = json.load(f)


class DataFetcher:
    base_path = os.getcwd() + "/dumps"
    if not os.path.exists(base_path):
        os.mkdir(base_path)

    def __init__(self, custom_date, period="week"):
        self.end_date = datetime.combine(custom_date.date(), datetime.min.time())

        if period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
        elif period == "month":
            self.start_date = self.end_date.replace(day=1)
        else:
            self.start_date = self.end_date

        self.period = period

        logger.info(f"Getting data for dates: {self.start_date} to {self.end_date}")

    def get_memberships_data(self):
        logger.info("Reading membership data from dumps folder of ga_dashboard")
        dumps_base_path = os.environ["DP_DUMPS_PATH"]
        db_memberships = pd.read_parquet(f"{dumps_base_path}/membership.parquet")

        db_memberships = db_memberships[
            (db_memberships["end_date"] >= self.start_date)
            & (db_memberships["created_at"] <= self.end_date)
        ]
        db_memberships["Month"] = db_memberships["created_at"].apply(
            lambda x: x.strftime("%b'%y")
        )
        return db_memberships

    def get_redemptions_data(self):
        logger.info("Reading redemptions data from dumps folder of ga_dashboard")
        dumps_base_path = os.environ["DP_DUMPS_PATH"]
        db_redemptions = pd.read_parquet(f"{dumps_base_path}/redemptions.parquet")

        db_redemptions = db_redemptions[
            (db_redemptions["created_at"] >= self.start_date)
            & (db_redemptions["created_at"] <= self.end_date)
        ]
        db_redemptions["Month"] = db_redemptions["created_at"].apply(
            lambda x: x.strftime("%b'%y")
        )
        return db_redemptions

    def get_do_pay_data(self):
        file_path = (
            DataFetcher.base_path
            + f"/doPayData_{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )
        if os.path.exists(file_path):
            logger.info("Using extract")
            do_pay_data = pd.read_parquet(file_path)
            return do_pay_data
        else:
            logger.info("Getting transactions data")
            do_pay_data = get_data_from_server(
                queries.do_pay_query.format(
                    sdate=self.start_date.strftime("%Y-%m-%d"),
                    edate=self.end_date.strftime("%Y-%m-%d"),
                ),
                creds.get("Dineout"),
            )
            logger.info("Cleaning transactions data")
            do_pay_data["ot_instant_discount_value"] = pd.to_numeric(
                do_pay_data["ot_instant_discount_value"], errors="coerce"
            )
            do_pay_data["ot_instant_discount_value"].fillna(0, inplace=True)
            do_pay_data["gmv"] = (
                do_pay_data["amount"] - do_pay_data["ot_instant_discount_value"]
            )
            do_pay_data["Month"] = do_pay_data["created_on"].apply(
                lambda x: x.strftime("%b'%y")
            )

            logger.info("Saving data to dumps")
            do_pay_data.to_parquet(file_path)

            return do_pay_data

    def get_do_pay_new_diners(self):
        file_name = "newDinersDoPay"
        use_extract = False
        present = (False, "NA")
        file_path = (
            DataFetcher.base_path
            + f"/{file_name}_till_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )

        for file in os.listdir(DataFetcher.base_path):
            if file_name in file:
                present = (True, DataFetcher.base_path + f"/{file}")

        if present[0]:
            present_till = re.search(
                "[a-zA-Z]+_till_([\d]{4}-[\d]{2}-[\d]{2}).parquet", present[1]
            ).group(1)
            present_till = datetime.strptime(present_till, "%Y-%m-%d")
            if present_till > (self.end_date - relativedelta(days=5)):
                use_extract = True

        if use_extract:
            logger.info("Using extract")
            new_diners_transactions = pd.read_parquet(present[1])
            return new_diners_transactions
        else:
            logger.info("Getting new transaction diners")
            new_diners_transactions = get_data_from_server(
                queries.new_diners_transactions,
                creds.get("Dineout"),
            )
            logger.info("Saving data to dumps")
            new_diners_transactions.to_parquet(file_path)

            return new_diners_transactions

    def get_reserve_new_diners(self):
        file_name = "newDinersReserve"
        use_extract = False
        present = (False, "NA")
        file_path = (
            DataFetcher.base_path
            + f"/{file_name}_till_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )

        for file in os.listdir(DataFetcher.base_path):
            if file_name in file:
                present = (True, DataFetcher.base_path + f"/{file}")

        if present[0]:
            present_till = re.search(
                "[a-zA-Z]+_till_([\d]{4}-[\d]{2}-[\d]{2}).parquet", present[1]
            ).group(1)
            present_till = datetime.strptime(present_till, "%Y-%m-%d")
            if present_till > (self.end_date - relativedelta(days=5)):
                use_extract = True

        if use_extract:
            logger.info("Using extract")
            new_diners_reserve = pd.read_parquet(present[1])
            return new_diners_reserve
        else:
            logger.info("Getting dopay new diners")
            new_diners_reserve = get_data_from_server(
                queries.new_diners_bookings,
                creds.get("Dineout"),
            )
            logger.info("Saving data to dumps")
            new_diners_reserve.to_parquet(file_path)
            return new_diners_reserve

    def get_sign_up_data(self):
        file_name = "signUpData"
        use_extract = False
        present = (False, "NA")
        file_path = (
            DataFetcher.base_path
            + f"/{file_name}_till_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )

        for file in os.listdir(DataFetcher.base_path):
            if file_name in file:
                present = (True, DataFetcher.base_path + f"/{file}")

        if present[0]:
            present_till = re.search(
                "[a-zA-Z]+_till_([\d]{4}-[\d]{2}-[\d]{2}).parquet", present[1]
            ).group(1)
            present_till = datetime.strptime(present_till, "%Y-%m-%d")
            if present_till > (self.end_date - relativedelta(days=5)):
                use_extract = True

        if use_extract:
            logger.info("Using extract")
            sign_up_data = pd.read_parquet(present[1])
            return sign_up_data
        else:
            logger.info("Getting SignUp data")
            sign_up_data = get_data_from_server(
                queries.sign_up_query,
                creds.get("Dineout"),
            )
            logger.info("Saving data to dumps")
            sign_up_data.to_parquet(file_path)
            return sign_up_data

    def get_reservation_data(self):
        file_path = (
            DataFetcher.base_path
            + f"/reservationData_{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )
        if os.path.exists(file_path):
            logger.info("Using extract")
            reservations = pd.read_parquet(file_path)
            return reservations
        else:
            logger.info("Getting reservations data")
            reservations = get_data_from_server(
                queries.bookings_query.format(
                    sdate=self.start_date.strftime("%Y-%m-%d"),
                    edate=self.end_date.strftime("%Y-%m-%d"),
                ),
                creds.get("Dineout"),
            )
            reservations.to_parquet(file_path)
            return reservations

    def get_mau_data(self):
        if self.period == "day":
            query = queries.mau_data_day
        elif self.period == "week":
            query = queries.mau_data_week
        elif self.period == "month":
            query = queries.mau_data_month

        fname_period = self.period.title()
        file_name = "mauData" + fname_period
        logger.debug(f"Searching for: {file_name}")
        use_extract = False
        file_path = (
            DataFetcher.base_path
            + f"/{file_name}_{self.start_date.strftime('%Y-%m-%d')}_to_{self.end_date.strftime('%Y-%m-%d')}.parquet"
        )

        for file in os.listdir(DataFetcher.base_path):
            f = DataFetcher.base_path + "/" + file
            if file_path == f:
                use_extract = True

        if use_extract:
            logger.info("Using extract")
            data = pd.read_parquet(file_path)

            return data
        else:
            logger.info("Getting MAU data")
            data = get_data_from_server(
                query=query.format(
                    sdate=self.start_date.strftime('%Y-%m-%d'),
                    edate=self.end_date.strftime('%Y-%m-%d')
                ),
                server_creds=creds.get("Torqus-ReadOnly"),
            )
            logger.info("Saving data to dumps")

            if self.period == "week":
                data = data[
                    (data["week_start_date"] >= self.start_date.date())
                    & (data["week_start_date"] <= self.end_date.date())
                ]
            elif self.period == "month":
                data["month"] = data["month"].dt.date
                data = data[
                    (data["month"] >= self.start_date.date())
                    & (data["month"] <= self.end_date.date())
                ]
            else:
                data["date"] = data["date"].dt.date
                data = data[(data["date"] == self.end_date.date())]

            data.to_parquet(file_path)
            return data
