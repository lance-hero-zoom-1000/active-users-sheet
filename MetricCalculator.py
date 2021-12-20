from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os, json, pygsheets
from FormatHelper import FormatHelper

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
gc = pygsheets.authorize(os.environ["GOOGLE_OAUTH_CREDENTIALS"])


class MetricCalculator:

    # BELOW ARE SOME CLASS VARIABLES
    # SOME ARE DEFINED DURING SOME FUNCTION PERFORMED BY AND INSTANCE
    # THEY ARE:
    # - period
    # - col_name

    active_users_dict = {
        "start": (3, 2),
        "end": 11,
        "subsections": [5, 6, 8, 9],
        "key": "Active Users",
    }
    rdp_viewed_users_dict = {
        "start": (31, 2),
        "end": 39,
        "subsections": [5, 6, 8, 9],
        "key": "RDP Viewed Users",
    }
    dopay_users_dict = {
        "start": (50, 2),
        "end": 55,
        "subsections": [5, 6],
        "key": "Dineout Pay Transacted Users",
    }
    dopay_transactions_dict = {
        "start": (57, 2),
        "end": 68,
        "subsections": [5, 6, 11, 12],
        "key": "Dineout Pay Transactions and GMV",
    }
    dp_users_in_db_dict = {
        "start": (88, 2),
        "end": 94,
        "subsections": [3, 4, 5, 6, 7],
        "key": "Users with Active DP Subscription",
    }
    dp_active_users_dict = {
        "start": (96, 2),
        "end": 102,
        "subsections": [3, 4, 5, 6, 7],
        "key": "DP Users who launched App",
    }
    dp_redemption_users_dict = {
        "start": (112, 2),
        "end": 118,
        "subsections": [3, 4, 5, 6, 7],
        "key": "DP Users who redeemed",
    }
    dp_redemptions_dict = {
        "start": (128, 2),
        "end": 134,
        "subsections": [3, 4, 5, 6, 7],
        "key": "Total Redemptions by DP Users",
    }

    sheet = gc.open_by_url(creds.get("sheets").get("ga_sheet_automation"))

    @staticmethod
    def remove_nulls_from_list(li):
        container = []
        if isinstance(li, (list, np.ndarray, pd.Series)):
            for value in li:
                if (pd.notnull(value)) & (value not in ["null", "NA", "nan"]):
                    container.append(value)
        return container

    @staticmethod
    def subscription_type(li):
        if "Paid" in li:
            return "paid"
        elif "prime" in li:
            return "prime"
        elif (
            ("dopass_migration" in li)
            | ("doplus_migration" in li)
            | ("gp_migration" in li)
        ):
            return "migration"
        else:
            return "others"

    @classmethod
    def get_column_name(cls, data):
        if cls.period == "week":
            s = data["week_start_date"].unique()[0]
            e = data["week_end_date"].unique()[0]

            col_name = s.strftime("%d-%b") + " to " + e.strftime("%d-%b")
        elif cls.period == "month":
            s = data["month"].unique()[0]
            s = pd.to_datetime(s)
            col_name = s.strftime("%b'%y")
        else:
            s = data["date"].unique()[0]
            s = pd.to_datetime(s)
            col_name = s.strftime("%d-%b-%Y")

        cls.col_name = col_name
        logger.info(col_name)
        return col_name

    def __init__(self, custom_date, period="week"):
        self.end_date = datetime.combine(custom_date.date(), datetime.min.time())
        self.period = period

        # SET PERIOD AS CLASS VARIABLE TOO AS IT WILL B USED
        # FOR CALCULATING COL_NAME
        MetricCalculator.period = period

        if self.period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
            self.automation_tab = MetricCalculator.sheet.worksheet_by_title(
                "Week Summary"
            )
        elif self.period == "month":
            self.start_date = self.end_date.replace(day=1)
            self.automation_tab = MetricCalculator.sheet.worksheet_by_title(
                "Month Summary"
            )
        else:
            self.start_date = self.end_date
            self.automation_tab = MetricCalculator.sheet.worksheet_by_title(
                "Daily Summary"
            )

        logger.info(f"Getting data for dates: {self.start_date} to {self.end_date}")

    def active_users(self, **kwargs):
        data = kwargs.get("mau_data")

        table = {
            "No. of Users who launched the App (A)": data["users"].sum(),
            "DP members": data["users"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Non DP (Logged in users)": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
            ].sum(),
            "New Users": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
            "Non Logged in users": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
            ].sum(),
            "New Users ": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users ": data["users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.active_users_dict.get("key"), col_name]
        return table

    def rdp_viewed_users(self, **kwargs):
        data = kwargs.get("mau_data")

        table = {
            "No. of Users who Viewed RDP (B)": data["restaurants_visited_users"].sum(),
            "DP members": data["restaurants_visited_users"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Non DP (Logged in users)": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
            ].sum(),
            "New Users": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
            "Non Logged in users": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
            ].sum(),
            "New Users ": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users ": data["restaurants_visited_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] != "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.rdp_viewed_users_dict.get("key"), col_name]
        return table

    def dopay_users(self, **kwargs):
        data = kwargs.get("mau_data")

        table = {
            "No. of Users who Transacted via Dineout Pay": data[
                "do_pay_transacted_users"
            ].sum(),
            "DP members": data["do_pay_transacted_users"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Non DP (Logged in users)": data["do_pay_transacted_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
            ].sum(),
            "New Users": data["do_pay_transacted_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users": data["do_pay_transacted_users"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.dopay_users_dict.get("key"), col_name]
        return table

    def dopay_transactions(self, **kwargs):
        data = kwargs.get("mau_data")

        table = {
            "Total Dineout Pay Transactions": data["do_pay_transactions"].sum(),
            "DP members": data["do_pay_transactions"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Non DP (Logged in users)": data["do_pay_transactions"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
            ].sum(),
            "New Users": data["do_pay_transactions"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users": data["do_pay_transactions"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
            " ": " ",
            "Total Dineout Pay GMV": data["do_pay_gmv"].sum(),
            "DP members ": data["do_pay_gmv"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Non DP (Logged in users) ": data["do_pay_gmv"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
            ].sum(),
            "New Users ": data["do_pay_gmv"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 1)
            ].sum(),
            "Returning Users ": data["do_pay_gmv"][
                (data["subscription_type"] == "non_dp_members")
                & (data["login_status"] == "active")
                & (data["new_visitor_flag"] == 0)
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.dopay_transactions_dict.get("key"), col_name]
        return table

    def dp_active_users(self, **kwargs):
        data = kwargs.get("mau_data")
        if isinstance(data, pd.DataFrame):
            pass
        elif data == None:
            raise ValueError("Please pass MAU data as keyword argument 'mau_data'")

        table = {
            "Total DP Users Active on App": data["users"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Paid": data["users"][data["subscription_type"] == "paid"].sum(),
            "Prime": data["users"][data["subscription_type"] == "prime"].sum(),
            "HDFC": data["users"][data["subscription_type"] == "hdfc"].sum(),
            "Migration": data["users"][data["subscription_type"] == "migration"].sum(),
            "Others/Unpaid": data["users"][data["subscription_type"] == "others"].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.dp_active_users_dict.get("key"), col_name]
        return table

    def dp_users_in_db(self, **kwargs):
        memberships = kwargs.get("membership")

        if isinstance(memberships, pd.DataFrame):
            pass
        elif memberships == None:
            raise ValueError(
                "Please pass memberhips data as keyword argument 'membership'"
            )

        memberships.sort_values(
            by=["diner_id", "end_date"], ascending=[True, False], inplace=True
        )
        paid_memberships = memberships.loc[
            memberships["subscription_type"] == "Paid"
        ].copy()
        paid_memberships = (
            paid_memberships.groupby(["diner_id"])
            .agg(
                {
                    "subscription_type": lambda x: list(np.hstack(x)),
                    "end_date": "first",
                    "card_name": lambda x: list(np.hstack(x)),
                }
            )
            .reset_index()
        )
        memberships = memberships.loc[
            ~memberships["diner_id"].isin(paid_memberships["diner_id"].unique())
        ].copy()
        memberships = (
            memberships.groupby(["diner_id"])
            .agg(
                {
                    "subscription_type": lambda x: list(np.hstack(x)),
                    "end_date": "first",
                    "card_name": lambda x: list(np.hstack(x)),
                }
            )
            .reset_index()
        )
        memberships = pd.concat([memberships, paid_memberships], ignore_index=True)
        memberships["subscription_type"] = memberships["subscription_type"].apply(
            lambda x: MetricCalculator.subscription_type(x)
        )
        memberships["card_name"] = memberships["card_name"].apply(
            lambda x: MetricCalculator.remove_nulls_from_list(x)
        )
        memberships["card_name"] = memberships["card_name"].apply(
            lambda x: max(set(x), key=x.count) if len(x) > 0 else np.nan
        )
        memberships["subscription_type"].loc[
            (memberships["subscription_type"].isin(["others"]))
            & (memberships["card_name"].str.contains("hdfc", case=False, na=False))
        ] = "hdfc"

        table = {
            "Total Users with Active DP Subscription": memberships[
                "diner_id"
            ].nunique(),
            "Paid": memberships["diner_id"][
                memberships["subscription_type"] == "paid"
            ].nunique(),
            "Prime": memberships["diner_id"][
                memberships["subscription_type"] == "prime"
            ].nunique(),
            "HDFC": memberships["diner_id"][
                memberships["subscription_type"] == "hdfc"
            ].nunique(),
            "Migration": memberships["diner_id"][
                memberships["subscription_type"] == "migration"
            ].nunique(),
            "Others/Unpaid": memberships["diner_id"][
                memberships["subscription_type"] == "others"
            ].nunique(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            raise AttributeError(
                "Need to calculate other metrics before calculating this. Call after using active_users"
            )

        table.columns = [MetricCalculator.dp_users_in_db_dict.get("key"), col_name]
        return table

    def dp_redemption_users(self, **kwargs):
        data = kwargs.get("mau_data")
        if isinstance(data, pd.DataFrame):
            pass
        elif data == None:
            raise ValueError("Please pass MAU data as keyword argument 'mau_data'")

        table = {
            "Total DP Users who redeemed": data["dp_redeemed_users"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Paid": data["dp_redeemed_users"][
                data["subscription_type"] == "paid"
            ].sum(),
            "Prime": data["dp_redeemed_users"][
                data["subscription_type"] == "prime"
            ].sum(),
            "HDFC": data["dp_redeemed_users"][
                data["subscription_type"] == "hdfc"
            ].sum(),
            "Migration": data["dp_redeemed_users"][
                data["subscription_type"] == "migration"
            ].sum(),
            "Others/Unpaid": data["dp_redeemed_users"][
                data["subscription_type"] == "others"
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.dp_redemption_users_dict.get("key"), col_name]
        return table

    def dp_redemptions(self, **kwargs):
        data = kwargs.get("mau_data")
        if isinstance(data, pd.DataFrame):
            pass
        elif data == None:
            raise ValueError("Please pass MAU data as keyword argument 'mau_data'")

        table = {
            "Total Redemptions": data["redemptions"][
                data["subscription_type"] != "non_dp_members"
            ].sum(),
            "Paid": data["redemptions"][data["subscription_type"] == "paid"].sum(),
            "Prime": data["redemptions"][data["subscription_type"] == "prime"].sum(),
            "HDFC": data["redemptions"][data["subscription_type"] == "hdfc"].sum(),
            "Migration": data["redemptions"][
                data["subscription_type"] == "migration"
            ].sum(),
            "Others/Unpaid": data["redemptions"][
                data["subscription_type"] == "others"
            ].sum(),
        }

        table = pd.DataFrame.from_dict(table, orient="index")
        table.reset_index(inplace=True)

        try:
            col_name = MetricCalculator.col_name
        except AttributeError:
            col_name = MetricCalculator.get_column_name(data)

        table.columns = [MetricCalculator.dp_redemptions_dict.get("key"), col_name]
        return table

    @staticmethod
    def get_data_from_sheet(bounds, wks):
        # bounds should be a dict of tuples with start and end
        start = bounds.get("start")
        end = bounds.get("end")
        cols = wks.get_row(start[0], include_tailing_empty=False)
        if len(cols) > 0:
            col_n = len(cols)
            latest_column = cols[-1]

            logger.info(latest_column)
            logger.info(MetricCalculator.col_name)
            logger.info(latest_column == MetricCalculator.col_name)
            if latest_column == MetricCalculator.col_name:
                col_n = col_n - 1

            df = wks.get_as_df(
                start=start,
                end=(end, col_n),
            )

            return df
        else:
            return None

    def update_sheet(
        self,
        data_to_update,
        bounds_dict,
        formatter=None,
    ):

        wks = self.automation_tab
        logger.debug(f"Using worksheet: {wks}")
        sheet_data = MetricCalculator.get_data_from_sheet(bounds_dict, wks)

        if isinstance(sheet_data, pd.DataFrame):
            if not sheet_data.empty:
                data_to_update = sheet_data.merge(
                    data_to_update, on=bounds_dict.get("key"), how="left"
                )

        if bounds_dict.get("copy_head") != None:
            copy_head = bounds_dict.get("copy_head")
        else:
            copy_head = True
        wks.set_dataframe(
            data_to_update, nan="", start=bounds_dict.get("start"), copy_head=copy_head
        )

        if isinstance(formatter, FormatHelper):
            formatter.format_worksheet(bounds_dict)

        return "Updated Successfully"
