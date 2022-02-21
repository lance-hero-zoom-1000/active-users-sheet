import pandas as pd
import numpy as np
import logging
import os, json, pygsheets
from Helper.GoogleSheets import BaseOperationsCity
from Helper.meta import top_cities, rename_city_columns

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
gc = pygsheets.authorize(service_account_file=os.environ["GADINEOUT_SERVICE_ACCOUNT"])


class MetricCalculatorCity(BaseOperationsCity):

    # BELOW ARE SOME CLASS VARIABLES
    # SOME ARE DEFINED DURING SOME FUNCTION PERFORMED BY AND INSTANCE
    # THEY ARE:
    # - period
    # - col_name

    active_users_dict = {
        "start": (146, 1),
        "end": 171,
        "key": "Citywise Active Users (Overall)",
    }
    dp_active_users_dict = {
        "start": (175, 1),
        "end": 200,
        "key": "DP Users who launched App",
    }
    loggedin_active_users_dict = {
        "start": (205, 1),
        "end": 230,
        "key": "Logged In Users who launched App (Non DP members)",
    }
    non_loggedin_active_users_dict = {
        "start": (235, 1),
        "end": 260,
        "key": "Non Logged In Users who launched App",
    }
    rdp_viewed_users_dict = {
        "start": (265, 1),
        "end": 290,
        "key": "Users who viewed RDP (Overall)",
    }

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
            print(data["date"].unique())
            
            s = data["date"].unique()[0]
            s = pd.to_datetime(s)
            col_name = s.strftime("%d-%b-%Y")

        cls.col_name = col_name
        logger.info(col_name)
        return col_name

    def __init__(self, custom_date, period="week"):
        super().__init__(custom_date, period)

        # SET PERIOD AS CLASS VARIABLE TOO AS IT WILL B USED
        # FOR CALCULATING COL_NAME
        MetricCalculatorCity.period = period

        logger.info(f"Getting data for dates: {self.start_date} to {self.end_date}")

    def calculate_active_users(self, **kwargs):
        data = kwargs.get("mau_data")

        data['City'] = data['custom_dimension_city'].apply(
            lambda x: x if x in top_cities else 'Others'
        )

        print(data)

        table = pd.DataFrame(data.groupby('City')['users'].sum())
        table.reset_index(inplace=True)
        
        try:
            col_name = MetricCalculatorCity.col_name
        except AttributeError:
            col_name = MetricCalculatorCity.get_column_name(data)

        col_names = (MetricCalculatorCity.active_users_dict.get("key"), col_name)
        table = rename_city_columns(table, col_names)

        return table

    def calculate_dp_active_users(self, **kwargs):
        data = kwargs.get("mau_data")

        data = data.loc[data["subscription_type"]!="non_dp_members"].copy()

        data['City'] = data['custom_dimension_city'].apply(
            lambda x: x if x in top_cities else 'Others'
        )

        table = pd.DataFrame(data.groupby('City')['users'].sum())
        table.reset_index(inplace=True)
        
        try:
            col_name = MetricCalculatorCity.col_name
        except AttributeError:
            col_name = MetricCalculatorCity.get_column_name(data)

        col_names = (MetricCalculatorCity.dp_active_users_dict.get("key"), col_name)
        table = rename_city_columns(table, col_names)

        return table

    def calculate_loggedin_active_users(self, **kwargs):
        data = kwargs.get("mau_data")

        data = data.loc[
            (data["subscription_type"]=="non_dp_members")
            & (data["login_status"]=="active")
        ].copy()

        data['City'] = data['custom_dimension_city'].apply(
            lambda x: x if x in top_cities else 'Others'
        )

        table = pd.DataFrame(data.groupby('City')['users'].sum())
        table.reset_index(inplace=True)
        
        try:
            col_name = MetricCalculatorCity.col_name
        except AttributeError:
            col_name = MetricCalculatorCity.get_column_name(data)

        col_names = (MetricCalculatorCity.loggedin_active_users_dict.get("key"), col_name)
        table = rename_city_columns(table, col_names)

        return table

    def calculate_non_loggedin_active_users(self, **kwargs):
        data = kwargs.get("mau_data")

        data = data.loc[
            (data["subscription_type"]=="non_dp_members")
            & (data["login_status"]!="active")
        ].copy()

        data['City'] = data['custom_dimension_city'].apply(
            lambda x: x if x in top_cities else 'Others'
        )

        table = pd.DataFrame(data.groupby('City')['users'].sum())
        table.reset_index(inplace=True)
        
        try:
            col_name = MetricCalculatorCity.col_name
        except AttributeError:
            col_name = MetricCalculatorCity.get_column_name(data)

        col_names = (MetricCalculatorCity.non_loggedin_active_users_dict.get("key"), col_name)
        table = rename_city_columns(table, col_names)

        return table

    def calculate_rdp_views_overall(self, **kwargs):
        data = kwargs.get("mau_data")

        data['City'] = data['custom_dimension_city'].apply(
            lambda x: x if x in top_cities else 'Others'
        )

        table = pd.DataFrame(data.groupby('City')['restaurants_visited_users'].sum())
        table.reset_index(inplace=True)
        
        try:
            col_name = MetricCalculatorCity.col_name
        except AttributeError:
            col_name = MetricCalculatorCity.get_column_name(data)

        col_names = (MetricCalculatorCity.rdp_viewed_users_dict.get("key"), col_name)
        table = rename_city_columns(table, col_names)

        return table

    