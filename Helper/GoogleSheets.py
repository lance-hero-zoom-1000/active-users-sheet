from datetime import datetime
from Helper.FormatHelper import FormatHelper
import pandas as pd
import numpy as np
import logging
from Helper.meta import get_credentials
from datetime import timedelta
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    datefmt="%d-%b-%Y %H:%M:%S",
)

file_handler = logging.FileHandler(f"logs/ga_sheet_automation.log")
stream_handler = logging.StreamHandler()

file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class BaseOperations:
    creds = get_credentials()
    gc = get_credentials(type="gc")
    sheet = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1RH3YmlHvQgyAZaCSzL3Tp2KXIb0oARedkLWuRow6rMU/edit?usp=sharing"
    )

    def __init__(self, end_date: datetime, period: str):
        self.end_date = datetime.combine(end_date.date(), datetime.max.time())
        self.period = period

        if self.period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
            self.wks = BaseOperations.sheet.worksheet_by_title("Week Summary")
            self.formatter = FormatHelper("Week Summary")

        elif self.period == "month":
            self.start_date = self.end_date.replace(day=1)
            self.wks = BaseOperations.sheet.worksheet_by_title("Month Summary")
            self.formatter = FormatHelper("Month Summary")

        elif self.period == "dod":
            self.start_date = self.end_date
            self.wks = BaseOperations.sheet.worksheet_by_title("DoD Summary")
            self.formatter = FormatHelper("DoD Summary")

        else:
            self.start_date = self.end_date
            self.wks = BaseOperations.sheet.worksheet_by_title("Daily Summary")
            self.formatter = FormatHelper("Daily Summary")

        self.end_date = datetime.combine(self.end_date.date(), datetime.max.time())
        self.start_date = datetime.combine(self.start_date.date(), datetime.min.time())

    def get_data_from_sheet(self, bounds):
        # bounds should be a dict of tuples with start and end
        logger.debug(f"Retreiving data from Google sheets")
        wks = self.wks
        start = bounds.get("start")
        end = bounds.get("end")
        cols = wks.get_row(start[0], include_tailing_empty=False)
        if len(cols) > 0:
            col_n = len(cols)
            logger.debug(f"No. of columns detected in Google sheet: {col_n}")
            latest_month = cols[-1]

            # comparision month is dependant on period
            if self.period == "week":
                comparitor = (
                    self.start_date.strftime("%d-%b")
                    + " to "
                    + (self.start_date + relativedelta(days=6)).strftime("%d-%b")
                )
            elif self.period == "day":
                comparitor = self.end_date.strftime("%d-%b-%Y")
            else:
                comparitor = self.end_date.strftime("%b'%y")

            if latest_month == comparitor:
                col_n = col_n - 1

            df = wks.get_as_df(
                start=start,
                end=(end, col_n),
            )

            return df
        else:
            logger.debug(f"Google sheet bounds blank, (bounds provided): {bounds}")
            return None

    def update_sheet(
        self,
        data_to_update,
        bounds_dict,
        format=False,
    ):
        sheet_data = BaseOperations.get_data_from_sheet(self, bounds_dict)

        if isinstance(sheet_data, pd.DataFrame):
            if not sheet_data.empty:
                data_to_update = sheet_data.merge(
                    data_to_update, on=bounds_dict.get("key"), how="left"
                )

        self.wks.set_dataframe(data_to_update, nan="", start=bounds_dict.get("start"))

        if format:
            self.formatter.format_worksheet(bounds_dict)

        return "Updated Successfully"

    def clearCustomRange(self, bounds):
        start = bounds["start"]
        end = bounds["end"]

        logger.debug(f"Bounds: {bounds}")

        # END ONLY DEFINES THE ROW NUMBER, WE NEED TO FIND THE DYNAMIC COLUMN NUMBER TOO
        cols = self.wks.get_row(start[0], include_tailing_empty=False)
        dynamic_end = None
        if len(cols) > 0:
            col_n = len(cols)

            # REMEMBER THAT WE HAVE TO SUPPLY START AND END AS A TUPLE.
            # THE END BOUND IS STORED AS AN INT AND THEN CONVERTED TO TUPLE AFTER
            # WE FIND THE COL COORDINDATE DYNAMICALLY
            dynamic_end = (end, col_n)

            logger.debug(f"Clear Range coordinates: {start} to {dynamic_end}")

        # ALSO WE ARE DEFINDING wks (worksheet) in the __init__ function,
        # we can simply use self.wks instead of self.sheet.worksheet_by_title

        # REVISIED CODE
        # DOCUMENTATION DEFINES THAT WE CAN USE * TO CLEAR ALL FIELDS
        # INSTEAD OF JUST USER-ENTERED VALUES
        if dynamic_end != None:
            self.wks.clear(start=start, end=dynamic_end, fields="*")


class BaseOperationsCity:
    creds = get_credentials()
    gc = get_credentials(type="gc")
    sheet = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1RH3YmlHvQgyAZaCSzL3Tp2KXIb0oARedkLWuRow6rMU/edit?usp=sharing"
    )

    def __init__(self, end_date: datetime, period: str):
        self.end_date = datetime.combine(end_date.date(), datetime.min.time())
        self.period = period

        if self.period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
            self.wks = BaseOperationsCity.sheet.worksheet_by_title("Week Summary")
            self.formatter = FormatHelper("Week Summary", city=True)

        elif self.period == "month":
            self.start_date = self.end_date.replace(day=1)
            self.wks = BaseOperationsCity.sheet.worksheet_by_title("Month Summary")
            self.formatter = FormatHelper("Month Summary", city=True)

        elif self.period == "dod":
            self.start_date = self.end_date
            self.wks = BaseOperations.sheet.worksheet_by_title("DoD Summary")
            self.formatter = FormatHelper("DoD Summary")

        else:
            self.start_date = self.end_date
            self.wks = BaseOperationsCity.sheet.worksheet_by_title("Daily Summary")
            self.formatter = FormatHelper("Daily Summary", city=True)

        self.end_date = datetime.combine(self.end_date.date(), datetime.max.time())
        self.start_date = datetime.combine(self.start_date.date(), datetime.min.time())

    def update_sheet(
        self,
        data_to_update,
        bounds_dict,
        format=False,
    ):
        sheet_data = BaseOperations.get_data_from_sheet(self, bounds_dict)

        if isinstance(sheet_data, pd.DataFrame):
            if not sheet_data.empty:
                sheet_cols = sheet_data.columns.tolist()

                sheet_data["Region"].replace("", np.nan, inplace=True)
                sheet_data["Region"].fillna(method="ffill", inplace=True)

                if sheet_data[sheet_cols[1]].iloc[-1] == "Grand Total":
                    sheet_data["Region"].iloc[-1] = ""

                data_to_update = sheet_data.merge(
                    data_to_update, on=["Region", sheet_cols[1]], how="left"
                )

        self.wks.set_dataframe(data_to_update, nan="", start=bounds_dict.get("start"))

        if format:
            self.formatter.format_worksheet(bounds_dict)

        return "Updated Successfully"

    def clearCustomRange(self, bounds):
        BaseOperations.clearCustomRange(self, bounds)
