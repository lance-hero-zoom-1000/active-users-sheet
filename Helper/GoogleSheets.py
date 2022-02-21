from datetime import datetime
from Helper.FormatHelper import FormatHelper
import pandas as pd
import numpy as np
import logging
from Helper.meta import get_credentials
from datetime import timedelta

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
        creds.get("sheets").get("ga_sheet_automation")
    )

    def __init__(self, end_date: datetime, period: str):
        self.end_date = datetime.combine(
            end_date.date(), datetime.min.time()
        )
        self.period = period

        if self.period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
            self.wks = BaseOperations.sheet.worksheet_by_title(
                "Week Summary"
            )
            self.formatter = FormatHelper("Week Summary")

        elif self.period == "month":
            self.start_date = self.end_date.replace(day=1)
            self.wks = BaseOperations.sheet.worksheet_by_title(
                "Month Summary"
            )
            self.formatter = FormatHelper("Month Summary")
        else:
            self.start_date = self.end_date
            self.wks = BaseOperations.sheet.worksheet_by_title(
                "Daily Summary"
            )
            self.formatter = FormatHelper("Daily Summary")

        self.end_date = datetime.combine(
            end_date.date(), datetime.max.time()
        )
        

    @staticmethod
    def get_data_from_sheet(bounds, wks):
        # bounds should be a dict of tuples with start and end
        logger.debug(f"Retreiving data from Google sheets")
        start = bounds.get("start")
        end = bounds.get("end")
        cols = wks.get_row(start[0], include_tailing_empty=False)
        if len(cols) > 0:
            col_n = len(cols)
            logger.debug(f"No. of columns detected in Google sheet: {col_n}")
            latest_month = cols[-1]

            if latest_month == datetime.today().strftime("%b'%y"):
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
        sheet_data = BaseOperations.get_data_from_sheet(bounds_dict, self.wks)

        if isinstance(sheet_data, pd.DataFrame):
            if not sheet_data.empty:
                data_to_update = sheet_data.merge(
                    data_to_update, on=bounds_dict.get("key"), how="left"
                )

        self.wks.set_dataframe(data_to_update, nan="", start=bounds_dict.get("start"))

        if format:
            self.formatter.format_worksheet(bounds_dict)

        return "Updated Successfully"


class BaseOperationsCity:
    creds = get_credentials()
    gc = get_credentials(type="gc")
    sheet = gc.open_by_url(
        creds.get("sheets").get("ga_sheet_automation")
    )

    def __init__(self, end_date: datetime, period: str):
        self.end_date = datetime.combine(
            end_date.date(), datetime.min.time()
        )
        self.period = period

        if self.period == "week":
            self.start_date = self.end_date - timedelta(self.end_date.weekday())
            self.wks = BaseOperationsCity.sheet.worksheet_by_title(
                "Week Summary"
            )
            self.formatter = FormatHelper("Week Summary", city=True)

        elif self.period == "month":
            self.start_date = self.end_date.replace(day=1)
            self.wks = BaseOperationsCity.sheet.worksheet_by_title(
                "Month Summary"
            )
            self.formatter = FormatHelper("Month Summary", city=True)
        else:
            self.start_date = self.end_date
            self.wks = BaseOperationsCity.sheet.worksheet_by_title(
                "Daily Summary"
            )
            self.formatter = FormatHelper("Daily Summary", city=True)

        self.end_date = datetime.combine(
            end_date.date(), datetime.max.time()
        )

    def update_sheet(
        self,
        data_to_update,
        bounds_dict,
        format=False,
    ):
        sheet_data = BaseOperations.get_data_from_sheet(bounds_dict, self.wks)

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
