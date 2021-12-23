import pygsheets
import os, json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    datefmt="%d-%b-%Y %H:%M:%S",
)

file_handler = logging.FileHandler(f"logs/ga_sheet_automation.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.debug("Initializing custom formats")

with open(os.environ["DINEOUT_DB_CREDENTIALS"], "r") as f:
    creds = json.load(f)
gc = pygsheets.authorize(os.environ["GOOGLE_OAUTH_CREDENTIALS"])


class SheetFormats:
    # MODEL CELL FOR TABLE TITLES
    TABLE_TITLE = pygsheets.Cell("A1")
    TABLE_TITLE.set_text_format("fontFamily", "Cambria")
    TABLE_TITLE.set_text_format("fontSize", 14)
    TABLE_TITLE.set_text_format("bold", True)
    TABLE_TITLE.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.LEFT
    )

    # MODEL CELL FOR TABLE COLUMNS
    TABLE_COLUMNS = pygsheets.Cell("A1")
    TABLE_COLUMNS.wrap_strategy = "WRAP"
    TABLE_COLUMNS.set_text_format("fontFamily", "Cambria")
    TABLE_COLUMNS.set_text_format("fontSize", 10)
    TABLE_COLUMNS.set_text_format("bold", True)
    TABLE_COLUMNS.set_text_format("foregroundColor", (0, 0, 0, 0))
    TABLE_COLUMNS.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="dd-mmm-yyyy"
    )
    TABLE_COLUMNS.color = (1, 0.95, 0.8)
    TABLE_COLUMNS.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.CENTER
    )
    TABLE_COLUMNS.set_vertical_alignment(
        pygsheets.custom_types.VerticalAlignment.MIDDLE
    )

    # MODEL CELL FOR TABLE INDEX
    TABLE_INDEX = pygsheets.Cell("A1")
    TABLE_INDEX.set_text_format("fontFamily", "Cambria")
    TABLE_INDEX.set_text_format("fontSize", 9)
    TABLE_INDEX.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.LEFT
    )

    # MODEL CELL FOR TABLE VALUES
    TABLE_VALUES = pygsheets.Cell("A1")
    TABLE_VALUES.set_text_format("fontFamily", "Cambria")
    TABLE_VALUES.set_text_format("fontSize", 9)
    TABLE_VALUES.set_text_format("bold", False)
    TABLE_VALUES.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_VALUES.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )

    # MODEL CELL FOR TABLE VALUE SUB SECTION
    TABLE_SUB_SECTION = pygsheets.Cell("A1")
    TABLE_SUB_SECTION.set_text_format("fontFamily", "Cambria")
    TABLE_SUB_SECTION.set_text_format("fontSize", 8)
    TABLE_SUB_SECTION.set_text_format("italic", True)
    TABLE_SUB_SECTION.set_text_format("foregroundColor", (0, 0, 1, 1))
    TABLE_SUB_SECTION.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_SUB_SECTION.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )

    # MODEL CELL FOR TABLE VALUE RATES E.G. TRANSACTION PER USER 2.3
    TABLE_VALUE_RATES = pygsheets.Cell("A1")
    TABLE_VALUE_RATES.set_text_format("fontFamily", "Cambria")
    TABLE_VALUE_RATES.set_text_format("fontSize", 9)
    TABLE_VALUE_RATES.set_text_format("bold", False)
    TABLE_VALUE_RATES.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_VALUE_RATES.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="##0.0"
    )

    # MODEL CELL FOR CITY LEVEL STYLES


class FormatHelper(SheetFormats):
    sheet = gc.open_by_url(creds.get("sheets").get("ga_sheet_automation"))

    def __init__(self, worksheet_title):
        super().__init__()

        self.wks = FormatHelper.sheet.worksheet_by_title(worksheet_title)

    def format_worksheet(self, bounds):
        for value in [
            "TABLE_VALUES",
            "TABLE_INDEX",
            "TABLE_COLUMNS",
            "TABLE_SUB_SECTION",
            "TABLE_VALUE_RATES",
            "TABLE_BORDERS",
        ]:
            FormatHelper.format_worksheet_meta(self, bounds, update_type=value)

    def format_worksheet_meta(self, bounds, update_type="TABLE_VALUES"):
        start = bounds.get("start")
        end_row = bounds.get("end")
        subsections = bounds.get("subsections")
        rates = bounds.get("rates")
        n_col = self.wks.get_row(
            start[0],
            include_tailing_empty=False,
        )
        n_col = len(n_col)
        if update_type == "TABLE_VALUES":
            logger.debug(f"Updating Formats: {update_type}")
            row, col = start
            row = row + 1
            col = col + 1
            logger.debug(
                f"Updating table values: start={(row, col)}, end={(end_row, n_col)}"
            )
            trange = pygsheets.DataRange(
                start=(row, col), end=(end_row, n_col), worksheet=self.wks
            )
            trange.apply_format(self.TABLE_VALUES)
        elif update_type == "TABLE_INDEX":
            logger.debug(f"Updating Formats: {update_type}")
            row, col = start
            logger.debug(
                f"Updating table index: start={(row, col)}, end={(end_row, col)}"
            )
            trange = pygsheets.DataRange(
                start=(row, col), end=(end_row, col), worksheet=self.wks
            )
            trange.apply_format(self.TABLE_INDEX)
        elif update_type == "TABLE_COLUMNS":
            logger.debug(f"Updating Formats: {update_type}")
            row, col = start
            logger.debug(f"Updating table index: start={start}, end={(row, n_col)}")
            trange = pygsheets.DataRange(
                start=start, end=(row, n_col), worksheet=self.wks
            )
            trange.apply_format(self.TABLE_COLUMNS)
        elif update_type == "TABLE_BORDERS":
            logger.debug(f"Updating Formats: {update_type}")
            row, col = start
            logger.debug(
                f"Updating table borders: start={start}, end={(end_row, n_col)}"
            )
            trange = pygsheets.DataRange(
                start=start,
                end=(end_row, n_col),
                worksheet=self.wks,
            )
            trange.update_borders(
                red=169 / 255,
                green=194 / 255,
                blue=240 / 255,
                inner_horizontal=True,
                inner_vertical=True,
                top=True,
                right=True,
                bottom=True,
                left=True,
                style="SOLID",
            )
        elif update_type == "TABLE_SUB_SECTION":
            logger.debug(f"Updating Formats: {update_type}")
            if not subsections == None:
                for row in subsections:
                    # since row numbers in subsection are relative to start row
                    row = row + (start[0] - 1)
                    # UPDATE SUBSECTION START AND END DIFFERENTLY FOR CITY
                    sub_section_start = (row, start[1])
                    sub_section_end = (row, n_col)
                    logger.debug(
                        f"Updating table subsection: start:B{row}, end: {n_col}{row}"
                    )
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_SUB_SECTION)
        elif update_type == "TABLE_VALUE_RATES":
            logger.debug(f"Updating Formats: {update_type}")
            if not rates == None:
                for row in rates:
                    row = row + (start[0] - 1)

                    rates_start = (row, start[1] + 1)
                    rates_end = (row, n_col)
                    sub_section_start = (row, start[1])
                    sub_section_end = (row, start[1])
                    logger.debug(
                        f"Updating table rates: start:{rates_start}, end: {rates_end}"
                    )
                    trange = pygsheets.DataRange(
                        start=rates_start, end=rates_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_VALUE_RATES)
                    # ALSO UPDATE THE INDEX TO TABLE_VALUES_SUB_SECTION FORMAT
                    logger.debug(
                        f"Updating table rates subsection: start:{rates_start}, end: {rates_end}"
                    )
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_SUB_SECTION)
