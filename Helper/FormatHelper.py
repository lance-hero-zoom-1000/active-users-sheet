import pygsheets
from Helper.meta import get_credentials
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    datefmt="%d-%b-%Y %H:%M:%S",
)

file_handler = logging.FileHandler(f"logs/non_revenue_metrics_automation.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info("Initializing custom formats")

creds = get_credentials()
gc = get_credentials(type="gc")


class SheetFormats:
    # MODEL CELL FOR TABLE COLUMNS
    TABLE_COLUMNS = pygsheets.Cell("A1")
    TABLE_COLUMNS.wrap_strategy = "WRAP"
    TABLE_COLUMNS.set_text_format("fontFamily", "Cambria")
    TABLE_COLUMNS.set_text_format("fontSize", 10)
    TABLE_COLUMNS.set_text_format("bold", True)
    TABLE_COLUMNS.set_text_format("foregroundColor", (0, 0, 0, 0))
    TABLE_COLUMNS.color = (164 / 255, 194 / 255, 244 / 255)
    TABLE_COLUMNS.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.CENTER
    )
    TABLE_COLUMNS.set_vertical_alignment(
        pygsheets.custom_types.VerticalAlignment.MIDDLE
    )
    TABLE_COLUMNS.set_number_format(
        format_type=pygsheets.FormatType.DATE, pattern="dd-mmm-yyyy"
    )

    # MODEL CELL FOR MERGED HEADERS
    MERGED_HEADER = pygsheets.Cell("A1")
    MERGED_HEADER.wrap_strategy = "WRAP"
    MERGED_HEADER.set_text_format("fontFamily", "Cambria")
    MERGED_HEADER.set_text_format("fontSize", 10)
    MERGED_HEADER.set_text_format("bold", True)
    MERGED_HEADER.set_text_format("foregroundColor", (0, 0, 0, 0))
    MERGED_HEADER.color = (243 / 255, 243 / 255, 243 / 255)
    MERGED_HEADER.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.CENTER
    )
    MERGED_HEADER.set_vertical_alignment(
        pygsheets.custom_types.VerticalAlignment.MIDDLE
    )

    # MODEL CELL FOR TABLE INDEX
    TABLE_INDEX = pygsheets.Cell("A1")
    TABLE_INDEX.set_text_format("fontFamily", "Cambria")
    TABLE_INDEX.set_text_format("fontSize", 9)
    TABLE_INDEX.set_text_format("bold", False)
    TABLE_INDEX.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )

    # MODEL CELL FOR HEADERS WITHIN TABLE INDEX
    TABLE_INDEX_HEADER = pygsheets.Cell("A1")
    TABLE_INDEX_HEADER.set_text_format("fontFamily", "Cambria")
    TABLE_INDEX_HEADER.set_text_format("fontSize", 9)
    TABLE_INDEX_HEADER.set_text_format("bold", True)
    TABLE_INDEX_HEADER.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.LEFT
    )
    TABLE_INDEX_HEADER.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )

    # MODEL CELL FOR TABLE INDEX - LIGHT THEME
    TABLE_INDEX_LIGHT = pygsheets.Cell("A1")
    TABLE_INDEX_LIGHT.set_text_format("fontFamily", "Cambria")
    TABLE_INDEX_LIGHT.set_text_format("fontSize", 8)
    TABLE_INDEX_LIGHT.set_text_format("bold", False)
    TABLE_INDEX_LIGHT.set_text_format("italic", True)
    TABLE_INDEX_LIGHT.set_text_format(
        "foregroundColor", (100 / 255, 100 / 255, 100 / 255, 0)
    )
    TABLE_INDEX_LIGHT.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_INDEX_LIGHT.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )

    # MODEL CELL FOR HEADERS WITHIN TABLE INDEX
    TABLE_INDEX_HEADER_VALUE = pygsheets.Cell("A1")
    TABLE_INDEX_HEADER_VALUE.set_text_format("fontFamily", "Cambria")
    TABLE_INDEX_HEADER_VALUE.set_text_format("fontSize", 9)
    TABLE_INDEX_HEADER_VALUE.set_text_format("bold", True)
    TABLE_INDEX_HEADER_VALUE.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_INDEX_HEADER_VALUE.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )

    # MODEL CELL FOR SUB HEADER
    REGION_SUB_TOTAL = pygsheets.Cell("A1")
    REGION_SUB_TOTAL.set_text_format("fontFamily", "Cambria")
    REGION_SUB_TOTAL.set_text_format("fontSize", 9)
    REGION_SUB_TOTAL.set_text_format("bold", True)
    REGION_SUB_TOTAL.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )
    REGION_SUB_TOTAL.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    REGION_SUB_TOTAL.set_text_format("fontSize", 9)
    REGION_SUB_TOTAL.set_text_format("bold", True)
    REGION_SUB_TOTAL.set_text_format("foregroundColor", (0, 0, 0, 0))
    REGION_SUB_TOTAL.color = (243 / 255, 243 / 255, 243 / 255)

    # MODEL CELL FOR SUB HEADER INDEX
    REGION_SUB_INDEX = pygsheets.Cell("A1")
    REGION_SUB_INDEX.set_text_format("fontFamily", "Cambria")
    REGION_SUB_INDEX.set_text_format("fontSize", 9)
    REGION_SUB_INDEX.set_text_format("bold", True)
    REGION_SUB_INDEX.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )
    REGION_SUB_INDEX.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.LEFT
    )
    REGION_SUB_INDEX.set_text_format("fontSize", 9)
    REGION_SUB_INDEX.set_text_format("bold", True)
    REGION_SUB_INDEX.set_text_format("foregroundColor", (0, 0, 0, 0))
    REGION_SUB_INDEX.color = (243 / 255, 243 / 255, 243 / 255)

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
    TABLE_SUB_SECTION.set_text_format("bold", True)
    TABLE_SUB_SECTION.set_text_format("italic", True)
    TABLE_SUB_SECTION.set_text_format("foregroundColor", (0, 0, 1, 1))
    TABLE_SUB_SECTION.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_SUB_SECTION.set_number_format(
        format_type=pygsheets.FormatType.PERCENT, pattern="0.0%"
    )

    # MODEL CELL FOR TABLE VALUE RATES E.G. TRANSACTION PER USER 2.3
    TABLE_AVG_RATES = pygsheets.Cell("A1")
    TABLE_AVG_RATES.set_text_format("fontFamily", "Cambria")
    TABLE_AVG_RATES.set_text_format("fontSize", 8)
    TABLE_AVG_RATES.set_text_format("bold", True)
    TABLE_AVG_RATES.set_text_format("italic", True)
    TABLE_AVG_RATES.set_text_format("foregroundColor", (0, 0, 1, 1))
    TABLE_AVG_RATES.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_AVG_RATES.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0.0"
    )

    # MODEL CELL FOR TABLE VALUE RATES E.G. AVG GMV PER RESTAURANT 1,200
    TABLE_VALUE_RATES = pygsheets.Cell("A1")
    TABLE_VALUE_RATES.set_text_format("fontFamily", "Cambria")
    TABLE_VALUE_RATES.set_text_format("fontSize", 8)
    TABLE_VALUE_RATES.set_text_format("bold", True)
    TABLE_VALUE_RATES.set_text_format("italic", True)
    TABLE_VALUE_RATES.set_text_format("foregroundColor", (0, 0, 1, 1))
    TABLE_VALUE_RATES.set_horizontal_alignment(
        pygsheets.custom_types.HorizontalAlignment.RIGHT
    )
    TABLE_VALUE_RATES.set_number_format(
        format_type=pygsheets.FormatType.NUMBER, pattern="#,##0"
    )


class FormatHelper(SheetFormats):
    sheet = gc.open_by_url(
        creds.get("sheets").get("ga_sheet_automation")
    )

    def __init__(self, worksheet_title, city=False):
        super().__init__()
        self.wks = FormatHelper.sheet.worksheet_by_title(worksheet_title)
        self.city = city

    def format_worksheet(self, bounds):
        # formats to apply
        if self.city:
            formats_list = [
                "TABLE_VALUES",
                "TABLE_INDEX",
                "TABLE_INDEX_HEADER",
                "REGION_SUB_TOTAL",
                "TABLE_COLUMNS",
                "TABLE_SUB_SECTION",
                "TABLE_AVG_RATES",
                "TABLE_VALUE_RATES",
                "TABLE_REGION",
                "TABLE_BORDERS",
            ]
        else:
            formats_list = [
                "TABLE_VALUES",
                "TABLE_INDEX",
                "TABLE_INDEX_HEADER",
                "TABLE_INDEX_LIGHT",
                "TABLE_COLUMNS",
                "TABLE_SUB_SECTION",
                "TABLE_AVG_RATES",
                "TABLE_VALUE_RATES",
                "TABLE_BORDERS",
            ]

        for value in formats_list:
            FormatHelper.format_worksheet_meta(self, bounds, update_type=value)

    def format_worksheet_meta(self, bounds, update_type="TABLE_VALUES"):
        # IF CITY=TRUE, THEN LAST ROW SHOULD BE BOLD
        # 1ST COLUMN WILL HAVE MERGE OPERATIONS (REGION)
        # DATASET WILL ALWAYS START AT 3RD COLUMN OF GSHEET AND MERGE OPERATIONS WILL ALSO START AT 3RD COLUMN GSHEET
        # OR MERGE OPERATION SHOULB BE ON 2ND AND 3RD COLUMN OF GSHEET

        if self.city:
            start = bounds.get("start")
            end_row = bounds.get("end")

            region_start = start
            region_end = (end_row, start[1])
            start = (start[0], start[1] + 1)

            subsections = bounds.get("subsections")

            value_rates = bounds.get("value_rates")
            avg_rates = bounds.get("avg_rates")

            if (
                (not avg_rates == None)
                or (not subsections == None)
                or (not value_rates == None)
            ):
                headers = None
                region_headers = None
            else:
                # TABLE INDEX HEADER WILL BE THE GRAND TOTAL
                headers = [
                    (end_row - start[0]) + 1,  # last row
                ]
                region_headers = [
                    start[0] + 7,
                    start[0] + 14,
                    start[0] + 22,
                    end_row - 2,
                ]

            n_col = self.wks.get_row(
                start[0],
                include_tailing_empty=False,
            )
            n_col = len(n_col)
        else:
            start = bounds.get("start")
            end_row = bounds.get("end")
            subsections = bounds.get("subsections")
            headers = bounds.get("headers")
            light = bounds.get("light")
            value_rates = bounds.get("value_rates")
            avg_rates = bounds.get("avg_rates")
            n_col = self.wks.get_row(
                start[0],
                include_tailing_empty=False,
            )
            n_col = len(n_col)

        logger.info(f"Updating Formats: {update_type}")
        if update_type == "TABLE_VALUES":
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
            row, col = start
            logger.debug(
                f"Updating table index: start={(row, col)}, end={(end_row, col)}"
            )
            trange = pygsheets.DataRange(
                start=(row, col), end=(end_row, col), worksheet=self.wks
            )
            trange.apply_format(self.TABLE_INDEX)
        elif update_type == "TABLE_COLUMNS":
            row, col = start
            logger.debug(f"Updating table index: start={start}, end={(row, n_col)}")
            trange = pygsheets.DataRange(
                start=start, end=(row, n_col), worksheet=self.wks
            )
            trange.apply_format(self.TABLE_COLUMNS)
        elif update_type == "TABLE_BORDERS":
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
                red=164 / 255,
                green=194 / 255,
                blue=244 / 255,
                inner_horizontal=True,
                inner_vertical=True,
                top=True,
                right=True,
                bottom=True,
                left=True,
                style="SOLID",
            )
        elif update_type == "TABLE_SUB_SECTION":
            if not subsections == None:
                for row in subsections:
                    # since row numbers in subsection are relative to start row
                    row = row + (start[0] - 1)
                    sub_section_start = (row, start[1])
                    sub_section_end = (row, n_col)
                    logger.debug(
                        f"Updating table subsection: start:B{row}, end: {n_col}{row}"
                    )
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_SUB_SECTION)
        elif update_type == "TABLE_INDEX_HEADER":
            if not headers == None:
                for row in headers:
                    row = row + (start[0] - 1)
                    sub_section_start = (row, start[1])
                    sub_section_value_start = (row, start[1] + 1)
                    sub_section_end = (row, n_col)
                    logger.debug(
                        f"Updating table subsection: start:B{row}, end: {n_col}{row}"
                    )
                    # index
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_INDEX_HEADER)
                    # value
                    trange = pygsheets.DataRange(
                        start=sub_section_value_start,
                        end=sub_section_end,
                        worksheet=self.wks,
                    )
                    trange.apply_format(self.TABLE_INDEX_HEADER_VALUE)
        elif update_type == "TABLE_INDEX_LIGHT":
            if not light == None:
                for row in light:
                    row = row + (start[0] - 1)
                    sub_section_start = (row, start[1])
                    sub_section_end = (row, n_col)
                    logger.debug(
                        f"Updating table light: start:B{row}, end: {n_col}{row}"
                    )
                    # index
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_INDEX_LIGHT)
        elif update_type == "REGION_SUB_TOTAL":
            if not region_headers == None:
                for row in region_headers:
                    sub_section_start = (row + 1, start[1])
                    sub_section_end = (row + 1, n_col)

                    logger.debug(
                        f"Updating table region sub total: start:B{row}, end: {n_col}{row}"
                    )

                    trange = pygsheets.DataRange(
                        start=(sub_section_start[0], sub_section_start[1] + 1),
                        end=sub_section_end,
                        worksheet=self.wks,
                    )
                    trange.apply_format(self.REGION_SUB_TOTAL)

                    trange = pygsheets.DataRange(
                        start=(sub_section_start[0], sub_section_start[1]),
                        end=(sub_section_start[0], sub_section_start[1]),
                        worksheet=self.wks,
                    )
                    trange.apply_format(self.REGION_SUB_INDEX)
        elif update_type == "TABLE_VALUE_RATES":
            if not value_rates == None:
                for row in value_rates:
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
        elif update_type == "TABLE_AVG_RATES":
            if not avg_rates == None:
                for row in avg_rates:
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
                    trange.apply_format(self.TABLE_AVG_RATES)
                    # ALSO UPDATE THE INDEX TO TABLE_VALUES_SUB_SECTION FORMAT
                    logger.debug(
                        f"Updating table rates subsection: start:{rates_start}, end: {rates_end}"
                    )
                    trange = pygsheets.DataRange(
                        start=sub_section_start, end=sub_section_end, worksheet=self.wks
                    )
                    trange.apply_format(self.TABLE_SUB_SECTION)
        elif update_type == "TABLE_REGION":
            print(f"Region coordinates: {region_start} & {region_end}")

            # REGION TABLE INDEX
            row, col = region_start
            logger.debug(
                f"Updating region index: start={(row, col)}, end={(end_row, col)}"
            )
            trange = pygsheets.DataRange(
                start=(row, col), end=(end_row, col), worksheet=self.wks
            )
            trange.apply_format(self.MERGED_HEADER)

            # FORMAT REGION HEADER
            logger.debug(
                f"Updating region header: start={region_start}, end={region_start}"
            )
            trange = pygsheets.DataRange(
                start=region_start, end=region_start, worksheet=self.wks
            )
            trange.apply_format(self.TABLE_COLUMNS)

            # REGION BORDERS
            logger.debug(
                f"Updating region borders: start={region_start}, end={region_end}"
            )
            trange = pygsheets.DataRange(
                start=region_start,
                end=region_end,
                worksheet=self.wks,
            )
            trange.update_borders(
                red=164 / 255,
                green=194 / 255,
                blue=244 / 255,
                inner_horizontal=True,
                inner_vertical=True,
                top=True,
                right=True,
                bottom=True,
                left=True,
                style="SOLID",
            )

            # MERGE SETS
            if (
                (not avg_rates == None)
                or (not subsections == None)
                or (not value_rates == None)
            ):
                north_start = (region_start[0] + 1, region_start[1])
                north_end = (north_start[0] + 6, region_start[1])
                south_start = (north_end[0] + 1, region_start[1])
                south_end = (south_start[0] + 5, region_start[1])
                west_start = (south_end[0] + 1, region_start[1])
                west_end = (west_start[0] + 6, region_start[1])
            else:
                north_start = (region_start[0] + 1, region_start[1])
                north_end = (north_start[0] + 7, region_start[1])
                south_start = (north_end[0] + 1, region_start[1])
                south_end = (south_start[0] + 6, region_start[1])
                west_start = (south_end[0] + 1, region_start[1])
                west_end = (west_start[0] + 7, region_start[1])

            self.wks.merge_cells(
                start=north_start,
                end=north_end,
            )
            self.wks.merge_cells(
                start=south_start,
                end=south_end,
            )
            self.wks.merge_cells(
                start=west_start,
                end=west_end,
            )
