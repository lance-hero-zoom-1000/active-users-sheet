import os, json

from dotenv import load_dotenv

if os.path.exists("/home/lawrence.veigas/projects/creds/.env"):
    ok = load_dotenv("/home/lawrence.veigas/projects/creds/.env")
else:
    ok = load_dotenv("/users/lawrence.veigas/downloads/projects/creds/.env")
print("Environment variables loaded ", ok)

import pygsheets
import pandas as pd
import numpy as np
from datetime import datetime

bookings_src_platform_mapping = {
    1: "Website",
    4: "Android",
    9: "Android",
    11: "Android",
    12: "Android",
    5: "iOS",
    10: "iOS",
    13: "iOS",
    7: "mSite",
    2: "inbound",
    3: "Affiliate",
    8: "Others",
}

region_list = [
    "North",
    "North",
    "North",
    "North",
    "North",
    "North",
    "North",
    "South",
    "South",
    "South",
    "South",
    "South",
    "South",
    "West",
    "West",
    "West",
    "West",
    "West",
    "West",
    "West",
    "Others",
]

top_cities = [
    "Agra",
    "Chandigarh",
    "Delhi",
    "Jaipur",
    "Lucknow",
    "Ludhiana",
    "Udaipur",
    "Bangalore",
    "Chennai",
    "Hyderabad",
    "Indore",
    "Kochi",
    "Kolkata",
    "Ahmedabad",
    "Goa",
    "Mumbai",
    "Nagpur",
    "Pune",
    "Surat",
    "Vadodara",
    "Others",
]

# FUNC TO GET CREDENTIALS
def get_credentials(type="creds"):
    """Use function to get credentials which will be used to query dineout servers or connect to google sheets

    Args:
        type (str, optional): Type of credential needed. Defaults to "creds".
                              - creds: dineout credentials as a dict
                              - gc: google client to connect to google sheets

    Raises:
        ValueError: If type not in choices provided, then valueError Exception will be raised

    Returns:
        if "creds" then python dictionary
        if "gc" then google client type (pygsheets.Client)
    """
    if type == "creds":
        with open(os.environ["DINEOUT_DB_CREDENTIALS"], "r") as f:
            creds = json.load(f)
        return creds
    elif type == "gc":
        gc = pygsheets.authorize(service_account_file=os.environ["GADINEOUT_SERVICE_ACCOUNT"])
        return gc
    else:
        raise ValueError(
            f"credentials of type: {type} don't exist. Please select between 'creds' and 'gc'"
        )


# FUNCTION TO HANDLE UNIQUE USER CALCULATION FOR MULTICITY USERS
def mode_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Custom mode function for specific need.
    The default mode operation (scipy.stats.mode) returns the mode correctly. However,
    if there is more than one mode, it returns the lowest value among modes which is
    not what is needed for our program.

    In case there is a conflict amount modes, we want the program to return the mode
    which occured the "lastest".
    E.g. If a diner has transacted at Delhi twice, Mumbai once and Goa twice.
    To mode to be returned is either Delhi or Goa. The exact way to determine
    this is to check where did the diner make his "latest" transaction. In our example,
    let's assume the diner made his latest transaction in Goa. Hence, Goa will be returned
    as the mode.

    Do note: In our example, the latest transaction might have occured in Mumbai, however, since
    it is not even a candidate for the mode, we ignore it completely.

    Args:
        data (pd.DataFrame): DataFrame on which mode function is to be calculated.
        kwargs:
            - groupby: the variable to use for grouping
            - groupvar: synonymous to "values" in pandas.pivot_table. groupvar will the dimension grouped
            - conflict: conflict will be used to resolve cases with more than one mode

    Returns:
        pd.DataFrame: DataFrame with only conflicting cases
    """

    # VARIABLE TO GROUP ON. E.G. group by diner_id and count citys
    groupvar = kwargs.get("groupvar")
    # VARIABLE TO GROUPBY
    groupby = kwargs.get("groupby")
    # IN CASE THERE ARE TWO VARIABLES WITH EQUAL SIZE, VARIABLE WITH THE MAX CONFLICT VALUE WILL BE RETURNED
    conflict = kwargs.get("conflict")

    multi_diners = (
        data.groupby(groupby)[groupvar]
        .apply(lambda x: len(x.unique()))
        .sort_values(ascending=False)
        .reset_index()
    )
    multi_diners = multi_diners[multi_diners[groupvar] > 1].copy()

    holder = {}
    for diner in multi_diners[groupby].unique():
        user = data[data[groupby] == diner].copy()

        city_list, counts = np.unique(user[groupvar], return_counts=True)
        check = city_list[np.where(counts == counts.max())]
        if len(check) > 1:
            cities = user[user[groupvar].isin(check)].copy()
            index = cities.index

            max_index = index[cities[conflict] == cities[conflict].max()]
            city = cities.at[max_index[0], groupvar]

            holder[diner] = city
        else:
            holder[diner] = check[0]

    x = pd.DataFrame.from_dict(holder, orient="index", columns=[conflict])
    x.reset_index(inplace=True)
    x.columns = [groupby, groupvar]

    return x


def rename_city_columns(
    df: pd.DataFrame, values: tuple, totals=True
) -> pd.DataFrame:
    """Used in city metrics calculations to rename the columns and add subtotals if needed
    TODO: define doc properly
    Args:
        df (pd.DataFrame): [description]
        col_name (str): [description]
        end_date (datetime): [description]
        totals (bool, optional): [description]. Defaults to True.

    Returns:
        pd.DataFrame: [description]
    """
    
    df.columns = [values[0], values[1]]

    # CUSTOM SORT ACCORDING TO REGION-WISE CITY
    df[values[0]] = pd.Categorical(df[values[0]], top_cities)
    df.sort_values(values[0], inplace=True)

    # INSERT REGION DATA
    df.insert(0, "Region", region_list)

    # CONVERT CATEGORICAL DATATYPE BACK TO OBJECT
    df[values[0]] = df[values[0]].astype(str)

    if totals:
        # ADD GRAND TOTAL
        df.loc["Total", :] = df.sum(axis=0)
        df.at["Total", values[0]] = "Grand Total"
        df.at["Total", "Region"] = ""

        # ADD REGION WISE SUM
        north = df[df["Region"] == "North"].copy()
        north.loc["Total", :] = north.sum(axis=0)
        north.at["Total", values[0]] = "North Total"
        north.at["Total", "Region"] = "North"

        south = df[df["Region"] == "South"].copy()
        south.loc["Total", :] = south.sum(axis=0)
        south.at["Total", values[0]] = "South Total"
        south.at["Total", "Region"] = "South"

        west = df[df["Region"] == "West"].copy()
        west.loc["Total", :] = west.sum(axis=0)
        west.at["Total", values[0]] = "West Total"
        west.at["Total", "Region"] = "West"

        misc = df[df["Region"].isin(["Others", ""])]

        final = pd.concat([north, south, west, misc])

        return final
    else:
        return df
