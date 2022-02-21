from Helper.MetricCalculatorCity import MetricCalculatorCity
from Helper.DataFetcher import DataFetcher
from datetime import datetime
from main import calculate_date_range
import sys

def update_citywise(drange, period="week"):
    f = len(drange)
    
    for i, d in enumerate(drange):
        i = i+1

        if i==f:
            format=True
        else:
            format=False
        
        print(f"Updating for {d}, period: {period}")
        
        df = DataFetcher(d, period=period)
        updater = MetricCalculatorCity(d, period=period)

        mau_data = df.get_mau_data()

        # calculate different tables
        a = updater.calculate_active_users(mau_data=mau_data)
        b = updater.calculate_dp_active_users(mau_data=mau_data)
        c = updater.calculate_loggedin_active_users(mau_data=mau_data)
        d = updater.calculate_loggedin_active_users(mau_data=mau_data)
        e = updater.calculate_rdp_views_overall(mau_data=mau_data)

        # update sheet
        updater.update_sheet(a, MetricCalculatorCity.active_users_dict, format=format)
        updater.update_sheet(b, MetricCalculatorCity.dp_active_users_dict, format=format)
        updater.update_sheet(c, MetricCalculatorCity.loggedin_active_users_dict, format=format)
        updater.update_sheet(d, MetricCalculatorCity.non_loggedin_active_users_dict, format=format)
        updater.update_sheet(e, MetricCalculatorCity.rdp_viewed_users_dict, format=format)

        del df, updater, mau_data

if __name__ == '__main__':

    period = sys.argv[1]
    
    if period == "day":
        sdate = datetime(2021,12,1)
        edate = datetime(2022,2,20)
    elif period=="week":
        sdate = datetime(2021,11,1)
        edate = datetime(2022,2,20)
    elif period=="month":
        sdate = datetime(2021,10,1)
        edate = datetime(2022,2,20)

    drange = calculate_date_range(sdate, edate, period=period)

    update_citywise(drange, period=period)
    