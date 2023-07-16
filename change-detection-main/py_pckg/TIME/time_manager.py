from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta


# This function gets returns the day of today, and the corresponding date once we substract the monthes the user set in the config file (config/config.fg)
def get_time_period(nbr_monthes) :

    today = date.today()

    first_month = date.today() + relativedelta(months=-int(nbr_monthes))
    
    today_str       = int(today.strftime("%Y%m%d"))
    first_month_str = int(first_month.strftime("%Y%m%d"))
    

    return first_month_str, today_str
