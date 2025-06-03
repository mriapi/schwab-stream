from datetime import datetime, timezone
import pandas_market_calendars as mcal
import calendar
import pytz
from datetime import timedelta

import json




def seconds_until_even_minute_utc(current_time_utc):
    # Calculate the next even minute boundary
    next_even_minute = (current_time_utc + timedelta(minutes=1)).replace(second=0, microsecond=0)
    
    # Calculate the difference in seconds
    time_difference = next_even_minute - current_time_utc
    seconds_until_next_even_minute = time_difference.total_seconds()
    
    return seconds_until_next_even_minute


def is_market_open2(open_offset=0, close_offset=0):
    OPEN_HOUR = 9
    OPEN_MINUTE = 30
    CLOSE_HOUR = 16
    CLOSE_MINUTE = 0
    return_val = False

    


    # Get the current time in Eastern Time Zone
    eastern = pytz.timezone('US/Eastern')
    current_time_utc = datetime.now(pytz.utc)
    current_eastern_time = current_time_utc.astimezone(eastern)

    # Determine if today is a weekday (Monday to Friday)
    is_weekday = current_eastern_time.weekday() < 5  # Monday is 0 and Sunday is 6

    # Define market open and close times with adjustments
    market_open_time = current_eastern_time.replace(hour=OPEN_HOUR, minute=OPEN_MINUTE) + timedelta(minutes=open_offset)
    market_close_time = current_eastern_time.replace(hour=CLOSE_HOUR, minute=CLOSE_MINUTE) + timedelta(minutes=close_offset)

    # Create the market_open_flag
    market_open_flag = is_weekday and (market_open_time <= current_eastern_time <= market_close_time)

    seconds_to_next_minute = seconds_until_even_minute_utc(current_time_utc)

    nyse_open_flag = is_nyse_open_today()

    if nyse_open_flag == False:
        # print(f'nyse is not open today')
        return nyse_open_flag, current_eastern_time, seconds_to_next_minute

    return market_open_flag, current_eastern_time, seconds_to_next_minute



def seconds_until_even_minute():
    # Get the current local time
    current_time = datetime.now()
    
    # Calculate the next even minute boundary
    next_even_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
        
    # Calculate the difference in seconds
    time_difference = next_even_minute - current_time
    seconds_until_next_even_minute = time_difference.total_seconds()
    
    return seconds_until_next_even_minute



# # Example Usage:
# open_offset = 2
# close_offset = -180
# market_open_flag, current_eastern_time = is_market_open2(open_offset, close_offset)
# print(f"With open_offset {open_offset} and close_offset {close_offset}, Market Open: {market_open_flag}")
# print(f"Current Eastern Time: {current_eastern_time}")
# print(f"Current Eastern Time (hh:mm:ss): {current_eastern_time.strftime('%H:%M:%S')}")
# print(f"Current Day of the Week: {current_eastern_time.strftime('%A')}")





# def calculate_token_ages(file_path):
#     with open(file_path, 'r') as file:
#         data = json.load(file)

#     refresh_token_issued = datetime.fromisoformat(data["refresh_token_issued"])
#     print(f'refresh_token_issued type:{type(refresh_token_issued)}, data:{refresh_token_issued}')


#     access_token_issued = datetime.fromisoformat(data["access_token_issued"])



#     now = datetime.now()
#     minutes_since_refresh = (now - refresh_token_issued).total_seconds() / 60
#     minutes_since_access = (now - access_token_issued).total_seconds() / 60

#     return (minutes_since_refresh, minutes_since_access)


def calculate_token_ages(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    refresh_token_issued = datetime.fromisoformat(data["refresh_token_issued"])
    access_token_issued = datetime.fromisoformat(data["access_token_issued"])

    now = datetime.now(timezone.utc)
    minutes_since_refresh = (now - refresh_token_issued).total_seconds() / 60
    minutes_since_access = (now - access_token_issued).total_seconds() / 60

    return (minutes_since_refresh, minutes_since_access)


# file_path = r'C:\Users\mri17\OneDrive\python\schwabdev\tokens.json'
# file_path = r'C:\Users\mri17\OneDrive\python\tokens.json'


def refresh_expiration_days():
    tokens_file_path = r"C:\MEIC\cred\tokens_mri.json"
    minutes_since_refresh, minutes_since_access = calculate_token_ages(tokens_file_path)
    days_since_refresh = minutes_since_refresh / 1440
    days_until_refresh_expires = 7 - days_since_refresh
    return days_until_refresh_expires

def is_nyse_open_today():

    # Get NYSE trading calendar
    nyse = mcal.get_calendar("NYSE")

    # Get today's date and check the nyse schedule
    today = datetime.today().strftime("%Y-%m-%d")
    schedule = nyse.schedule(start_date=today, end_date=today)

    # # Get tomorrow's date and check the nyse schedule
    # tomorrow = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    # schedule = nyse.schedule(start_date=tomorrow, end_date=tomorrow)

    
    is_open = not schedule.empty

    # print(f"NYSE is_open today? {is_open}") 
    return is_open   



# minutes_since_refresh, minutes_since_access = calculate_token_ages(tokens_file_path)
# days_since_refresh = minutes_since_refresh / 1440
# days_till_refresh_expires = 7 - days_since_refresh

# print(f'minutes_since_refresh:{minutes_since_refresh}')
# print(f'days_since_refresh:{days_since_refresh:.2f}')
# print(f'days_till_refresh_expires:{days_till_refresh_expires:.2f}')


# print(f'minutes_since_access:{minutes_since_access}')


# days_left = refresh_expiration_days()
# print(f'refresh days left:{days_left:.2f}')


# nyse_open_flag = is_nyse_open_today()
# print(f'nyse_open_flag:{nyse_open_flag}')










