from datetime import datetime, timezone
import calendar
import pytz
from datetime import timedelta


def is_market_open2(open_offset=0, close_offset=0):
    OPEN_HOUR = 9
    OPEN_MINUTE = 30
    CLOSE_HOUR = 16
    CLOSE_MINUTE = 0

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

    return market_open_flag, current_eastern_time



# # Example Usage:
# open_offset = 2
# close_offset = -180
# market_open_flag, current_eastern_time = is_market_open2(open_offset, close_offset)
# print(f"With open_offset {open_offset} and close_offset {close_offset}, Market Open: {market_open_flag}")
# print(f"Current Eastern Time: {current_eastern_time}")
# print(f"Current Eastern Time (hh:mm:ss): {current_eastern_time.strftime('%H:%M:%S')}")
# print(f"Current Day of the Week: {current_eastern_time.strftime('%A')}")





