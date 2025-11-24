# import mri_schwab_lib
# import meic_config

# from datetime import datetime, time
# import pytz


# from datetime import datetime, timezone
# import time



# loop_count = 0

# while True:

#     myBuyingPower = mri_schwab_lib.get_option_buying_power()
#     current_time = datetime.now()
#     current_time_str = current_time.strftime('%H:%M:%S')
#     print(f'buying power:{myBuyingPower}      Pacific time: {current_time_str}')

#     loop_count += 1
#     if loop_count % 60 == 59:

#         pass




#         # List of times in Eastern Time
#         # config_meic_times = ["11:58", "12:28", "12:58", "13:13", "13:58", "14:28", "14:43", "14:58"]
#         meic_times = meic_config.config_meic_times
#         # Convert strings to time objects
#         eastern_times = [time.fromisoformat(t) for t in meic_times]

#         # Get current time in Pacific Time
#         pacific = pytz.timezone("US/Pacific")
#         eastern = pytz.timezone("US/Eastern")
#         now_pacific = datetime.now(pacific)

#         # Convert current time to Eastern Time
#         now_eastern = now_pacific.astimezone(eastern)
#         current_eastern_time = now_eastern.time()

#         # Count passed and remaining times
#         passed = sum(1 for t in eastern_times if t < current_eastern_time)
#         remaining = len(eastern_times) - passed

#         # Output results
#         print(f"Total times: {len(eastern_times)}")
#         print(f"Passed times: {passed}")
#         print(f"Remaining times: {remaining}")





#     time.sleep(1)




import mri_schwab_lib
import meic_config

from datetime import datetime, time
import pytz
from time import sleep  # Use only for sleep, avoids conflict with datetime.time

loop_count = 0
last_buying_power = 0
last_current_available = 0
total_get_buying_power_errors = 0

myBuyingPower, currentAvailble = mri_schwab_lib.get_option_buying_power()
last_buying_power = myBuyingPower
last_current_available = currentAvailble

while True:

    sleep(1)
    loop_count += 1
    # Get buying power and current Pacific time
    
    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')
    current_secs_int = current_time.second
    print(f'buying power: {last_buying_power}, current available:{last_current_available}      Pacific time: {current_time_str}')

    

    # Every 60 seconds, check Eastern time status
    # if loop_count % 10 == 2:
    if current_secs_int % 10 == 7:
        print(f'\n')
        myBuyingPower, currentAvailble = mri_schwab_lib.get_option_buying_power()

        if myBuyingPower is None or currentAvailble is None:
            print(f'problem getting buying power, myBuyingPower:{myBuyingPower}, currentAvailable:{currentAvailble}')
            total_get_buying_power_errors  += 1
            continue

        last_buying_power = myBuyingPower
        last_current_available = currentAvailble



        meic_times = meic_config.config_meic_times  # List of "HH:MM" strings in Eastern Time

        # Convert strings to datetime.time objects
        eastern_times = [time.fromisoformat(t) for t in meic_times]

        # Get current time in Pacific Time
        pacific = pytz.timezone("US/Pacific")
        eastern = pytz.timezone("US/Eastern")
        now_pacific = datetime.now(pacific)

        # Convert to Eastern Time
        now_eastern = now_pacific.astimezone(eastern)
        current_eastern_time = now_eastern.time()

        # Count passed and remaining times
        passed = sum(1 for t in eastern_times if t < current_eastern_time)
        remaining = len(eastern_times) - passed

        if remaining > 0 and currentAvailble is not None:
            availble_per_remaining = currentAvailble / remaining
        else:
            availble_per_remaining = 0

        if remaining > 0 and myBuyingPower is not None:
            bp_per_remaining = myBuyingPower / remaining
        
        else:
            bp_per_remaining = 0

        # Output results
        print(f"Total times: {len(eastern_times)}")
        print(f"Passed times: {passed}")
        print(f"Remaining times: {remaining}")
        print(f'cash available:{currentAvailble}, buying power:{myBuyingPower}')
        print(f'cash availble per entry for remining entry times:{availble_per_remaining:.2f}')
        print(f'per entry buying power for remaining entries:{bp_per_remaining:.2f}')
        print(f'total errors getting buying power {total_get_buying_power_errors}')
        print(f'\n')

    