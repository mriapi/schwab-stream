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

while True:
    # Get buying power and current Pacific time
    myBuyingPower, myInitialBalance = mri_schwab_lib.get_option_buying_power()
    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')
    print(f'buying power: {myBuyingPower}, initial balance:{myInitialBalance}      Pacific time: {current_time_str}')

    loop_count += 1

    # Every 60 seconds, check Eastern time status
    if loop_count % 10 == 2:
        print(f'\n')
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

        if len(eastern_times) > 0 and myInitialBalance is not None:
            bp_per_all_entries = myInitialBalance / len(eastern_times)
        else:
            bp_per_all_entries = myInitialBalance

        if remaining > 0 and myBuyingPower is not None:
            bp_per_remaining = myBuyingPower / remaining
        
        else:
            bp_per_remaining = myBuyingPower

        # Output results
        print(f"Total times: {len(eastern_times)}")
        print(f"Passed times: {passed}")
        print(f"Remaining times: {remaining}")
        print(f'buying power per entry at start of session:{bp_per_all_entries}')
        print(f'per entry buying power for remaining entries:{bp_per_remaining}')
        print(f'\n')

    sleep(1)