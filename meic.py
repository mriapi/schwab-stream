import threading
import queue
import paho.mqtt.client as mqtt
import time
import os
from dotenv import load_dotenv
import schwabdev
import pandas as pd
from datetime import datetime, timezone, timedelta
import pytz
from tzlocal import get_localzone  # To get the local timezone
import warnings
import json
# import spread_picker_B
import recommender
from tabulate import tabulate
import calendar
import random
import market_open
import order
import math
import positions




# Define the list of entry times in EASTERN TIME, in 24-hour clock format
# entry_times = ["13:00", "13:30", "14:00", "14:30", "14:45"]
# entry_times = ["10:43", "12:58", "13:28", "13:58", "14:28"]
# entry_times = ["15:07"]

# #               10:00    10:30    10:45    11:15    11:30    11:45
# entry_times = ["12:58", "13:28", "13:43", "14:13", "14:28", "14:43"]

# #               6:45    10:00    10:30    11:15    11:30    11:45
# entry_times = ["9:45", "12:58", "13:30", "14:15", "14:30", "15:21"]


# #               6:45    07:00    07:15    07:30    07:45    08:00    08:15    08:30    08:45    09:00
# entry_times = ["9:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30", "11:45", "12:00"]

# #               12:15
# entry_times = ["15:14"]

#               09:45    10:00    10:30    11:00    11:15    11:30
entry_times = ["12:45", "13:00", "13:30", "14:00", "14:15", "14:30"]


real_trading_flag = True


# def show_times(entry_times):
#     print("Eastern times")
#     times_as_str = [time.strftime("%H:%M") for time in entry_times]
#     print(", ".join(times_as_str))


# def show_times(entry_times):
#     print("Eastern times")
#     times_as_str = [time.strftime("%H:%M") for time in entry_times]
#     print(", ".join(times_as_str))

#     times_as_12_hour = [datetime.strptime(t, "%H:%M").strftime("%I:%M %p") for t in times_as_str]
#     print(", ".join(times_as_12_hour))



def show_times(entry_times):
    print("\nEastern times")
    times_as_str = [time.strftime("%H:%M") for time in entry_times]
    print(",    ".join(times_as_str))

    times_as_12_hour = [datetime.strptime(t, "%H:%M").strftime("%I:%M %p") for t in times_as_str]
    print(", ".join(times_as_12_hour))

    print("\nPacific Times")
    pacific_times = [(datetime.strptime(t, "%H:%M") - timedelta(hours=3)).strftime("%I:%M %p") for t in times_as_str]
    print(", ".join(pacific_times))
    print()











def get_meic_dated_desination_dir():
    base_dir = r"C:\MEIC\tranche"

    # Get the current date in yymmdd format
    current_date = datetime.now().strftime('%y%m%d')

    # Create the full directory path
    full_dir = os.path.join(base_dir, f"data_{current_date}")

    # Create the directory if it does not already exist
    os.makedirs(full_dir, exist_ok=True)

    return full_dir




def post_tranche_data(new_post):
    """
    Append the new_post string to a file named tranche_info.txt in the specified directory.
    
    :param directory_path: The path to the directory where the file is located.
    :param new_post: The string to be appended to the file.
    """

    tranche_post_dated_dir = get_meic_dated_desination_dir()
    file_path = os.path.join(tranche_post_dated_dir, "tranche_info.txt")
    
    # Ensure the directory exists
    os.makedirs(tranche_post_dated_dir, exist_ok=True)
    
    # Open the file in append mode (create if it does not exist)
    with open(file_path, 'a') as file:
        file.write(new_post + "\n")
    
    # print(f'Appended new post to file: {file_path}')



# post_test_str = r'my tranche test string 1'
# post_tranche_data(post_test_str)
# post_test_str = r'my tranche test string 2'
# post_tranche_data(post_test_str)





# Define the Eastern Time zone
eastern = pytz.timezone('US/Eastern')



# Convert entry times to time objects
entry_times = [datetime.strptime(t, "%H:%M").time() for t in entry_times]

# Get the current time in Eastern Time zone
current_time = datetime.now(eastern)

# Load processed_times with entry times that have already been crossed
processed_times = {
    entry_time for entry_time in entry_times
    if eastern.localize(datetime.combine(current_time.date(), entry_time)) <= current_time
}

# Create an empty DataFrame with the desired headers
positions_df = pd.DataFrame(columns=["sym", "put_call", "qty", "trade_price", "now_price"])

positions_lock = threading.Lock()

my_hash = ""

gbl_short_positions = []
gbl_long_positions = []



quote_df_lock = threading.Lock()

global mqtt_client

global gbl_total_message_count
gbl_total_message_count = 0

global prev_request_id
prev_request_id = ""

global gbl_round_trip_start

end_flag = False



def load_env_variables():
    
    # parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    # env_file_path = os.path.join(parent_dir, '.env')
    # load_dotenv(env_file_path)

    load_dotenv()  # load environment variables from .env file

    app_key = os.getenv('MY_APP_KEY')
    secret_key = os.getenv('MY_SECRET_KEY')
    tokens_file = os.getenv('TOKENS_FILE_PATH')

    # print(f'my_local_app_key: {app_key}, my_local_secret_key: {secret_key}')
    # print(f'tokens_file type: {type(tokens_file)}, value: {tokens_file}')

    return app_key, secret_key, tokens_file




# Define a queue for inter-thread communication
message_queue = queue.Queue()

# MQTT settings
BROKER_ADDRESS = "localhost"



GRID_PUB_REQUEST_TOPIC = "schwab/spx/grid/request/"
GRID_SUB_RESPONSE_TOPIC = "schwab/spx/grid/response/#"

# Callback function when the client connects to the broker
def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("meic: Connected to MQTT broker.")
        client.subscribe(GRID_SUB_RESPONSE_TOPIC)
        print(f"meic: Subscribed to topic: {GRID_SUB_RESPONSE_TOPIC}")


    else:
        print(f"Failed to connect with error code: {rc}")

# Callback function when a message is received
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # print(f"grid tester: Received topic type:{type(topic)}, payload type:{type(payload)}")


    # payload may be empty or may not be json data
    try:
        # Attempt to parse the JSON data
        payload_dict = json.loads(payload)

    except json.JSONDecodeError:
        print("meic Payload is not valid JSON")
        return

    except Exception as e:
        print(f"meic An error occurred: {e} while trying load json data")
        return
        

    # print(f"01 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

    # Put the topic and payload into the queue as a tuple
    message_queue.put((topic, payload))





def persist_string(string_data):
    # Define the directory and ensure it exists
    directory = r"C:\MEIC\tranche"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"tranches_{current_datetime}.txt"
    file_path = os.path.join(directory, filename)

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Open the file in append mode, creating it if it doesn't exist, and append the string data
    with open(file_path, 'a') as file:
        file.write(string_data + '\n')



def persist_list(list_data):

    # print(f'in persist_list, list_data:\n{list_data}')

    # Define the directory and ensure it exists
    directory = r"C:\MEIC\tranche"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"tranches_{current_datetime}.txt"
    file_path = os.path.join(directory, filename)

    try:
    
        with open(file_path, "a") as file:
            # json.dump(list_data, file, indent=4)  # Indent for human-readable formatting

            for item in list_data:
                # Check if the required keys are present

                if 'symbol' in item and 'bid' in item and 'ask' in item:

                    bid_fl = float(item['bid'])
                    bid_str = f"{bid_fl:.2f}"
                    ask_fl = float(item['ask'])
                    ask_str = f"{ask_fl:.2f}"
                    last_fl = None
                    last_str = "None"

                    # if bid is 0 we are not interested in preserving the data
                    if bid_fl == 0.0 or bid_fl >= 10:
                        continue
                    

                    # Format the string
                    formatted_string = f"symbol:{item['symbol']}, bid:{bid_str}, ask:{ask_str}"
                    if 'last' in item:
                        last_fl = float(item['last'])
                        last_str = f"{last_fl:.2f}"
                        formatted_string += f", last:{last_str}"

                    if "SPXW24" in formatted_string:


                        # FIX ME

                        # Strip characters 8th through 17th, 19th, and 24th through 26th
                        revised_string = (
                            formatted_string[:7] +  # Keep everything up to the 7th character
                            formatted_string[17:18] +  # Keep the 18th character
                            formatted_string[19:23] +  # Keep characters 20th through 23rd
                            formatted_string[26:]  # Keep everything from the 27th character onwards
                        )

                        print("Original string:", formatted_string)
                        print("Revised string:", revised_string)
                        formatted_string = revised_string



                    file.write(formatted_string + '\n')

    except Exception as e:
        print(f"Error persist_list(): {e}")





def spread_data(short_opt, long_opt, spx_price):

    


    try:

        if len(long_opt) < 1 or len(short_opt)< 1:
            return {}
        
        # Extract data from the single-item lists
        short_opt = short_opt[0]
        long_opt = long_opt[0]

        # Retrieve necessary values
        short_symbol = short_opt['symbol']
        short_strike = short_opt['STRIKE']
        long_symbol = long_opt['symbol']
        long_strike = long_opt['STRIKE']
        short_bid = short_opt['bid']
        long_ask = long_opt['ask']

        # Calculate required values
        net = abs(short_opt['bid'] - long_opt['ask'])
        width = abs(short_strike - long_strike)
        otm_offset = abs(spx_price - short_strike)

        # Create the spread list with the required keys
        spread = {
            'short_symbol': short_symbol,
            'short_strike': short_strike,
            'long_symbol': long_symbol,
            'long_strike': long_strike,
            'net': net,
            'width': width,
            'otm_offset': otm_offset,
            'short_bid' : short_bid,
            'long_ask' : long_ask
        }


        return spread

    except KeyError as e:
        print(f"Missing key: {e}. Ensure 'short_opt' and 'long_opt' have 'symbol', 'bid', 'ask', and 'STRIKE' keys.")
    except Exception as e:
        print(f"1325  An error occurred: {e}")
        print(f'size of short_opt:{len(short_opt)}, size of long_opt:{len(long_opt)},')

    return {}  # Return an empty spread in case of an exception   



def get_syms(short_opt, long_opt):

    # print(f'in display_syms, short_opt type{type(short_opt)}, data:\n{short_opt}')
    # print(f'in display_syms, long_opt type{type(long_opt)}, data:\n{long_opt}')

    short_sym = ""
    long_sym = ""

    
    # Format the output according to the provided structure
    try:


        # Save symbols into string variables
        short_sym = short_opt[0]['symbol']
        long_sym = long_opt[0]['symbol']


 
    except KeyError as e:
        print(f"Missing key: {e}. Ensure all required keys are present in the short and long option lists.")
        return "", ""
    except Exception as e:
        print(f"1325 An error occurred: {e} while trying to get symbols for short/long options")
        return "", ""
    

    return short_sym, long_sym



def display_spread(label_str, spread_list):


    # print(f'spread_list type:{type(spread_list)}, data:{spread_list}')

    if 'net' not in spread_list:
        disp_str = f'{label_str}:  No recommendation.'
        print(disp_str)
        # persist_string(disp_str)
        return

    
    # Format the output according to the provided structure
    try:
        net_fl = float(spread_list['net'])
        net_str = f"{net_fl:.2f}"
        short_bid_fl = float(spread_list['short_bid'])
        short_bid_str = f"{short_bid_fl:.2f}"
        long_ask_fl = float(spread_list['long_ask'])
        long_ask_str = f"{long_ask_fl:.2f}"


        output = (
            f"{spread_list['short_strike']}/{spread_list['long_strike']}  "
            f"net: {net_str}  "
            f"width: {spread_list['width']}  "
            f"otm offset: {spread_list['otm_offset']:.2f}  "  # Format to 2 decimal places "
            f"premiums: {short_bid_str}/{long_ask_str}"
        )
        # print(output)
    except KeyError as e:
        print(f"Missing key: {e}. 2863 Ensure all required keys are present in spread_list.")
        return
    except Exception as e:
        print(f"1225 An error occurred: {e}")
        print(f'size of spread_list:{len(spread_list)}')
        return

    
    disp_str = f'{label_str} {output}'
    print(disp_str)
    persist_string(disp_str)


def display_syms_only(option_list):
    try:
        for option in option_list:
            print(f"Symbol: {option['symbol']}")

    except KeyError as e:
        print(f"KeyError: Missing key {e}. Exiting display_syms_only().")
        return
    except Exception as e:
        print(f"An error occurred: {e}. Exiting display_syms_only().")
        return




# Thread function to process messages from the queue
def process_message(schwab_client):
    global spx_last_fl
    global gbl_total_message_count
    global end_flag
    global processed_times

    while True:
        if end_flag == True:
            print(f'meic process_message() end_flag True, returning')
            return



        # Get the (topic, message) tuple from the queue
        # topic, payload = message_queue.get()

        try:
            topic, payload = message_queue.get(timeout=1)  # 1 second timeout

        except queue.Empty: 
            continue






        gbl_total_message_count += 1


        payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")


        if "schwab/spx/grid/response" in topic:
            
            request_id = topic.split('/')[-1]

            if "meic" in request_id:
                if request_id != prev_request_id:
                    print(f'Request ID mismatch!!! prev_request_id:<{prev_request_id}>, received request_id:<{request_id}>')
                    time.sleep(0.01)
                    continue


            end_time = datetime.now()
            current_time = end_time.strftime('%H:%M:%S')
            elapsed_time = end_time - gbl_round_trip_start
            # Extract the total milliseconds from the elapsed time
            elapsed_milliseconds = int(elapsed_time.total_seconds() * 1000)

            # Print the elapsed time in milliseconds
            display_str = f'meic: tranche grid data received at {current_time} Pacific Time.  Elapsed grid request/response time: {elapsed_milliseconds} mS'
            print(display_str)
            persist_string(display_str)

            if not gbl_short_positions:
                info_str = f'No existing short positions'
            else:
                info_str = f'\nexisting short positions: {gbl_short_positions}'

            print(info_str)
            persist_string(display_str)

            if not gbl_long_positions:
                info_str = f'No existing long positions'
            else:
                info_str = f'\nexisting long positions: {gbl_long_positions}\n'

            print(info_str)
            persist_string(display_str)



            # print(f'grid responose topic:<{topic}>, payload_dict type{type(payload_dict)}, data:\n{payload_dict}')
            
            (call_short,
                call_long,
                put_short,
                put_long,
                spx_price,
                atm_straddle,
                target_credit) = recommender.generate_recommendation(gbl_short_positions, gbl_long_positions,payload_dict)
            

            # ensue that we have all four recommendations
            cs_len = len(call_short)
            cl_len = len(call_long)
            ps_len = len(put_short)
            pl_len = len(put_long)


            # if we are missing one or more of the four recommendations
            if cs_len == 0 or cl_len == 0 or ps_len == 0 or pl_len == 0:
                print(f'MEIC: at least one of the spread options could not be recommended')
                if cs_len == 0:
                    print(f'call short was not selected')
                if cl_len == 0:
                    print(f'call long was not selected')
                if ps_len == 0:
                    print(f'put short was not selected')
                if pl_len == 0:
                    print(f'put long was not selected')

                time.sleep(0.1)
                continue


            # print(f'call short:{call_short}')
            # print(f'call long:{call_long}')
            # print(f'put short:{put_short}')
            # print(f'put long:{put_long}')
            # print(f'target credit:{target_credit}')

                

            call_spread = spread_data(call_short, call_long, spx_price)
            put_spread = spread_data(put_short, put_long, spx_price)


            # ensure that the final net credit is not too low
            MIN_NET = 0.90

            call_net = 0.00
            put_net = 0.00


            if 'net' in call_spread:
                net_value = call_spread.get('net')
                
                net_fl = float(net_value)
                call_net = net_fl
                # print(f'call_spread call_net:{call_net}')

            if 'net' in put_spread:
                net_value = put_spread.get('net')
                # print(f'put_spread net:{net_value}')
                net_fl = float(net_value)
                put_net = net_fl
                # print(f'put_spread put_net:{put_net}')

            if call_net < MIN_NET:
                print(f'call_net too low:{call_net}')
                print(f'gbl_short_positions:\n{gbl_short_positions}')
                print(f'gbl_long_positions:\n{gbl_long_positions}')

                
                # (r_last_call_list, 
                #  r_last_call_short_list, 
                #  r_last_call_long_list, 
                #  r_last_put_list, 
                #  r_last_put_short_list, 
                #  r_last_put_long_list) = recommender.get_last_short_long_lists()
                
                # # print(f'selected call short:\n{call_short}')
                # print(f'selected call_short:')
                # display_syms_only(call_short)

                # # print(f'selected call long:\n{call_long}')
                # print(f'selected call_long:')
                # display_syms_only(call_long)


                # # print(f'last_call_list:\n{r_last_call_list}')
                # print(f'r_last_call_list:')
                # display_syms_only(r_last_call_list)


                # # print(f'last_call_short_list:\n{r_last_call_short_list}')
                # print(f'r_last_call_short_list:')
                # display_syms_only(r_last_call_short_list)


                # # print(f'last_call_long_list:\n{r_last_call_long_list}')
                # print(f'r_last_call_long_list:')
                # display_syms_only(r_last_call_long_list)



            if put_net < MIN_NET:
                print(f'put_net too low:{put_net}')
                print(f'gbl_short_positions:\n{gbl_short_positions}')
                print(f'gbl_long_positions:\n{gbl_long_positions}')

                # (r_last_call_list, 
                #  r_last_call_short_list, 
                #  r_last_call_long_list, 
                #  r_last_put_list, 
                #  r_last_put_short_list, 
                #  r_last_put_long_list) = recommender.get_last_short_long_lists()
                
                # # print(f'selected put short:\n{put_short}')
                # print(f'selected put short:')
                # display_syms_only(put_short)

                # # print(f'selected put long:\n{put_long}')
                # print(f'selected put long:')
                # display_syms_only(put_long)

                # # print(f'r_last_put_list:\n{r_last_put_list}')
                # print(f'r_last_put_list:')
                # display_syms_only(r_last_put_list)

                # # print(f'r_last_put_short_list:\n{r_last_put_short_list}')
                # print(f'r_last_put_short_list:')
                # display_syms_only(r_last_put_short_list)


                # # print(f'r_last_put_long_list:\n{r_last_put_long_list}')
                # print(f'r_last_put_long_list:')
                # display_syms_only(r_last_put_long_list)



            if 'net' in call_spread and 'net' in put_spread and call_net >= MIN_NET and put_net >= MIN_NET:


                current_time_local = datetime.now()  # Get the current datetime object for comparison
                current_time_local_str = current_time_local.strftime('%H:%M:%S')
                

                current_time = datetime.now(eastern)
                current_time_str = current_time.strftime('%H:%M:%S')

                print(f'Checking entry_times at {current_time_str} (Eastern)')

                new_entry_match_flag = False
                placed_order_flag = False

                for entry_time in entry_times:
                    
                    # Convert entry_time to today's datetime in Eastern Time
                    entry_time_today = eastern.localize(datetime.combine(current_time.date(), entry_time))

                    # Check if the time is crossed but not processed
                    if entry_time_today <= current_time and entry_time not in processed_times:

                        print(f'{current_time_str} matches an entry time')
                        new_entry_match_flag = True
                        

                        # Check if it's within 5 minutes of the crossed time
                        if (current_time - entry_time_today).total_seconds() <= 300:
                            placed_order_flag = True

                            if real_trading_flag == True:
                                live_str = "LIVE LIVE LIVE LIVE"

                            else:
                                live_str = "PAPER PAPER PAPER"


                            try:
                                target_credit_fl = float(target_credit)

                            except Exception as e:
                                target_credit_fl = 0.00

                            info_str = f'\n\n     !PLACING {live_str} ORDER! at {current_time_str} (Eastern), spx:{spx_price}\n  atm straddle:{atm_straddle}, target credit:{target_credit_fl:.2f} real trading?:{real_trading_flag}\n\n'
                            print(info_str)
                            post_tranche_data(info_str)

                            call_order_form, call_order_id, call_order_details = \
                                order.enter_spread_with_triggers(real_trading_flag, schwab_client, my_hash, "CALL", call_short, call_long, qty=1)
                            
                            put_order_form, put_order_id, put_order_details = \
                                order.enter_spread_with_triggers(real_trading_flag, schwab_client, my_hash, "PUT", put_short, put_long, qty=1)
                        
                            # print()
                            # print(f'call_order_form type:{type(call_order_form)}, data:{call_order_form}')
                            # print(f'call_order_id type:{type(call_order_id)}, value:{call_order_id}')
                            # print(f'call_order_details type:{type(call_order_details)}, value:{call_order_details}')

                            # print()
                            # print(f'put_order_form type:{type(put_order_form)}, data:{put_order_form}')
                            # print(f'put_order_id type:{type(put_order_id)}, value:{put_order_id}')
                            # print(f'put_order_details type:{type(put_order_details)}, value:{put_order_details}')

                            # print()




                        else:
                            print(f"Skipped task for entry time {entry_time} (more than 5 minutes late)")

                        # Mark the time as processed
                        processed_times.add(entry_time)

                if new_entry_match_flag:
                    print(f'{entry_time} ({current_time_local_str} Pacific) IS a new entry time')

                else:
                    print(f'{entry_time} ({current_time_local_str} Pacific) is not a new entry time')


                pass

            else:
                info_str = f'Did not place the order'
                print(info_str)
                post_tranche_data(info_str)

                if 'net' not in call_spread:
                    info_str = f'net not in call_spread:'
                    print(info_str)
                    post_tranche_data(info_str)

                if 'net' not in put_spread:
                    info_str = f'net not in put_spread:'
                    print(info_str)
                    post_tranche_data(info_str)
                    
                if call_net >= MIN_NET:
                    info_str = f'call_net to small:{call_net}'
                    print(info_str)
                    post_tranche_data(info_str)
                    
                if put_net >= MIN_NET:
                    info_str = f'put_net to small:{call_net}'
                    print(info_str)
                    post_tranche_data(info_str)



            atm_string = f'SPX:{spx_price:.2f}, ATM straddle:{atm_straddle:.2f}'
            print(atm_string)
            persist_string(atm_string)
            display_spread("Call", call_spread)
            display_spread(" Put", put_spread)


            if 'net' in call_spread or 'net' in put_spread:
                display_str = "Symbols:"
                persist_string(display_str)
                print(display_str)


            if 'net' in call_spread:
                call_short_sym, call_long_sym = get_syms(call_short, call_long)
                call_syms = f'{call_short_sym}/{call_long_sym}'
                call_syms_display = "Call " + call_syms
                persist_string(call_syms_display)
                print(call_syms_display)

            if 'net' in put_spread:
                put_short_sym, put_long_sym = get_syms(put_short, put_long)
                put_syms = f'{put_short_sym}/{put_long_sym}'
                put_syms_display = " Put " + put_syms
                persist_string(put_syms_display)
                print(put_syms_display)






            
            
 
            pass

        else:
            print(f'received unexpected topic:<{topic}>, payload_dict type{type(payload_dict)}, data:\n{payload_dict}')
            pass



        time.sleep(0.1)



def publish_grid_request():
    global mqtt_client
    global prev_request_id
    global gbl_round_trip_start


    topic = GRID_PUB_REQUEST_TOPIC 

    # Get the current time
    current_time = datetime.now()

    # Format the time as hhmmssmmmm, where "mmmm" is milliseconds
    req_id = "meic" + current_time.strftime('%H%M%S') + f"{current_time.microsecond // 1000:04d}"


    topic = topic + req_id

    # print(f'publishing topic:<{topic}>')

    mqtt_client.publish(topic, " ")

    gbl_round_trip_start = datetime.now()



    prev_request_id = req_id

    pass




def meic_entry(schwab_client):
    global quote_df
    global end_flag
    global gbl_short_positions
    global gbl_long_positions

    display_quote_throttle = 0

    seconds_to_next_minute = market_open.seconds_until_even_minute() + 3

    seconds_to_minute_int = math.floor(seconds_to_next_minute)

    while True:
        time.sleep(1)

        if end_flag == True:
            print(f'meic meic_entry() end_flag True, returning')
            return


        display_quote_throttle += 1


        # periodically request the current grid.  The response triggers further action
        # if display_quote_throttle % 20 == 15:

        seconds_to_next_minute -= 1
        seconds_to_minute_int -= 1

        # print(f'seconds_to_minute_int:{seconds_to_minute_int}')

        if seconds_to_minute_int % 10 == 20:
        # if seconds_to_minute_int == 55:
            print("\nGetting account details with positions")
            account_details = schwab_client.account_details(my_hash, fields="positions").json()

            print(f'account_details type:{type(account_details)}, data:\n{account_details}')

            pass



        if seconds_to_next_minute <= 0:

            # try get account details and short/long legs here
            get_positions_success_flag, gbl_short_positions, gbl_long_positions = positions.get_positions2()

            # print(f'get_positions2():\nsuccess flag:{get_positions_success_flag}')


            # if not gbl_short_positions:
            #     print(f'No existing short positions')
            # else:
            #     print(f'\nexisting short positions: {gbl_short_positions}')

            # if not gbl_long_positions:
            #     print(f'No existing long positions')
            # else:
            #     print(f'\nexisting long positions: {gbl_long_positions}\n')



            seconds_to_next_minute = market_open.seconds_until_even_minute() + 3
            seconds_to_minute_int = math.floor(seconds_to_next_minute)
        
            print(f'\n=============================\nRequesting SPX grid data')
            publish_grid_request()
            pass


def is_market_open():
    # global gbl_market_open_flag

    now = datetime.now(timezone.utc)
    # print(f'934 now type:{type(now)}, value:{now}')

    # Determine if the current day is Monday through Friday
    day_of_week = now.weekday()
    # Convert the integer to the corresponding weekday name 
    weekday_name = calendar.day_name[day_of_week]
    is_weekday = 0 <= day_of_week <= 4

    if is_weekday:
        # print("Today is a weekday (Monday through Friday).")
        weekday_flag = True
    else:
        # print("Today is not a weekday (Monday through Friday).")
        weekday_flag = False

    # set Eastern Time Zone
    eastern = pytz.timezone('US/Eastern')

    # Get the current time in Eastern Time
    current_time = datetime.now(eastern)

    

    # set markets daily start/end times

    start_time = current_time.replace(hour=9, minute=30, second=10, microsecond=0)
    end_time = current_time.replace(hour=15, minute=59, second=50, microsecond=0)

    # eastern_time_str = current_time.strftime('%H:%M:%S')
    # end_time_str = end_time.strftime('%H:%M:%S')
 

    if weekday_flag == False or current_time < start_time or current_time > end_time:
        # print(f'Market is not open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
        # gbl_market_open_flag = False
        return False
    
    # print(f'Market IS open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
    

    # gbl_market_open_flag = True

    return True



def wait_for_market_to_open():
    throttle_wait_display = 0
    print(f'meic: waiting for market to open 2')

    while True:
        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=2, close_offset=0)
        if market_open_flag:
            break

        throttle_wait_display += 1
        # print(f'throttle_wait_display: {throttle_wait_display}')
        if throttle_wait_display % 3 == 2:
            current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
            current_eastern_day = current_eastern_time.strftime('%A')



            # eastern = pytz.timezone('US/Eastern')
            # current_time = datetime.now(eastern)
            # eastern_time_str = current_time.strftime('%H:%M:%S')

            print(f'meic: waiting for market to open, current East time: {current_eastern_day} {current_eastern_hhmmss}')

            pass


        time.sleep(10)





def meic_loop():
    global mqtt_client
    global end_flag
    global my_hash

    try:


        throttle_wait_display = 0

        print(f'meic: waiting for market to open 1')

        while True:
            if is_market_open():
                break

            throttle_wait_display += 1
            # print(f'throttle_wait_display: {throttle_wait_display}')
            if throttle_wait_display % 3 == 2:

                eastern = pytz.timezone('US/Eastern')
                current_time = datetime.now(eastern)
                eastern_time_str = current_time.strftime('%H:%M:%S')

                print(f'waiting for market to open, current East time: {eastern_time_str}')

                pass


            time.sleep(10)

        print(f'meic: market is open')


        # Initialize MQTT client
        mqtt_client = mqtt.Client()
        # mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        # Assign callback functions
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message

        # Connect to the MQTT broker
        print("Connecting to MQTT broker...")
        mqtt_client.connect(BROKER_ADDRESS)

        app_key, secret_key, my_tokens_file = load_env_variables()

        # create schwabdev client
        schwab_client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)
        linked_accounts = schwab_client.account_linked().json()
        # print(f'linked_accounts type:{type(linked_accounts)}, data:\n{linked_accounts}\n')

        try:
            # Extract the hashValue
            my_hash = linked_accounts[0]['hashValue']
            # print("Hash value found:", my_hash)
        except KeyError:
            # Handle the case where 'hashValue' is not present
            print("Key 'hashValue' not found in the linked_accounts list.")
            end_flag = True
            return

        


        # Start the keyboard thread
        # keboard_thread = threading.Thread(target=keyboard_handler_task, name="keyboard_handler_task")
        # keboard_thread.daemon = True  # Daemonize thread to exit with the main program
        # keboard_thread.start()



        # Start the message processing thread
        processing_thread = threading.Thread(target=process_message, name="process_message", args=(schwab_client,))
        processing_thread.daemon = True  # Daemonize thread to exit with the main program
        processing_thread.start()

        # Start the meic_entry thread
        # meic_entry_thread = threading.Thread(target=meic_entry, name="meic_entry")
        meic_entry_thread = threading.Thread(target=meic_entry, name="meic_entry", args=(schwab_client,))

        meic_entry_thread.daemon = True  # Daemonize thread to exit with the main program
        meic_entry_thread.start()

        # Start the MQTT client loop (handles reconnects and message callbacks)
        # mqtt_client.loop_forever()

        while True:
                mqtt_client.loop(timeout=1.0)  # process network traffic, with a 1-second timeout
                # time.sleep(1) 
                market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=2, close_offset=0)


                if market_open_flag == False:
                    end_flag = True
                    current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
                    print(f'meic: Market is now closed at {current_eastern_hhmmss}, shutting down MQTT')
                    mqtt_client.loop_stop()  # Stop the MQTT loop
                    mqtt_client.disconnect()  # Disconnect from the MQTT broker
                    break

                if end_flag == True:
                    break
                    

        
        
        end_flag = True

        print(f'meic: waiting for processing_thread and meic_entry_thread to end and join')
        processing_thread.join()
        meic_entry_thread.join()
        print(f'meic:processing_thread and meic_entry_thread have finished')


    except Exception as e:
        print(f"Error in meic_loop(): {e}")
        end_flag = True
        return




# Main function to set up MQTT client and start the processing thread
def main():

    if real_trading_flag == True:
        info_str = f'++++++++++ LIVE ++++++++++'

    else:
        info_str = f'---------- PAPER ----------'
        

    print(f'\n\n')
    print(f'Paper/Live Mode: {info_str}')
    print(f'Paper/Live Mode: {info_str}')
    print(f'Paper/Live Mode: {info_str}')
    print(f'Paper/Live Mode: {info_str}')
    print(f'\n\n')
    

    print(f'\nScheduled Entry Times:')
    show_times(entry_times)

    while True:
        wait_for_market_to_open()
        meic_loop()


# Entry point of the program
if __name__ == "__main__":
    print(f'meic: calling main')
    main()
    print(f'meic: returned from main')
