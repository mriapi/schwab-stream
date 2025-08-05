import threading
import queue
import logging
from paho.mqtt.enums import CallbackAPIVersion
import paho.mqtt.client as mqtt
import time
import os
from dotenv import load_dotenv
# import schwabdev
import pandas as pd
from datetime import datetime, timezone, timedelta
import warnings
import json
import pytz
import calendar
import pytz
import market_open
import mri_schwab_lib


MARKET_OPEN_OFFSET = 1
MARKET_CLOSE_OFFSET = 0

DEBUG_CHAIN_SYM = "P06030"

quote_df_lock = threading.Lock()
global quote_df
quote_df_lcnt_1 = 0
quote_df_lcnt_2 = 0
quote_df_lcnt_3 = 0
quote_df_lcnt_4 = 0
quote_df_lcnt_5 = 0
quote_df_lcnt_6 = 0
quote_df_lcnt_7 = 0



mqtt_client = None


global gbl_total_message_count
gbl_total_message_count = 0

global time_since_last_quereied
time_since_last_quereied = 0

global time_since_last_stream
time_since_last_stream = 0


market_open_flag = False



logging.basicConfig(
    filename="mri_log2.log",  # Log file name
    level=logging.INFO,  # Set logging level
    format="%(asctime)s - %(levelname)s - %(message)s"
)




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





spx_last_fl = None
spx_bid_fl = None
spx_ask_fl = None

# Define a queue for inter-thread communication
message_queue = queue.Queue()

# MQTT settings
BROKER_ADDRESS = "localhost"



# MQTT mode
MQTT_MODE_RAW = 0
MQTT_MODE_TOPICS = 1
mqtt_mode = MQTT_MODE_RAW

if mqtt_mode == MQTT_MODE_TOPICS:
    SPX_LAST_TOPIC = "schwab/stock/SPX/last"
    SPX_OPT_BID_ASK_LAST_TOPIC = "schwab/option/spx/basic/#"
    SPX_OPT_BID_ASK_LAST_CHECK = "schwab/option/spx/basic/"
    SPX_SCHWAB_STREAM = ""
    SPX_SCHWAB_QUERIED = ""
    SPX_SCHWAB_CHAIN = ""

elif mqtt_mode == MQTT_MODE_RAW:
    SPX_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_CHECK = ""
    SPX_SCHWAB_STREAM = "schwab/stream"
    SPX_SCHWAB_QUERIED = "schwab/queried"
    SPX_SCHWAB_CHAIN = "schwab/chain"

else:
    SPX_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_CHECK = ""
    SPX_SCHWAB_STREAM = ""
    SPX_SCHWAB_QUERIED = ""   
    SPX_SCHWAB_CHAIN = ""


GRID_REQUEST_TOPIC = "schwab/spx/grid/request/#"
GRID_RESPONSE_TOPIC = "schwab/spx/grid/response/"
GRID_REFUSE_TOPIC = "schwab/spx/grid/resfuse/"
CHAIN_REQUEST_TOPIC = "schwab/spx/chain/request"

# Callback function when the client connects to the broker
# def on_connect(client, userdata, flags, rc):
def on_connect(client, userdata, flags, rc, properties):

    try:

        if rc == 0:

            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M:%S')

            print(f'At {time_str} connected to MQTT broker successfully.')
            if mqtt_mode == MQTT_MODE_TOPICS:
                client.subscribe(SPX_LAST_TOPIC)
                print(f"Subscribed to topic: {SPX_LAST_TOPIC}")
                client.subscribe(SPX_OPT_BID_ASK_LAST_TOPIC)
                print(f"Subscribed to topic: {SPX_OPT_BID_ASK_LAST_TOPIC}")

            elif mqtt_mode == MQTT_MODE_RAW:
                client.subscribe(SPX_SCHWAB_STREAM)
                print(f"Subscribed to topic: {SPX_SCHWAB_STREAM}")
                client.subscribe(SPX_SCHWAB_QUERIED)
                print(f"Subscribed to topic: {SPX_SCHWAB_QUERIED}")
                client.subscribe(SPX_SCHWAB_CHAIN)
                print(f"Subscribed to topic: {SPX_SCHWAB_CHAIN}")

                SCHWAB_BASE_TOPIC = "schwab/#"
                client.subscribe(SCHWAB_BASE_TOPIC)
                print(f"Subscribed to topic: {SCHWAB_BASE_TOPIC}")

            client.subscribe(GRID_REQUEST_TOPIC)
            print(f"Subscribed to topic: {GRID_REQUEST_TOPIC}")



        else:
            print(f"Failed to connect with error code: {rc}")

    except Exception as e:

        current_time_local = datetime.now()  # Get the current datetime object for comparison
        current_time_local_str = current_time_local.strftime('%H:%M:%S')

        info_str = f'grid general on_connect exception: {e} at {current_time_local_str}'
        print(info_str)
        logging.error(info_str)


topic_rx_cnt = 0
throttle_rx_cnt_display = 0
total_on_message_cnt = 0
on_message_dbg = 0

# Callback function when a message is received
def on_message(client, userdata, msg):
    global total_on_message_cnt, on_message_dbg
    global gbl_got_grid_request_ts_str1

    try:

        total_on_message_cnt += 1
        

        topic = msg.topic
        payload = msg.payload.decode()


        if "heartbeat" in payload:
            hb_msg = " (heartbeat)"
            
        else:
            hb_msg = ""

        if len(hb_msg) > 1:
            print(f'grid on_message notify heartbeat received')

        if "grid/request" in topic:
            current_time = datetime.now()
            gbl_got_grid_request_ts_str1 = current_time.strftime('%H:%M:%S.%f')[:-3]


        if "schwab/spx/grid/request" in topic:
        # if "schwab/spx/grid/xxxxxxx" in topic:

            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M:%S.%f')[:-3]

            request_id = topic.split('/')[-1]
            quote_df_copy = quote_df.copy()
            rows_with_nan_bid_ask = quote_df_copy[['bid', 'ask']].isna().any(axis=1).sum()

            if time_since_last_quereied < 90 and time_since_last_stream < 20:

                publish_grid(quote_df_copy, rows_with_nan_bid_ask, request_id)
                print(f'published grid response, id:{request_id}, from on message at {time_str}')

            else:
                publish_refusal()
                print(f'published grid REFUSAL, id:{request_id}, from on message at {time_str}')

            return
















        # Put the topic and payload into the queue as a tuple
        on_message_dbg = 10702
        message_queue.put((topic, payload))
        on_message_dbg = 10703

    except Exception as e:

        current_time_local = datetime.now()  # Get the current datetime object for comparison
        current_time_local_str = current_time_local.strftime('%H:%M:%S')

        info_str = f'grid general on_message exception: {e} at {current_time_local_str}'
        print(info_str)
        logging.error(info_str)

        return



def purge_stale_spxw_symbols_warning(quote_df, payload_dict):
    # Extract live symbols from payload_dict
    live_spxw_symbols = {
        item['symbol'] for item in payload_dict.get('data', [])
        if item.get('symbol', '').startswith('SPXW')
    }

    with quote_df_lock:
        # Create a boolean mask of rows to keep
        mask = quote_df['symbol'].apply(
            lambda s: not s.startswith('SPXW') or s in live_spxw_symbols
        )
        quote_df[:] = quote_df[mask].reset_index(drop=True)


def purge_stale_spxw_symbols():
    global quote_df, payload_dict  # Access globals if used that way

    # Build a set of current live SPXW symbols from incoming data
    live_spxw_symbols = {
        item['symbol'] for item in payload_dict.get('data', [])
        if item.get('symbol', '').startswith('SPXW')
    }

    # Safely update quote_df in-place
    with quote_df_lock:
        filtered_df = quote_df[
            quote_df['symbol'].apply(
                lambda s: not s.startswith('SPXW') or s in live_spxw_symbols
            )
        ].reset_index(drop=True)

        quote_df = filtered_df  # Clean reassignment to avoid dtype warnings



# Takes spx chain quote and updates quote_df entries 
def update_quotes_from_chain(payload_dict):
    global quote_df  # Assumes quote_df is declared globally elsewhere
    global quote_df_lock
    global quote_df_lcnt_1



    if "data" not in payload_dict:
        return
    




    now_str = lambda: datetime.now().strftime('%H:%M:%S:%f')[:-3]  # hh:mm:ss:<ms> format

    
    for item in payload_dict["data"]:
        symbol = item.get("symbol")
        if symbol is None:
            continue

        quote_df_lcnt_1 = 1
        with quote_df_lock:

            matching_rows = quote_df.index[quote_df["symbol"] == symbol].tolist()
            if not matching_rows:
                continue

            row_idx = matching_rows[0]

            

            quote_df.at[row_idx, "bid"] = item.get("bid")
            quote_df.at[row_idx, "ask"] = item.get("ask")
            quote_df.at[row_idx, "last"] = item.get("last")

            quote_df.at[row_idx, "bid_time"] = now_str()
            quote_df.at[row_idx, "ask_time"] = now_str()
            quote_df.at[row_idx, "last_time"] = now_str()

            # if DEBUG_CHAIN_SYM in symbol:
            #     print(f'grid: from  chain, updating {symbol}, current bid:{item.get("bid")}, current ask:{item.get("ask")}, current last:{item.get("last")}')

        quote_df_lcnt_1 = 0

    # purge_stale_spxw_symbols(quote_df, payload_dict)


    # print(f'\ngrid update quotes from chain at {now_str}, payload_dict type:{type(payload_dict)}, data:\n{payload_dict}\n')
    # with quote_df_lock:
    #     print(f'quote_df:\n{quote_df}\n')


# Function to update the quote DataFrame
def add_to_quote_tbl(topic, payload):
    global quote_df
    global quote_df_lock
    global quote_df_lcnt_2

    # Check if the topic starts with the desired prefix
    if topic.startswith("schwab/option/spx/basic"):
        
        # Extract symbol and quote type
        parts = topic.split('/')
        symbol = parts[4]  # 5th level in topic
        quote_type = parts[5]  # Last level in topic ('bid', 'ask', 'last')
        
        # Parse payload as a float (handle invalid payloads gracefully)
        try:
            value = float(payload)
        except ValueError:
            print(f"Invalid payload: {payload} for topic: {topic}")
            return

        # Get the current time
        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S:%f')[:-3]

        temp_loaded_flag = False
        temp_last = None
        temp_row_index = None
        temp_bid = None
        temp_ask = None
        temp_sym = None


        quote_df_lcnt_2 = 1
        with quote_df_lock:    

            # Check if the symbol already exists in quote_df
            if symbol in quote_df['symbol'].values:
                # Update the existing row
                row_index = quote_df.index[quote_df['symbol'] == symbol][0]
                if quote_type == 'bid':
                    quote_df.at[row_index, 'bid'] = value
                    quote_df.at[row_index, 'bid_time'] = time_str
                elif quote_type == 'ask':
                    quote_df.at[row_index, 'ask'] = value
                    quote_df.at[row_index, 'ask_time'] = time_str
                elif quote_type == 'last':
                    temp_last = value
                    temp_row_index = row_index
                    temp_bid = quote_df.at[row_index, 'bid']
                    temp_ask = quote_df.at[row_index, 'ask']
                    temp_sym = quote_df.at[row_index, 'symbol']
                    temp_loaded_flag = True

                    quote_df.at[row_index, 'last'] = value
                    quote_df.at[row_index, 'last_time'] = time_str
            else:
                # Create a new row for the symbol
                new_row = pd.DataFrame([{
                    'symbol': symbol,
                    'bid': value if quote_type == 'bid' else None,
                    'bid_time': time_str if quote_type == 'bid' else None,
                    'ask': value if quote_type == 'ask' else None,
                    'ask_time': time_str if quote_type == 'ask' else None,
                    'last': value if quote_type == 'last' else None,
                    'last_time': time_str if quote_type == 'last' else None
                }], columns=quote_df.columns)


                with warnings.catch_warnings():
                    warnings.simplefilter(action='ignore', category=FutureWarning)
                    quote_df = pd.concat([quote_df, new_row], ignore_index=True)



        # if temp_loaded_flag == True:
        #     # print()
        #     # print(f'checking last value for {temp_sym} at row {temp_row_index} last:{temp_last} bid:{temp_bid} ask:{temp_ask}')
        #     # print(f'topic:{topic} payload:{payload}')
            
        #     # Split the string at the decimal point
        #     parts = payload.split('.')

        #     # Determine the number of decimal places
        #     if len(parts) > 1:
        #         decimal_places = len(parts[1])
        #     else:
        #         decimal_places = 0

        #     print(f'The number of decimal places in the payload is: {decimal_places}')
        #     print()




            
            
            temp_loaded_flag = False

        quote_df_lcnt_2 = 0



# Function to update the quote DataFrame
def add_to_quote_tbl2(sym,bid,ask,last):
    global quote_df
    global quote_df_lock
    global quote_df_lcnt_3

    if bid == None and ask == None and last == None:
        print(f'{sym} has all None values')
        return
    

    # Get the current time
    current_time = datetime.now()
    time_str = current_time.strftime('%H:%M:%S:%f')[:-3]
    

    bid_time = time_str
    ask_time = time_str
    last_time = time_str


    temp_loaded_flag = False
    temp_last = None
    temp_row_index = None
    temp_bid = None
    temp_ask = None
    temp_sym = None

    # sym_stripped = sym.replace(" ", "")

    # print(f'add_to_quote_tbl2 sym_stripped:{sym_stripped}, bid type{type(bid)}, bid val:{bid}')
    # print(f'bid type{type(bid)}, bid val:{bid}, ask type{type(ask)}, bid val:{ask}, last type{type(last)}, last val:{last}')
    
    quote_df_lcnt_3 = 1
    with quote_df_lock:  
        # symbol = sym_stripped  
        symbol = sym

        # if the symbol already exists in quote_df, we update the values
        if symbol in quote_df['symbol'].values:

            # print(f'{symbol} was found in table, updating values')

            # Update the existing row
            row_index = quote_df.index[quote_df['symbol'] == symbol][0]
            if bid != None:
                quote_df.at[row_index, 'bid'] = bid
                quote_df.at[row_index, 'bid_time'] = time_str

            if ask != None:
                quote_df.at[row_index, 'ask'] = ask
                quote_df.at[row_index, 'ask_time'] = time_str

            if last != None:
                quote_df.at[row_index, 'last'] = last
                quote_df.at[row_index, 'last_time'] = time_str
        
        # else the symbol did not exit in the table so we add a new row
        else:
            # print(f'{symbol} was not found in table, creating a new row')
            if bid == None:
                bid_time = None
            if ask == None:
                ask_time = None
            if last == None:
                last_time = None

            # Create a new row for the symbol
            new_row = pd.DataFrame([{
                'symbol': symbol,
                'bid': bid,
                'bid_time': bid_time,
                'ask': ask,
                'ask_time': ask_time,
                'last': last,
                'last_time': last_time
            }], columns=quote_df.columns)

            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=FutureWarning)
                # quote_df = pd.concat([quote_df, new_row], ignore_index=True)
                if not new_row.empty:
                    quote_df = pd.concat([quote_df.dropna(axis=1, how="all"), new_row.dropna(axis=1, how="all")], ignore_index=True)

            pass

    quote_df_lcnt_3 = 1
            



def process_stream(topic, payload_dict):
    global spx_last_fl
    global spx_bid_fl
    global spx_ask_fl

    equitity_key = None



    # Check for the existence of 'data'
    if "data" in payload_dict:
        # Loop through each item in 'data'
        for item in payload_dict["data"]:
            # Check for 'service' and handle accordingly
            if "service" in item:
                service = item["service"]
                if service == "LEVELONE_EQUITIES":
                    # print(f"streamed equities service: {service}")
                    # Print timestamp in Eastern time zone
                    if "timestamp" in item:
                        ts = item["timestamp"]
                        eastern = pytz.timezone("US/Eastern")
                        timestamp_dt = datetime.fromtimestamp(ts / 1000, eastern)
                        # print(f"streamed equities timestamp (ET): {timestamp_dt}")
                    # Process content
                    if "content" in item:
                        for content in item["content"]:

                            if "key" in content:
                                # print(f"streamed equities key: {content['key']}")
                                equitity_key = content['key']
                                # print(f'equitity_key type:{type(equitity_key)}, value:{equitity_key}')

                            if "bid" in content:
                                # print(f"streamed equities bid: {content['bid']}")
                                if equitity_key == "$SPX":
                                    spx_bid_fl = float(content['bid'])
                                pass
                            if "ask" in content:
                                # print(f"streamed equities ask: {content['ask']}")
                                if equitity_key == "$SPX":
                                    spx_ask_fl = float(content['ask'])
                                pass
                            if "last" in content:
                                # print(f"streamed equities last: {content['last']}")
                                if equitity_key == "$SPX":
                                    spx_last_fl = float(content['last'])
                                    # print(f'found $SPX in equitity_key:{equitity_key}, value:{spx_last_fl}')
                                    
                                    add_to_quote_tbl2(equitity_key, spx_bid_fl, spx_ask_fl, spx_last_fl)

                            


                elif service == "LEVELONE_OPTIONS":


                    # print(f"streamed options service: {service}")
                    # Print timestamp in Eastern time zone
                    if "timestamp" in item:
                        ts = item["timestamp"]
                        eastern = pytz.timezone("US/Eastern")
                        timestamp_dt = datetime.fromtimestamp(ts / 1000, eastern)
                        # print(f"streamed options timestamp (ET): {timestamp_dt}")
                    # Process content
                    if "content" in item:


                        for content in item["content"]:

                            opt_sym = None
                            opt_bid = None
                            opt_ask = None
                            opt_last = None

                            if "key" in content:
                                # print(f"streamed options key: {content['key']}")
                                opt_sym = content['key']
                            if "bid" in content:
                                # print(f"streamed options bid: {content['bid']}")
                                opt_bid = content['bid']
                            if "ask" in content:
                                # print(f"streamed options ask: {content['ask']}")
                                opt_ask = content['ask']
                            if "last" in content:
                                # print(f"streamed options last: {content['last']}")
                                opt_last = content['last']

                            if opt_sym != None:
                                if opt_bid == None and opt_ask == None and opt_last == None:
                                    # print(f'{opt_sym} has all None values 2, payload_dict:\n{payload_dict}')
                                    pass

                                else:




                                    # if DEBUG_CHAIN_SYM in opt_sym:
                                    #     print(f'grid: from stream, updating {opt_sym}, new bid:{opt_bid}, new ask:{opt_ask}, new_last:{opt_last}')

                                    # print(f'opt_sym type:{type(opt_sym)}, value:{opt_sym}')
                                    # print(f'opt_bid type:{type(opt_bid)}, value:{opt_bid}')
                                    # print(f'opt_ask type:{type(opt_ask)}, value:{opt_ask}')

                                    # if opt_ask == 0.05:
                                    #     print(f'opt_ask is 0.05, opt_bid:{opt_bid}')















                                    # Check if opt_sym does not exist in the 'symbol' column

                                    # strike_val = mri_schwab_lib.get_strike_value_from_sym(opt_sym)

                                    # if spx_last_fl != None:
                                    #     strike_offset = abs(strike_val - spx_last_fl)
                                    # else:
                                    #     strike_offset = 999

                                    # found_in_tbl_flag = opt_sym in quote_df["symbol"].values

                                    # if found_in_tbl_flag or strike_offset <= 10:
                                    #     # print(f'streamed adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
                                    #     add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)

                                    #     if not found_in_tbl_flag:
                                    #         print(f'added near ATM {opt_sym} to table')


                                    # else:
                                    #     # else the streamed symbol is either not in quote_df or not not clost to ATM
                                    #     pass

                                    add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)


                                    

                                

def process_queried(topic, payload_dict):
    global spx_last_fl


    # print(f"process_queried() topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

    # Iterate through the items in payload_dict
    for key, value in payload_dict.items():
        if "assetMainType" in value and value["assetMainType"] == "OPTION":
            opt_sym = None
            opt_bid = None
            opt_ask = None
            opt_last = None

            # print(f"Processing queried item: {key}")
            
            
            # Check and print symbol
            if "symbol" in value:
                # print(f" queried Symbol: {value['symbol']}")
                opt_sym = value['symbol']
            
            # Check and print quote[bidPrice]
            if "quote" in value and "bidPrice" in value["quote"]:
                # print(f"queried Bid Price: {value['quote']['bidPrice']}")
                opt_bid = float(value['quote']['bidPrice'])
            
            # Check and print quote[askPrice]
            if "quote" in value and "askPrice" in value["quote"]:
                # print(f" queried Ask Price: {value['quote']['askPrice']}")
                opt_ask = float(value['quote']['askPrice'])


            # if DEBUG_CHAIN_SYM in opt_sym:
            #     print(f'grid: from  query, updating {opt_sym}, new bid:{opt_bid}, new ask:{opt_ask}')



            # print(f'queried adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
            add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)


# Thread function to process messages from the queue
def grid_supervisor():
    global quote_df_lcnt_1, quote_df_lcnt_2, quote_df_lcnt_3, quote_df_lcnt_4
    global quote_df_lcnt_5, quote_df_lcnt_6, quote_df_lcnt_7


    gs_loop = 0
    while True:
        time.sleep(1)

        if not market_open_flag:
            quote_df_lcnt_1 = 0
            quote_df_lcnt_2 = 0
            quote_df_lcnt_3 = 0
            quote_df_lcnt_4 = 0            
            quote_df_lcnt_5 = 0
            quote_df_lcnt_6 = 0
            quote_df_lcnt_7 = 0
            continue

        # gs_loop += 1

        # if gs_loop % 10 == 2:
        #     print(f'grid supervisor {gs_loop}')

        if quote_df_lcnt_1 > 0:
            quote_df_lcnt_1 += 1
            if quote_df_lcnt_1 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_1: {quote_df_lcnt_1}\n')

        if quote_df_lcnt_2 > 0:
            quote_df_lcnt_2 += 1
            if quote_df_lcnt_2 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_2: {quote_df_lcnt_2}\n')

        if quote_df_lcnt_3 > 0:
            quote_df_lcnt_3 += 1
            if quote_df_lcnt_3 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_3: {quote_df_lcnt_3}\n')

        if quote_df_lcnt_4 > 0:
            quote_df_lcnt_4 += 1
            if quote_df_lcnt_4 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_4: {quote_df_lcnt_4}\n')

        if quote_df_lcnt_5 > 0:
            quote_df_lcnt_5 += 1
            if quote_df_lcnt_5 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_5: {quote_df_lcnt_5}\n')

        if quote_df_lcnt_6 > 0:
            quote_df_lcnt_6 += 1
            if quote_df_lcnt_6 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_6: {quote_df_lcnt_6}\n')

        if quote_df_lcnt_7 > 0:
            quote_df_lcnt_7 += 1
            if quote_df_lcnt_7 < 1:
                print(f'\n!!!!! long lock for quote_df_lcnt_7: {quote_df_lcnt_7}\n')







pm_prev_market_open_flag = False
def pm_is_market_open():
    global pm_prev_market_open_flag
    global market_open_flag

    # FIX ME TODO make sure this works okay 
    market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
    
    if market_open_flag != pm_prev_market_open_flag:
        if market_open_flag:
            print(f'grid process message has detected that market is now open')

        else:
            print(f'grid process message has detected that market is now closed')

        pm_prev_market_open_flag = market_open_flag

    return market_open_flag


gbl_got_grid_request_ts_str2 = ""
gbl_got_grid_request_ts_str1 = ""

# Thread function to process messages from the queue
def process_message():
    global spx_last_fl
    global gbl_total_message_count
    global market_open_flag
    global time_since_last_stream
    global time_since_last_quereied
    global on_message_dbg
    global quote_df
    global quote_df_lock
    global quote_df_lcnt_4
    global gbl_got_grid_request_ts_str2


    

    # # ensure that the market is open
    # while True:
    #     time.sleep(1)
    #     if market_open_flag == True:
    #         break




    # # Get the (topic, message) tuple from the queue
    # topic, payload = message_queue.get()

    # throttle_market_close_check = 0


    stream_message_cnt = 0

    no_message_cnt = 0

    my_open_flag = False

    while True:
        
        # throttle_market_close_check += 1
        # if throttle_market_close_check % 20 == 18:
        #     print(f'checking for market open')
        #     market_open_flag, current_eastern_time, seconds_to_next_minute, seconds_to_next_minute  = market_open.is_market_open2(
        #         open_offset=MARKET_OPEN_OFFSET, 
        #         close_offset=MARKET_CLOSE_OFFSET)
            
        #     print(f'market_open_flag:{market_open_flag}')
            

        # FIX ME this was not commented out
        # if market_open_flag == False:
        #     print(f'grid process_message(): market is now closed')
        #     return


        # print(f'calling .get')
        
        # Get the (topic, message) tuple from the queue, but time out if no message is received
        try:
            on_message_dbg = 12710
            topic, payload = message_queue.get(timeout=5)  # Wait for up to 5 seconds
            on_message_dbg = 12712
        except queue.Empty:
            # print(".get: timeout.")
            on_message_dbg = 12714

            my_open_flag = pm_is_market_open()
            no_message_cnt += 1
            if not my_open_flag and no_message_cnt % 10 == 2:
                current_time = datetime.now()
                my_time_str = current_time.strftime('%H:%M:%S.%f')[:-3]
                print(f'\ngrid pm no msg market closed at {my_time_str}')
            continue

        # if "schwab/spx/grid/request" in topic:
        if "schwab/spx/grid/xxxx" in topic:
            current_time = datetime.now()
            gbl_got_grid_request_ts_str2 = current_time.strftime('%H:%M:%S.%f')[:-3]
             


        gbl_total_message_count += 1
        # print(f'dbg grid process message returned from .get, gbl_total_message_count:{gbl_total_message_count}')

        

        # payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

        # payload may be empty or may not be json data
        try:
            # Attempt to parse the JSON data
            payload_dict = json.loads(payload)
            on_message_dbg = 10714

        except json.JSONDecodeError:
            # print("Payload is not valid JSON")
            payload_dict = {}
            on_message_dbg = 10716

        except Exception as e:
            print(f"8394 An error occurred: {e} while trying load json data")
            payload_dict = {}
            on_message_dbg = 10718


        my_open_flag = pm_is_market_open()
        if not my_open_flag:
            # print(f'grid process message disdarding topic <{topic}> because market is closed')

            if gbl_total_message_count % 5 == 2:
                current_time = datetime.now()
                my_time_str = current_time.strftime('%H:%M:%S.%f')[:-3]
                print(f'\ngrid pm msg received market closed at {my_time_str}')
            continue


        if "schwab/stream" in topic:
            on_message_dbg = 10720
            process_stream(topic, payload_dict)
            on_message_dbg = 10722
            time_since_last_stream = 0

            stream_message_cnt += 1

            if stream_message_cnt == 3 or stream_message_cnt == 10:
                print(f'grid: publishing chain request')
                publish_chain_request()


        elif "schwab/queried" in topic:
            on_message_dbg = 10730
            process_queried(topic, payload_dict)
            on_message_dbg = 10732
            time_since_last_quereied = 0

        elif "schwab/chain" in topic:
            # print(f'\n29300 received schwab/chain')
            # print(f'received schwab/chain, payload_dict type:{type(payload_dict)}, data:\n{payload_dict}')
            on_message_dbg = 10740
            update_quotes_from_chain(payload_dict)
            on_message_dbg = 10742
            pass


        # elif "schwab/spx/grid/request" in topic:
        elif "schwab/spx/grid/xxxx" in topic:

            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M:%S.%f')[:-3]


            
            on_message_dbg = 10750
            # print(f'grid request topic type:{type(topic)}, topic:<{topic}>')
            # Parse the topic to extract the last level
            request_id = topic.split('/')[-1]

            on_message_dbg = 10751

            # print(f'10751 got grid request topic at {time_str} request id:{request_id}')

            # Print the result to confirm
            # print(f'request_id:<{request_id}>')

            quote_df_lcnt_4 = 1

            # FIX ME TODO
            # with quote_df_lock: 
            #     quote_df_sorted = quote_df.sort_values(by='symbol')
            quote_df_sorted = quote_df

            quote_df_lcnt_4 = 0

            on_message_dbg = 10752

    
            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df_sorted[['bid', 'ask']].isna().any(axis=1).sum()

            if time_since_last_quereied < 90 and time_since_last_stream < 20:

                current_time = datetime.now()
                send_grid_response_ts_str = current_time.strftime('%H:%M:%S.%f')[:-3]
                
                publish_grid(quote_df_sorted, rows_with_nan_bid_ask, request_id)

                print(f'grid request 1 {gbl_got_grid_request_ts_str1}, request 2:{gbl_got_grid_request_ts_str2}, response at {send_grid_response_ts_str}')

            else:
                current_time = datetime.now()
                time_str = current_time.strftime('%H:%M:%S')
                print(f'grid refused to publish at {time_str} because too much time since last data')
                print(f'time_since_last_quereied:{time_since_last_quereied},  time_since_last_stream:{time_since_last_stream}')
                publish_refusal(quote_df_sorted, rows_with_nan_bid_ask, request_id)



            pass



        # Process the message (print in this example)
        
        # print(f'Processing message.  topic:<{topic}>, payload:<{payload}>')
        # print(f'topic type:{type(topic)}, topic payload:{type(payload)}')

        if "stock/SPX/last" in topic:
            on_message_dbg = 10760
            

            spx_last_fl = float(payload)
            # print(f'new SPX value:{spx_last_fl}')

            on_message_dbg = 10762

        if SPX_OPT_BID_ASK_LAST_CHECK in topic:
            on_message_dbg = 10764
            # print(f'recieved SPX option bid/ask/last topic')
            add_to_quote_tbl(topic, payload)
            on_message_dbg = 10766



        time.sleep(0.1)


mqtt_pub_lock = threading.Lock()        


def publish_chain_request():

    my_topic = CHAIN_REQUEST_TOPIC
    my_payload = " "

    with mqtt_pub_lock:
        mqtt_client.publish(my_topic, my_payload)
    pass


# Function to publish grid via MQTT
def publish_grid(quote_df_sorted, rows_with_nan_bid_ask, request_id):
    global mqtt_client


    my_topic = GRID_RESPONSE_TOPIC + request_id


    # now_time = datetime.now()
    # now_time_str = now_time.strftime('%H:%M:%S.%f')[:-3]
    # print(f'grid publishing grid response at {now_time_str}, topic:{my_topic}, request id:{request_id}')

    # print(f'in publish_grid, rows_with_nan_bid_ask:{rows_with_nan_bid_ask}')
    # print(f'grid manager publishing topic:<{my_topic}>')

    # Force publishing of an empty json list
    # rows_with_nan_bid_ask = 1

    if rows_with_nan_bid_ask > 2:
        print(f'publishing empty json data because bid ask quantity:{rows_with_nan_bid_ask}, topic:{my_topic}')
        json_data = json.dumps([])

    else:
        json_data = quote_df_sorted.to_json(orient='records')

    with mqtt_pub_lock:
        mqtt_client.publish(my_topic, json_data)

    # # Load JSON string into a Python dictionary for pretty printing
    # parsed_json = json.loads(json_data)
    # print(f'grid data:\n{json.dumps(parsed_json, indent=4)}')


# Function to publish grid via MQTT
def publish_refusal(quote_df_sorted, rows_with_nan_bid_ask, request_id):
    global mqtt_client

    my_topic = GRID_REFUSE_TOPIC + request_id

    now_time = datetime.now()
    now_time_str = now_time.strftime('%H:%M:%S.%f')[:-3]
    print(f'grid publishing grid refusal at {now_time_str}, topic:{my_topic}, request id:{request_id}')

    json_data = json.dumps([])

    with mqtt_pub_lock:
        mqtt_client.publish(my_topic, json_data)





def grid_handling():
    global quote_df
    global quote_df_lock
    global quote_df_lcnt_5

    global time_since_last_stream
    global time_since_last_quereied

    initialize_data()


    # # Ensure that the market is open
    # while True:
    #     time.sleep(1)
    #     if market_open_flag == True:
    #         break



    no_none_nan_flag = False    # indicates when all bid and ask have values

    display_quote_throttle = 0
    total_reported_rows = 0

    total_loop_count = 0
    time_since_last_stream = 0
    time_since_last_quereied = 0

    gh_loop = 0

    while True:
        time.sleep(1)

        gh_loop += 1

        my_market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
        
        if not my_market_open_flag:
            initialize_quote_df()
            initialize_data()
            time.sleep(9)

            if gh_loop % 5 == 4:
                now_time = datetime.now()
                now_time_str = now_time.strftime('%H:%M:%S.%f')[:-3]
                print(f'\ngrid gh market closed at {now_time_str} local')

            continue


        display_quote_throttle += 1

        total_loop_count += 1
        time_since_last_stream += 1
        time_since_last_quereied += 1

        if my_market_open_flag == True and (time_since_last_quereied > 5 or time_since_last_stream > 5):
            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M:%S')
            print(f'grid: total loop:{total_loop_count}, since stream:{time_since_last_stream}, since queried:{time_since_last_quereied}, omc:{total_on_message_cnt}, omd:{on_message_dbg} at {time_str}')

        else:
            pass

        if my_market_open_flag == False:
            print(f'grid gh market is now closed, continuing')
            return

        



        # print(f'display_quote_throttle:{display_quote_throttle}')
        # print('grid_handling loop')

        

        pd.set_option('display.max_rows', None)
        quote_df_lcnt_5 = 1
        with quote_df_lock: 


            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()

            # Total number of rows
            total_rows = len(quote_df)

            # Sort the DataFrame by the 'symbol' column
            quote_df_sorted = quote_df.sort_values(by='symbol')

        quote_df_lcnt_5 = 0



        # Select the columns you want to print
        columns_to_print = ['symbol', 'bid', 'ask', 'last']

        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S')

        # periodically Print the sorted DataFrame with the selected columns
        if display_quote_throttle % 10 == 8:

            # print(f'dbg grid handling loop')

            if rows_with_nan_bid_ask > 0:
                print(f'grid: # rows:{total_rows}, # Nan/None in bid/ask:{rows_with_nan_bid_ask}, # messages:{gbl_total_message_count} at {time_str} ')
                no_none_nan_flag = False

            else:
                num_rows = len(quote_df)
                if num_rows > 20:
                    if no_none_nan_flag == False:
                        print(f'grid: ++++++ All bid and ask now have quoted values at {time_str}, rows:{num_rows} ++++++')
                        no_none_nan_flag = True

            if total_reported_rows < total_rows:
                print(f'grid: A new high for total rows:{total_rows} at {time_str}, Number of Nan/None in bid/ask:{rows_with_nan_bid_ask}')
                total_reported_rows = total_rows



            # print(f'\n{time_str}  SPX:{spx_last_fl}, grid:\n{quote_df_sorted[columns_to_print]}')


            # print(f'Total number of rows: {total_rows}, message count: {gbl_total_message_count}')
            # print(f'Number of rows with NaN or None in bid or ask: {rows_with_nan_bid_ask}')
            # print(f'# rows:{total_rows}, # Nan/None in bid/ask:{rows_with_nan_bid_ask}, # messages:{gbl_total_message_count}')
            # publish_grid(quote_df_sorted, rows_with_nan_bid_ask, "dummy_req_id")
            pass


        # periodically save the sorted dataframe to a .json file
        if display_quote_throttle % 60 == 5:
            # quote_json = quote_df_sorted.to_dict(orient="records")
            # pretty_json = json.dumps(quote_json, indent=4)
            # # print(f'pretty_json:\n{pretty_json}')
            # # current_datetime = datetime.now().strftime("%y%mdd%H%M%S")
            # current_datetime = datetime.now().strftime("%y%m%d%H%M%S")
            # print(f'current_datetime:{current_datetime}')
            # directory = r"C:\MEIC\chain_data"
            # filename = f"quote_{current_datetime}.json"
            # file_path = os.path.join(directory, filename)
            
            # # Ensure the directory exists
            # os.makedirs(directory, exist_ok=True)
            
            # # Save the JSON data to the file with indentation for readability
            # with open(file_path, "w") as file:
            #     json.dump(quote_json, file, indent=4)  # Indent for human-readable formatting

            # print(f'saved chain data to {file_path}')

            pass
                

            

        # periodically call the spread recommender
        quote_interval_modulo = 60
        quote_interval_remainder = quote_interval_modulo - 13
        quote_interval_warning = quote_interval_remainder - 2
        
        if display_quote_throttle % quote_interval_modulo == quote_interval_warning:
            # print(f'\n\n*************** quote in 2 seconds ******************\n\n')
            pass

        if display_quote_throttle % quote_interval_modulo == quote_interval_remainder:

            pass


            # # Total number of rows
            # total_rows = len(quote_df)

            # # Count the number of rows with NaN or None values in 'bid' or 'ask'
            # rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()

            # ROW_NEEDED = 50
            # MAX_NAN = 2

            # if total_rows >= ROW_NEEDED and rows_with_nan_bid_ask <= MAX_NAN:
            #     quote_json = quote_df_sorted.to_dict(orient="records")

            
            # else:
            #     pass
            #     # print(f'waiting for enough rows ({ROW_NEEDED}), current:{total_rows}')
            #     # print(f'and/or fewer than {MAX_NAN} None/Nan in bid/ask, current {rows_with_nan_bid_ask}')


def is_market_open():
    global market_open_flag

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
        return False
    
    # print(f'Market IS open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
    

    # market_open_flag = True

    return True


def wait_for_market_to_open():
    global market_open_flag
    market_open_flag = False

    throttle_wait_display = 0
    # print(f'grid: checking market hours')

    while True:

        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
        # market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=-599, close_offset=-844)

        if market_open_flag:
            break

        throttle_wait_display += 1
        # print(f'throttle_wait_display: {throttle_wait_display}')
        if throttle_wait_display % 6 == 1:

            initialize_quote_df()
            initialize_data()


            current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
            current_eastern_day = current_eastern_time.strftime('%A')



            # eastern = pytz.timezone('US/Eastern')
            # current_time = datetime.now(eastern)
            # eastern_time_str = current_time.strftime('%H:%M:%S')

            print(f'\ngrid: waiting for market to open, current East time: {current_eastern_day} {current_eastern_hhmmss}')

            pass


        time.sleep(10)


def initialize_data():
    global gbl_total_message_count
    global time_since_last_quereied
    global time_since_last_stream
    global market_open_flag
    global spx_last_fl
    global spx_bid_fl
    global spx_ask_fl
    global topic_rx_cnt
    global throttle_rx_cnt_display
    global total_on_message_cnt, on_message_dbg


    gbl_total_message_count = 0
    time_since_last_quereied = 0
    time_since_last_stream = 0
    market_open_flag = False
    spx_last_fl = None
    spx_bid_fl = None
    spx_ask_fl = None
    topic_rx_cnt = 0
    throttle_rx_cnt_display = 0
    total_on_message_cnt = 0
    on_message_dbg = 0
    


def initialize_quote_df():
    global quote_df, quote_df_lock
    global quote_df_lcnt_6

    quote_df_lcnt_6 = 1
    with quote_df_lock: 

        # Create/empty/initialize quotes_df
        quote_df = pd.DataFrame(columns=['symbol', 'bid', 'bid_time', 'ask', 'ask_time', 'last', 'last_time'])
    
    quote_df_lcnt_6 = 0


def display_quote_df():
    global quote_df, quote_df_lock
    global quote_df_lcnt_7

    quote_df_lcnt_7 = 1
    with quote_df_lock: 
        print(f'displaying quote_df:\n{quote_df}')

    quote_df_lcnt_7 = 0


def grid_loop():
    global mqtt_client
    global market_open_flag
    global quote_df
    # global schwab_client

    initialize_quote_df()
    
    
    while True:

        print(f'grid: start of grid loop while loop to wait for market open')


        wait_for_market_to_open()
        # Note: globals are initialized in wait_for_market_to_open() while market is not open

        print(f'grid: market is open')


        # # Initialize MQTT client
        # # mqtt_client = mqtt.Client()
        # mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

        # # Assign callback functions
        # mqtt_client.on_connect = on_connect
        # mqtt_client.on_message = on_message

        # # Connect to the MQTT broker
        # print("Connecting to MQTT broker...")
        # mqtt_client.connect(BROKER_ADDRESS)

        app_key, secret_key, my_tokens_file = load_env_variables()

        # # create schwabdev client
        # schwab_client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)


        # Start the keyboard thread
        # keboard_thread = threading.Thread(target=keyboard_handler_task, name="keyboard_handler_task")
        # keboard_thread.daemon = True  # Daemonize thread to exit with the main program
        # keboard_thread.start()





        # # Start the message processing thread
        # print(f'starting process message thread')
        # processing_message_thread = threading.Thread(target=process_message, name="process_message")
        # processing_message_thread.daemon = True  # Daemonize thread to exit with the main program
        # processing_message_thread.start()

        

        # Start the grid_handling thread
        # grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")
        # print(f'starting grid handling thread')
        # grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")
        # grid_handling_thread.daemon = True  # Daemonize thread to exit with the main program
        # grid_handling_thread.start()

        # Start the MQTT client loop (handles reconnects and message callbacks)
        # mqtt_client.loop_forever()

        # loop while market is open
        mkt_loop_cnt = 0
        while True:
            # mqtt_client.loop(timeout=10.0)  # process network traffic, with a 1-second timeout
            time.sleep(10) 

            mkt_loop_cnt += 1

            # if mkt_loop_cnt % 3 == 2:
            #     display_quote_df()

            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
            # market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=-599, close_offset=-844)

            if market_open_flag == False: 
                break

        
        
        print(f'grid loop: market is now closed, shutting down')



        # print(f'grid: waiting for processing_thread to finish')
        # processing_message_thread.join()
        # print(f'grid:processing_thread has finished')

        # print(f'grid: waiting for grid_handling_thread to finish')
        # grid_handling_thread.join()
        # print(f'grid:grid_handling_thread has finished')

        # print(f'grid: worker thread(s) have (all) joined')



def mqtt_services():
    global mqtt_client

    # print(f"[DEBUG] 1 mqtt_client type: {type(mqtt_client)}")


    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    # print(f"[DEBUG] 2 mqtt_client type: {type(mqtt_client)}")

    # Assign callback functions
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to the MQTT broker
    print("Connecting to MQTT broker...")
    mqtt_client.connect(BROKER_ADDRESS)

    time.sleep(2)

    mqtt_client.loop_forever()



# Main function to set up MQTT client and start the processing thread
def main():
    global mqtt_client
    global market_open_flag
    global quote_df
    global time_since_last_quereied 
    global time_since_last_stream

    initialize_quote_df()
    initialize_data()

    sup_thread = threading.Thread(target=grid_supervisor, name="grid_supervisor")
    sup_thread.daemon = True  # Daemonize thread to exit with the main program
    sup_thread.start()

    # Start MQTT service thread, which will always run 
    mqtt_thread = threading.Thread(target=mqtt_services, daemon=True)
    mqtt_thread.start()

    # Start the message processing thread
    print(f'starting process message thread')
    processing_message_thread = threading.Thread(target=process_message, name="process_message")
    processing_message_thread.daemon = True  # Daemonize thread to exit with the main program
    processing_message_thread.start()

    print(f'starting grid handling thread')
    grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")
    grid_handling_thread.daemon = True  # Daemonize thread to exit with the main program
    grid_handling_thread.start()





    while True:

        time.sleep(1)
        continue

        # initialize_quote_df()
        # initialize_data()


        # wait_for_market_to_open()
        # print(f'grid: market is open')

        # time_since_last_quereied = 0
        # time_since_last_stream = 0

        # grid_loop()
        # # grid_loop does not return
        # print(f'grid returned from grid loop')


        # # Initialize MQTT client
        # mqtt_client = mqtt.Client()
        # # mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        # # Assign callback functions
        # mqtt_client.on_connect = on_connect
        # mqtt_client.on_message = on_message

        # # Connect to the MQTT broker
        # print("Connecting to MQTT broker...")
        # mqtt_client.connect(BROKER_ADDRESS)

        # app_key, secret_key, my_tokens_file = load_env_variables()

        # # create schwabdev client
        # schwab_client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)


        # # Start the keyboard thread
        # # keboard_thread = threading.Thread(target=keyboard_handler_task, name="keyboard_handler_task")
        # # keboard_thread.daemon = True  # Daemonize thread to exit with the main program
        # # keboard_thread.start()



        # # Start the message processing thread
        # processing_thread = threading.Thread(target=process_message, name="process_message")
        # processing_thread.daemon = True  # Daemonize thread to exit with the main program
        # processing_thread.start()

        # # Start the grid_handling thread
        # # grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")
        # grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling", args=(schwab_client,))

        # grid_handling_thread.daemon = True  # Daemonize thread to exit with the main program
        # grid_handling_thread.start()

        # # Start the MQTT client loop (handles reconnects and message callbacks)
        # # mqtt_client.loop_forever()

        # # loop while market is open
        # while True:
        #     mqtt_client.loop(timeout=10.0)  # process network traffic, with a 1-second timeout
        #     # time.sleep(10) 
        #     market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
        #     # market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset= -228)
            
        #     if market_open_flag == False:
        #         break


        # mqtt_client.disconnect()
        # mqtt_client.loop_stop()



        # print(f'grid: waiting for processing_thread to finish')
        # processing_thread.join()
        # print(f'grid:processing_thread has finished')

        # print(f'grid: waiting for grid_handling_thread to finish')
        # grid_handling_thread.join()
        # print(f'grid:grid_handling_thread has finished')

        # print(f'grid: restarting main loop')

        


# Entry point of the program
if __name__ == "__main__":
    # print(f'grid: calling main')
    print(f'grid: startup')
    main()
    print(f'grid: returned from main, exiting')
