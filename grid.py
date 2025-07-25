import threading
import queue
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


MARKET_OPEN_OFFSET = 2
MARKET_CLOSE_OFFSET = 0

DEBUG_CHAIN_SYM = "P06000"

quote_df_lock = threading.Lock()
global quote_df

global mqtt_client
mqtt_client = None


global gbl_total_message_count
gbl_total_message_count = 0

global time_since_last_quereied
time_since_last_quereied = 0

global time_since_last_stream
time_since_last_stream = 0


market_open_flag = False



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
CHAIN_REQUEST_TOPIC = "schwab/spx/chain/request"

# Callback function when the client connects to the broker
def on_connect(client, userdata, flags, rc):

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

        client.subscribe(GRID_REQUEST_TOPIC)
        print(f"Subscribed to topic: {GRID_REQUEST_TOPIC}")



    else:
        print(f"Failed to connect with error code: {rc}")


topic_rx_cnt = 0
throttle_rx_cnt_display = 0

# Callback function when a message is received
def on_message(client, userdata, msg):
    

    topic = msg.topic
    payload = msg.payload.decode()


    # print(f'Received topic type:{type(topic)}, topic:<{topic}>')
    # print(f'payload type:{type(payload)},\ndata:<{payload}>')

    # try:
    #     payload_dict = json.loads(payload)
    #     print(f"01 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")
    # except Exception as e:
    #     print(f"grid manager on_message() An error occurred: {e} while trying to convert payload to json")


    # Put the topic and payload into the queue as a tuple
    # print(f'calling .put')
    message_queue.put((topic, payload))
    # print(f'returned from .put')





    # global topic_rx_cnt
    # global throttle_rx_cnt_display

    # topic_rx_cnt += 1
    # throttle_rx_cnt_display += 1
    # if throttle_rx_cnt_display % 5 == 4:
    #     print(f'topic_rx_cnt:{topic_rx_cnt}')



# Takes spx chain quote and updates quote_df entries 
def update_quotes_from_chain(payload_dict):
    global quote_df  # Assumes quote_df is declared globally elsewhere
    if "data" not in payload_dict:
        return

    now_str = lambda: datetime.now().strftime('%H:%M:%S:%f')[:-3]  # hh:mm:ss:<ms> format

    with quote_df_lock:
        for item in payload_dict["data"]:
            symbol = item.get("symbol")
            if symbol is None:
                continue

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

            if DEBUG_CHAIN_SYM in symbol:
                print(f'from  chain, updating {symbol}, new bid:{item.get("bid")}, new ask:{item.get("ask")}')



# Function to update the quote DataFrame
def add_to_quote_tbl(topic, payload):
    global quote_df

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



# Function to update the quote DataFrame
def add_to_quote_tbl2(sym,bid,ask,last):
    global quote_df

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

                                    if DEBUG_CHAIN_SYM in opt_sym:
                                        print(f'from stream, updating {opt_sym}, new bid:{opt_bid}, new ask:{opt_ask}')



                                    # print(f'opt_bid type:{type(opt_bid)}, value:{opt_bid}')
                                    # print(f'opt_ask type:{type(opt_ask)}, value:{opt_ask}')

                                    # if opt_ask == 0.05:
                                    #     print(f'opt_ask is 0.05, opt_bid:{opt_bid}')


                                    # print(f'streamed adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
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


            if DEBUG_CHAIN_SYM in opt_sym:
                print(f'from  query, updating {opt_sym}, new bid:{opt_bid}, new ask:{opt_ask}')



            # print(f'queried adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
            add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)




# Thread function to process messages from the queue
def process_message():
    global spx_last_fl
    global gbl_total_message_count
    global market_open_flag
    global time_since_last_stream
    global time_since_last_quereied

    # ensure that the market is open
    while True:
        time.sleep(1)
        if market_open_flag == True:
            break




    # # Get the (topic, message) tuple from the queue
    # topic, payload = message_queue.get()

    # throttle_market_close_check = 0


    stream_message_cnt = 0

    while True:
        
        # throttle_market_close_check += 1
        # if throttle_market_close_check % 20 == 18:
        #     print(f'checking for market open')
        #     market_open_flag, current_eastern_time, seconds_to_next_minute, seconds_to_next_minute  = market_open.is_market_open2(
        #         open_offset=MARKET_OPEN_OFFSET, 
        #         close_offset=MARKET_CLOSE_OFFSET)
            
        #     print(f'market_open_flag:{market_open_flag}')
            

        
        if market_open_flag == False:
            print(f'grid process_message(): market is now closed')
            return


        # print(f'calling .get')
        
        # Get the (topic, message) tuple from the queue, but time out if no message is received
        try:
            topic, payload = message_queue.get(timeout=5)  # Wait for up to 5 seconds
        except queue.Empty:
            # print(".get: timeout.")
            continue

        gbl_total_message_count += 1
        # print(f'returned from .get, gbl_total_message_count:{gbl_total_message_count}')



        # payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

        # payload may be empty or may not be json data
        try:
            # Attempt to parse the JSON data
            payload_dict = json.loads(payload)

        except json.JSONDecodeError:
            # print("Payload is not valid JSON")
            payload_dict = {}

        except Exception as e:
            print(f"8394 An error occurred: {e} while trying load json data")
            payload_dict = {}
  


        if "schwab/stream" in topic:
            process_stream(topic, payload_dict)
            time_since_last_stream = 0

            stream_message_cnt += 1

            if stream_message_cnt == 3 or stream_message_cnt == 10:
                print(f'grid: publishing chain request')
                publish_chain_request()


        elif "schwab/queried" in topic:
            process_queried(topic, payload_dict)
            time_since_last_quereied = 0

        elif "schwab/chain" in topic:
            print(f'\n29300 received schwab/chain')
            # print(f'received schwab/chain, payload_dict type:{type(payload_dict)}, data:\n{payload_dict}')
            update_quotes_from_chain(payload_dict)
            pass


        elif "schwab/spx/grid/request" in topic:
            # print(f'grid request topic type:{type(topic)}, topic:<{topic}>')
            # Parse the topic to extract the last level
            request_id = topic.split('/')[-1]

            # Print the result to confirm
            # print(f'request_id:<{request_id}>')


            with quote_df_lock: 
                quote_df_sorted = quote_df.sort_values(by='symbol')

    
            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df_sorted[['bid', 'ask']].isna().any(axis=1).sum()

            if time_since_last_quereied < 90 and time_since_last_stream < 20:
                publish_grid(quote_df_sorted, rows_with_nan_bid_ask, request_id)

            else:
                current_time = datetime.now()
                time_str = current_time.strftime('%H:%M:%S')
                print(f'grid refused to publish at {time_str} because too much time since last data')
                print(f'time_since_last_quereied:{time_since_last_quereied},  time_since_last_stream:{time_since_last_stream}')


            pass



        # Process the message (print in this example)
        
        # print(f'Processing message.  topic:<{topic}>, payload:<{payload}>')
        # print(f'topic type:{type(topic)}, topic payload:{type(payload)}')

        if "stock/SPX/last" in topic:
            spx_last_fl = float(payload)
            # print(f'new SPX value:{spx_last_fl}')

        if SPX_OPT_BID_ASK_LAST_CHECK in topic:
            # print(f'recieved SPX option bid/ask/last topic')
            add_to_quote_tbl(topic, payload)



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

    # print(f'in publish_grid, rows_with_nan_bid_ask:{rows_with_nan_bid_ask}')
    # print(f'grid manager publishing topic:<{my_topic}>')

    # Force publishing of an empty json list
    # rows_with_nan_bid_ask = 1

    if rows_with_nan_bid_ask > 0:
        json_data = json.dumps([])

    else:
        json_data = quote_df_sorted.to_json(orient='records')

    with mqtt_pub_lock:
        mqtt_client.publish(my_topic, json_data)

    # # Load JSON string into a Python dictionary for pretty printing
    # parsed_json = json.loads(json_data)
    # print(f'grid data:\n{json.dumps(parsed_json, indent=4)}')









def grid_handling():
    global quote_df
    global time_since_last_stream
    global time_since_last_quereied


    # Ensure that the market is open
    while True:
        time.sleep(1)
        if market_open_flag == True:
            break



    no_none_nan_flag = False    # indicates when all bid and ask have values

    display_quote_throttle = 0
    total_reported_rows = 0

    total_loop_count = 0
    time_since_last_stream = 0
    time_since_last_quereied = 0

    while True:
        time.sleep(1)
        display_quote_throttle += 1

        total_loop_count += 1
        time_since_last_stream += 1
        time_since_last_quereied += 1

        if market_open_flag == True and (time_since_last_quereied > 5 or time_since_last_stream > 5):
            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M:%S')
            print(f'grid: total loop:{total_loop_count}, since stream:{time_since_last_stream}, since queried:{time_since_last_quereied} at {time_str}')

        else:
            pass

        if market_open_flag == False:
            print(f'grid_handling: market is now closed, exiting thread')
            return

        



        # print(f'display_quote_throttle:{display_quote_throttle}')
        # print('grid_handling loop')

        

        pd.set_option('display.max_rows', None)
        with quote_df_lock: 


            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()

            # Total number of rows
            total_rows = len(quote_df)

            # Sort the DataFrame by the 'symbol' column
            quote_df_sorted = quote_df.sort_values(by='symbol')



        # Select the columns you want to print
        columns_to_print = ['symbol', 'bid', 'ask', 'last']

        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S')

        # periodically Print the sorted DataFrame with the selected columns
        if display_quote_throttle % 10 == 8:

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


            current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
            current_eastern_day = current_eastern_time.strftime('%A')



            # eastern = pytz.timezone('US/Eastern')
            # current_time = datetime.now(eastern)
            # eastern_time_str = current_time.strftime('%H:%M:%S')

            print(f'grid: waiting for market to open, current East time: {current_eastern_day} {current_eastern_hhmmss}')

            pass


        time.sleep(10)


def initialize_data():
    global mqtt_client
    global gbl_total_message_count
    global time_since_last_quereied
    global time_since_last_stream
    global market_open_flag
    global spx_last_fl
    global spx_bid_fl
    global spx_ask_fl
    global topic_rx_cnt
    global throttle_rx_cnt_display


    mqtt_client = None
    gbl_total_message_count = 0
    time_since_last_quereied = 0
    time_since_last_stream = 0
    market_open_flag = False
    spx_last_fl = None
    spx_bid_fl = None
    spx_ask_fl = None
    topic_rx_cnt = 0
    throttle_rx_cnt_display = 0
    


def initialize_quote_df():
    global quote_df, quote_df_lock

    with quote_df_lock: 

        # Create/empty/initialize quotes_df
        quote_df = pd.DataFrame(columns=['symbol', 'bid', 'bid_time', 'ask', 'ask_time', 'last', 'last_time'])


def display_quote_df():
    global quote_df, quote_df_lock

    with quote_df_lock: 
        print(f'displaying quote_df:\n{quote_df}')


def grid_loop():
    global mqtt_client
    global market_open_flag
    global quote_df
    # global schwab_client

    initialize_quote_df()
    
    
    while True:


        wait_for_market_to_open()

        print(f'grid: market is open')


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

        # # create schwabdev client
        # schwab_client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)


        # Start the keyboard thread
        # keboard_thread = threading.Thread(target=keyboard_handler_task, name="keyboard_handler_task")
        # keboard_thread.daemon = True  # Daemonize thread to exit with the main program
        # keboard_thread.start()



        # Start the message processing thread
        processing_thread = threading.Thread(target=process_message, name="process_message")
        processing_thread.daemon = True  # Daemonize thread to exit with the main program
        processing_thread.start()

        # Start the grid_handling thread
        # grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")
        grid_handling_thread = threading.Thread(target=grid_handling, name="grid_handling")

        grid_handling_thread.daemon = True  # Daemonize thread to exit with the main program
        grid_handling_thread.start()

        # Start the MQTT client loop (handles reconnects and message callbacks)
        # mqtt_client.loop_forever()

        # loop while market is open
        mkt_loop_cnt = 0
        while True:
            mqtt_client.loop(timeout=10.0)  # process network traffic, with a 1-second timeout
            # time.sleep(10) 

            mkt_loop_cnt += 1
            # if mkt_loop_cnt % 3 == 2:
            #     display_quote_df()

            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
            # market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=-599, close_offset=-844)

            if market_open_flag == False:
                break

        print(f'grid_loop: market is now closed, shutting down')


        mqtt_client.disconnect()
        mqtt_client.loop_stop()



        print(f'grid: waiting for processing_thread to finish')
        processing_thread.join()
        print(f'grid:processing_thread has finished')

        print(f'grid: waiting for grid_handling_thread to finish')
        grid_handling_thread.join()
        print(f'grid:grid_handling_thread has finished')

        print(f'grid: restarting main loop')






# Main function to set up MQTT client and start the processing thread
def main():
    global mqtt_client
    global market_open_flag
    global quote_df
    global time_since_last_quereied 
    global time_since_last_stream


    while True:

        initialize_quote_df()
        initialize_data()


        wait_for_market_to_open()

        print(f'grid: market is open')

        time_since_last_quereied = 0
        time_since_last_stream = 0

        grid_loop()


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
