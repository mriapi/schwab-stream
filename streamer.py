


import asyncio
import paho.mqtt.client as mqtt
import threading
import time
import json
import logging
import websockets
import websockets.exceptions
from datetime import datetime, timezone
import requests
import aiohttp
import os
import market_open


MARKET_OPEN_OFFSET = 1


global websocket
websocket = None  # Global WebSocket connection

call_strike_list = []  # Global list for call options
put_strike_list = []   # Global list for put options

strike_list_lock = threading.Lock()

global mqtt_client_tx
mqtt_client_tx = None


# FIX ME
mqtt_publish_lock = threading.Lock()

global mqtt_intialized
mqtt_intialized = False

rx_refreshToken = None
rx_accessToken = None
rx_acctHash = None
rx_streamerUrl = None
rx_customerId = None
rx_correlId = None
rx_channel = None
rx_functionId = None


streamer_socket_url = None
sch_client_customer_id = None
sch_client_correl_id = None
sch_client_channel = None
sch_client_function_id = None

quit_flag = False
socket_active = False

global get_quote_fail_count
get_quote_fail_count = 0



get_current_day_history_lock = threading.Lock()
chain_data_lock = threading.Lock()

rx_msg_func_call_count = 0


REQUESTS_GET_TIMEOUT = 10



# Suppress WebSocket internal errors
logging.getLogger("websockets").setLevel(logging.CRITICAL)


logging.basicConfig(
    filename="mri_log2.log",  # Log file name
    level=logging.INFO,  # Set logging level
    format="%(asctime)s - %(levelname)s - %(message)s"
)





# ----------------------------- MQTT -------------------------------

# MQTT Broker Details
BROKER_ADDRESS = "localhost"
PORT_NUMBER = 1883
CREDS_INFO_TOPIC = "mri/creds/info"


async def reset_rx():

    global rx_refreshToken
    global rx_accessToken
    global rx_acctHash
    global rx_streamerUrl
    global rx_customerId
    global rx_correlId
    global rx_channel
    global rx_functionId


    rx_refreshToken = None
    rx_accessToken = None
    rx_acctHash = None
    rx_streamerUrl = None
    rx_customerId = None
    rx_correlId = None
    rx_channel = None
    rx_functionId = None







def on_message(client, userdata, msg):
    """ Callback function triggered when a message is received """

    global rx_refreshToken
    global rx_accessToken
    global rx_acctHash
    global rx_streamerUrl
    global rx_customerId
    global rx_correlId
    global rx_channel
    global rx_functionId

    topic = msg.topic
    payload = msg.payload.decode()


    # print(f"Received message on topic '{topic}': {payload}")

    
    if CREDS_INFO_TOPIC in topic:
        # Convert string to JSON
        payload_json = json.loads(payload)

        prev_access_tokan = rx_accessToken

        # Extract values with prefixed variable names
        tok_cnt = 0
        for key, value in payload_json.items():
            tok_cnt += 1
            globals()[f"rx_{key}"] = value

        # print(f'\n5109 received rx_ tokens, tok_cnt::{tok_cnt}')

        # print(f'\n\n5100 rx_refreshToken:{rx_refreshToken}')
        # print(f'5101 rx_accessToken:{rx_accessToken}')
        # print(f'5102 rx_acctHash:{rx_acctHash}')
        # print(f'5103 rx_streamerUrl:{rx_streamerUrl}')
        # print(f'5104 rx_customerId:{rx_customerId}')
        # print(f'5105 rx_correlId:{rx_correlId}')
        # print(f'5106 rx_channel:{rx_channel}')
        # print(f'5107 rx_functionId:{rx_functionId}\n')

        if prev_access_tokan != rx_accessToken:
            current_time = datetime.now()
            current_time_str = current_time.strftime('%H:%M:%S')
            print(f'5109 new access token at {current_time_str} Local.  prev:{prev_access_tokan}, new:{rx_accessToken}')





def on_connect(client, userdata, flags, rc):
    """ Callback function triggered when the client connects to the broker """
    print(f"Connected to MQTT Broker with result code {rc}")
    client.subscribe(CREDS_INFO_TOPIC)
    print(f"Subscribed to topic: {CREDS_INFO_TOPIC}")


# FIX ME
async_mqtt_publish_lock = asyncio.Lock()


async def aio_publish_quote(topic, payload):
    global mqtt_client_tx




    try:

        if not mqtt_client_tx:
            print(f'could not aio publish quote, mqtt_client_tx is None')
            return
        
    except Exception as e:
        print(f'mqtt_client_tx error: {e}')
        return



    # print(f'apq 72002')

    async with async_mqtt_publish_lock:
        # print(f'apq 72004')
        loop = asyncio.get_running_loop()
        # print(f'apq 72006')

        try:
            # print(f'apq 83024')
            loop.call_soon_threadsafe(mqtt_client_tx.publish, topic, payload)  # Thread-safe publish
            # print(f'apq 83026')
        except Exception as e:
            # print(f'apq 83028')
            info_str = f'2010 Error in aio MQTT publish: {e}'
            print(info_str)
            logging.error(info_str)

    # print(f'apq 83040')





CREDS_REQUEST_TOPIC = "mri/creds/request"

async def aio_publish_request_creds():

    topic = CREDS_REQUEST_TOPIC
    payload = " "
    # print(f'pds 58002')
    await aio_publish_quote(topic, payload)
    # print(f'pds 58004')

     


def mqtt_services():
    """ Runs the MQTT client in a separate thread """
    global quit_flag
    global mqtt_client_tx
    global mqtt_intialized

    client = mqtt.Client()
    # client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client_tx = client
    mqtt_intialized = True

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_ADDRESS, PORT_NUMBER, keepalive=60)

    client.loop_start()

    while not quit_flag:
        time.sleep(1)

    print("MQTT service terminating...")
    client.loop_stop()




def mqtt_setup():
    """ Creates and starts MQTT thread """
    global quit_flag
    mqtt_thread = threading.Thread(target=mqtt_services, daemon=True)
    mqtt_thread.start()

    while not quit_flag:
        time.sleep(1)

    mqtt_thread.join()
    print("MQTT setup thread exiting.")


# ---------------------- Poller ----------------------------------

global todays_epoch_time
todays_epoch_time = None

global spx_open
spx_open = None
global spx_high
spx_high = None
global spx_low
spx_low = None
global spx_close
spx_close = None
global ohlc_get_time
ohlc_get_time = None
global chain_strike_cnt
chain_strike_cnt = None


def get_today_in_epoch():
    global todays_epoch_time 

    try:

        # Calculate the time in milliseconds since the UNIX epoch
        now = datetime.now(timezone.utc)

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        todays_epoch_time  = int((now - epoch).total_seconds() * 1000.0)

    except Exception as e:
        info_str = f'1252 get today in epoch error:{e}'
        logging.error(info_str)
        print(info_str)





async def wait_for_rx_credentials(timeout=30):
    """ Wait for all rx_ credentials with a timeout to prevent infinite loop """
    global rx_streamerUrl, rx_accessToken, rx_refreshToken
    global rx_acctHash, rx_channel, rx_correlId
    global rx_customerId, rx_functionId

    await aio_publish_request_creds()

    start_time = asyncio.get_running_loop().time()
    
    while asyncio.get_running_loop().time() - start_time < timeout:
        if all([
            rx_streamerUrl, rx_accessToken, rx_refreshToken,
            rx_acctHash, rx_channel, rx_correlId,
            rx_customerId, rx_functionId
        ]):
            print("All rx_ credentials initialized")
            return

        print("rx_ credentials not initialized yet, retrying...")
        await asyncio.sleep(2)
        await aio_publish_request_creds()

    raise TimeoutError("Timeout waiting for rx_ credentials.")      






# Function to publish quotes via MQTT
def publish_quote(topic, payload):
    global mqtt_client_tx

    if mqtt_client_tx == None:
        print(f'cannont publish quote, mqtt_client_tx is None')

    # Use the lock to ensure thread safety
    with mqtt_publish_lock:
        mqtt_client_tx.publish(topic, payload)




def publish_raw_queried_quote(data):
    global time_since_last_queried_pub

    # pretty_json = json.dumps(data, indent=2)
    # print(f'in publish_raw_streamed_quote, pretty_json type:{type(pretty_json)}, data:\n{pretty_json}')

    json_str = json.dumps(data)
    # print(f'in publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')


    topic = "schwab/queried"
    publish_quote(topic, json_str)
    time_since_last_queried_pub = 0
    pass




def get_opt_quote(sym):
    global get_quote_success_count
    global get_quote_fail_count

    success_flag = False

    if rx_accessToken == None:
        print(f'unable to get opt quote, rx_accessToken is None')
        return success_flag

    quotes_response_json = None
    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')

    # current_tokens_mod_date = get_modification_date(tokens_file_path)
    # if current_tokens_mod_date != tokens_file_mod_date:
    #     print(f'641-1 tokens file has been modified at {current_time_str}')
    #     print(f'641-2 old access token:{rx_accessToken}')
    #     get_file_tokens()
    #     print(f'641-3 new access token:{accessToken}')

    opt_list_str = sym
        

    # with stike_list_lock:
    #     opt_list_str = ", ".join(syms)


    # Define the API endpoint
    url = "https://api.schwabapi.com/marketdata/v1/quotes"

    # Define query parameters
    params = {
        "symbols": opt_list_str,
        "fields": "quote",
        "indicative": "false"
    }

    # Define headers
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {rx_accessToken}"
    }
    
    # with get_opt_quote_lock:

    # print(f'get_opt_quote\n  url:{url}\n  params:{params}\n  headers:{headers}')

    try:

        # Make the GET request
        response = requests.get(url, params=params, headers=headers, timeout=REQUESTS_GET_TIMEOUT)

    except Exception as e:
        get_quote_fail_count += 1

        error_message = str(e)

        if "Failed to resolve 'api.schwabapi.com'" in error_message:
            info_str = f"3100 Detected network error. Failed to resolve 'api.schwabapi.com'. returning"
            print(info_str)
            logging.error(info_str)

        else:
            info_str = f'3101 exception requesting quotes :{e} at {current_time_str}, returning'
            logging.error(info_str)
            print(info_str)

        return success_flag
    
    try:

        if response.status_code != 200:
            info_str = f'1460 get_opt_quotes request failed:{response.status_code} at {current_time_str}, returning'
            print(info_str)
            logging.error(info_str)
            return success_flag
        

        
    except Exception as e:
        get_quote_fail_count += 1
        info_str = f'1470 exception requesting quotes :{e} at {current_time_str}, returning'
        print(info_str)
        logging.error(info_str)
        return success_flag

    try:

        quote_response_json = response.json()
        # print(f'single quote quote_response_json json type:{type(quote_response_json)}:\n{quote_response_json}') 
        # pretty_json = json.dumps(quote_response_json, indent=2)
        # print(f'single quote requests.get pretty_json type:{type(pretty_json)}:\n{pretty_json}') 

    except Exception as e:
        get_quote_fail_count += 1
        info_str = f'734 exception requesting quotes :{e} at {current_time_str}, returning' 
        print(info_str)
        logging.error(info_str)
        return success_flag 
    

    symbol_key = list(quote_response_json.keys())[0]  # Get the first key dynamically
    has_quote_key = "quote" in quote_response_json.get(symbol_key, {})

    # print(f'Key "quote" exists in quote_response_json: {has_quote_key}')

    if has_quote_key == True:
        publish_raw_queried_quote(quote_response_json)
        success_flag = True

    else:
        print(f'Key "quote" does not exist in the quote data')

    return success_flag




def get_spx_current_today_ohlc():
    global get_current_day_history_lock
    global spx_open, spx_high, spx_low, spx_close
    global ohlc_get_time, chain_strike_cnt
    global todays_epoch_time, rx_accessToken

    spx_day_ohlc = None

    if todays_epoch_time is None or rx_accessToken is None:
        print(f'unable to get ohlc, todays_epoch_time: {todays_epoch_time}, rx_accessToken: {rx_accessToken}')
        return None

    try:
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        params = {
            "symbol": "$SPX",
            "periodType": "month",
            "period": 1,
            "frequencyType": "daily",
            "frequency": 1,
            "startDate": todays_epoch_time,
            "endDate": todays_epoch_time,
            "needExtendedHoursData": "false",
            "needPreviousClose": "false"
        }

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {rx_accessToken}"
        }

        response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

        if response.status_code == 200:
            spx_day_ohlc = response.json()

            if 'candles' in spx_day_ohlc and 'empty' in spx_day_ohlc and not spx_day_ohlc['empty']:
                first_candle = spx_day_ohlc['candles'][0]

                with get_current_day_history_lock:
                    spx_open = first_candle['open']
                    spx_high = first_candle['high']
                    spx_low = first_candle['low']
                    spx_close = first_candle['close']
                    ohlc_get_time = datetime.now()

                    day_high_distance = abs(spx_close - spx_high)
                    day_low_distance = abs(spx_close - spx_low)
                    max_distance = max(day_high_distance, day_low_distance)
                    chain_strike_cnt = int(max_distance / 5) + 50

                print(f'285 chain_strike_cnt: {chain_strike_cnt} at {ohlc_get_time.strftime("%H:%M:%S")}')
            else:
                print("SPX data response is empty or malformed.")
        else:
            print(f"Failed to fetch SPX data. Status code: {response.status_code}")
            return None

        return spx_day_ohlc

    except requests.exceptions.Timeout:
        info_str = f'3010 Request timed out while fetching SPX OHLC data'
        logging.error(info_str)
        print(info_str)
        raise

    except Exception as e:
        info_str = f'3020 pricehistory error: {e}, could not get SPX o/h/l/c'
        logging.error(info_str)
        print(info_str)
        raise






def get_spx_option_chain():
    global chain_strike_cnt, rx_accessToken
    global chain_data_lock  # Assume this lock exists

    with chain_data_lock:
        local_strike_cnt = chain_strike_cnt
        local_accessToken = rx_accessToken

    if local_strike_cnt is None or local_accessToken is None:
        print(f'could not get option chain, chain_strike_cnt:{local_strike_cnt}, rx_accessToken:{local_accessToken}')
        return None

    try:
        today = datetime.now()
        myFromDate = myToDate = today.strftime('%Y-%m-%d')

        url = "https://api.schwabapi.com/marketdata/v1/chains"
        params = {
            "symbol": "$SPX",
            "contractType": "ALL",
            "strikeCount": local_strike_cnt,
            "includeUnderlyingQuote": "true",
            "strategy": "SINGLE",
            "fromDate": myFromDate,
            "toDate": myToDate
        }

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {local_accessToken}"
        }

        response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)
        response.raise_for_status()

        try:
            spx_chain = response.json()
        except requests.exceptions.JSONDecodeError:
            raise ValueError("3030 Error decoding JSON response from Schwab API")

        if not isinstance(spx_chain, dict):
            raise ValueError("3040 SPX chain response is not a dictionary")

        return spx_chain

    except requests.exceptions.ConnectionError as conn_err:
        raise RuntimeError("3050 Network error: Unable to connect to Schwab API") from conn_err
    
    except requests.exceptions.Timeout as timeout_err:
        raise RuntimeError("3060 Request timeout: Schwab API took too long to respond") from timeout_err
    
    except requests.exceptions.HTTPError as http_err:
        print(f"3070 HTTP error: {http_err}, response: {response.text}")
        raise RuntimeError(f"3072 HTTP error occurred: {http_err}") from http_err
    
    except Exception as err:
        raise RuntimeError(f"3080 Unexpected error occurred: {err}") from err





def parse_spx_chain(spx_chain):
    """Parses the SPX option chain and extracts relevant strike symbols."""
    global call_strike_list, put_strike_list
    global strike_list_lock

    # print(f'in parse_spx_chain')

    if not isinstance(spx_chain, dict):
        print("Invalid spx_chain format")
        return

    call_map = spx_chain.get("callExpDateMap", {})
    put_map = spx_chain.get("putExpDateMap", {})

    for exp_date, strikes in call_map.items():
        for strike_price, options in strikes.items():
            for option in options:
                symbol = option.get("symbol", "")
                if symbol.startswith("SPXW  ") and "C0" in symbol:
                    with strike_list_lock:
                        if symbol not in call_strike_list:
                            call_strike_list.append(symbol)

    print(f'2072 call_strike_list size: {len(call_strike_list)}')

    for exp_date, strikes in put_map.items():
        for strike_price, options in strikes.items():
            for option in options:
                symbol = option.get("symbol", "")
                if symbol.startswith("SPXW  ") and "P0" in symbol:
                    with strike_list_lock:
                        if symbol not in put_strike_list:
                            put_strike_list.append(symbol)

    print(f'2074 put_strike_list size: {len(put_strike_list)}')





def polling_services():
    """ Periodically polls the schwab API for SPX and option quotes  """
    global quit_flag
    global put_strike_list, call_strike_list

    # print(f'streamer: polling services bc 100')
    print(f'Starting pollins services ....')


    while not quit_flag: # polling services outer (session) loop

        # print(f'streamer: polling services bc 200')


        polling_loop_cnt = 0

        next_put_list_ix = 0
        next_call_list_ix = 0


        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)

        if not market_open_flag:
            put_strike_list = []
            call_strike_list = []

        print(f'initial streamer polling services market open check, market_open_flag:{market_open_flag}')
        ps_wait_market_open_cnt = 0
       
        while not market_open_flag:
            ps_wait_market_open_cnt += 1
          
            # print(f'streamer: polling services wait to open {ps_wait_market_open_cnt}')

            if ps_wait_market_open_cnt  % 60 == 59:
                current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
                print(f'streamer: polling services: waiting for market to open, current easten time:{current_eastern_hhmmss}')
            time.sleep(1)
            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
            continue



        print(f'streamer: polling services bc 400')


        current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')

        print(f'streamer polling services: market is now open, current easten time:{current_eastern_hhmmss}')
            

        get_today_in_epoch()

        print(f'streamer: polling services bc 400')


        asyncio.run(wait_for_rx_credentials(timeout=60))  # Runs the async function
        if rx_accessToken == None:
            # print(f'streamer: polling services bc 500')
            print(f'streamer: polling services gave up waiting for rx_credentials to be initialized')


        print(f'streamer: polling services bc 410')


        while not quit_flag: # innner polling services session loop
            polling_loop_cnt += 1

            # print(f'streamer: polling services bc 420')

            # print(f'pollig services polling_loop_cnt:{polling_loop_cnt}')


            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)

            if not market_open_flag:
                # print(f'streamer: polling services bc 430')
                print(f'polling services: market is no longer open, breaking to outer session loop')
                break



            











            # if len(call_strike_list) > 0:
            #     with strike_list_lock:
            #         temp_call_strike_list = call_strike_list

            #     for i in range(4):
            #         next_sym = temp_call_strike_list[next_call_list_ix]
            #         # print(f'calling get_opt_quote for call {next_sym}')
            #         get_opt_quote(next_sym)
            #         next_call_list_ix += 1
            #         if next_call_list_ix >= len(temp_call_strike_list):
            #             next_call_list_ix = 0

            #         time.sleep(0.10)



            with strike_list_lock:
                if len(call_strike_list) > 0:
                    temp_call_strike_list = call_strike_list.copy()
                else:
                    temp_call_strike_list = []

            if len(temp_call_strike_list) > 0:
                # next_call_list_ix = 0  # Local index
                for i in range(4):
                    next_sym = temp_call_strike_list[next_call_list_ix]
                    # print(f'calling get_opt_quote for call {next_sym}')

                    get_quote_success = get_opt_quote(next_sym)
                    if not get_quote_success:
                        print(f'call get_quote_failed for {next_sym}')
                        time.sleep(1)

                    next_call_list_ix += 1
                    if next_call_list_ix >= len(temp_call_strike_list):
                        next_call_list_ix = 0
                    time.sleep(0.10)








            # if len(put_strike_list) > 0:
            #     with strike_list_lock:
            #         temp_put_strike_list = put_strike_list

            #     for i in range(4):
            #         next_sym = temp_put_strike_list[next_put_list_ix]
            #         # print(f'calling get_opt_quote for put {next_sym}')
            #         get_opt_quote(next_sym)
            #         next_put_list_ix += 1
            #         if next_put_list_ix >= len(temp_put_strike_list):
            #             next_put_list_ix = 0

            #         time.sleep(0.10)



            with strike_list_lock:
            
                if len(put_strike_list) > 0:
                    temp_put_strike_list = put_strike_list.copy()
                else:
                    temp_put_strike_list = []

                if len(temp_put_strike_list) > 0:
                    # next_put_list_ix = 0  # Local index
                    for i in range(4):
                        next_sym = temp_put_strike_list[next_put_list_ix]
                        # print(f'calling get_opt_quote for put {next_sym}')

                        get_quote_success = get_opt_quote(next_sym)
                        if not get_quote_success:
                            print(f'put get_quote_failed for {next_sym}')
                            time.sleep(1)

                        next_put_list_ix += 1
                        if next_put_list_ix >= len(temp_put_strike_list):
                            next_put_list_ix = 0

                        time.sleep(0.10)








            




            # print(f'streamer: polling services bc 510')


            # if polling_loop_cnt % 20 == 2:
            if polling_loop_cnt % 60 == 2 or len(put_strike_list) == 0 or len(call_strike_list) == 0:

                # print(f'2630 trying to get ohlc')
                # print(f'streamer: polling services bc 520')

                try:

                    # print(f'streamer: polling services bc 598 calling get spx current today')

                    returned_ohlc = get_spx_current_today_ohlc()

                    # print(f'streamer: polling returned_ohlc type:{type(returned_ohlc)}, value:{returned_ohlc}')
                    # print(f'streamer: polling spx_high:{spx_high}, spx_low:{spx_low}')

                    if spx_high == None or spx_low == None or returned_ohlc == None:
                        print(f'poller does not have all high/low. spx_high:{spx_high:.2f}, spx_low:{spx_low:.2f}')

                    
                    try:


                        # print(f'streamer: polling services bc 530')

                        # print(f'2632 trying to get spx chain')

                        spx_chain = get_spx_option_chain()
                        if spx_chain is not None:
                            # print(f'2634 trying to get call/put list from chain data')
                            # print(f'streamer: polling services bc 600 calling parse spx chain')
                            parse_spx_chain(spx_chain)

                        else:
                            info_str = f'2534 could not get chain data'
                            print(info_str)
                            logging.warning(info_str)


                    except Exception as e:
                        info_str = f'2677 get spx chain error:{e}'
                        print(info_str)
                        logging.error(info_str)


                    else:
                        print(f'0832 poller spx_high:{spx_high}, spx_low:{spx_low}')

                except Exception as e:
                    info_str = f'2674 get ohlc error:{e}, could not get SPX o/h/l/c'
                    print(info_str)
                    logging.error(info_str)

            if polling_loop_cnt % 120 == 75:
                get_today_in_epoch()
            time.sleep(1)

        # innner polling services session loop


    # polling services outer (session) loop

    print("pollig service terminating...")







def quote_polling_setup():
    """ Creates and starts schwab quote thread """
    global quit_flag
    quote_polling_thread = threading.Thread(target=polling_services, daemon=True)
    quote_polling_thread.start()

    while not quit_flag:
        time.sleep(1)

    quote_polling_thread.join()
    print("schwab quote setup thread exiting.")







# ------------------------ Streamer --------------------------------

async def schwab_setup():
    """ Creates, starts, and joins the streamer_services coroutine """
    global quit_flag
    print("Schwab setup starting streamer services...")

    await streamer_services()  # Run streamer until quit_flag is True

    print("Schwab setup terminating...")    





async def get_user_preferences():
    global streamer_socket_url
    global sch_client_customer_id
    global sch_client_correl_id
    global sch_client_channel
    global sch_client_function_id
    global rx_accessToken

    # print(f'205 rx_accessToken:{rx_accessToken}')

    # print(f'gup 0010')

    success_flag = False

    current_time_str = datetime.now().strftime('%H:%M:%S')
    print(f'\ngetting userPreference at {current_time_str}')

    url = "https://api.schwabapi.com/trader/v1/userPreference"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {rx_accessToken}"
    }

    # print(f'gup 0012')



    # async with aiohttp.ClientSession() as session:
    #     async with session.get(url, headers=headers, timeout=REQUESTS_GET_TIMEOUT) as response:
    #         # print(f'8061 userPreference:{response.status}')
    #         userPreference_data = await response.json()  # Use await to process JSON


    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=REQUESTS_GET_TIMEOUT) as response:
                userPreference_data = await response.json()  # Process JSON response
                # return userPreference_data

    except asyncio.TimeoutError:
        print("Timeout occurred while trying to fetch user preferences.")
        return False

    except aiohttp.ClientError as e:
        print(f"Request failed due to client error: {e}")
        return False





    # print(f'gup 0014')
           

    if response.status == 200:

        # print(f'gup 0016')

        try:
            userPreference_data = await response.json()
            streamer_info = userPreference_data.get("streamerInfo", [{}])[0]

            streamer_socket_url = streamer_info.get("streamerSocketUrl", "")
            sch_client_customer_id = streamer_info.get("schwabClientCustomerId", "")
            sch_client_correl_id = streamer_info.get("schwabClientCorrelId", "")
            sch_client_channel = streamer_info.get("schwabClientChannel", "")
            sch_client_function_id = streamer_info.get("schwabClientFunctionId", "")

            # print(f'\n145 userPreference settings:')
            # print(f"Streamer Socket URL: {streamer_socket_url}")
            # print(f"Schwab Client Customer ID: {sch_client_customer_id}")
            # print(f"Schwab Client Correl ID: {sch_client_correl_id}")
            # print(f"Schwab Client Channel: {sch_client_channel}")
            # print(f"Schwab Client Function ID: {sch_client_function_id}")

            success_flag = True

        except (KeyError, TypeError, ValueError) as e:
            info_str = f'2030 Error parsing user preferences: {e}'
            print(info_str)
            logging.error(info_str)


        # print(f'gup 0018')


        return success_flag  

            


    else:
        info_str = f'3375 userPreference Error {response.status}: {await response.text()}'
        print(info_str)
        logging.error(info_str)

    # print(f'gup 0020')  

    return success_flag





async def aio_translate_quote_key_names(json_message):
    for item in json_message['data']:
        if item['service'] == 'LEVELONE_EQUITIES':
            for content in item['content']:
                if '1' in content:
                    content['bid'] = content.pop('1')
                if '2' in content:
                    content['ask'] = content.pop('2')
                if '3' in content:
                    content['last'] = content.pop('3')
                if '4' in content:
                    content['bid size'] = content.pop('4')
                if '5' in content:
                    content['ask size'] = content.pop('5')         
                if '6' in content:
                    content['ask ID'] = content.pop('6')                    
                if '7' in content:
                    content['bid ID'] = content.pop('7')  
                if '8' in content:
                    content['total volume'] = content.pop('8')  

        elif item['service'] == 'LEVELONE_OPTIONS':
            for content in item['content']:
                if '1' in content:
                    content['description'] = content.pop('1')                
                if '2' in content:
                    content['bid'] = content.pop('2')
                if '3' in content:
                    content['ask'] = content.pop('3')
                if '4' in content:
                    content['last'] = content.pop('4')
                if '5' in content:
                    content['high'] = content.pop('5')
                if '6' in content:
                    content['low'] = content.pop('6')
                if '7' in content:
                    content['close'] = content.pop('7')
                if '8' in content:
                    content['total volume'] = content.pop('8')
                if '10' in content:
                    content['volatility'] = content.pop('10')
                if '28' in content:
                    content['delta'] = content.pop('28')
                if '29' in content:
                    content['gamma'] = content.pop('29')
                if '30' in content:
                    content['theta'] = content.pop('30')
                if '31' in content:
                    content['vega'] = content.pop('31')
                if '32' in content:
                    content['rho'] = content.pop('32')


        elif item['service'] == 'LEVELONE_FUTURES':
            for content in item['content']:
                for content in item['content']:
                    if '1' in content:
                        content['bid'] = content.pop('1')
                    if '2' in content:
                        content['ask'] = content.pop('2')
                    if '3' in content:
                        content['price'] = content.pop('3')
                    if '4' in content:
                        content['bid size'] = content.pop('4')
                    if '5' in content:
                        content['ask size'] = content.pop('5')         
                    if '6' in content:
                        content['bid ID'] = content.pop('6')                    
                    if '7' in content:
                        content['ask ID'] = content.pop('7')  
                    if '8' in content:
                        content['total volume'] = content.pop('8')  

        else:
            pass



    return(json_message)




async def aio_extract_spx_last(item):
    global gbl_spx_last, last_extracted_spx_time

    try:

        if 'content' in item and isinstance(item['content'], list):
            for entry in item['content']:
                if 'key' in entry and entry['key'] == '$SPX' and 'last' in entry:
                    gbl_spx_last = entry['last']
                    last_extracted_spx_time = datetime.now()

                    # print(f'\n>>>>>>>>>>\ngbl_spx_last type:{type(gbl_spx_last)}, value:{gbl_spx_last:.2f}')
                    # print(f'last_extracted_spx_time type:{type(last_extracted_spx_time)}, value:{last_extracted_spx_time}')
                    break

    except Exception as e:
        info_str = f'2040 aio_extract_spx_last error:{e}'
        print(info_str)
        logging.error(info_str)
        # pass




async def process_received_message(last_message):

    json_message = json.loads(last_message)
    # print(f'pm json_message type{type(json_message)}, data:\n{json_message}')

    if 'data' in json_message:

        json_message = await aio_translate_quote_key_names(json_message)

        # print(f'data json_message type{type(json_message)}, data:\n{json_message}')


        # # Check for existence of LEVELONE_EQUITIES and LEVELONE_OPTIONS
        # services = {entry["service"] for entry in json_message.get("data", [])}
        # has_equities = "LEVELONE_EQUITIES" in services
        # has_options = "LEVELONE_OPTIONS" in services
        # print(f'LEVELONE_EQUITIES found?:{has_equities}  LEVELONE_OPTIONS found?:{has_options}  ')



        await aio_publish_raw_streamed_quote(json_message)

        for item in json_message.get("data", []):
                    service = item.get("service")
                    if service == "LEVELONE_EQUITIES":
                        # TODO CHECK
                        await aio_extract_spx_last(item)


    elif 'notify' in json_message:
        # print(f'notify message type:{type(json_message)}, data:\n{json_message}')

        # Extract heartbeat value if it exists
        notify_list = json_message.get("notify", [])

        time = None
        for item in notify_list:
            if "heartbeat" in item:
                timestamp_ms = int(item["heartbeat"])  # Convert string to integer
                time = datetime.fromtimestamp(timestamp_ms / 1000)  # Convert to datetime
                print(f"heartbeat at {time.strftime('%Y-%m-%d %H:%M:%S')} Local Time")
                # publish_raw_streamed_quote(item)
                await aio_publish_raw_streamed_notify(json_message)
                break  # Stop searching after finding the first heartbeat




        pass

    elif 'response' in json_message:
        # print(f'response message type:{type(json_message)}, data:\n{json_message}')
        pass


    else:
        # print(f'unsupported message type:\n{json_message}')
        pass




async def aio_is_valid_response(data):
    """ Validates API response format. """
    
    # print(f'is_valid_response() response type:{type(data)}, data:\n{data}')
    
    try:
        # Ensure data is a dictionary (parse JSON string if necessary)
        if isinstance(data, str):
            data = await asyncio.to_thread(json.loads, data)  # Run JSON parsing in a separate thread


        if "response" in data and isinstance(data["response"], list):
            return True  # Standard API response
        if "notify" in data and isinstance(data["notify"], list):
            return True  # Heartbeat and notifications
        if "data" in data and isinstance(data["data"], list):
            return True  # Subscription and market data updates

        
        print(f'IVR unknown is returning false, data type:{type(data)} data content:{data}')
        return False  # Unknown format
    
    except json.JSONDecodeError as e:
        info_str = f'2050 error in IVR: Failed to parse JSON - {e}'
        print(info_str)
        logging.error(info_str)
        return False
    

    except Exception as e:
        info_str = f'2060 error in IVR: {e}'
        print(info_str)
        logging.error(info_str)
        return False
    




websocket_lock = asyncio.Lock()



async def subscribe_level_one_equities(customer_id, correl_id, symbols, fields):
    global websocket


    # print(f'websocket type:{type(websocket)}')

    success_flag = False

    subscription_request = {
        "requests": [{
            "service": "LEVELONE_EQUITIES",
            "requestid": 2,
            "command": "SUBS",
            "SchwabClientCustomerId": customer_id,
            "SchwabClientCorrelId": correl_id,
            "parameters": {
                "keys": symbols,
                "fields": fields
            }
        }]
    }

    # print(f'sloe 2')

    try:

        await websocket.send(json.dumps(subscription_request))

    except Exception as e:
        print(f'sloe error 1: {e}')

    # print(f'sloe 4')

    async with websocket_lock:  # Lock WebSocket interactions
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=5)  # Timeout after 5 seconds
        except asyncio.TimeoutError:
            logging.error("1380 LOE Timeout while waiting for WebSocket response.")
            print("1390 LOE Timeout while waiting for WebSocket response.")
            return


    if not await aio_is_valid_response(response):
        info_str = f'1400 subscribe_level_one_equities() failed valid check'
        print(info_str)
        # logging.error(info_str)

    else:
        info_str = f'1410 subscribe_level_one_equities() succeeded'
        print(info_str)
        # logging.info(info_str)
        success_flag = True

    return success_flag





async def subscribe_level_one_options(customer_id, correl_id, symbol_list, fields):
    global websocket
    global strike_list_lock


    with strike_list_lock:
        opt_list_str = ", ".join(symbol_list)


    subscription_request = {
        "requests": [
            {
                "service": "LEVELONE_OPTIONS",
                "requestid": 7,
                "command": "ADD",
                "SchwabClientCustomerId": customer_id,
                "SchwabClientCorrelId": correl_id,
                "parameters": {
                    # "keys": symbol_list,
                    # "keys": temp_sym_list,
                    "keys": opt_list_str,

                    "fields": fields
                }
            }
        ]
    }

    await websocket.send(json.dumps(subscription_request))

    async with websocket_lock:  # Lock WebSocket interactions
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=5)  # Timeout after 5 seconds
        except asyncio.TimeoutError:
            logging.error("1350 LOO Timeout while waiting for WebSocket response.")
            print("LOO Timeout while waiting for WebSocket response.")
            return


    if not await aio_is_valid_response(response):
        info_str = '1360 subscribe_level_one_optionss() failed valid check'
        print(info_str)
        logging.error(info_str)
    else:

        # info_str = '1370 subscribe_level_one_optionss() was valid'
        # print(info_str)
        # logging.info(info_str)
        pass








# async def receive_messages(duration=60, caller=0):
#     """ Continuously receives and processes messages from the WebSocket for `duration` seconds. """
#     global websocket, rx_msg_func_call_count
#     global socket_active

#     print(f'rcv msgs caller:{caller}')


#     invalid_message_count = 0
#     max_invalid_messages = 5

#     rx_msg_func_call_count += 1

#     # print(f'09330 duration:{duration}')


#     try:
#         async with asyncio.timeout(duration):
#             while True:
#                 try:

#                     # print(f'09340 calling websocket.recv() at {await aio_get_current_time_str()}')
#                     response = await websocket.recv()
#                     # print(f'09342 returned from websocket.recv() at {await aio_get_current_time_str()}')

#                     print(f'09346 response type{type(response)}), value<{response}')


#                 except Exception as e:
#                     print(f'rcv msgs 1 error: {e}')
#                     raise
                

#     except TimeoutError:
#         # This error is expected with  asyncio.timeout() 
#         # logging.info(f"1080 Message receiving stopped after {duration} seconds.")
#         # print(f'30952 expected Message receiving timeoutError at {await aio_get_current_time_str()}, returning')
#         return

                
#     except Exception as e:
#         print(f'rcv msgs 2 error: {e}')
#         raise
                




async def receive_messages(duration=60, caller=0):
    """ Continuously receives and processes messages from the WebSocket for `duration` seconds. """
    global websocket, rx_msg_func_call_count
    global socket_active

    return_success = True

    # print(f'rcv msgs caller:{caller}')

    invalid_message_count = 0
    max_invalid_messages = 5

    rx_msg_func_call_count += 1

    try:
        async with asyncio.timeout(duration):
            while True:
                try:
                    response = await websocket.recv()




                    if not await aio_is_valid_response(response):
                        logging.warning("1040 Received invalid message format")
                        print("0930 Received invalid message format")
                        invalid_message_count += 1

                        # if invalid_message_count >= max_invalid_messages:
                        #     logging.error("1050 Too many invalid messages received, forcing WebSocket reconnection.")
                        #     print(f'9036 calling exp back reconn')
                        #     await exponential_backoff_reconnect()
                        #     print("Restarting receive messages after forced reconnect.")
                        #     await asyncio.sleep(3)
                        #     print(f'37100 calling receive messages')
                        #     await receive_messages()
                        #     return

                    else:
                        await process_received_message(response)
                        invalid_message_count = 0




                    # print(f'09346 response type{type(response)}), value<{response}')



                    # try:
                    #     # Parse JSON response
                    #     data = json.loads(response)

                    #     # Check if "response" or "data" exists, then scan for "service" and "command"
                    #     records = data.get("response", []) + data.get("data", [])

                    #     info_str = f'\nresponse >>  '

                    #     for record in records:
                    #         info_str += f'\n'
                    #         if "service" in record:
                    #             info_str += f'Service: {record["service"]}'
                                
                    #         if "command" in record:
                    #             info_str += f',  Command: {record["command"]}'

                    #         if "content" in record:
                    #             info_str += f',  Content: {record["content"]}'

                    #     print(f'{info_str}')


                    # except json.JSONDecodeError:
                    #     print("49002 Invalid JSON response format.")
                    #     return False

                    # except Exception as e:
                    #     print(f'49004 general error: {e}')
                    #     return False




                    # try:
                    #     # Parse the JSON string
                    #     data = json.loads(response)

                    #     # Check if "notify" exists and contains data
                    #     if "notify" in data and isinstance(data["notify"], list):
                    #         for item in data["notify"]:
                    #             if "heartbeat" in item:

                    #                 epoch_ms = item["heartbeat"] # Example epoch timestamp (milliseconds)


                    #                 # print(f'4955 epoch_ms type{type(epoch_ms )}, value:{epoch_ms}')


                    #                 epoch_sec = int(epoch_ms) / 1000  # convert to seconds

                    #                 dt = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
                    #                 formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')

                    #                 # print(f"Formatted time: {formatted_time}")


                    #                 # Convert to UTC datetime
                    #                 utc_dt = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)

                    #                 # Convert to local time
                    #                 local_dt = utc_dt.astimezone()  # Converts to the local timezone automatically
                    #                 formatted_local_time = local_dt.strftime('%Y-%m-%d %H:%M:%S')


                    #                 print(f'heartbeat at {formatted_local_time} (Local)')
                    #                 break  # Exit loop after first valid heartbeat

                    # except json.JSONDecodeError:
                    #     print("Invalid JSON response received.")













                except OSError as e:
                    print(f"WebSocket error suppressed: {e}")
                    logging.error(f"WebSocket error suppressed: {e}")
                    return False  # Exit function gracefully

                except websockets.exceptions.ConnectionClosedError:
                    print("WebSocket connection closed. ")
                    logging.error("WebSocket connection closed error.")
                    return False  # Ensure function exits cleanly

                except websockets.exceptions.WebSocketException as e:
                    print(f"WebSocket error caught: {e}")
                    logging.error(f"WebSocket error caught: {e}")
                    return False  # Ensure function exits cleanly


                except Exception as e:
                    print(f'WebSocket error general: {e}')  # Log error, but don't raise
                    logging.error(f'rcv msgs 1 error: {e}')
                    # return  # Ensure function exits gracefully
                    return False

    except TimeoutError:
        # print(f'timeout error') # This is expected due to asyncio.timeout
        return True  # Expected timeout, exit normally

    except Exception as e:
        print(f'rcv msgs 2 error (suppressed): {e}')  # Log error, but don't raise
        logging.error(f'rcv msgs 2 error: {e}')
        return False  # Ensure function exits cleanly





async def login_to_schwab_streamer():
    global websocket
    global socket_active

    success_flag = False
    timeout_duration = 10  # Timeout in seconds

    # print(f'74000 login to schwab streamer')

    # Close the old WebSocket if it's still open
    if websocket and not websocket.closed:
        print("2404 previous websocket was not closed, Close previous WebSocket connection before reconnecting.")
        await websocket.close()
        await asyncio.sleep(2)  # Give time for closure before reconnecting


    # print(f'74002 login to schwab streamer')
        

    # # Establish new WebSocket connection
    # websocket = await websockets.connect(streamer_socket_url)

    try:
        # Set timeout for connection attempt
        websocket = await asyncio.wait_for(websockets.connect(streamer_socket_url), timeout=timeout_duration)
        socket_active = True 
        # print(f'74004 login to schwab streamer')

    except asyncio.TimeoutError:
        print(f'74005 WebSocket connection attempt timed out')
        socket_active = False
    except Exception as e:
        print(f'74006 WebSocket connection failed due to error: {e}')
        socket_active = False


    # if websocket:
    #     print(f'74007 No websocket')
    #     socket_active = False
    #     return success_flag
    
    # else:
    #     print(f'74008 websocket was created')


    # print(f'websocket type:{type(websocket)}, value:{websocket}')

    # Check if websocket is active and not None
    if socket_active and websocket is not None:
        # print(f'74052 WebSocket connection established: {websocket}')
        pass

    else:
        print('74053 WebSocket connection failed.')
        return False




    api_login_form = {
        "requests": [
            {
                "requestid": "1",
                "service": "ADMIN",
                "command": "LOGIN",
                "SchwabClientCustomerId": rx_customerId,
                "SchwabClientCorrelId": rx_correlId,
                "parameters": {
                    "Authorization": rx_accessToken,
                    "SchwabClientChannel": rx_channel,
                    "SchwabClientFunctionId": rx_functionId
                }
            }
        ]
    }



    try:
        print(f'74010 login to schwab streamer')

        websocket = await websockets.connect(streamer_socket_url)
        await websocket.send(json.dumps(api_login_form))
        response = await websocket.recv()

        # print(f'74012 login to schwab streamer')

        resp_json = json.loads(response)
        # print(f'\n9973 login connect resp_json:{resp_json}\n')

        # Extract 'code' value from the first item in 'response'
        if "response" in resp_json and resp_json["response"]:
            resp_code = resp_json["response"][0]["content"].get("code", -1)  # Default to -1 if 'code' is missing
        else:
            resp_code = -1  # Indicate an invalid response structure

        if resp_code == 0:
            info_str = f'1300 API Login successful'
            print(info_str)
            success_flag = True

        else:
            info_str = f'1301 API Login UNsuccessful'
            print(info_str)



    except Exception as e:
        info_str = f"1340 Error logging into Schwab API: {e}"
        print(info_str)


    # print(f'74022 end of login to schwab streamer')

    return success_flag




async def streamer_after_hours():
    global quit_flag
    global socket_active
    global mqtt_intialized

    error_flag = False


    info_str = f'Streamer after hours started......'
    # logging.info(info_str)
    print(f'{info_str}')

    wait_cnt = 0

    while(1):
        print(f'outer loop')


        # wait for the mqtt client to be initialized
        while(1):
            # print(f'ss waiting for mqtt_intialized')
            if mqtt_intialized == True:
                break

            # print(f'ss waiting mqtt 20')
            await asyncio.sleep(2)
            # print(f'ss waiting mqtt 22')
                



        # print(f'ss 50200 mqtt is initialized')

        await reset_rx()
        # print(f'ss 50202')
        await aio_publish_request_creds()
        # print(f'ss 50204')
        error_flag = False


        wait_cnt = 0

        while(1):

            # print(f'ss 20300')


            if rx_accessToken != None:
                # print(f'ss 20302')
                print(f'rx_accessToken was initialized on wait_cnt {wait_cnt}')
                await asyncio.sleep(1)
                # print(f'ss 20304')
                break


            # print(f'ss 20306')

            await asyncio.sleep(1)  # Non-blocking sleep
            # print(f'ss 20308')
            wait_cnt += 1
            await aio_publish_request_creds()

            # print(f'ss 20308')

            # print(f'wait_cnt:{wait_cnt}')

            # print(f'ss 20310')

        
        wait_cnt = 0

        print(f'starting loop 2')
        while(1):
            await asyncio.sleep(1)
            wait_cnt += 1
            user_preferences_success = await get_user_preferences()
            # print(f'user_preferences_success:{user_preferences_success}, cnt:{wait_cnt}')

            if not user_preferences_success:
                print(f'user preferences failed, continuing')
                break

            else:
                print(f'user preferences succeeded')

            login_success = await login_to_schwab_streamer()  # Attempt login

            if not login_success:
                print(f'login_success failed, continuing')
                continue


            sub_LOE_success = await subscribe_level_one_equities(sch_client_customer_id, sch_client_correl_id, "$SPX", "0,1,2,3,4,5,8,10")

            if not sub_LOE_success:
                print(f'sub_LOE failed, continuing')
                continue

            else:
                # print(f'sub_LOE_success')
                pass

            print(f'starting loop 3')

            while(1):

                # print(f'after hours processing')
                market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
                
                if market_open_flag == True:
                    print(f'after hours processing, market is now open, returning')
                    return

                # print(f'calling rcv')
                rcv_msgs_success = await receive_messages(duration=10,caller=10)


                if not rcv_msgs_success:
                    print(f'post rcv msgs 3 error')
                    error_flag = True
                    break


            if error_flag:
                break

        if error_flag:
            continue


async def aio_get_current_time_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def aio_publish_raw_streamed_notify(data):
    global time_since_last_stream_pub

    json_str = json.dumps(data)
    # print(f'in aio_publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')

    topic = "schwab/notify"
    await aio_publish_quote(topic, json_str)
    time_since_last_stream_pub = 0
    pass



async def aio_publish_raw_streamed_quote(data):
    global time_since_last_stream_pub

    json_str = json.dumps(data)
    # print(f'in aio_publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')

    topic = "schwab/stream"
    await aio_publish_quote(topic, json_str)
    time_since_last_stream_pub = 0
    pass





async def aio_get_call_strike_list():
    """ Safely retrieves the latest call strike list """
    global call_strike_list

    await asyncio.to_thread(strike_list_lock.acquire)  # Corrected usage
    try:
        return list(call_strike_list)  # Return a copy to avoid modification issues
    finally:
        strike_list_lock.release()  # Always release the lock

async def aio_get_put_strike_list():
    """ Safely retrieves the latest put strike list """
    global put_strike_list

    await asyncio.to_thread(strike_list_lock.acquire)  # Corrected usage
    try:
        return list(put_strike_list)  # Return a copy to avoid modification issues
    finally:
        strike_list_lock.release()  # Always release the lock




async def subscribe_to_options():

    success_flag = False

    if call_strike_list == None or len(call_strike_list) == 0:
        info_str = f'2201 unable to subscribe to options, call_strike_list:{call_strike_list}'
        print(info_str)
        logging.error(info_str)
        return False
    
    if put_strike_list == None or len(put_strike_list) == 0:
        info_str = f'2202 unable to subscribe to options, put_strike_list:{put_strike_list}'
        print(info_str)
        logging.error(info_str)
        return False
    
    if sch_client_customer_id == None or sch_client_correl_id == None :
        info_str = f'2203 unable to subscribe to options, sch_client_customer_id:{sch_client_customer_id}, sch_client_correl_id:{sch_client_correl_id}'
        print(info_str)
        logging.error(info_str)
        return False

        

    time_str = await aio_get_current_time_str()
    print(f'17912 doing option subscriptions at {time_str}, Local Time')

    call_list_copy = await aio_get_call_strike_list()
    put_list_copy = await aio_get_put_strike_list()

    try:
        await subscribe_level_one_options(sch_client_customer_id, sch_client_correl_id, call_list_copy, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32")
        await subscribe_level_one_options(sch_client_customer_id, sch_client_correl_id, put_list_copy, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32")

        last_subscription_time = time.time()  # Reset timestamp after subscription
        # print(f' initial last_subscription_time:{last_subscription_time}')
        success_flag = True


    except Exception as e:
        info_str = f'1430 Error in initial subscribe_level_one_options(): {e}'
        logging.error(info_str)
        print(info_str)

    return success_flag






async def streamer_during_hours():
    
    global quit_flag
    global socket_active
    global mqtt_intialized

    error_flag = False


    info_str = f'Streamer during hours started......'
    # logging.info(info_str)
    print(f'{info_str}')

    wait_cnt = 0

    while(1):
        print(f'dh outer loop')


        # wait for the mqtt client to be initialized
        while(1):
            # print(f'ss dh waiting for mqtt_intialized')
            if mqtt_intialized == True:
                break

            # print(f'ss dh waiting mqtt 20')
            await asyncio.sleep(2)
            # print(f'ss dh waiting mqtt 22')
                



        # print(f'ss dh 50200 mqtt is initialized')

        await reset_rx()
        # print(f'ss dh 50202')
        await aio_publish_request_creds()
        # print(f'ss dh 50204')
        error_flag = False


        wait_cnt = 0

        while(1):

            # print(f'ss dh 20300')


            if rx_accessToken != None:
                # print(f'ss dh 20302')
                print(f'rx_accessToken was initialized on wait_cnt {wait_cnt}')
                await asyncio.sleep(1)
                # print(f'ss dh 20304')
                break


            # print(f'ss dh 20306')

            await asyncio.sleep(1)  # Non-blocking sleep
            # print(f'ss dh 20308')
            wait_cnt += 1
            await aio_publish_request_creds()

            # print(f'ss dh 20308')

            # print(f'wait_cnt:{wait_cnt}')

            # print(f'ss dh 20310')

        
        wait_cnt = 0

        print(f'dh starting loop 2')
        while(1):
            await asyncio.sleep(1)
            wait_cnt += 1
            user_preferences_success = await get_user_preferences()
            # print(f'dh user_preferences_success:{user_preferences_success}, cnt:{wait_cnt}')

            if not user_preferences_success:
                print(f'dh user preferences failed, continuing')
                break

            else:
                print(f'dh user preferences succeeded')

            login_success = await login_to_schwab_streamer()  # Attempt login

            if not login_success:
                print(f'dh login_success failed, continuing')
                continue


            sub_LOE_success = await subscribe_level_one_equities(sch_client_customer_id, sch_client_correl_id, "$SPX", "0,1,2,3,4,5,8,10")

            if not sub_LOE_success:
                print(f'dh sub_LOE failed, continuing')
                continue

            else:
                # print(f'dh sub_LOE_success')
                pass


            
            # wait for the call/put lists to be initialized
            wait_put_call_cnt = 0
            while wait_put_call_cnt < 200:
                print(f'waiting for strike lists to be initialized, wait_put_call_cnt:{wait_put_call_cnt}')
                if len(call_strike_list) > 0 and len(put_strike_list) > 0:
                    print(f'strike lists have been initialized, wait_put_call_cnt:{wait_put_call_cnt}')
                    break

                await asyncio.sleep(1)






            print(f'dh starting loop 3')

            while(1):

                print(f'dh during hours processing')
                market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)
                
                if market_open_flag == False:
                    print(f'during hours processing, market is now closed, returning')
                    return
                
                # print(f'88301 calling subscribe to options')

                sub_opt_success = await subscribe_to_options()
                if not sub_opt_success:
                    info_str = f'3882 subscribe options failed'
                    logging.warning(info_str)
                    print(info_str)

                # print(f'88302 returned from subscribe to options')


                # print(f'dh calling rcv')
                rcv_msgs_success = await receive_messages(duration=60,caller=12)


                if not rcv_msgs_success:
                    print(f'dh post rcv msgs 3 error')
                    error_flag = True
                    break


            if error_flag:
                break

        if error_flag:
            continue








async def streamer_services():


    while(1):
        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=MARKET_OPEN_OFFSET, close_offset=0)

        if market_open_flag == False:
            await streamer_after_hours()
            continue

        else:
            await streamer_during_hours()
            continue

    return










    """ Infinite work loop that returns if quit_flag is set to True """
    global quit_flag
    global socket_active
    global mqtt_intialized

    error_flag = False


    info_str = f'Streamer services started......'
    # logging.info(info_str)
    print(f'{info_str}')

    wait_cnt = 0

    while(1):
        print(f'outer loop')


        # wait for the mqtt client to be initialized
        while(1):
            # print(f'ss waiting for mqtt_intialized')
            if mqtt_intialized == True:
                break

            # print(f'ss waiting mqtt 20')
            await asyncio.sleep(2)
            # print(f'ss waiting mqtt 22')
                



        # print(f'ss 50200 mqtt is initialized')

        await reset_rx()
        # print(f'ss 50202')
        await aio_publish_request_creds()
        # print(f'ss 50204')
        error_flag = False


        wait_cnt = 0

        while(1):

            # print(f'ss 20300')


            if rx_accessToken != None:
                # print(f'ss 20302')
                print(f'rx_accessToken was initialized on wait_cnt {wait_cnt}')
                await asyncio.sleep(1)
                # print(f'ss 20304')
                break


            # print(f'ss 20306')

            await asyncio.sleep(1)  # Non-blocking sleep
            # print(f'ss 20308')
            wait_cnt += 1
            await aio_publish_request_creds()

            # print(f'ss 20308')

            # print(f'wait_cnt:{wait_cnt}')

            # print(f'ss 20310')

        
        wait_cnt = 0

        print(f'starting loop 2')
        while(1):
            await asyncio.sleep(1)
            wait_cnt += 1
            user_preferences_success = await get_user_preferences()
            # print(f'user_preferences_success:{user_preferences_success}, cnt:{wait_cnt}')

            if not user_preferences_success:
                print(f'user preferences failed, continuing')
                break

            else:
                print(f'user preferences succeeded')

            login_success = await login_to_schwab_streamer()  # Attempt login

            if not login_success:
                print(f'login_success failed, continuing')
                continue


            sub_LOE_success = await subscribe_level_one_equities(sch_client_customer_id, sch_client_correl_id, "$SPX", "0,1,2,3,4,5,8,10")

            if not sub_LOE_success:
                print(f'sub_LOE failed, continuing')
                continue

            else:
                # print(f'sub_LOE_success')
                pass

            print(f'starting loop 3')

            while(1):


                # print(f'calling rcv')
                rcv_msgs_success = await receive_messages(duration=10,caller=10)

                # print(f'returned from rcv')

                if not rcv_msgs_success:
                    print(f'post rcv msgs 3 error')
                    error_flag = True
                    break


            if error_flag:
                break

        if error_flag:
            continue











        











async def main():
    global quit_flag


    info_str = f'streamer startup . . . .'
    print(info_str)
    logging.info(info_str)

    # Start MQTT setup in a separate thread
    setup_mqtt_thread = threading.Thread(target=mqtt_setup, daemon=True)
    setup_mqtt_thread.start()

    # Start quote polling setup in a separate thread
    setup_polling_thread = threading.Thread(target=quote_polling_setup, daemon=True)
    setup_polling_thread.start()

    # Start Schwab setup as an async task
    schwab_task = asyncio.create_task(schwab_setup())


    # try:
    #     while not quit_flag:
    #         await asyncio.sleep(1)  # Keep main thread alive
    # except KeyboardInterrupt:
    #     print("\nCTRL-C detected in main. Setting quit_flag to True.")
    #     quit_flag = True


    try:
        while not quit_flag:
            await asyncio.sleep(1)  # Keep main thread alive

    except KeyboardInterrupt:
        info_str = f'\n3310 CTRL-C detected in main. Setting quit_flag to True.'
        logging.error(info_str)
        print(info_str)
        quit_flag = True

    except asyncio.CancelledError:
        info_str = f'3320 Asyncio task cancelled.  quit_flag:{quit_flag}. Cleaning up...'
        logging.error(info_str)
        print(f'\n{info_str}')
        quit_flag = True

    # Wait for Schwab setup to finish
    await schwab_task


    info_str = f'9990 waiting for threads to join'
    logging.error(info_str)
    print(f'\n{info_str}')
    quit_flag = True


    # Wait for MQTT setup thread to terminate
    setup_mqtt_thread.join()

    # Wait for polling setup thread to terminate
    setup_polling_thread.join()

    info_str = f'9992 program is terminating'
    logging.error(info_str)
    print(f'\n{info_str}')
    quit_flag = True

if __name__ == "__main__":
    asyncio.run(main())













