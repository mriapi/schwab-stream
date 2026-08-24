
import json
import threading
import requests
import logging
import time
import websocket
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import zoneinfo   # Python 3.9+
from zoneinfo import ZoneInfo
import json

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ET = zoneinfo.ZoneInfo("America/New_York")
# UTC = zoneinfo.ZoneInfo("UTC")

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<






# Suppress WebSocket internal errors
logging.getLogger("websockets").setLevel(logging.CRITICAL)


logging.basicConfig(
    filename="strm2.log",  # Log file name
    level=logging.INFO,  # Set logging level
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -----------------------------
# Global shared state
# -----------------------------
CREDS = {}
CREDS_READY = threading.Event()

SPX_VALUE = None
SPX_TIMESTAMP = None
SPX_LOCK = threading.Lock()

todays_epoch_time = None
rx_accessToken = None
stream_login_success = False






import threading

SPX_OHLC_LOCK = threading.Lock()

SPX_OHLC_DATA = {
    "open": None,
    "high": None,
    "low": None,
    "close": None,
    "timestamp_et": None,   # ET timestamp of when OHLC was fetched
    
    "raw_candle": None    # full Schwab candle dict
}

chain_srike_cnt = None


def save_spx_ohlc(candle_dict):
    """
    Save SPX O/H/L/C and timestamp in a thread-safe global structure.
    candle_dict must be the Schwab candle: {'open':..., 'high':..., ...}
    """

    global SPX_OHLC_DATA

    c_low = candle_dict["low"]
    c_high = candle_dict["high"]



    # Convert Schwab datetime (ms since epoch) → ET
    dt_utc = datetime.fromtimestamp(candle_dict["datetime"] / 1000, tz=timezone.utc)
    dt_et = dt_utc.astimezone(ET)

    with SPX_OHLC_LOCK:
        SPX_OHLC_DATA["open"] = candle_dict["open"]
        SPX_OHLC_DATA["high"] = candle_dict["high"]
        SPX_OHLC_DATA["low"] = candle_dict["low"]
        SPX_OHLC_DATA["close"] = candle_dict["close"]
        SPX_OHLC_DATA["timestamp_et"] = dt_et
        SPX_OHLC_DATA["raw_candle"] = candle_dict




def read_spx_ohlc():
    """
    Return a copy of the saved SPX OHLC data in a thread-safe manner.
    """
    with SPX_OHLC_LOCK:
        # return a shallow copy so callers cannot mutate the global
        return dict(SPX_OHLC_DATA)





# -----------------------------
# MQTT: receive Schwab credentials
# -----------------------------
def on_mqtt_message(client, userdata, msg):
    global CREDS
    global rx_accessToken

    try:
        payload = json.loads(msg.payload.decode())
        CREDS = payload
        # print("Received Schwab credentials via MQTT")
        CREDS_READY.set()   # signal get_quote() thread

        rx_accessToken = CREDS["accessToken"]
    except Exception as e:
        print("Error parsing MQTT payload:", e)


def mqtt_thread():
    client = mqtt.Client()
    client.on_message = on_mqtt_message

    client.connect("localhost", 1883, 60)
    client.subscribe("mri/creds/info")

    client.loop_forever()


# -----------------------------
# Schwab WebSocket streaming
# -----------------------------


# def build_spx_request(creds, syms):
#     return json.dumps({
#         "requests": [
#             {
#                 "service": "LEVELONE_EQUITIES",
#                 "command": "SUBS",
#                 "requestid": "2",
#                 "SchwabClientCustomerId": creds["customerId"],
#                 "SchwabClientCorrelId": creds["correlId"],
#                 "parameters": {
#                     "keys": "$SPX",
#                     "fields": "0,1,2,3,4,5,8,10"
#                 }
#             }
#         ]
#     })



def build_spx_request(creds, syms):
    return json.dumps({
        "requests": [
            {
                "service": "LEVELONE_EQUITIES",
                "command": "SUBS",
                "requestid": "2",
                "SchwabClientCustomerId": creds["customerId"],
                "SchwabClientCorrelId": creds["correlId"],
                "parameters": {
                    "keys": ",".join(syms),
                    "fields": "0,1,2,3,4,5,8,10"
                }
            }
        ]
    })


def build_opt_request(creds, syms):
    return json.dumps({
        "requests": [
            {
                "service": "LEVELONE_OPTIONS",
                "command": "ADD",
                "requestid": "3",
                "SchwabClientCustomerId": creds["customerId"],
                "SchwabClientCorrelId": creds["correlId"],
                "parameters": {
                    "keys": ",".join(syms),
                    "fields": "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"
                }
            }
        ]
    })



def convert_schwab_timestamp_to_et(ms_timestamp):
    """
    Convert Schwab millisecond timestamp to Eastern Time hh:mm:ss.mmm
    """
    # Convert milliseconds → datetime in UTC
    dt_utc = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)

    # Convert UTC → Eastern Time
    dt_et = dt_utc.astimezone(ET)

    # Format with microseconds, then trim to milliseconds
    return dt_et.strftime("%H:%M:%S.%f")[:-3]





def process_spx_message(msg):
    global SPX_VALUE, SPX_TIMESTAMP


    print(f'msg:{msg}')

    try:
        data = json.loads(msg)
        quote = data.get("data", [{}])[0]

        # Always update timestamp
        ts = quote.get("timestamp")
        if ts is None:
            return

        # Convert to Eastern Time
        ts_et = convert_schwab_timestamp_to_et(ts)

        content_list = quote.get("content", [])
        if not content_list:
            print(f"SPX_TIMESTAMP updated (ET={ts_et}), last price unchanged (SPX_VALUE={SPX_VALUE})")
            with SPX_LOCK:
                SPX_TIMESTAMP = ts
            return

        content = content_list[0]

        # Field "3" = last price
        last_price = content.get("3")

        # print(f'- {last_price} (SPX LAST)')

        with SPX_LOCK:
            SPX_TIMESTAMP = ts

            if last_price is not None:
                SPX_VALUE = last_price
                # print(f"Updated SPX_VALUE={SPX_VALUE}, SPX_TIMESTAMP={SPX_TIMESTAMP}, ET={ts_et}")
            else:
                # print(f"SPX_TIMESTAMP updated (ET={ts_et}), last price unchanged (SPX_VALUE={SPX_VALUE})")
                pass

    except Exception as e:
        print("Error processing SPX message:", e)





def get_quote():
    """
    Main quote thread:
    - waits for MQTT credentials
    - connects to Schwab WebSocket
    - LOGIN
    - SUBSCRIBE to SPX
    - handles reconnects
    """

    global stream_login_success

    CREDS_READY.wait()
    creds = CREDS

    ws_url = creds["streamerUrl"]
    print(f'2390923 ws_url:{ws_url}')

    # Build LOGIN message
    login_msg = json.dumps({
        "requests": [
            {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": "1",
                "SchwabClientCustomerId": creds["customerId"],
                "SchwabClientCorrelId": creds["correlId"],
                "parameters": {
                    "Authorization": creds["accessToken"],
                    "SchwabClientChannel": creds["channel"],
                    "SchwabClientFunctionId": creds["functionId"]
                }
            }
        ]
    })


    syms = ["$SPX"]
    # syms = ["$SPX", 'SPXW  260723C07420000', 'SPXW  260723P07415000']

    # Build SPX subscription message
    subs_msg = build_spx_request(creds, syms)
    print(f'subs_msg:{subs_msg}')


    # ---------------------------------------------------------
    # Embedded on_message() handler
    # ---------------------------------------------------------
    def on_message(ws, msg):
        global stream_login_success

        # print(f'got msg in on_message')

        try:
            data = json.loads(msg)

            # -------------------------
            # Handle RESPONSE messages
            # -------------------------
            if "response" in data:
                resp = data["response"][0]
                service = resp.get("service")
                command = resp.get("command")
                content = resp.get("content", {})
                code = content.get("code", -1)

                # ADMIN / LOGIN response
                if service == "ADMIN" and command == "LOGIN":
                    stream_login_success = True

                    if code == 0:
                        print("LOGIN successful, sending SUBS")
                        ws.send(subs_msg)
                    else:
                        print("LOGIN failed:", content)
                        ws.close()
                    return

                # QUOTE / SUBS response
                if service == "QUOTE" and command == "SUBS":
                    if code == 0:
                        print("SUBS successful — streaming SPX now")
                    else:
                        print("SUBS failed:", content)
                        ws.close()
                    return

            # -------------------------
            # Handle QUOTE data messages
            # -------------------------
            if "data" in data:
                # print(f'found data in the on_message')
                process_spx_message(msg)

        except Exception as e:
            print("Error in on_message:", e)

    # ---------------------------------------------------------
    # Error + close handlers
    # ---------------------------------------------------------
    def on_error(ws, err):
        print("WS error:", err)

    def on_close(ws, code, msg):
        print(f"WS closed: {code} {msg}")

    # ---------------------------------------------------------
    # Main reconnect loop
    # ---------------------------------------------------------
    while True:
        try:
            print("Connecting to Schwab streamer:", ws_url)

            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            # Start WebSocket thread
            wst = threading.Thread(target=ws.run_forever)
            wst.daemon = True
            wst.start()

            # Allow connection to establish
            time.sleep(2)

            # Send LOGIN
            print("Sending LOGIN request.")
            ws.send(login_msg)

            # Keep thread alive; reconnect if thread dies
            while wst.is_alive():
                time.sleep(1)

        except Exception as e:
            print("Exception in get_quote():", e)

        print("Reconnecting in 5 seconds...")
        time.sleep(5)



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


def calc_chain_strike_cnt(spx_low, spx_high):
    if spx_low == None or spx_high == None:
        return None
    
    c_distance = abs(spx_high - spx_low)
    print(f'c_distance:{c_distance}')
    my_chain_srike_cnt = int(c_distance / 5) + 50
    print(f'my_chain_srike_cnt:{my_chain_srike_cnt}')
    return my_chain_srike_cnt


def get_spx_option_chain():
    global chain_strike_cnt, rx_accessToken
    global chain_data_lock  # Assume this lock exists


    local_accessToken = rx_accessToken
    ohlc = read_spx_ohlc()

    high = ohlc['high']
    low = ohlc['low']


    local_strike_cnt = calc_chain_strike_cnt(high, low)

    if local_accessToken is None or local_strike_cnt is None:
        print(f'Dont have rx_accessToken')
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







#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
REQUESTS_GET_TIMEOUT = 10

def get_spx_current_today_ohlc():
    """
    Fetch today's SPX daily OHLC candle from Schwab.
    Returns the full JSON dict or None.
    """

    get_today_in_epoch()

    if todays_epoch_time is None or rx_accessToken is None:
        print(f'unable to get ohlc, todays_epoch_time: {todays_epoch_time}, rx_accessToken: {rx_accessToken}')
        return None

    start_date = todays_epoch_time
    end_date = todays_epoch_time + 86400000   # +1 day in ms

    url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
    params = {
        "symbol": "$SPX",
        "periodType": "month",
        "period": 1,
        "frequencyType": "daily",
        "frequency": 1,
        "startDate": start_date,
        "endDate": end_date,
        "needExtendedHoursData": "false",
        "needPreviousClose": "false"
    }

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {rx_accessToken}"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

        if response.status_code != 200:
            print(f"Failed to fetch SPX data. Status code: {response.status_code}")
            print(f"Response text: {response.text}")
            return None

        try:
            data = response.json()
        except Exception as e:
            print(f"JSON decode error: {e}")
            print(f"Raw response: {response.text}")
            return None

        # Token expired?
        if isinstance(data, dict) and "error" in data:
            print(f"Schwab API error: {data['error']}")
            return None

        if data.get("empty", True):
            print("SPX data response is empty (weekend/holiday or no trading yet).")
            return None

        candles = data.get("candles", [])
        if not candles:
            print("SPX data response has no candles.")
            return None

        first = candles[0]

        required_fields = ["open", "high", "low", "close", "datetime"]
        if not all(k in first for k in required_fields):
            print(f"Malformed candle: {first}")
            return None

        return data

    except requests.exceptions.Timeout:
        info_str = 'Request timed out while fetching SPX OHLC data'
        logging.error(info_str)
        print(info_str)
        return None

    except Exception as e:
        info_str = f'pricehistory error: {e}, could not get SPX o/h/l/c'
        logging.error(info_str)
        print(info_str)
        return None












# REQUESTS_GET_TIMEOUT = 10

# def get_spx_current_today_ohlc():
#     global spx_open, spx_high, spx_low, spx_close
#     global ohlc_get_time, chain_strike_cnt


#     get_today_in_epoch()


#     if todays_epoch_time is None or rx_accessToken is None:
#         print(f'unable to get ohlc, todays_epoch_time: {todays_epoch_time}, rx_accessToken: {rx_accessToken}')
#         return None

#     # Schwab behaves better when endDate > startDate
#     start_date = todays_epoch_time
#     end_date = todays_epoch_time + 86400000   # +1 day in ms

#     url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
#     params = {
#         "symbol": "$SPX",
#         "periodType": "month",
#         "period": 1,
#         "frequencyType": "daily",
#         "frequency": 1,
#         "startDate": start_date,
#         "endDate": end_date,
#         "needExtendedHoursData": "false",
#         "needPreviousClose": "false"
#     }

#     headers = {
#         "accept": "application/json",
#         "Authorization": f"Bearer {rx_accessToken}"
#     }

#     try:
#         response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

#         if response.status_code != 200:
#             print(f"Failed to fetch SPX data. Status code: {response.status_code}")
#             print(f"Response text: {response.text}")
#             return None

#         data = response.json()

#         # Validate structure
#         if data.get("empty", True):
#             print("SPX data response is empty.")
#             return None

#         candles = data.get("candles", [])
#         if not candles:
#             print("SPX data response has no candles.")
#             return None

#         first = candles[0]

#         # Validate candle fields
#         required_fields = ["open", "high", "low", "close"]
#         if not all(k in first for k in required_fields):
#             print(f"Malformed candle: {first}")
#             return None

#         # # Update shared state
#         # with get_current_day_history_lock:
#         #     spx_open = first["open"]
#         #     spx_high = first["high"]
#         #     spx_low = first["low"]
#         #     spx_close = first["close"]
#         #     ohlc_get_time = datetime.now()

#         #     # Your chain strike count logic
#         #     day_high_distance = abs(spx_close - spx_high)
#         #     day_low_distance = abs(spx_close - spx_low)
#         #     max_distance = max(day_high_distance, day_low_distance)
#         #     chain_strike_cnt = int(max_distance / 5) + 50

#         # print(f'chain_strike_cnt: {chain_strike_cnt} at {ohlc_get_time.strftime("%H:%M:%S")}')

#         return data

#     except requests.exceptions.Timeout:
#         info_str = 'Request timed out while fetching SPX OHLC data'
#         logging.error(info_str)
#         print(info_str)
#         return None

#     except Exception as e:
#         info_str = f'pricehistory error: {e}, could not get SPX o/h/l/c'
#         logging.error(info_str)
#         print(info_str)
#         return None






# -----------------------------
# Worker thread: print SPX updates
# -----------------------------




#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def do_work():
    last_val = None
    last_ts = None
    last_spx_candle_cnt = 0

    # wait for stream login
    while stream_login_success is False:
        time.sleep(2)
        print('waiting for stream login')

    while True:
        with SPX_LOCK:
            val = SPX_VALUE
            ts = SPX_TIMESTAMP

        if val is not None and ts is not None:
            if val != last_val or ts != last_ts:

                # Schwab timestamp → UTC → ET
                dt_utc = datetime.fromtimestamp(ts / 1000, tz=UTC)
                dt_et = dt_utc.astimezone(ET)

                # Current ET time
                now_et = datetime.now(tz=ET)

                # Compute elapsed time
                delta = now_et - dt_et
                elapsed_ms = delta.total_seconds() * 1000
                elapsed_str = f"{elapsed_ms/1000:.3f}"

                # Format Schwab timestamp
                ts_et = dt_et.strftime("%H:%M:%S.%f")[:-3]

                print(
                    f"do work last SPX: {val:.2f}   "
                    f"Timestamp: {ts_et}   "
                    f"Elapsed: {elapsed_str} seconds"
                )

                last_val = val
                last_ts = ts

        # Periodic OHLC refresh
        last_spx_candle_cnt += 1
        if last_spx_candle_cnt % 20 == 19:
            print('getting last SPX candle')
            spx_candle_data = get_spx_current_today_ohlc()

            if spx_candle_data is not None:
                candle = spx_candle_data["candles"][0]
                save_spx_ohlc(candle)

                print(
                    f"\n"
                    f'    {candle['close']:.2f} <<<<\n'
                    f"Saved SPX OHLC: "
                    f"O={candle['open']}, H={candle['high']}, "
                    f"L={candle['low']}, C={candle['close']}, "
                    f"ET={SPX_OHLC_DATA['timestamp_et']}"
                )
            else:
                print("No SPX candle data returned.")

        time.sleep(0.2)








# def do_work():
#     last_val = None
#     last_ts = None

#     last_spx_candle_cnt = 0

#     # wait for stream login
#     while stream_login_success is False:
#         time.sleep(2)
#         print(f'waiting for stream login')
#         pass


#     while True:
#         with SPX_LOCK:
#             val = SPX_VALUE
#             ts = SPX_TIMESTAMP

#         if val is not None and ts is not None:
#             if val != last_val or ts != last_ts:

#                 # Convert Schwab timestamp → ET datetime
#                 dt_et = datetime.fromtimestamp(ts / 1000, tz=UTC).astimezone(ET)

#                 # Current ET time
#                 now_et = datetime.now(tz=ET)

#                 # Compute elapsed time
#                 delta = now_et - dt_et

#                 # Convert to seconds.milliseconds
#                 elapsed_ms = delta.total_seconds() * 1000
#                 elapsed_str = f"{elapsed_ms/1000:.3f}"   # seconds.milliseconds

#                 # Format Schwab timestamp as hh:mm:ss.mmm
#                 ts_et = dt_et.strftime("%H:%M:%S.%f")[:-3]

#                 print(
#                     f"do work last SPX: {val:.2f}   "
#                     f"Timestamp: {ts_et}   "
#                     f"Elapsed: {elapsed_str} seconds   "
#                     # f"ts_et type:{type(ts_et)}"
#                 )

#                 last_val = val
#                 last_ts = ts

        
#         last_spx_candle_cnt += 1
#         if last_spx_candle_cnt % 20 == 19:
#             print(f'getting last spx candle')
#             spx_candle_data = get_spx_current_today_ohlc()
#             print(f'spx_candle_data type:{type(spx_candle_data)}, contents:\n{spx_candle_data}\n')
#         time.sleep(0.2)







# def do_work():
#     last_val = None
#     last_ts = None

#     while True:
#         with SPX_LOCK:
#             val = SPX_VALUE
#             ts = SPX_TIMESTAMP

#         if val is not None and ts is not None:
#             if val != last_val or ts != last_ts:
#                 ts_et = convert_schwab_timestamp_to_et(ts)


#                 # print(f"do work last SPX: {val}   Timestamp: {ts_et}        ({ts})")
#                 print(f"do work last SPX: {val}   Timestamp: {ts_et}  ts_et type:{type(ts_et)}")
#                 last_val = val
#                 last_ts = ts

#         time.sleep(0.2)




def extract_option_symbols(chain_dict):
    """
    Extracts all option symbols from callExpDateMap and putExpDateMap.
    Returns a list of strings.
    """

    symbols = []

    # --- Extract CALL symbols ---
    call_map = chain_dict.get("callExpDateMap", {})
    for exp_key, strikes in call_map.items():
        for strike, contracts in strikes.items():
            for contract in contracts:
                sym = contract.get("symbol")
                if sym:
                    symbols.append(sym)

    # --- Extract PUT symbols ---
    put_map = chain_dict.get("putExpDateMap", {})
    for exp_key, strikes in put_map.items():
        for strike, contracts in strikes.items():
            for contract in contracts:
                sym = contract.get("symbol")
                if sym:
                    symbols.append(sym)

    return symbols



def chain():


        # wait for stream login
    while stream_login_success is False:
        time.sleep(2)
        print('waiting for stream login')

    time.sleep(4)


    while(1):
        print(f'in chain, getting option chain')
        my_chain = get_spx_option_chain()
        # print(f'my_chain type:{type(my_chain)}')
        # print(f'my_chain type:{type(my_chain)}, data:\n{my_chain}')

        # with open("chain.json", "w") as f:
        #     json.dump(my_chain, f, indent=4)


        sym_list = extract_option_symbols(my_chain)
        print(f'sym_list type:{type(sym_list)}, data:\n{sym_list}')


        time.sleep(10)


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print("Starting MQTT, Schwab quote thread, and worker thread.")

    t1 = threading.Thread(target=mqtt_thread, daemon=True)
    t2 = threading.Thread(target=get_quote, daemon=True)
    t3 = threading.Thread(target=do_work, daemon=True)
    t4 = threading.Thread(target=chain, daemon=True)

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    while True:
        time.sleep(1)
