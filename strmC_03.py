import json
import threading
import requests
import logging
import time
import websocket
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import queue

# -----------------------------
# Time zones
# -----------------------------
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# -----------------------------
# Logging
# -----------------------------
logging.getLogger("websockets").setLevel(logging.CRITICAL)

logging.basicConfig(
    filename="strm2.log",
    level=logging.INFO,
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

SPX_OHLC_LOCK = threading.Lock()
SPX_OHLC_DATA = {
    "open": None,
    "high": None,
    "low": None,
    "close": None,
    "timestamp_et": None,
    "raw_candle": None
}

todays_epoch_time = None
rx_accessToken = None
stream_login_success = False
levelone_equities_sub_success = False

REQUESTS_GET_TIMEOUT = 10

# Thread-safe queue for WebSocket sends
WS_SEND_QUEUE = queue.Queue()


# -----------------------------
# Helpers: OHLC storage
# -----------------------------
def save_spx_ohlc(candle_dict):
    """
    Save SPX O/H/L/C and timestamp in a thread-safe global structure.
    candle_dict must be the Schwab candle: {'open':..., 'high':..., ...}
    """
    dt_utc = datetime.fromtimestamp(candle_dict["datetime"] / 1000, tz=timezone.utc)
    dt_et = dt_utc.astimezone(ET)

    with SPX_OHLC_LOCK:
        SPX_OHLC_DATA["open"] = candle_dict["open"]
        SPX_OHLC_DATA["high"] = candle_dict["high"]
        SPX_OHLC_DATA["low"] = candle_dict["low"]
        SPX_OHLC_DATA["close"] = candle_dict["close"]
        SPX_OHLC_DATA["timestamp_et"] = dt_et
        SPX_OHLC_DATA["raw_candle"] = candle_dict
        # print(f'saved spx OHLC')

    my_ohlc = SPX_OHLC_DATA
    print(f'saved 1 my_ohlc:{my_ohlc}')


def read_spx_ohlc():
    """
    Return a copy of the saved SPX OHLC data in a thread-safe manner.
    """

    # my_ohlc = SPX_OHLC_DATA
    # print(f'read 2 my_ohlc:{my_ohlc}')


    with SPX_OHLC_LOCK:
        return_dict = dict(SPX_OHLC_DATA)

    # print(f'read 3 my_ohlc return_dict:{return_dict}')

    return return_dict


# -----------------------------
# MQTT: receive Schwab credentials
# -----------------------------
def on_mqtt_message(client, userdata, msg):
    global CREDS, rx_accessToken

    try:
        payload = json.loads(msg.payload.decode())
        CREDS = payload
        CREDS_READY.set()
        rx_accessToken = CREDS.get("accessToken")
    except Exception as e:
        print("Error parsing MQTT payload:", e)


def mqtt_thread():
    client = mqtt.Client()
    client.on_message = on_mqtt_message

    client.connect("localhost", 1883, 60)
    client.subscribe("mri/creds/info")

    client.loop_forever()


# -----------------------------
# Schwab request builders
# -----------------------------
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


def build_add_opt_request(creds, syms):
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
                    # "fields": "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"  # works
                    # "fields": "0,1,2,3,4" # works
                    # "fields": "2,3,4" # does not work
                    "fields": "0,1,2,3,4,10,11,25,28,29,30,38"
                }
            }
        ]
    })


def convert_schwab_timestamp_to_et(ms_timestamp):
    """
    Convert Schwab millisecond timestamp to Eastern Time hh:mm:ss.mmm
    """
    dt_utc = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)
    dt_et = dt_utc.astimezone(ET)
    return dt_et.strftime("%H:%M:%S.%f")[:-3]


# -----------------------------
# SPX streaming message handler
# -----------------------------
def process_spx_message(msg):
    global SPX_VALUE, SPX_TIMESTAMP

    print(f'processing streaming message:{msg}')

    try:
        data = json.loads(msg)
        quote_list = data.get("data", [])
        if not quote_list:
            return

        quote = quote_list[0]
        ts = quote.get("timestamp")
        if ts is None:
            return

        ts_et = convert_schwab_timestamp_to_et(ts)

        content_list = quote.get("content", [])
        content = content_list[0] if content_list else {}

        last_price = content.get("3")

        with SPX_LOCK:
            SPX_TIMESTAMP = ts
            if last_price is not None:
                SPX_VALUE = last_price
                # print(f"Updated SPX_VALUE={SPX_VALUE}, ET={ts_et}")
            else:
                # print(f"SPX_TIMESTAMP updated (ET={ts_et}), last price unchanged")
                pass

    except Exception as e:
        print("Error processing SPX message:", e)


# -----------------------------
# WebSocket send helper (for other threads)
# -----------------------------
def enqueue_ws_message(msg):
    """
    Other threads call this to request a WebSocket send.
    The get_quote thread drains WS_SEND_QUEUE and calls ws.send().
    """

    print(f'queueing message:{msg}')
    WS_SEND_QUEUE.put(msg)


# -----------------------------
# Schwab WebSocket streaming
# -----------------------------
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
    global levelone_equities_sub_success

    CREDS_READY.wait()
    creds = CREDS

    ws_url = creds["streamerUrl"]
    print(f'ws_url: {ws_url}')

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

    # syms = ["$SPX", "$SPY"]
    syms = ["$SPX"]
    subs_msg = build_spx_request(creds, syms)
    print(f'subs_msg: {subs_msg}')

    def on_message(ws, msg):
        global stream_login_success
        global levelone_equities_sub_success

        # print(f'raw ws message:{msg}')

        try:
            data = json.loads(msg)

            if "response" in data:
                resp = data["response"][0]
                service = resp.get("service")
                command = resp.get("command")
                content = resp.get("content", {})
                code = content.get("code", -1)

                if service == "ADMIN" and command == "LOGIN":
                    stream_login_success = True

                    if code == 0:
                        print("LOGIN successful, sending SUBS")
                        ws.send(subs_msg)
                    else:
                        print("LOGIN failed:", content)
                        ws.close()
                    return

                if service == "QUOTE" and command == "SUBS":
                    if code == 0:
                        print("SUBS successful — streaming SPX now")
                    else:
                        print("SUBS failed:", content)
                        ws.close()
                    return
                
                if service == "LEVELONE_EQUITIES" and command == "SUBS":
                    msg = data["response"][0]["content"]["msg"]
                    if msg == "SUBS command succeeded":
                        # print("LEVELONE_EQUITIES Subscription succeeded")
                        levelone_equities_sub_success = True
                    else:
                        print(f"LEVELONE_EQUITIES Subscription failed: {msg}")
                        levelone_equities_sub_success = False

                    # print(f'gq1 levelone_equities_sub_success:{levelone_equities_sub_success}')


            if "data" in data:
                process_spx_message(msg)

        except Exception as e:
            print("Error in on_message:", e)

        # print(f'gq2 levelone_equities_sub_success:{levelone_equities_sub_success}')

    def on_error(ws, err):
        print("WS error:", err)

    def on_close(ws, code, msg):
        print(f"WS closed: {code} {msg}")

    while True:
        try:
            print("Connecting to Schwab streamer:", ws_url)

            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            wst = threading.Thread(target=ws.run_forever, daemon=True)
            wst.start()

            time.sleep(2)

            print("Sending LOGIN request.")
            ws.send(login_msg)

            # Main loop: keep connection alive and drain send queue
            while wst.is_alive():
                try:
                    # Drain queued messages (from other threads)
                    while not WS_SEND_QUEUE.empty():
                        next_msg = WS_SEND_QUEUE.get_nowait()
                        # print(f"Sending queued WS message: {next_msg}")
                        ws.send(next_msg)
                except Exception as e:
                    print("Error sending queued message:", e)

                time.sleep(0.2)

        except Exception as e:
            print("Exception in get_quote():", e)

        print("Reconnecting in 5 seconds...")
        time.sleep(5)


# -----------------------------
# SPX OHLC + option chain helpers
# -----------------------------
def get_today_in_epoch():
    global todays_epoch_time

    try:
        now = datetime.now(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        todays_epoch_time = int((now - epoch).total_seconds() * 1000.0)
    except Exception as e:
        info_str = f'get_today_in_epoch error: {e}'
        logging.error(info_str)
        print(info_str)


def calc_chain_strike_cnt(spx_low, spx_high):
    if spx_low is None or spx_high is None:
        return None

    c_distance = abs(spx_high - spx_low)
    print(f'c_distance: {c_distance}')
    my_chain_strike_cnt = int(c_distance / 5) + 50
    # my_chain_strike_cnt = int(c_distance / 5) + 30
    print(f'my_chain_strike_cnt: {my_chain_strike_cnt}')
    return my_chain_strike_cnt


def get_spx_option_chain():
    global rx_accessToken

    local_accessToken = rx_accessToken
    ohlc = read_spx_ohlc()
    print(f'gsoc ohlc type:{type(ohlc)}, value:{ohlc}')

    high = ohlc.get('high')
    low = ohlc.get('low')

    print(f'gsoc high:{high}, low:{low}')

    local_strike_cnt = calc_chain_strike_cnt(high, low)

    if local_accessToken is None or local_strike_cnt is None:
        print(f'Dont have rx_accessToken:{local_accessToken} or valid strike count {local_strike_cnt}')
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
            raise ValueError("Error decoding JSON response from Schwab API")

        if not isinstance(spx_chain, dict):
            raise ValueError("SPX chain response is not a dictionary")

        return spx_chain

    except requests.exceptions.ConnectionError as conn_err:
        raise RuntimeError("Network error: Unable to connect to Schwab API") from conn_err

    except requests.exceptions.Timeout as timeout_err:
        raise RuntimeError("Request timeout: Schwab API took too long to respond") from timeout_err

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err}, response: {response.text}")
        raise RuntimeError(f"HTTP error occurred: {http_err}") from http_err

    except Exception as err:
        raise RuntimeError(f"Unexpected error occurred: {err}") from err


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
    end_date = todays_epoch_time + 86400000

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


# -----------------------------
# Worker thread: print SPX updates + periodic OHLC
# -----------------------------
def do_work():
    last_val = None
    last_ts = None
    last_spx_candle_cnt = 0

    while not stream_login_success:
        time.sleep(2)
        print('do_work waiting for stream login')

    while True:
        with SPX_LOCK:
            val = SPX_VALUE
            ts = SPX_TIMESTAMP

        if val is not None and ts is not None:
            if val != last_val or ts != last_ts:
                dt_utc = datetime.fromtimestamp(ts / 1000, tz=UTC)
                dt_et = dt_utc.astimezone(ET)

                now_et = datetime.now(tz=ET)
                delta = now_et - dt_et
                elapsed_ms = delta.total_seconds() * 1000
                elapsed_str = f"{elapsed_ms / 1000:.3f}"

                ts_et = dt_et.strftime("%H:%M:%S.%f")[:-3]

                print(
                    f"do work last SPX: {val:.2f}   "
                    f"Timestamp: {ts_et}   "
                    f"Elapsed: {elapsed_str} seconds"
                )

                last_val = val
                last_ts = ts

        last_spx_candle_cnt += 1
        if last_spx_candle_cnt % 20 == 2:
            print('getting last SPX candle')
            spx_candle_data = get_spx_current_today_ohlc()
            print(f'last SPX candle data:{spx_candle_data}')


            if spx_candle_data is not None:
                candle = spx_candle_data["candles"][0]

                # print(f'saving candle {candle}')
                save_spx_ohlc(candle)

                ohlc_copy = read_spx_ohlc()
                print(
                    f"\n"
                    f"    {candle['close']:.2f} <<<<\n"
                    f"Saved SPX OHLC: "
                    f"O={candle['open']}, H={candle['high']}, "
                    f"L={candle['low']}, C={candle['close']}, "
                    f"ET={ohlc_copy['timestamp_et']}"
                )
            else:
                print("No SPX candle data returned.")

        time.sleep(0.2)


# -----------------------------
# Option chain helpers
# -----------------------------
def extract_option_symbols(chain_dict):
    """
    Extracts all option symbols from callExpDateMap and putExpDateMap.
    Returns a list of strings.
    """
    symbols = []

    call_map = chain_dict.get("callExpDateMap", {})
    for strikes in call_map.values():
        for contracts in strikes.values():
            for contract in contracts:
                sym = contract.get("symbol")
                if sym:
                    symbols.append(sym)

    put_map = chain_dict.get("putExpDateMap", {})
    for strikes in put_map.values():
        for contracts in strikes.values():
            for contract in contracts:
                sym = contract.get("symbol")
                if sym:
                    symbols.append(sym)

    return symbols

def normalize_option_symbols(sym_list):
    return [" ".join(s.split()) for s in sym_list]


def chain_task():

    global levelone_equities_sub_success


    while not stream_login_success:
        time.sleep(2)
        print('chain waiting for stream login')

    print('chain good for stream login')



    while not levelone_equities_sub_success:
        time.sleep(2)
        print(f'chain waiting for levelone_equities_sub_success:{levelone_equities_sub_success}')

    print('chain good for levelone equities sub success')

    candle_data = False
    while not candle_data:
        my_candle = read_spx_ohlc()

        if my_candle.get("raw_candle") is None:
            print("raw_candle is None")
        else:
            print("raw_candle has a value")
            candle_data = True
            break

        time.sleep(2)

    print('chain good for candle data')





    

    time.sleep(1)

    while True:
        print('in chain, getting option chain')
        my_chain = get_spx_option_chain()
        if my_chain is None:
            print("No option chain data returned.")
        else:
            chain_sym_list = extract_option_symbols(my_chain)


            add_opt_payload = build_add_opt_request(CREDS, chain_sym_list)
            enqueue_ws_message(add_opt_payload)


        time.sleep(60)


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print("Starting MQTT, Schwab quote thread, worker thread, and chain thread.")

    t1 = threading.Thread(target=mqtt_thread, daemon=True)
    t2 = threading.Thread(target=get_quote, daemon=True)
    t3 = threading.Thread(target=do_work, daemon=True)
    t4 = threading.Thread(target=chain_task, daemon=True)

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    while True:
        time.sleep(1)


"""
Streaming option fields
0	Symbol 
1	Description
2	Bid Price	 
3	Ask Price	 
4	Last Price	 
5	High Price
6	Low Price
7	Close Price
8	Total Volume
9	Open Interest 
10	Volatility
11	Money Intrinsic Value
12	Expiration Year 	 	 	 
13	Multiplier	 	 	 	 
14	Digits	 	 	 
15	Open Price	 	 
16	Bid Size
17	Ask Size
18	Last Size
19	Net Change
20	Strike Price	 
21	Contract Type	 	 	 	 
22	Underlying	 	 	 	 
23	Expiration Month	 	 	 	 
24	Deliverables	 	 	 	 
25	Time Value	 	 	 
26	Expiration Day	 	 	 	 
27	Days to Expiration	 	 	 	 
28	Delta 	 	 	 
29	Gamma	 	 	 	 
30	Theta	 	 	 	 
31	Vega 	 	 	 
32	Rho 	 	 	 
33	Security Status	
34	Theoretical Option Value 	 	 	 
35	Underlying Price	 	 	 	 
36	UV Expiration	 	 	 	 
37	Mark Price	 
38	Quote Time in Long	
39	Trade Time in Long	
40	Exchange
41	Exchange Name	 
42	Last Trading Day	 
43	Settlement Type 
44	Net Percent Change
45	Mark Price Net Change
46	Mark Price Percent Change
47	Implied Yield	 	 	 	 
48	isPennyPilot	 	 	 	 
49	Option Root 	 	 	 
50	52 Week High	 	 	 	 
51	52 Week Low	 	 	 	 
52	Indicative Ask Price
53	Indicative Bid Price
54	Indicative Quote Time	 
55	Exercise Type 
"""