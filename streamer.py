


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





global rx_streamerUrl
rx_streamerUrl = None
global rx_accessToken
rx_accessToken = None
global rx_refreshToken
rx_refreshToken = None
global rx_acctHash
rx_acctHash = None
global rx_channel
rx_channel = None
global rx_correlId
rx_correlId = None
global rx_customerId
rx_customerId = None
global rx_functionId
rx_functionId = None

streamer_socket_url = None
sch_client_customer_id = None
sch_client_correl_id = None
sch_client_channel = None
sch_client_function_id = None

call_strike_list = []  # Global list for call options
put_strike_list = []   # Global list for put options

strike_list_lock = threading.Lock()

global websocket
websocket = None  # Global WebSocket connection

REQUESTS_GET_TIMEOUT = 10


# Global quit flag
quit_flag = False

tokens_file_mod_date = None

global get_quote_fail_count
get_quote_fail_count = 0


get_current_day_history_lock = threading.Lock()
chain_data_lock = threading.Lock()




# Set up logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# # Configure logging to write only to a file
logging.basicConfig(
    filename="mri_log.log",  # Log file name
    level=logging.INFO,  # Set logging level
    format="%(asctime)s - %(levelname)s - %(message)s"
)




mqtt_publish_lock = threading.Lock()
global mqtt_client_tx
mqtt_client_tx = None

# MQTT Broker Details
BROKER_ADDRESS = "localhost"
PORT_NUMBER = 1883
CREDS_INFO_TOPIC = "mri/creds/info"

# ------------------ MQTT Client ------------------

def on_connect(client, userdata, flags, rc):
    """ Callback function triggered when the client connects to the broker """
    print(f"Connected to MQTT Broker with result code {rc}")
    client.subscribe(CREDS_INFO_TOPIC)
    print(f"Subscribed to topic: {CREDS_INFO_TOPIC}")

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










def mqtt_services():
    """ Runs the MQTT client in a separate thread """
    global quit_flag
    global mqtt_client_tx

    client = mqtt.Client()
    # client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client_tx = client

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

# ------------------ Schwab Streaming ------------------


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


async_mqtt_publish_lock = asyncio.Lock()

# called by async functions
async def aio_publish_quote(topic, payload):
    global mqtt_client_tx

    if not mqtt_client_tx:
        print(f'could not aio publish quote, mqtt_client_tx is None')


    async with async_mqtt_publish_lock:
        loop = asyncio.get_running_loop()

        try:
            loop.call_soon_threadsafe(mqtt_client_tx.publish, topic, payload)  # Thread-safe publish
        except Exception as e:
            info_str = f'2010 Error in aio MQTT publish: {e}'
            print(info_str)
            logging.error(info_str)



sync_mqtt_publish_lock = threading.Lock()

def sync_publish_quote(topic, payload):
    global mqtt_client_tx

    with sync_mqtt_publish_lock:
        try:
            mqtt_client_tx.publish(topic, payload)  # Standard locking
        except Exception as e:
            info_str = f'2020 Error in sync MQTT publish: {e}'
            print(info_str)
            logging.error(info_str)




CREDS_REQUEST_TOPIC = "mri/creds/request"

async def aio_publish_request_creds():

    topic = CREDS_REQUEST_TOPIC
    payload = " "
    await aio_publish_quote(topic, payload)
    pass
        



# async def wait_for_rx_credentials():
#     """ Asynchronous function to wait for all rx_ credentials to be set """


#     await aio_publish_request_creds()

#     while True:
#         if (rx_streamerUrl is not None
#             and rx_accessToken is not None
#             and rx_refreshToken is not None
#             and rx_acctHash is not None
#             and rx_channel is not None
#             and rx_correlId is not None
#             and rx_customerId is not None
#             and rx_functionId is not None):

#             print("All rx_ credentials initialized")
#             return

#         else:
#             await asyncio.sleep(2)  # Non-blocking wait
#             print("rx_ credentials not initialized yet")
#             await aio_publish_request_creds()


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



async def get_user_preferences():
    global streamer_socket_url
    global sch_client_customer_id
    global sch_client_correl_id
    global sch_client_channel
    global sch_client_function_id

    success_flag = False

    current_time_str = datetime.now().strftime('%H:%M:%S')
    print(f'\ngetting userPreference at {await aio_get_current_time_str()}')

    url = "https://api.schwabapi.com/trader/v1/userPreference"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {rx_accessToken}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=REQUESTS_GET_TIMEOUT) as response:
            print(f'8061 userPreference:{response.status}')
            userPreference_data = await response.json()  # Use await to process JSON
           

    if response.status == 200:
        try:
            userPreference_data = await response.json()
            streamer_info = userPreference_data.get("streamerInfo", [{}])[0]

            streamer_socket_url = streamer_info.get("streamerSocketUrl", "")
            sch_client_customer_id = streamer_info.get("schwabClientCustomerId", "")
            sch_client_correl_id = streamer_info.get("schwabClientCorrelId", "")
            sch_client_channel = streamer_info.get("schwabClientChannel", "")
            sch_client_function_id = streamer_info.get("schwabClientFunctionId", "")

            print(f'\n145 userPreference settings:')
            print(f"Streamer Socket URL: {streamer_socket_url}")
            print(f"Schwab Client Customer ID: {sch_client_customer_id}")
            print(f"Schwab Client Correl ID: {sch_client_correl_id}")
            print(f"Schwab Client Channel: {sch_client_channel}")
            print(f"Schwab Client Function ID: {sch_client_function_id}")

            success_flag = True

        except (KeyError, TypeError, ValueError) as e:
            info_str = f'2030 Error parsing user preferences: {e}'
            print(info_str)
            logging.error(info_str)


        return success_flag            


    else:
        info_str = f'3375 userPreference Error {response.status}: {await response.text()}'
        print(info_str)
        logging.error(info_str)

    return success_flag



async def aio_get_current_time_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")





async def aio_publish_raw_streamed_quote(data):
    global time_since_last_stream_pub

    json_str = json.dumps(data)
    # print(f'in aio_publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')

    topic = "schwab/stream"
    await aio_publish_quote(topic, json_str)
    time_since_last_stream_pub = 0
    pass




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

async def safe_websocket_reconnect():
    """ Safely closes and reconnects WebSocket before restarting receive_messages() """
    global websocket

    async with websocket_lock:  # Ensure exclusive access to websocket
        if websocket and not websocket.closed:
            try:
                await websocket.close()
                await asyncio.sleep(1)
            except websockets.exceptions.ConnectionClosedError:
                info_str = "1010 WebSocket was already closed, skipping close."
                logging.warning(info_str)
                print(info_str)
            except Exception as e:
                info_str = f'1020 Unexpected error while closing WebSocket: {e}'
                logging.warning(info_str)
                print(info_str)

        logging.info("1030 Calling exponential_backoff_reconnect()")
        await exponential_backoff_reconnect()
        await asyncio.sleep(3)


global rx_msg_func_call_count
rx_msg_func_call_count = 0  # Initialize global call counterspxspx



async def receive_messages(duration=60):
    """ Continuously receives and processes messages from the WebSocket for `duration` seconds. """
    global websocket, rx_msg_func_call_count
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

                        if invalid_message_count >= max_invalid_messages:
                            logging.error("1050 Too many invalid messages received, forcing WebSocket reconnection.")
                            print(f'9036 calling exp back reconn')
                            await exponential_backoff_reconnect()
                            print("Restarting receive messages after forced reconnect.")
                            await asyncio.sleep(3)
                            print(f'37100 calling receive messages')
                            await receive_messages()
                            return

                    else:
                        await process_received_message(response)
                        invalid_message_count = 0

                except asyncio.exceptions.IncompleteReadError:
                    logging.warning("1060 Incomplete read error. Attempting reconnect.")
                    print("Incomplete read error. Attempting reconnect.")
                    await safe_websocket_reconnect()
                    print(f'37101 calling receive messages')
                    await receive_messages()
                    return

                except websockets.exceptions.ConnectionClosedError:
                    logging.warning("1070 WebSocket connection lost unexpectedly. Attempting reconnect.")
                    print("WebSocket connection lost unexpectedly. Attempting reconnect.")
                    await safe_websocket_reconnect()
                    print(f'37102 calling receive messages after connecion lost/reconnect')
                    await receive_messages()
                    return

                except Exception as e:
                    logging.warning(f"1080 General WebSocket error: {e}, attempting reconnect.")
                    print(f"1082 General WebSocket error: {e}, attempting reconnect.")
                    await safe_websocket_reconnect()
                    print(f'37103 calling receive messages')
                    await receive_messages()
                    return

    except TimeoutError:
        # This error is expected with  asyncio.timeout() 
        # logging.info(f"1080 Message receiving stopped after {duration} seconds.")
        # print(f"Message receiving stopped after {duration} seconds.")
        pass

    except Exception as e:
        info_str = f'1090 Unexpected error receiving messages: {e}'
        logging.error(info_str)
        print(info_str)

















# async def receive_messages(duration=60):
#     """ Continuously receives and processes messages from the WebSocket for `duration` seconds. """
#     global websocket, rx_msg_func_call_count
#     invalid_message_count = 0  # Track consecutive invalid messages
#     max_invalid_messages = 5   # Threshold before forcing reconnect

#     # print(f'rcv msg duration:{duration}')



#     # Increment call counter
#     rx_msg_func_call_count += 1
#     # print(f'rx_msg_func_call_count:{rx_msg_func_call_count} at {await aio_get_current_time_str()} Local Time')


#     try:
#         start_time = time.time()
#         while time.time() - start_time < duration:
#             try:
#                 response = await websocket.recv()

#                 if not await aio_is_valid_response(response):
#                     logging.warning("1100 Received invalid message format")
#                     print("0930 Received invalid message format")
#                     invalid_message_count += 1  # Increment invalid message count

#                     # If too many invalid messages occur in a row, trigger reconnect
#                     if invalid_message_count >= max_invalid_messages:
#                         logging.error("1110 Too many invalid messages received, forcing WebSocket reconnection.")
#                         print(f'9036 calling exp back reconn')
#                         await exponential_backoff_reconnect()
#                         print("Restarting receive messages after forced reconnect.")
#                         await asyncio.sleep(3)  # Short pause before resuming
#                         print(f'37100 calling receive messages')
#                         await receive_messages()  # Restart WebSocket listening
#                         return  # Exit current loop after reconnect

#                 else:
#                     # print(f'0931 received api valid resonse, response:{response}, processing')
#                     await process_received_message(response)
#                     # print(f'0932 returned from process received message')
#                     invalid_message_count = 0  # Reset count on valid message

#             except asyncio.exceptions.IncompleteReadError:
#                 info_str = "1120 Incomplete read error. Attempting reconnect."
#                 logging.warning(info_str)
#                 print(info_str)
#                 await safe_websocket_reconnect()
#                 print(f'37101 calling receive messages')
#                 await receive_messages()
#                 return  

#             except websockets.exceptions.ConnectionClosedError:
#                 info_str = f'1130 WebSocket connection lost unexpectedly at {await aio_get_current_time_str()}. Attempting reconnect.'
#                 logging.warning(info_str)
#                 print(info_str)
#                 await safe_websocket_reconnect()
#                 print(f'37102 calling receive messages')
#                 await receive_messages()
#                 return

#             except Exception as e:
#                 info_str = f"1140 General WebSocket error: {e}, attempting reconnect."
#                 logging.warning(info_str)
#                 print(info_str)
#                 await safe_websocket_reconnect()
#                 print(f'37103 calling receive messages')
#                 await receive_messages()
#                 return

#     except asyncio.TimeoutError:
#         logging.warning("1150 Timeout while receiving messages.")

#     except Exception as e:
#         logging.error(f"1160 Unexpected error receiving messages: {e}")






async def exponential_backoff_reconnect():
    """ Implements retry logic with exponential backoff for WebSocket reconnection. """
    global websocket

    # Ensure proper WebSocket closure before reconnecting
    if websocket and not websocket.closed:
        try:
            print("2402 previous websocket was not closed, attempting to close it.")
            await websocket.close()
            await asyncio.sleep(1)  # Allow time for closure before reconnecting
        except websockets.exceptions.ConnectionClosedError:
            logging.warning("1170 WebSocket already closed, skipping close.")
        except Exception as e:
            info_str = f'1180 Unexpected error while closing WebSocket: {e}'
            logging.error(info_str)
            print(info_str)

    # Reset websocket instance before fresh reconnection
    websocket = None  

    backoff_times = [1, 2, 4, 8]  # Exponential backoff sequence in seconds
    reconnect_retry_cnt = 0  # Keep scoped locally

    for attempt, delay in enumerate(backoff_times, 1):
        logging.info(f"1190  Reconnection attempt {attempt}, waiting {delay} seconds...")
        await asyncio.sleep(delay)  # Non-blocking sleep

        try:
            reconnect_retry_cnt += 1

            logging.info(f"1200 Attempting streamer login, retry count: {reconnect_retry_cnt}")
            await login_to_schwab_streamer()  # Attempt login

            info_str = f'1210 Reconnection successful at {await aio_get_current_time_str()}'
            logging.info(info_str)
            return  # Exit function on success
        except websockets.exceptions.ConnectionClosedError as e:
            info_str = f'1220 WebSocket closed error during attempt {attempt}: {e}'
            logging.error(info_str)
            print(info_str)

        except asyncio.TimeoutError:
            info_str = f'1230 Timeout occurred during attempt {attempt}.'
            logging.error(info_str)
            print(info_str)

        except Exception as e:
            info_str = f'1240 General failure on attempt {attempt}: {e}'
            logging.error(info_str)
            print(info_str)

    info_str = f'1250 Max reconnection attempts reached, shutting down.'
    logging.error(info_str)
    print(info_str)









async def logout_from_schwab_streamer():
    global websocket

    if not websocket:
        return
    
    if not websocket.open:
        return
    

    # # Close the old WebSocket if it's still open
    # if websocket and not websocket.closed:
    #     print("2404 previous websocket was not closed, Closing previous WebSocket connection before reconnecting.")
    #     await websocket.close()
    #     await asyncio.sleep(2)  # Give time for closure before reconnecting
        

    # # Establish new WebSocket connection
    # websocket = await websockets.connect(streamer_socket_url)


    api_logout_form = {
        "requests": [
            {
                "requestid": "1",
                "service": "ADMIN",
                "command": "LOGOUT",
                "SchwabClientCustomerId": rx_customerId,
                "SchwabClientCorrelId": rx_correlId,
            }
        ]
    }



    try:
        # websocket = await websockets.connect(streamer_socket_url)


        await websocket.send(json.dumps(api_logout_form))
        response = await websocket.recv()

        resp_json = json.loads(response)
        print(f'\n9673 logout connect resp_json:{resp_json}\n')

        # Extract 'code' value from the first item in 'response'
        if "response" in resp_json and resp_json["response"]:
            resp_code = resp_json["response"][0]["content"].get("code", -1)  # Default to -1 if 'code' is missing
        else:
            resp_code = -1  # Indicate an invalid response structure

        print(f'9674 Login response code: {resp_code}')

        # Determine success based on resp_code
        if resp_code == 0:
            print(f'1260  API Login successful at {await aio_get_current_time_str()}')
        else:
            print(f"1270  API Login NOT successful, code: {resp_code}")
            logging.error(f"1280 API Login failed with code {resp_code}")


    except Exception as e:
        info_str = f"1290 Error logging out Schwab API: {e}"
        print(info_str)
        logging.error(info_str)






async def login_to_schwab_streamer():
    global websocket

    success_flag = False

    # Close the old WebSocket if it's still open
    if websocket and not websocket.closed:
        print("2404 previous websocket was not closed, Closing previous WebSocket connection before reconnecting.")
        await websocket.close()
        await asyncio.sleep(2)  # Give time for closure before reconnecting
        

    # Establish new WebSocket connection
    websocket = await websockets.connect(streamer_socket_url)


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
        websocket = await websockets.connect(streamer_socket_url)
        await websocket.send(json.dumps(api_login_form))
        response = await websocket.recv()

        resp_json = json.loads(response)
        # print(f'\n9973 login connect resp_json:{resp_json}\n')

        # Extract 'code' value from the first item in 'response'
        if "response" in resp_json and resp_json["response"]:
            resp_code = resp_json["response"][0]["content"].get("code", -1)  # Default to -1 if 'code' is missing
        else:
            resp_code = -1  # Indicate an invalid response structure

        print(f'Login response code: {resp_code}')

        # Determine success based on resp_code
        if resp_code == 0:
            print(f'1300 API Login successful at {await aio_get_current_time_str()}')
            success_flag = True
        else:
            print(f"1310 API Login NOT successful, code: {resp_code}")
            logging.error(f"1320 API Login failed with code {resp_code}")


    except websockets.exceptions.ConnectionClosedError as e:
        info_str = f"1330  WebSocket connection closed unexpectedly: {e}, calling exponential_backoff_reconnect()"
        print(info_str)
        logging.error(info_str)
        print(f'9032 calling exp back reconn')
        await exponential_backoff_reconnect()
        print(f'110 returned from exponential_backoff_reconnect')

        print("Restarting receive_messages() after successful reconnect.")
        print(f'37104 calling receive messages')
        await receive_messages()  # Restart WebSocket message loop


    except Exception as e:
        info_str = f"1340 Error logging into Schwab API: {e}"
        print(info_str)
        logging.error(info_str)

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

        info_str = '1370 subscribe_level_one_optionss() was valid'
        print(info_str)
        logging.info(info_str)





async def subscribe_level_one_equities(customer_id, correl_id, symbols, fields):
    global websocket

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

    await websocket.send(json.dumps(subscription_request))

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
        logging.error(info_str)
    else:
        info_str = f'1410 subscribe_level_one_equities() succeeded'
        print(info_str)
        logging.info(info_str)
        success_flag = True

    return success_flag



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

        


    print(f'17912 doing option subscriptions at {await aio_get_current_time_str()}, Local Time')

    call_list_copy = await aio_get_call_strike_list()
    put_list_copy = await aio_get_put_strike_list()

    try:
        await subscribe_level_one_options(sch_client_customer_id, sch_client_correl_id, call_list_copy, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32")
        await subscribe_level_one_options(sch_client_customer_id, sch_client_correl_id, put_list_copy, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32")

        last_subscription_time = time.time()  # Reset timestamp after subscription
        print(f' initial last_subscription_time:{last_subscription_time}')
        success_flag = True


    except Exception as e:
        info_str = f'1430 Error in initial subscribe_level_one_options(): {e}'
        logging.error(info_str)
        print(info_str)

    return success_flag





async def streamer_services():
    """ Infinite work loop that returns if quit_flag is set to True """
    global quit_flag
    print("Streamer services started...")




    # streamer service outer (session) loop
    while not quit_flag: # outer (session) loop

        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)
        # market_open_flag, current_eastern_time, seconds_to_next_minute = await asyncio.to_thread(market_open.is_market_open2(open_offset=4, close_offset=0))

        # # print(f'Streamer checking market open, market_open_flag:{market_open_flag}')
        # wait_market_open_cnt = 0
        # while not market_open_flag:
        #     time.sleep(1)
        #     wait_market_open_cnt += 1
        #     if wait_market_open_cnt % 60 == 3:
        #         print(f'streamer: outer loop, waiting for market open {await aio_get_current_time_str()} (Local)')
        #     market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)
        #     # market_open_flag, current_eastern_time, seconds_to_next_minute = await asyncio.to_thread(market_open.is_market_open2(open_offset=4, close_offset=0))




        print("Streamer waiting for mqtt client to be created")
        wait_mqtt_cnt = 0
        while mqtt_client_tx == None:
            wait_mqtt_cnt += 1
            await asyncio.sleep(1)  # Non-blocking sleep for 1 second
            if wait_mqtt_cnt % 20 == 19:
                info_str = f'Streamer still waiting for mqtt client to be created, cnt:{wait_mqtt_cnt}'
                print(info_str)
                logging.warning(info_str)




        try:

            await wait_for_rx_credentials(timeout=30) 

        except Exception as e:
            info_str = f"1440  wait_for_rx_credentials failed: {e}"
            print(info_str)
            logging.error(info_str)
            return
        
        user_preferences_success = await get_user_preferences()

        if not user_preferences_success:
            info_str = f'3853 user preferences failed'
            logging.info(info_str)
            print(info_str)



        logging.info(f"1450  Initial attempt streamer login")
        login_success = await login_to_schwab_streamer()  # Attempt login

        if not login_success:
            info_str = f'3857 login failed'
            logging.info(info_str)
            print(info_str)



        sub_LOE_success = await subscribe_level_one_equities(sch_client_customer_id, sch_client_correl_id, "$SPX", "0,1,2,3,4,5,8,10")

        if not sub_LOE_success:
            info_str = f'3873 subscribe level one equities failed'
            logging.info(info_str)
            print(info_str)



        # print(f'Streamer checking market open, market_open_flag:{market_open_flag}')
        wait_market_open_cnt = 0
        while not market_open_flag:
            wait_market_open_cnt += 1
            # print(f'37273 calling receive messages')
            await receive_messages(duration=10)
            # print(f'37274 returned  receive messages wait_cnt:{wait_put_call_cnt}, at {await aio_get_current_time_str()} (Local)')

            if wait_market_open_cnt % 6 == 5:
                print(f'streamer: outer loop, waiting for market open {await aio_get_current_time_str()} (Local)')
            
            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)
            # market_open_flag, current_eastern_time, seconds_to_next_minute = await asyncio.to_thread(market_open.is_market_open2(open_offset=4, close_offset=0))





        # wait for the call/put lists to be initialized
        wait_put_call_cnt = 0
        while wait_put_call_cnt < 30:
            print(f'waiting for strike lists to be initialized, wait_put_call_cnt:{wait_put_call_cnt}')
            if len(call_strike_list) > 0 and len(put_strike_list) > 0:
                print(f'strike lists have been initialized, wait_put_call_cnt:{wait_put_call_cnt}')
                break

            await asyncio.sleep(1)



        strm_loop_cnt = 0

        while not quit_flag:
            strm_loop_cnt += 1
            # await asyncio.sleep(1)  # Simulate work
            print(f'Streamer work loop, strm_loop_cnt:{strm_loop_cnt}')


            print(f'88301 calling subscribe to options')

            sub_opt_success = await subscribe_to_options()
            if not sub_opt_success:
                info_str = f'3882 subscribe options failed'
                logging.warning(info_str)
                print(info_str)

            print(f'88302 returned from subscribe to options')




            print(f'37105 calling receive messages')
            await receive_messages()

            print(f'37106 returned from receive messages')




        print("Streamer services terminating...")

    # while not quit_flag: # outer (session) loop




async def schwab_setup():
    """ Creates, starts, and joins the streamer_services coroutine """
    global quit_flag
    print("Schwab setup starting streamer services...")

    await streamer_services()  # Run streamer until quit_flag is True

    print("Schwab setup terminating...")



# ------------------ Schwab quote polling ------------------

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





# def get_spx_current_today_ohlc():
#     global get_current_day_history_lock
#     global spx_open
#     global spx_high
#     global spx_low
#     global spx_close
#     global ohlc_get_time
#     global chain_strike_cnt

#     spx_day_ohlc = None

#     if todays_epoch_time == None or rx_accessToken == None:
#         print(f'unable to get ohlc, todays_epoch_time:{todays_epoch_time}, rx_accessToken{rx_accessToken}')
#         return spx_day_ohlc


#     try:

#         url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
#         params = {
#             "symbol": "$SPX",
#             "periodType": "month",
#             "period": 1,
#             "frequencyType": "daily",
#             "frequency": 1,
#             "startDate": todays_epoch_time,  # Using stored variable
#             "endDate": todays_epoch_time,    # Using stored variable
#             "needExtendedHoursData": "false",
#             "needPreviousClose": "false"
#         }

#         headers = {
#             "accept": "application/json",
#             "Authorization": f"Bearer {rx_accessToken}"
#         }

#         response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

#         # print(f'8071 pricehistory:{response.status_code}')
#         # print(f'8072 data:{response.json()}')






#         # print(f'775 response.status_code type:{type(response.status_code)}, data:{response.status_code}')
#         spx_day_ohlc = response.json()  # Parses JSON response if successful
#         # print(f'776 spx_day_ohlc type:{type(spx_day_ohlc)}, data:\n{spx_day_ohlc}') 


#         # Ensure 'candles' and 'empty' exist and 'empty' is False
#         if 'candles' in spx_day_ohlc and 'empty' in spx_day_ohlc and not spx_day_ohlc['empty']:
#             first_candle = spx_day_ohlc['candles'][0]  # Extract first candle entry

#             # if spx_open != None:
#             #     print(f'\nold high: {spx_high:.2f}       low:{spx_low:.2f}')

#             # Assign values to variables
#             spx_open = first_candle['open']
#             spx_high = first_candle['high']
#             spx_low = first_candle['low']
#             spx_close = first_candle['close']

#             # print(f'\nnew high: {spx_high:.2f}       low:{spx_low:.2f}\n')

#             ohlc_get_time = datetime.now()
#             ohlc_get_time_str = ohlc_get_time.strftime('%y-%m-%d %H:%M:%S')

#             day_high_distance = abs(spx_close - spx_high)
#             day_low_distance = abs(spx_close - spx_low)
#             max_distance = max(day_high_distance, day_low_distance)
#             # print(f'max_distance:{max_distance:.2f}')

#             chain_strike_cnt = (int)(max_distance / 5) + 50

#             current_time = datetime.now()
#             current_time_str = current_time.strftime('%H:%M:%S')
#             print(f'285 chain_strike_cnt:{chain_strike_cnt} at {current_time_str}')
            

#             # # Print extracted values for verification
#             # print(f"SPX Open type:{type(spx_open)}, value:{spx_open}")
#             # print(f"SPX High type:{type(spx_high)}, value:{spx_high}")
#             # print(f"SPX Low type:{type(spx_low)}, value:{spx_low}")
#             # print(f"SPX Close type:{type(spx_close)}, value:{spx_close}")
#             # print(f"OHLC Datetime Object: {ohlc_get_time}")
#             # print(f"OHLC Datetime String: {ohlc_get_time_str} local time")



#         return spx_day_ohlc



#     except Exception as e:
#         print(f'2974 pricehistory error:{e}, could not get SPX o/h/l/c')
#         pass
#         raise # This indicates an exception to the calling function so that it knows the call failed
















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



















# def parse_spx_chain(spx_chain):
#     """ Parses the SPX option chain and extracts relevant strike symbols """
#     global call_strike_list, put_strike_list  # Access global lists
#     global strike_list_lock


#     print(f'in parse_spx chain')
    
#     for exp_date, strikes in spx_chain.get("callExpDateMap", {}).items():
#         for strike_price, options in strikes.items():
#             for option in options:
#                 symbol = option.get("symbol", "")

#                 # print(f'in parse_spx_chain() for calls, found symbol:{symbol}')

#                 if symbol.startswith("SPXW  ") and "C0" in symbol:
#                     with strike_list_lock:
#                         if symbol not in call_strike_list:  # Ensure unique entries
#                             # print(f'adding {symbol} to call_strike_list')
#                             call_strike_list.append(symbol)
                

#     # print(f'new call_strike_list type{type(call_strike_list)}, size:{len(call_strike_list)}, data:\n{call_strike_list}')
#     print(f'new call_strike_list size:{len(call_strike_list)}')


#     for exp_date, strikes in spx_chain.get("putExpDateMap", {}).items():
#         for strike_price, options in strikes.items():
#             for option in options:
#                 symbol = option.get("symbol", "")

#                 # print(f'in parse_spx_chain() for puts, found symbol:{symbol}')
                
#                 # Check if symbol starts with "SPXW  " and contains "C0" or "P0"
#                 if symbol.startswith("SPXW  ") and "P0" in symbol:
#                     with strike_list_lock:
#                         if symbol not in put_strike_list:  # Ensure unique entries
#                             put_strike_list.append(symbol)

#     print(f'new put_strike_list size:{len(put_strike_list)}')


#     # print(f"Call Strike List: {call_strike_list}")
#     # print(f"Put Strike List: {put_strike_list}")










def parse_spx_chain(spx_chain):
    """Parses the SPX option chain and extracts relevant strike symbols."""
    global call_strike_list, put_strike_list
    global strike_list_lock

    print(f'in parse_spx_chain')

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

    print(f'new call_strike_list size: {len(call_strike_list)}')

    for exp_date, strikes in put_map.items():
        for strike_price, options in strikes.items():
            for option in options:
                symbol = option.get("symbol", "")
                if symbol.startswith("SPXW  ") and "P0" in symbol:
                    with strike_list_lock:
                        if symbol not in put_strike_list:
                            put_strike_list.append(symbol)

    print(f'new put_strike_list size: {len(put_strike_list)}')





# def get_spx_option_chain():

#     if chain_strike_cnt == None or rx_accessToken == None:
#         print(f'could not get option chain, chain_strike_cnt:{chain_strike_cnt}, rx_accessToken:{rx_accessToken}')
#         return None
    

#     try:
#         today = datetime.now()

#         # Format fromDate and toDate as strings in 'YYYY-MM-DD' format
#         myFromDate = today.strftime('%Y-%m-%d')
#         myToDate = today.strftime('%Y-%m-%d')

#         url = "https://api.schwabapi.com/marketdata/v1/chains"
#         params = {
#             "symbol": "$SPX",
#             "contractType": "ALL",
#             "strikeCount": chain_strike_cnt,
#             "includeUnderlyingQuote": "true",
#             "strategy": "SINGLE",
#             "fromDate": myFromDate,
#             "toDate": myToDate
#         }

#         headers = {
#             "accept": "application/json",
#             "Authorization": f"Bearer {rx_accessToken}"
#         }

#         response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

#         # print(f'8071 chains:{response.status_code}')
#         # print(f'8072 data:{response.json()}')

#         # Check HTTP response status
#         response.raise_for_status()  # Raises HTTPError automatically if status code is not 200

#         # Safely parse JSON response
#         try:
#             spx_chain = response.json()
#         except requests.exceptions.JSONDecodeError:
#             raise ValueError("Error decoding JSON response from Schwab API")

#         # print(f"SPX Option Chain Response type:{type(spx_chain)}, data:\n{spx_chain}")
#         return spx_chain

#     except requests.exceptions.ConnectionError as conn_err:
#         raise RuntimeError("Network error: Unable to connect to Schwab API") from conn_err
#     except requests.exceptions.Timeout as timeout_err:
#         raise RuntimeError("Request timeout: Schwab API took too long to respond") from timeout_err
#     except requests.exceptions.HTTPError as http_err:
#         raise RuntimeError(f"HTTP error occurred: {http_err}") from http_err
#     except Exception as err:
#         raise RuntimeError(f"Unexpected error occurred: {err}") from err
    









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






tokens_file_path = r"C:\MEIC\cred\tokens_mri.json"
acct_file_path = r"C:\MEIC\cred\acct_mri.json"

def get_modification_date(file):
    return datetime.fromtimestamp(os.path.getmtime(file))

def get_modification_date(file):
    return datetime.fromtimestamp(os.path.getmtime(file))





def get_opt_quote(sym):
    global get_quote_success_count
    global get_quote_fail_count

    if rx_accessToken == None:
        print(f'unable to get opt quote, rx_accessToken is None')
        return

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
        info_str = f'3100 exception requesting quotes :{e} at {current_time_str}, returning'
        logging.error(info_str)
        print(info_str)
        return
    
    try:

        if response.status_code != 200:
            info_str = f'1460 get_opt_quotes request failed:{response.status_code} at {current_time_str}, returning'
            print(info_str)
            logging.error(info_str)
            return
        

        
    except Exception as e:
        get_quote_fail_count += 1
        info_str = f'1470 exception requesting quotes :{e} at {current_time_str}, returning'
        print(info_str)
        logging.error(info_str)
        return

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
        return
    

    symbol_key = list(quote_response_json.keys())[0]  # Get the first key dynamically
    has_quote_key = "quote" in quote_response_json.get(symbol_key, {})

    # print(f'Key "quote" exists in quote_response_json: {has_quote_key}')

    if has_quote_key == True:
        publish_raw_queried_quote(quote_response_json)

    else:
        print(f'Key "quote" does not exist in the quote data')






def polling_services():
    """ Periodically polls the schwab API for SPX and option quotes  """
    global quit_flag
    global put_strike_list, call_strike_list

    print(f'streamer: polling services bc 100')


    while not quit_flag: # polling services outer (session) loop

        print(f'streamer: polling services bc 200')


        polling_loop_cnt = 0

        next_put_list_ix = 0
        next_call_list_ix = 0


        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)

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
            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)
            continue



        # print(f'streamer: polling services bc 400')


        current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')

        print(f'streamer polling services: market is now open, current easten time:{current_eastern_hhmmss}')
            

        get_today_in_epoch()
        # print(f'streamer: polling services bc 400')


        asyncio.run(wait_for_rx_credentials(timeout=30))  # Runs the async function
        if rx_accessToken == None:
            # print(f'streamer: polling services bc 500')
            print(f'streamer: polling services gave up waiting for rx_credentials to be initialized')


        # print(f'streamer: polling services bc 410')


        while not quit_flag: # innner polling services session loop
            polling_loop_cnt += 1

            # print(f'streamer: polling services bc 420')

            # print(f'pollig services polling_loop_cnt:{polling_loop_cnt}')


            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=4, close_offset=0)

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
                next_call_list_ix = 0  # Local index
                for i in range(4):
                    next_sym = temp_call_strike_list[next_call_list_ix]
                    # print(f'calling get_opt_quote for call {next_sym}')
                    get_opt_quote(next_sym)
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
                    next_put_list_ix = 0  # Local index
                    for i in range(4):
                        next_sym = temp_put_strike_list[next_put_list_ix]
                        # print(f'calling get_opt_quote for put {next_sym}')
                        get_opt_quote(next_sym)
                        next_put_list_ix += 1
                        if next_put_list_ix >= len(temp_put_strike_list):
                            next_put_list_ix = 0
                        time.sleep(0.10)








            




            # print(f'streamer: polling services bc 510')


            # if polling_loop_cnt % 20 == 2:
            if polling_loop_cnt % 60 == 2 or len(put_strike_list) == 0 or len(call_strike_list) == 0:

                print(f'2630 trying to get ohlc')
                # print(f'streamer: polling services bc 520')

                try:

                    returned_ohlc = get_spx_current_today_ohlc()

                    if spx_high == None or spx_low == None or returned_ohlc == None:
                        print(f'poller new spx_high:{spx_high:.2f}, new spx_low:{spx_low:.2f}')

                    
                    try:


                        # print(f'streamer: polling services bc 530')

                        print(f'2632 trying to get spx chain')

                        spx_chain = get_spx_option_chain()
                        if spx_chain is not None:
                            print(f'2634 trying to get call/put list from chain data')
                            # print(f'streamer: polling services bc 600')
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
                        print(f'poller high/low problem new spx_high:{spx_high}, new spx_low:{spx_low}')

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



# ------------------ Main Program ------------------

async def main():
    global quit_flag

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
        info_str = f'3320 Asyncio task cancelled. Cleaning up...'
        logging.error(info_str)
        print(f'\n{info_str}')
        quit_flag = True

    # Wait for Schwab setup to finish
    await schwab_task

    # Wait for MQTT setup thread to terminate
    setup_mqtt_thread.join()

    # Wait for polling setup thread to terminate
    setup_polling_thread.join()

    print("Program terminated.")

if __name__ == "__main__":
    asyncio.run(main())













