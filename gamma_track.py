import paho.mqtt.client as mqtt
import queue
import threading
import time
import json
from datetime import datetime, timezone
import pickle
import os
import csv
import pytz
import calendar
import market_open


# Initialize data storage
spx_last_prices = []
spxw_gamma_values = {}
spxw_delta_values = {}



# MQTT settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "schwab/stream"

# Queue to hold incoming messages
message_queue = queue.Queue()

def on_connect(client, userdata, flags, rc):
    """
    Callback function for when the client connects to the broker.
    """
    if rc == 0:
        print("Connected successfully to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    """
    Callback function for when a message is received from the broker.
    """
    try:
        payload = msg.payload.decode("utf-8")
        # print(f"Message received on topic {msg.topic}: {payload}")
        message_queue.put((msg.topic, payload))
    except Exception as e:
        print(f"Error processing message: {e}")





PICKLE_BASE_DIR = ""
PICKLE_DIR = ""
SPXW_GAMMA_VALUES_FILE = ""
SPXW_DELTA_VALUES_FILE = ""
SPX_LAST_PRICES_FILE = ""
SPXW_CSV_FILE_PATH = ""
SPX_CSV_FILE_PATH = ""


def initialize_dated_destination():
    global PICKLE_BASE_DIR
    global PICKLE_DIR
    global SPXW_GAMMA_VALUES_FILE
    global SPXW_DELTA_VALUES_FILE
    global SPX_LAST_PRICES_FILE
    global SPXW_CSV_FILE_PATH
    global SPX_CSV_FILE_PATH



    # File paths to store the historical data in the specified directory
    PICKLE_BASE_DIR = r"C:\MEIC\gamma_track"

    # PICKLE_DIR = r"C:\MEIC\gamma_track"

    # Get the current date in yymmdd format
    current_date = datetime.now().strftime('%y%m%d')

    # Create the full directory path
    PICKLE_DIR = os.path.join(PICKLE_BASE_DIR, f"data_{current_date}")

    # Create the directory if it does not already exist
    os.makedirs(PICKLE_DIR, exist_ok=True)

    SPXW_GAMMA_VALUES_FILE = os.path.join(PICKLE_DIR, 'spxw_gamma_values.pkl')
    SPXW_DELTA_VALUES_FILE = os.path.join(PICKLE_DIR, 'spxw_delta_values.pkl')
    SPX_LAST_PRICES_FILE = os.path.join(PICKLE_DIR, 'spx_last_prices.pkl')
    SPXW_CSV_FILE_PATH = os.path.join(PICKLE_DIR, 'spxw_gamma.csv')
    SPX_CSV_FILE_PATH = os.path.join(PICKLE_DIR, 'spx.csv')

    # Ensure the destination directory exists
    if not os.path.exists(PICKLE_DIR):
        os.makedirs(PICKLE_DIR)

    # Ensure persistence files exist
    if not os.path.exists(SPX_LAST_PRICES_FILE):
        with open(SPX_LAST_PRICES_FILE, 'wb') as f:
            pickle.dump(spx_last_prices, f)

    if not os.path.exists(SPXW_GAMMA_VALUES_FILE):
        with open(SPXW_GAMMA_VALUES_FILE, 'wb') as f:
            pickle.dump(spxw_gamma_values, f)

    if not os.path.exists(SPXW_DELTA_VALUES_FILE):
        with open(SPXW_DELTA_VALUES_FILE, 'wb') as f:
            pickle.dump(spxw_delta_values, f)



def persist_data(json_message):
    global spx_last_prices, spxw_gamma_values, spxw_delta_values

    # Load existing data from files
    # with open(SPX_LAST_PRICES_FILE, 'rb') as f:
    #     spx_last_prices = pickle.load(f)

    # Ensure the directory exists
    if not os.path.exists(PICKLE_DIR):
        os.makedirs(PICKLE_DIR)

    # Ensure persistence files exist
    if not os.path.exists(SPX_LAST_PRICES_FILE):
        with open(SPX_LAST_PRICES_FILE, 'wb') as f:
            pickle.dump(spx_last_prices, f)

    if not os.path.exists(SPXW_GAMMA_VALUES_FILE):
        with open(SPXW_GAMMA_VALUES_FILE, 'wb') as f:
            pickle.dump(spxw_gamma_values, f)

    if not os.path.exists(SPXW_DELTA_VALUES_FILE):
        with open(SPXW_DELTA_VALUES_FILE, 'wb') as f:
            pickle.dump(spxw_delta_values, f)

    with open(SPXW_GAMMA_VALUES_FILE, 'rb') as f:
        spxw_gamma_values = pickle.load(f)

    # Process the json_message
    data = json_message['data']
    for item in data:
        timestamp = datetime.fromtimestamp(item['timestamp'] / 1000)
        # if item['service'] == 'LEVELONE_EQUITIES':
        #     for content in item['content']:
        #         if content.get('key') == '$SPX' and 'last' in content:
        #             spx_last_prices.append((timestamp, content['last']))
        if item['service'] == 'LEVELONE_OPTIONS':
            for content in item['content']:
                key = content.get('key')

                if key and 'gamma' in content:
                    gamma_fl = float(content['gamma'])

                    # print(f'gamma_fl:{gamma_fl}')

                    # if gamma_fl >= 0.05:
                    if gamma_fl >= 0.03:
                    # if gamma_fl >= 0.001:

                        if key not in spxw_gamma_values:
                            spxw_gamma_values[key] = []

                        spxw_gamma_values[key].append((timestamp, content['gamma']))

                        with open(SPXW_GAMMA_VALUES_FILE, 'wb') as f:
                            pickle.dump(spxw_gamma_values, f)

                        # print(f'gamma was recorded: {gamma_fl}')
                        
                    else:
                        # print(f'gamma is too low to record: {gamma_fl}')
                        pass






                # if key and 'delta' and 'symbol' in content:
                #     delta_fl = float(content['delta'])

                #     delta_fl = abs(delta_fl)

                #     # print(f'delta_fl:{delta_fl}')

                #     if delta_fl >= 0.05:

                #         if key not in spxw_delta_values:
                #             spxw_delta_values[key] = []

                #         spxw_delta_values[key].append((timestamp, content['delta']))

                #         with open(SPXW_DELTA_VALUES_FILE, 'wb') as f:
                #             pickle.dump(spxw_delta_values, f)

                #         # print(f'delta was recorded: {gamma_fl}')
                        
                #     else:
                #         # print(f'delta is too low to record: {gamma_fl}')
                #         pass






def display_history():
    # Load existing data from files
    # with open(SPX_LAST_PRICES_FILE, 'rb') as f:
    #     spx_last_prices = pickle.load(f)

    with open(SPXW_GAMMA_VALUES_FILE, 'rb') as f:
        spxw_gamma_values = pickle.load(f)


    # print("SPX Last Prices History:")
    # for timestamp, price in spx_last_prices:
    #     print(f"Timestamp: {timestamp}, Price: {price}")

    print("\nSPXW Option Gamma Values History:")
    for option, gamma_values in spxw_gamma_values.items():
        print(f"Option: {option}")
        for timestamp, gamma in gamma_values:
            print(f"  Timestamp: {timestamp}, Gamma: {gamma}")


def item_history(option_symbol):
    # Load existing data from the gamma values file
    if os.path.exists(SPXW_GAMMA_VALUES_FILE):
        with open(SPXW_GAMMA_VALUES_FILE, 'rb') as f:
            spxw_gamma_values = pickle.load(f)
        
        if option_symbol in spxw_gamma_values:
            print(f"Option: {option_symbol}")
            for timestamp, gamma in spxw_gamma_values[option_symbol]:
                print(f"  Timestamp: {timestamp}, Gamma: {gamma}")
        else:
            print(f"No history found for option symbol: {option_symbol}")
    else:
        print("No data file found for gamma values.")


def spx_history():
    # Load existing data from the last prices file
    if os.path.exists(SPX_LAST_PRICES_FILE):
        with open(SPX_LAST_PRICES_FILE, 'rb') as f:
            spx_last_prices = pickle.load(f)
        
        print("SPX Last Prices History:")
        for timestamp, price in spx_last_prices:
            print(f"Timestamp: {timestamp}, Price: {price}")
    else:
        print("No data file found for last prices.")



def spx_to_csv():
    # Load existing data from the last prices file
    if os.path.exists(SPX_LAST_PRICES_FILE):
        with open(SPX_LAST_PRICES_FILE, 'rb') as f:
            spx_last_prices = pickle.load(f)
        
        # Write the data to a CSV file
        with open(SPX_CSV_FILE_PATH, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['Timestamp', 'Price'])  # Write the header
            for timestamp, price in spx_last_prices:
                csv_writer.writerow([timestamp, price])
        
        print(f"SPX last data has been saved to {SPX_CSV_FILE_PATH}")
    else:
        print("No data file found for last prices.")




def spxw_gamma_to_csv():
    # Load existing data from the gamma values file
    if os.path.exists(SPXW_GAMMA_VALUES_FILE):
        with open(SPXW_GAMMA_VALUES_FILE, 'rb') as f:
            spxw_gamma_values = pickle.load(f)
        
        # Write the data to a CSV file
        with open(SPXW_CSV_FILE_PATH, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['Option Symbol', 'Timestamp', 'Gamma'])  # Write the header
            for option, gamma_values in spxw_gamma_values.items():
                for timestamp, gamma in gamma_values:
                    csv_writer.writerow([option, timestamp, gamma])
        
        print(f"SPXW gamma data has been saved to {SPXW_CSV_FILE_PATH}")
    else:
        print("No data file found for gamma values.")










def purge_history():
    global spx_last_prices, spxw_gamma_values

    # Clear the in-memory data
    spx_last_prices = []
    spxw_gamma_values = {}

    # Clear the persisted files
    with open(SPX_LAST_PRICES_FILE, 'wb') as f:
        pickle.dump(spx_last_prices, f)

    with open(SPXW_GAMMA_VALUES_FILE, 'wb') as f:
        pickle.dump(spxw_gamma_values, f)






























def message_processor():
    """
    Task to process messages from the message queue.
    """

    display_throttle = 0
    while True:
        
        # topic, message = message_queue.get()

        try:
            topic, message = message_queue.get(timeout=1)  # 1 second timeout

        except queue.Empty: 
            continue


        # print(f"Processing message from topic {topic}: {message}")

        json_message = json.loads(message)
        pretty_json = json.dumps(json_message, indent=2)
        # print(f'topic:{topic}, json_message:\n{pretty_json}')

        persist_data(json_message)



        # Add message handling logic here
        # Example: Save to a database, forward to another service, etc.
        message_queue.task_done()

        display_throttle += 1
        if display_throttle % 60 == 58:
            # print(f'displaying all history')
            # display_history()

            # print(f'displaying  SPX history')
            # spx_history()

            # current_time = datetime.now()
            # time_str = current_time.strftime('%H:%M:%S')

            # # print(f'converting SPX history to csv at {time_str}')
            # # spx_to_csv()

            # print(f'converting SPXW gamma history to csv at {time_str}')
            # spxw_gamma_to_csv()

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


def gamma_track_loop():
        

    try:

        # Initialize MQTT client
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

    
        # Connect to MQTT broker
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        # Start the message processor in a separate thread
        processor_thread = threading.Thread(target=message_processor, daemon=True)
        processor_thread.start()

        # # Start the MQTT client loop
        # client.loop_forever()

        # Custom infinite loop to handle MQTT client loop
        while True:
            client.loop(timeout=1.0)  # process network traffic, with a 1-second timeout
            # time.sleep(1) 
            market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=0, close_offset=0)

            # current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
            # current_eastern_day = current_eastern_time.strftime('%A')
            # print(f'gamma_track: while market is open')
            # print(f'open flag:{market_open_flag}, current Eastern time: {current_eastern_day} {current_eastern_hhmmss}')

            if market_open_flag == False:
                current_eastern_hhmmss = current_eastern_time.strftime('%H:%M:%S')
                print(f'gamma_track: Market is closed at {current_eastern_hhmmss}, shutting down MQTT')
                client.loop_stop()  # Stop the MQTT loop
                client.disconnect()  # Disconnect from the MQTT broker
                return

    except Exception as e:
        print(f"Error in gamma_track_loop MQTT connection: {e}")
        return


def wait_for_market_to_open():
    throttle_wait_display = 0
    print(f'gamma_track: waiting for market to open')

    while True:
        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=0, close_offset=0)
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

            print(f'gamma_track: waiting for market to open, current Eastern time: {current_eastern_day} {current_eastern_hhmmss}')

            pass


        time.sleep(10)



def main():


    while True:
        wait_for_market_to_open()
        initialize_dated_destination()
        gamma_track_loop()



if __name__ == "__main__":
    main()
