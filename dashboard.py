import json
import threading
import pandas as pd
import schwabdev
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timezone, timedelta
import pytz
# import positions

import json

print()


use_back_data = False

backdata_directory = r"C:\MEIC\account\atest1"
account_details_file = "test_account_details_250114_075640.json"
orders_file = "test_orders_list_250114_075640.json"
transactions_file = "test_transactions_250114_075640.json"

full_account_filespec = backdata_directory + "\\" + account_details_file
full_orders_filespec = backdata_directory +  "\\" + orders_file
full_transactions_filespec = backdata_directory +  "\\" + transactions_file

print(f'full_account_filespec:<{full_account_filespec}>')
print(f'full_orders_filespec:<{full_orders_filespec}>')
print(f'full_transactions_filespec:<{full_transactions_filespec}>')



def get_dash_dated_desination_dir():
    base_dir = r"C:\MEIC\dash"

    # Get the current date in yymmdd format
    current_date = datetime.now().strftime('%y%m%d')

    # Create the full directory path
    full_dir = os.path.join(base_dir, f"data_{current_date}")

    # Create the directory if it does not already exist
    os.makedirs(full_dir, exist_ok=True)

    return full_dir

def post_dash_data(new_post):
    """
    Append the new_post string to a file named tranche_info.txt in the specified directory.
    
    :param directory_path: The path to the directory where the file is located.
    :param new_post: The string to be appended to the file.
    """

    now_time = datetime.now()
    # current_time_str = now_time.strftime('%Y%M%d_%H%M%S')
    current_time_str = now_time.strftime("%y%m%d_%H%M%S")

    filename_str = f'dash_info_{current_time_str}.txt'

    tranche_post_dated_dir = get_dash_dated_desination_dir()
    file_path = os.path.join(tranche_post_dated_dir, filename_str)
    
    # Ensure the directory exists
    os.makedirs(tranche_post_dated_dir, exist_ok=True)
    
    # Open the file in append mode (create if it does not exist)
    with open(file_path, 'a') as file:
        file.write(new_post + "\n")
    
    # print(f'Appended new post to file: {file_path}')





lock = threading.Lock()


# Create an empty DataFrame with the desired headers
positions_df = pd.DataFrame(columns=["sym", "put_call", "qty", "trade_price", "now_price"])


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



def reset_positions():
    global positions_df

    with lock:
        positions_df = pd.DataFrame(columns=["sym", "put_call", "qty", "trade_price", "now_price"])



def update_positions(my_positions):
    """
    Updates the global positions_df with new data from my_positions.

    Args:
        my_positions (list): A list of position dictionaries.
    """
    global positions_df

    # print(f'\nmy_positions:\n{my_positions}\n')

    try:
        with lock:
            # Clear all rows from positions_df while leaving the headers in place
            positions_df = positions_df.iloc[0:0]
            
            # Temporary list to hold new rows for positions_df
            new_rows = []
            
            # Reload positions_df with items from my_positions
            for position in my_positions:
                try:
                    sym = position['instrument']['symbol']
                    put_call = position['instrument']['putCall']
                    qty = int(position['longQuantity'] - position['shortQuantity'])
                    trade_price = float('nan')  # trade_price is not a number
                    now_price = position['averagePrice']
                    
                    # Append the new row as a dictionary to the list
                    new_rows.append({"sym": sym, "put_call": put_call, "qty": qty, "trade_price": trade_price, "now_price": now_price})
                except KeyError as e:
                    print(f"100 Error processing position: Missing key {e}")
                except Exception as e:
                    print(f"110 Error processing position: {e}")
            
            # Directly assign to positions_df if it's empty
            if positions_df.empty:
                positions_df = pd.DataFrame(new_rows)
            else:
                positions_df = pd.concat([positions_df, pd.DataFrame(new_rows)], ignore_index=True)

            
    except Exception as e:
        print(f"120 Error in update_positions: {e}")





def short_options():
    """
    Returns a list of sym strings from positions_df where qty is less than zero.

    Returns:
        list: A list of sym strings for short options.
    """
    global positions_df
    try:
        with lock:
            return positions_df[positions_df['qty'] < 0]['sym'].tolist()
    except Exception as e:
        print(f"Error in short_options: {e}")
        return []

def long_options():
    """
    Returns a list of sym strings from positions_df where qty is greater than zero.

    Returns:
        list: A list of sym strings for long options.
    """
    global positions_df
    try:
        with lock:
            return positions_df[positions_df['qty'] > 0]['sym'].tolist()
    except Exception as e:
        print(f"Error in long_options: {e}")
        return []
    

def save_dict_to_json_file(d, file_name):
    """
    Save a dictionary to a file in human-readable JSON format.
    
    :param d: The dictionary to be saved.
    :param file_name: The name of the file to save the data in.
    """

    dash_post_dated_dir = get_dash_dated_desination_dir()
    file_path = os.path.join(dash_post_dated_dir, file_name)

    with open(file_path, 'w') as file:
        json.dump(d, file, indent=4)


def load_dict_from_json_file(file_name):
    """
    Load a dictionary from a file containing JSON data.
    
    :param file_name: The name of the file to load the data from.
    :return: The dictionary containing the data from the file.
    """
    with open(file_name, 'r') as file:
        data = json.load(file)

    print(f'Loaded dictionary from file: {file_name}')
    return data
        

def display_dict_contents(d, indent=0):
    """
    Recursively display the contents of a dictionary in a human-readable format.
    
    :param d: The dictionary to be displayed.
    :param indent: The indentation level for nested structures.
    """
    for key, value in d.items():
        print(' ' * indent + f"{key}: ", end='')
        if isinstance(value, dict):
            print()
            display_dict_contents(value, indent + 2)
        elif isinstance(value, list):
            print()
            for item in value:
                if isinstance(item, dict):
                    display_dict_contents(item, indent + 2)
                else:
                    print(' ' * (indent + 2) + str(item))
        else:
            print(value)


# def convert_list_to_dict(orders_list):
#     """
#     Convert a list of orders into a dictionary with incremental keys.
    
#     :param orders_list: The list of orders to be converted.
#     :return: A dictionary with incremental keys as keys and orders as values.
#     """
#     return {i: order for i, order in enumerate(orders_list)}

def save_list_to_json_file(data, file_name):
    """
    Save a list to a file in human-readable JSON format.
    
    :param data: The list to be saved.
    :param file_name: The name of the file to save the data in.
    """

    dash_post_dated_dir = get_dash_dated_desination_dir()
    file_path = os.path.join(dash_post_dated_dir, file_name)

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    # print(f'saved list to file:{file_path}')


def load_list_from_json_file(file_name):
    """
    Load a list from a file containing JSON data.
    
    :param file_name: The name of the file to load the data from.
    :return: The list containing the data from the file.
    """
    with open(file_name, 'r') as file:
        data = json.load(file)

    print(f'loaded list from file: {file_name}')
    return data


def show_account(account_details):

    print(f'\n Account details and positions')
    print("-" * 40)

    securities_account = account_details['securitiesAccount']
    initial_cash_balance = securities_account['initialBalances']['cashBalance']
    current_cash_balance = securities_account['currentBalances']['cashBalance']
    


    total_positions_market_value = 0.00
    market_value = 0.00

    
    


    if "positions" in account_details["securitiesAccount"].keys():

        positions = account_details['securitiesAccount']['positions']
        for position in positions:
            # print(f'position:{position}')

            short_quantity = position['shortQuantity']
            long_quantity = position['longQuantity']
            symbol = position['instrument']['symbol']
            average_price = position['averagePrice']

            print(f'prev market_value:{market_value}')
            market_value = position['marketValue']
            print(f'new market_value:{market_value}')



            market_value_per_share = abs(market_value)/100
            current_day_cost = position['currentDayCost']

            if short_quantity > 0:
                # print(f'short_quantity is > 0')
                position_qty_str = f'-{short_quantity}'
                multiplier = -1

            elif long_quantity > 0:
                # print(f'long_quantity is > 0')
                position_qty_str = f'+{long_quantity}'
                multiplier = 1

            

            else:
                position_qty_str = f'0'
                multiplier = 0


            # print(f'market_value:{market_value}, multiplier{multiplier}')


            total_positions_market_value += market_value
            # print(f'market_value:{market_value},  total_positions_market_value:{total_positions_market_value}')

            
            print(f"Symbol: {symbol}")
            print(f"  Qty: {position_qty_str}")
            # print(f"  Short Quantity: {short_quantity}")
            # print(f"  Long Quantity: {long_quantity}")
            print(f"  Average Price: ${average_price}")
            print(f"  Market Value: ${market_value_per_share:.3f}")
            # print(f"  Current Day Cost: ${current_day_cost}")
            print("-" * 40)


    else:
        print(f'\nno current positions\n')



    P_L = current_cash_balance - initial_cash_balance + total_positions_market_value



    info_str = ""
    print(info_str)
    post_dash_data(info_str)


    info_str = f"Initial Balance: ${initial_cash_balance}"
    print(info_str)
    post_dash_data(info_str)

    info_str = f"Current Balance: ${current_cash_balance}"
    print(info_str)
    post_dash_data(info_str)

    info_str = f"Net P/L: ${P_L:.2f}"
    print(info_str)
    post_dash_data(info_str)

    info_str = ("=" * 50)
    print(info_str)
    post_dash_data(info_str)



def show_orders(my_orders):
    pacific_time_zone = pytz.timezone('US/Pacific')
    today = datetime.now(pacific_time_zone).date()
    
    for order in my_orders:
        entered_time = order.get('enteredTime', 'N/A')
        entered_time_local = convert_to_local_time(entered_time, pacific_time_zone)

        if isinstance(entered_time_local, str):
            entered_time_local_string = entered_time_local

        else:
            if entered_time_local.date() != today:
                continue

            entered_time_local_string = entered_time_local.strftime("%Y-%m-%d %H:%M:%S%z")
            entered_time_local_string = entered_time_local_string.rsplit('-', 1)[0]

            
        
        complex_order_strategy_type = order.get('complexOrderStrategyType', 'N/A')
        quantity = order.get('quantity', 'N/A')
        filled_quantity = order.get('filledQuantity', 'N/A')
        price = order.get('price', 'N/A')
        order_strategy_type = order.get('orderStrategyType', 'N/A')
        order_id = order.get('orderId', 'N/A')
        status = order.get('status', 'N/A')
        close_time = order.get('closeTime', 'N/A')

        close_time_local = convert_to_local_time(close_time, pacific_time_zone)

        if isinstance(close_time_local, str):
            close_time_local_string = close_time_local

        else:
            close_time_local_string = close_time_local.strftime("%Y-%m-%d %H:%M:%S%z")
            close_time_local_string = close_time_local_string.rsplit('-', 1)[0]

        print(f"Complex Order Strategy Type: {complex_order_strategy_type}")
        print(f"Quantity: {quantity}")
        print(f"Filled Quantity: {filled_quantity}")
        print(f"Price: ${price}")
        print(f"Order Strategy Type: {order_strategy_type}")
        print(f"Order ID: {order_id}")
        print(f"Status: {status}")
        print(f"Entered Time: {entered_time} (Local: {entered_time_local_string})")
        print(f"Close Time: {close_time} (Local: {close_time_local_string})")
        print("=" * 50)
        
        for leg in order.get('orderLegCollection', []):
            order_leg_type = leg.get('orderLegType', 'N/A')
            instrument_symbol = leg['instrument'].get('symbol', 'N/A')
            instruction = leg.get('instruction', 'N/A')
            put_call = leg['instrument'].get('putCall', 'N/A')
            
            print(f"  Order Leg Type: {order_leg_type}")
            print(f"  Instrument Symbol: {instrument_symbol}")
            print(f"  Instruction: {instruction}")
            print(f"  Put/Call: {put_call}")
            print("-" * 50)

def convert_to_local_time(time_str, time_zone):



    if time_str == "N/A":
        # print(f'CTLT returning because time was N/A')
        return "N/A"
    
    try:
        utc_time = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        utc_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S %Z")

    local_time = utc_time.astimezone(time_zone)

    return local_time



def show_transactions(transactions):

    print("-" * 50)
    transaction_cnt = 0

    
    for transaction in transactions:
        transaction_cnt += 1

        info_str = f"Transaction #{transaction_cnt}"
        print(info_str)
        post_dash_data(info_str)

        activity_id = transaction.get('activityId', 'N/A')
        position_id = transaction.get('positionId', 'N/A')
        order_id = transaction.get('orderId', 'N/A')
        time = transaction.get('time', 'N/A')
        trade_date = transaction.get('tradeDate', 'N/A')
        net_amount = transaction.get('netAmount', 'N/A')

        info_str = f"Activity ID: {activity_id}"
        print(info_str)
        post_dash_data(info_str)

        info_str = f"Position ID: {position_id}"
        print(info_str)
        post_dash_data(info_str)

        info_str = f"Order ID: {order_id}"
        print(info_str)
        post_dash_data(info_str)

        info_str = f"Time: {time}"
        print(info_str)
        post_dash_data(info_str)

        info_str = f"Trade Date: {trade_date}"
        print(info_str)
        post_dash_data(info_str)

        info_str = f"Net Amount: {net_amount}"
        print(info_str)
        post_dash_data(info_str)

        info_str = "Instruments >>>>"
        print(info_str)
        post_dash_data(info_str)

        info_str = ("-" * 50)
        print(info_str)
        post_dash_data(info_str)


        if 'transferItems' in transaction:
            for item in transaction['transferItems']:
                instrument = item.get('instrument', {})
                asset_type = instrument.get('assetType', 'N/A')
                status = instrument.get('status', 'N/A')
                symbol = instrument.get('symbol', 'N/A')
                closing_price = instrument.get('closingPrice', 'N/A')
                amount = item.get('amount', 'N/A')
                cost = item.get('cost', 'N/A')
                price = item.get('price', 'N/A')
                fee_type = item.get('feeType', 'N/A')

                info_str = f"    Instrument Asset Type: {asset_type}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Instrument Status: {status}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Instrument Symbol: {symbol}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Instrument Closing Price: {closing_price}"
                print(info_str)
                post_dash_data(info_str)


                info_str = f"    Amount: {amount}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Cost: {cost}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Price: {price}"
                print(info_str)
                post_dash_data(info_str)

                info_str = f"    Fee Type: {fee_type}"
                print(info_str)
                post_dash_data(info_str)

                info_str = ("-" * 50)
                print(info_str)
                post_dash_data(info_str)

    info_str = ("=" * 50)
    print(info_str)
    post_dash_data(info_str)







def get_positions3():
    # Check for the 'positions' key

    long_positions = []
    short_positions = []
    get_positions_success_flag = False

    app_key, secret_key, my_tokens_file = load_env_variables()

    retry_limit = 5

    while True:

        # try:

        client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)
        linked_accounts = client.account_linked().json()
        # print(f'linked_accounts type:{type(linked_accounts)}, data:\n{linked_accounts}\n')

        account_hash = linked_accounts[0].get('hashValue')
        # print(f'account_hash type:{type(account_hash)}, data:\n{account_hash}\n')




        if use_back_data == True:

            account_details = load_dict_from_json_file(full_account_filespec)

            
            info_str = f'\naccount/positions data using backdata file:{full_account_filespec}'
            print(info_str)
            post_dash_data(info_str)

            show_account(account_details)
            info_str = f'end of account/positions data\n'
            print(info_str)
            post_dash_data(info_str)



            my_orders = load_list_from_json_file(full_orders_filespec)
            info_str = f'orders data using backdata file:{full_orders_filespec}'
            print(info_str)
            post_dash_data(info_str)
            show_orders(my_orders)
            info_str = f'end of orders data\n'
            post_dash_data(info_str)

            my_transactions = load_dict_from_json_file(full_transactions_filespec)

            info_str = f'\nTransaction data using backdata file:{full_transactions_filespec}'
            print(info_str)
            post_dash_data(info_str)

            show_transactions(my_transactions)
            info_str = f'End of transaction data\n'
            print(info_str)
            post_dash_data(info_str)

        

            break









        now_time = datetime.now()
        # current_time_str = now_time.strftime('%Y%M%d_%H%M%S')
        current_time_str = now_time.strftime("%y%m%d_%H%M%S")

        









        my_transactions = client.transactions(account_hash, 
            datetime.now(timezone.utc) - timedelta(days=1),
            datetime.now(timezone.utc), 
            "TRADE").json()
        

        info_str = f'\ntransaction data:'
        print(info_str)
        post_dash_data(info_str)

        show_transactions(my_transactions)

        info_str = f'End of transaction data\n'
        print(info_str)
        post_dash_data(info_str)


        
        filename_str = f'test_transactions_{current_time_str}.json'

        save_dict_to_json_file(my_transactions, filename_str)

        

        my_orders = client.account_orders(account_hash, 
            datetime.now(timezone.utc) - timedelta(days=1), 
            datetime.now(timezone.utc)).json()
        
        info_str = f'\norders data:'
        print(info_str)
        post_dash_data(info_str)

        show_orders(my_orders)
        info_str = f'\nend of orders data\n'
        print(info_str)
        post_dash_data(info_str)
        
        
        # print(f'my_orders type:{type(my_orders)}, data:{my_orders}')

        # my_orders_dict = convert_list_to_dict(my_orders)

        filename_str = f'test_orders_list_{current_time_str}.json'
        save_list_to_json_file(my_orders, filename_str)




        account_details = client.account_details(account_hash, fields="positions").json()

        filename_str = f'test_account_details_{current_time_str}.json'
        save_dict_to_json_file(account_details, filename_str)
        
        info_str = f'\naccount/positions data:'
        print(info_str)
        post_dash_data(info_str)

        show_account(account_details)

        info_str = f'End of account/positions data\n'
        print(info_str)
        post_dash_data(info_str)

    
            
        # except Exception as e:
        #     print(f"get_positions3() 6, An error occurred: {e}, exiting positions processing")
        #     retry_limit -= 1
        #     if retry_limit > 0:
        #         time.sleep(0.1)
        #         continue
        #     else:
        #         get_positions_success_flag = False
        #         return get_positions_success_flag, short_positions, long_positions



        get_positions_success_flag = True

        break

        
    try:

        if 'pfcbFlag' not in account_details['securitiesAccount']:
            # Handle the case where 'positions' key is not present
            print("No pfcbFlag in account data. aborting get_positions()")
            get_positions_success_flag = False
            return get_positions_success_flag, short_positions, long_positions
        
    except Exception as e:
        print(f"get_positions2() 7, An error occurred: {e}, exiting positions processing")
        get_positions_success_flag = False
        return get_positions_success_flag, short_positions, long_positions
    

    try:
    

        if 'positions' in account_details['securitiesAccount']:
            my_positions = account_details['securitiesAccount']['positions']

            update_positions(my_positions)

            short_positions = short_options()
            long_positions = long_options()


            return get_positions_success_flag, short_positions, long_positions


        else:
            # Handle the case where 'positions' key is not present
            print("No positions in account data, resetting positions table")
            reset_positions()
            return get_positions_success_flag, short_positions, long_positions

    except Exception as e:
            print(f"get_positions2() 8, An error occurred: {e}, exiting positions processing")
            get_positions_success_flag = False
            return get_positions_success_flag, short_positions, long_positions
    


    



while True:

    now_time = datetime.now()
    current_time_str = now_time.strftime('%H:%M:%S')

    get_positions_success_flag, short_positions, long_positions = get_positions3()

    print()
    print(f'time:{current_time_str}, get_positions_success_flag:{get_positions_success_flag}')
    print(f'short_positions type:{type(short_positions)}, data:\n{short_positions}')
    print(f'long_positions type:{type(long_positions)}, data:\n{long_positions}')
    print()


    time.sleep(300)







