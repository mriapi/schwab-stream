import json
import threading
import pandas as pd
# import schwabdev
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timezone, timedelta
import pytz
import argparse
# import positions
import mri_schwab_lib






global my_transactions
global my_orders
global activity_date
activity_date = None


global start_of_date_dt
start_of_date_dt = None
global start_of_date_str
start_of_date_str = None

global end_of_date_dt
end_of_date_dt = None
global end_of_date_str
end_of_date_str = None


use_back_data = False

backdata_directory = r"C:\MEIC\account\atest1"
account_details_file = "test_account_details_250114_075640.json"
orders_file = "test_orders_list_250114_075640.json"
transactions_file = "test_transactions_250114_075640.json"

full_account_filespec = backdata_directory + "\\" + account_details_file
full_orders_filespec = backdata_directory +  "\\" + orders_file
full_transactions_filespec = backdata_directory +  "\\" + transactions_file

# print(f'full_account_filespec:<{full_account_filespec}>')
# print(f'full_orders_filespec:<{full_orders_filespec}>')
# print(f'full_transactions_filespec:<{full_transactions_filespec}>')



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
# positions_df = pd.DataFrame(columns=["sym", "put_call", "qty", "trade_price", "now_price"])


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
        




# def display_dict_contents(d, indent=0):
#     """
#     Recursively display the contents of a dictionary in a human-readable format.
    
#     :param d: The dictionary to be displayed.
#     :param indent: The indentation level for nested structures.
#     """
#     for key, value in d.items():
#         print(' ' * indent + f"{key}: ", end='')
#         if isinstance(value, dict):
#             print()
#             display_dict_contents(value, indent + 2)
#         elif isinstance(value, list):
#             print()
#             for item in value:
#                 if isinstance(item, dict):
#                     display_dict_contents(item, indent + 2)
#                 else:
#                     print(' ' * (indent + 2) + str(item))
#         else:
#             print(value)



def save_transactions_to_json(transactions):
    # Create the directory path in the format "C:\MEIC\log\YYMMDD"
    # today = datetime.today().strftime('%y%m%d')
    today = activity_date
    log_file_dir = f"C:\\MEIC\\log\\{today}"
    print(f'Saving transactions.json to {log_file_dir}')

    # Check for the existence of the directory and create it if it does not exist
    if not os.path.exists(log_file_dir):
        os.makedirs(log_file_dir)

    # Convert transactions to JSON format and save it to a file named transact.json
    json_file_path = os.path.join(log_file_dir, 'transactions.json')
    with open(json_file_path, 'w') as json_file:
        json.dump(transactions, json_file, indent=4)    

def save_orders_to_json(orders):
    # Create the directory path in the format "C:\MEIC\log\YYMMDD"
    # today = datetime.today().strftime('%y%m%d')
    today = activity_date
    log_file_dir = f"C:\\MEIC\\log\\{today}"
    print(f'Saving orders.json to {log_file_dir}')

    # Check for the existence of the directory and create it if it does not exist
    if not os.path.exists(log_file_dir):
        os.makedirs(log_file_dir)

    # Convert transactions to JSON format and save it to a file named transact.json
    json_file_path = os.path.join(log_file_dir, 'orders.json')
    with open(json_file_path, 'w') as json_file:
        json.dump(orders, json_file, indent=4)  


def main():
    global activity_date

    global start_of_date_dt
    global start_of_date_str
    global end_of_date_dt
    global end_of_date_str


    # Set up argument parser
    parser = argparse.ArgumentParser(description="Display the provided date or today's date if none is given.")
    parser.add_argument("date", type=str, nargs="?", help="Date in format YYYY-MM-DD")

    # Parse arguments
    args = parser.parse_args()

    # Determine the date to display
    if args.date:
        display_date = args.date
        print(f"\nUsing provided date: {display_date}\n")
    else:
        display_date = datetime.now().strftime("%Y-%m-%d")
        print(f'\n!! NO DATE PROVIDED, using todays date: {display_date}\n')

    activity_date = display_date

    my_account_number, my_account_hash = mri_schwab_lib.get_account()

    # print(f'884 my_account_hash:{my_account_hash}')

    start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date_dt = datetime.now(timezone.utc)

    orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)

    if orders:
        # print(f'\n 8974 orders type:{type(orders)}, data:\n{orders}\n')
        pass

    else:
        # print(f'\n 8974E no orders')
        pass
















    now_time = datetime.now()
    # current_time_str = now_time.strftime('%Y%M%d_%H%M%S')
    current_time_str = now_time.strftime("%y%m%d_%H%M%S")


    # print(f'activity_date type:{type(activity_date)}, value:{activity_date}')

    t_now = datetime.now(timezone.utc) 
    # print(f't_now  type:{type(t_now)}, value:{t_now}')

    # Convert activity_date from string to a datetime object
    new_date = datetime.strptime(activity_date, "%Y-%m-%d").date()

    # Create a datetime object that combines new_date with time & timezone from t_now
    new_date_time = datetime.combine(new_date, t_now.time()).replace(tzinfo=t_now.tzinfo)

    # print(f'new_date_time type:{type(new_date_time)}, value:{new_date_time}')


    # Replace the date in t_now while keeping the time and timezone
    t_activity_date = new_date_time.replace(year=new_date.year, month=new_date.month, day=new_date.day)

    # print(f't_activity_date: {t_activity_date}')


























    t_yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    # print(f't_yesterday  type:{type(t_yesterday)}, value:{t_yesterday}')
    # print(f't_t_now  type:{type(t_now)}, value:{t_now}')





    t_start_of_day = t_activity_date.replace(hour=0, minute=1, second=1, microsecond=1)
    start_of_date_dt = t_start_of_day
    # print(f'start_of_date_dt (activity_date) type:{type(start_of_date_dt)}, value:{start_of_date_dt}')

    t_end_of_day = t_activity_date.replace(hour=23, minute=59, second=1, microsecond=1)
    end_of_date_dt = t_end_of_day
    # print(f'end_of_date_dt (activity_date) type:{type(end_of_date_dt)}, value:{end_of_date_dt}')




    now_time = datetime.now()
    current_time_str = now_time.strftime('%H:%M:%S')


    my_transactions = mri_schwab_lib.get_transactions(start_of_date_dt, end_of_date_dt)
    # print(f'my_transactions type:{type(my_transactions)}')
    # print(f'my_transactions data:\n{my_transactions}')


    my_orders = mri_schwab_lib.get_orders (start_of_date_dt, end_of_date_dt)
    # print(f'my_orders type:{type(my_orders)}, data:\n{my_orders}')


    save_transactions_to_json(my_transactions)
    
    save_orders_to_json(my_orders)




if __name__ == "__main__":
    main()







