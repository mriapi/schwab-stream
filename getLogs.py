import json
import csv
import threading
import pandas as pd
# import schwabdev
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timezone, timedelta, date
from datetime import time as dt_time
import pytz
import argparse
# import positions
import mri_schwab_lib
import plotly.graph_objects as go
import numpy as np
import matplotlib.pyplot as plt








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




def create_return_dial(return_on_acct: float, filename: str = "balances_gauge.png"):
    # Clamp value to range [-3, 3]
    value = max(-3, min(3, return_on_acct))

    fig, ax = plt.subplots(figsize=(3, 2))
    ax.set_aspect('equal')
    ax.axis('off')

    # Map value (-3 to 3) → angle (180° to 0°)
    def value_to_angle(v):
        return 180 - (v + 3) * (180 / 6)

    # Draw colored arc segments
    segments = [
        (-3, -0.25, 'red'),
        (-0.25, 0.25, 'yellow'),
        (0.25, 3, 'green')
    ]

    for start, end, color in segments:
        angles = np.linspace(value_to_angle(start), value_to_angle(end), 100)
        x = np.cos(np.radians(angles))
        y = np.sin(np.radians(angles))
        ax.plot(x, y, lw=15, color=color, solid_capstyle='butt')

    # Draw tick marks (every 0.5)
    ticks = np.linspace(-3, 3, 13)
    for t in ticks:
        angle = value_to_angle(t)
        x_outer = np.cos(np.radians(angle))
        y_outer = np.sin(np.radians(angle))
        x_inner = 0.9 * x_outer
        y_inner = 0.9 * y_outer
        ax.plot([x_inner, x_outer], [y_inner, y_outer], color='black', lw=1)

    # Label major ticks
    major_labels = [-3, -2, -1, 0, 1, 2, 3]
    for t in major_labels:
        angle = value_to_angle(t)
        x = 1.15 * np.cos(np.radians(angle))
        y = 1.15 * np.sin(np.radians(angle))
        ax.text(x, y, f"{t}", ha='center', va='center', fontsize=8)

    # Title above dial
    # ax.text(0, 1.25, "% Daily Return On Account", ha='center', va='center', fontsize=10)
    ax.text(0, 1.32, "% Daily Return On Account", ha='center', va='center', fontsize=10)

    # Needle pivot (bottom center)
    pivot = np.array([0, 0])

    # Needle
    angle = value_to_angle(value)
    needle_length = 0.9
    x_end = needle_length * np.cos(np.radians(angle))
    y_end = needle_length * np.sin(np.radians(angle))

    ax.plot([pivot[0], x_end], [pivot[1], y_end], color='black', lw=2)
    ax.add_patch(plt.Circle(pivot, 0.05, color='black'))

    # Display value below pivot
    ax.text(0, -0.2, f"{value:.2f}%", ha='center', va='center', fontsize=9)

    # Frame limits
    ax.set_xlim(-1.3, 1.3)
    # ax.set_ylim(-0.3, 1.3)
    ax.set_ylim(-0.3, 1.4)

    # Save image
    plt.savefig(filename, bbox_inches='tight')
    plt.close(fig)


def my_get_balances(activity_date):

    retry_cnt = 0
    RETRY_MAX = 20


    while retry_cnt < RETRY_MAX:

        retry_cnt += 1

        try:

            initialBalance, currentBalance = mri_schwab_lib.get_balances()
            pnlAmount = float(currentBalance - initialBalance)
            pnlPercent = float((pnlAmount / initialBalance) * 100)

            print(f"\nInitial balance today: {initialBalance}, current balance: {currentBalance}")
            print(f"P/L: {pnlAmount:.2f}, {pnlPercent:.1f}%\n")

            break

        except Exception as e:
            print(f"error getting balances: {e}")
            time.sleep(1)

    if retry_cnt >= RETRY_MAX:
        print(f'could not get balances')

    else:

        balance_data = {
        "initialBalance": initialBalance,
        "currentBalance": currentBalance,
        "pnlAmount": pnlAmount,
        "pnlPercent": pnlPercent
        }

        # Convert to pretty JSON (2‑space indentation)
        balance_json = json.dumps(balance_data, indent=2)

        print(balance_json)

        today = activity_date
        log_file_dir = f"C:\\MEIC\\log\\{today}"
        print(f'Saving balances.json to {log_file_dir}')

        # Check for the existence of the directory and create it if it does not exist
        if not os.path.exists(log_file_dir):
            os.makedirs(log_file_dir)

        # Convert transactions to JSON format and save it to a file named transact.json
        json_file_path = os.path.join(log_file_dir, 'balances.json')
        with open(json_file_path, 'w') as json_file:
            json.dump(balance_data, json_file, indent=2)  


        # Save the same data in CSV format
    csv_file_spec = os.path.join(log_file_dir, 'balances.csv')



    with open(csv_file_spec, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        
        # Write header row (keys)
        writer.writerow(balance_data.keys())
        
        # Write data row (values)
        writer.writerow(balance_data.values())

        print(f"Saved balances.csv to {csv_file_spec}")

    

    csv_file_spec = os.path.join(log_file_dir, 'balances.csv')
    print(f'log_file_dir:{log_file_dir}')
    print(f'csv_file_spec:{csv_file_spec}')

    print(f'{csv_file_spec} contents:')

    with open(csv_file_spec, "r") as f:
        contents = f.read()
        print(contents)

    print(f'len of contents:{len(contents)}')




    print(f'csv_file_specfile size:')
    print(os.path.getsize(csv_file_spec))




    # -----------------------------
    # Load Data
    # -----------------------------
    df = pd.read_csv(csv_file_spec)
    print(f'df:\n{df}')

    pnl = df["pnlPercent"].iloc[0]

    pnl = float(pnl)

    create_return_dial(pnl,"balances_gauge.png")




    # # pnl = -3.0

    # min_val = -3
    # max_val = 3

    # # -----------------------------
    # # Create Gauge
    # # -----------------------------
    # fig = go.Figure(go.Indicator(
    #     mode="gauge+number",
    #     value=pnl,
    #     number={
    #         'suffix': '%',
    #         'valueformat': '.2f',
    #         'font': {'size': 28}
    #     },
    #     gauge={
    #         'axis': {
    #             'range': [min_val, max_val],
    #             'tickfont': {'size': 16}
    #         },
    #         'bar': {'color': "rgba(0,0,0,0)"},
    #         'steps': [
    #             {'range': [min_val, -0.3], 'color': "red"},
    #             {'range': [-0.3, 0.3], 'color': "yellow"},
    #             {'range': [0.3, max_val], 'color': "green"},
    #         ],
    #     }
    # ))

    # # -----------------------------
    # # Needle Geometry (FIXED)
    # # -----------------------------

    # # Horizontal center is correct
    # x_center = 0.5

    # # ✅ FIX: move pivot BELOW arc (true circle center)
    # y_center = 0.22   # tweak 0.20–0.25 if needed depending on layout

    # # Needle length (fits arc)
    # r = 0.48

    # # Plotly gauge doesn't span full 180°
    # SWEEP = 150  # stable across ranges

    # # Normalize pnl → 0..1
    # t = (pnl - min_val) / (max_val - min_val)
    # t = max(0, min(1, t))  # clamp

    # # Map to angle
    # theta_deg = 180 - ((180 - SWEEP) / 2 + t * SWEEP)
    # theta = np.deg2rad(theta_deg)

    # # Needle tip
    # x_tip = x_center + r * np.cos(theta)
    # y_tip = y_center + r * np.sin(theta)

    # # -----------------------------
    # # Add Needle
    # # -----------------------------
    # fig.add_shape(
    #     type="line",
    #     x0=x_center, y0=y_center,
    #     x1=x_tip, y1=y_tip,
    #     line=dict(color="black", width=4)
    # )

    # # -----------------------------
    # # Add Center Hub
    # # -----------------------------
    # hub_size = 0.02

    # fig.add_shape(
    #     type="circle",
    #     x0=x_center - hub_size,
    #     y0=y_center - hub_size,
    #     x1=x_center + hub_size,
    #     y1=y_center + hub_size,
    #     fillcolor="black",
    #     line_color="black"
    # )

    # # -----------------------------
    # # Final Layout
    # # -----------------------------
    # fig.update_layout(
    #     height=400,
    #     width=600,
    #     margin=dict(l=40, r=40, t=40, b=40)
    # )

    # fig.add_annotation(
    #     text="<b>% Daily Return On Account</b>",
    #     x=0.5,
    #     y=1.08,
    #     xref="paper",
    #     yref="paper",
    #     showarrow=False,
    #     font=dict(size=20)
    # )

    # # -----------------------------
    # # Save Image
    # # -----------------------------
    # gauge_png_file_spec = 'balances_gauge.png'
    # fig.write_image(gauge_png_file_spec)

    # # fig.show()






        



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
    print(f'activity_date type:{type(activity_date)}, value:{activity_date}')

    my_account_number, my_account_hash = mri_schwab_lib.get_account()

    # print(f'884 my_account_hash:{my_account_hash}')

    start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date_dt = datetime.now(timezone.utc)

    success_flag, orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)

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


    success_flag, my_orders = mri_schwab_lib.get_orders (start_of_date_dt, end_of_date_dt)
    # print(f'my_orders type:{type(my_orders)}, data:\n{my_orders}')


    save_transactions_to_json(my_transactions)
    
    save_orders_to_json(my_orders)


    # display_date is a string like "2026-02-13"
    try:
        # Convert string → date object
        display_dt = datetime.strptime(display_date, "%Y-%m-%d").date()

        # Get today's date in local/computer time
        today = date.today()

        if display_dt == today:
            print("display_date IS today's date")
        else:
            print("display_date is NOT today's date")

    except Exception as e:
        print(f"Error parsing display_date: {e}")


    my_get_balances(activity_date)



    print(f'display_date type:{type(display_date)}, value:{display_date}')




if __name__ == "__main__":
    main()







