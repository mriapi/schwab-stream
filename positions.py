import json
import threading
import pandas as pd
# import schwabdev
from dotenv import load_dotenv
import os
import time
import requests

client_lock = threading.Lock()


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

# Create an empty DataFrame with the desired headers
positions_df = pd.DataFrame(columns=["sym", "put_call", "qty", "trade_price", "now_price"])

lock = threading.Lock()

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
    




def get_positions(account_details):
    # Check for the 'positions' key


    



    try:

        if 'pfcbFlag' not in account_details['securitiesAccount']:
            # Handle the case where 'positions' key is not present
            print("No pfcbFlag in account data. aborting get_positions()")
            return
        
    except Exception as e:
        print(f"get_positions() 1, An error occurred: {e}, exiting positions processing")
        return
    

    try:
    

        if 'positions' in account_details['securitiesAccount']:
            my_positions = account_details['securitiesAccount']['positions']

            update_positions(my_positions)


            
            
            return



            print(f'my_positions type:{type(my_positions)}, data:\n{my_positions}')

    

    
            positions_json = json.dumps(my_positions, indent=4)
            print(f'positions_json type:{type(positions_json)}, data:\n{positions_json}')


            # position_cnt = 0
            
            # # Print the positions in human-readable format
            # print("Positions:")
            # for position in my_positions:
            #     print("\n")
            #     position_cnt += 1
            #     for key, value in position.items():
            #         if isinstance(value, dict):
            #             print(f"{key}:")
            #             for sub_key, sub_value in value.items():
            #                 print(f"  {sub_key}: {sub_value}")
            #         else:
            #             print(f"{key}: {value}")


        else:
            # Handle the case where 'positions' key is not present
            print("100 No positions in account data, resetting positions table")
            reset_positions()
            return

    except Exception as e:
            print(f"get_positions() 1, An error occurred: {e}, exiting positions processing")
            return



def get_positions2():
    # Check for the 'positions' key

    long_positions = []
    short_positions = []
    get_positions_success_flag = False

    app_key, secret_key, my_tokens_file = load_env_variables()

    retry_limit = 5

    while True:

        try:

            with client_lock:

                client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)
                linked_accounts = client.account_linked().json()
                # print(f'linked_accounts type:{type(linked_accounts)}, data:\n{linked_accounts}\n')

                account_hash = linked_accounts[0].get('hashValue')
                # print(f'account_hash type:{type(account_hash)}, data:\n{account_hash}\n')

                account_details = client.account_details(account_hash, fields="positions").json()
                # print(f'account_details type:{type(account_details)}, data:\n{account_details}\n')

        except Exception as e:
            print(f"get_positions2() 6, An error occurred: {e}, exiting positions processing")
            retry_limit -= 1
            if retry_limit > 0:
                time.sleep(0.1)
                continue
            else:
                get_positions_success_flag = False
                return get_positions_success_flag, short_positions, long_positions



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

            with client_lock:
                update_positions(my_positions)

            short_positions = short_options()
            long_positions = long_options()


            return get_positions_success_flag, short_positions, long_positions


        else:
            # Handle the case where 'positions' key is not present
            # print("200 No positions in account data, resetting positions table")
            reset_positions()
            return get_positions_success_flag, short_positions, long_positions

    except Exception as e:
            print(f"get_positions2() 8, An error occurred: {e}, exiting positions processing")
            get_positions_success_flag = False
            return get_positions_success_flag, short_positions, long_positions
    


tokens_file_path = r"C:\MEIC\cred\tokens_mri.json"
acct_file_path = r"C:\MEIC\cred\acct_mri.json"

def get_positions3():
    # Check for the 'positions' key

    long_positions = []
    short_positions = []
    get_positions_success_flag = False

    # print(f'p mri_tokens_file:{tokens_file_path}')

    try:
        # Open and read the JSON file
        with open(tokens_file_path, "r") as f:
            token_data = json.load(f)

    except Exception as e:
        print(f"110 positions.py Error opening file: {e}")
        return get_positions_success_flag, short_positions, long_positions
    
    try:

        # Extract values
        access_token_issue_date = token_data.get("access_token_issued")
        refresh_token_issue_date = token_data.get("refresh_token_issued")
        token_dict = token_data.get("token_dictionary", {})

        expires_time = token_dict.get("expires_in", 1800)  # Default to 1800 if missing
        token_type = token_dict.get("token_type", "Bearer")  # Default if missing
        scope = token_dict.get("scope", "api")  # Default if missing
        refresh_token = token_dict.get("refresh_token")
        access_token = token_dict.get("access_token")
        id_token = token_dict.get("id_token")

        # print(f'positons.py access_token:{access_token}')

    except Exception as e:
        print(f"120 positions.py extracting data: {e}")
        return get_positions_success_flag, short_positions, long_positions





    # Define the API URL
    url = "https://api.schwabapi.com/trader/v1/accounts?fields=positions"

    # Set headers for the request
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }


    try:

        # Make the API request
        response = requests.get(url, headers=headers)

    except Exception as e:
        print(f"1931 get_positions3(), An error occurred: {e}, exiting positions processing")
        get_positions_success_flag = False
        return get_positions_success_flag, short_positions, long_positions




    # Handle the response
    if response.status_code == 200:
        get_positions_success_flag = True

        # positions_data = response.json()
        account_details = response.json()
        # print(f'account_details type:{type(account_details)}, data:\n{account_details}')

        for account in account_details:
            account_number = account['securitiesAccount']['accountNumber']
            # print(f"\nAccount: {account_number}, Type: {account['securitiesAccount']['type']}")


            if 'positions' in account['securitiesAccount']:

                

                for position in account['securitiesAccount']['positions']:
                    instrument = position['instrument']
                    my_symbol = instrument['symbol']

                    # Check 13th character and print corresponding option type
                    option_type = "CALL" if my_symbol[12] == 'C' else "PUT" if my_symbol[12] == 'P' else "UNKNOWN"

                    # Extract strike price (characters 15 to 18)
                    strike = my_symbol[14:18]


                    # print(f"\n  Symbol: {instrument['symbol']} ({option_type} {strike})")
                    # print(f"  Description: {instrument['description']}")
                    # print(f"  Asset Type: {instrument['assetType']}")
                    # print(f"  Put/Call: {instrument.get('putCall', 'N/A')}")
                    # print(f"  Market Value: {position['marketValue']}")
                    # print(f"  Avg Price: {position.get('averagePrice', 'N/A')}")
                    # print(f"  Short Quantity: {position['shortQuantity']}")
                    # print(f"  Long Quantity: {position['longQuantity']}")
                    # print(f"  P/L Today: {position['currentDayProfitLoss']} ({position['currentDayProfitLossPercentage']}%)")


            else:
                # print(" No Positions ")
                pass



    else:
        print(f"Error: {response.status_code}, {response.text}")


        
    try:
        # if 'pfcbFlag' not in account_details['securitiesAccount']:
        if 'pfcbFlag' not in account_details[0]['securitiesAccount']:


            # Handle the case where 'positions' key is not present
            print("No pfcbFlag in account data. aborting get_positions()")
            get_positions_success_flag = False
            return get_positions_success_flag, short_positions, long_positions
        
    except Exception as e:
        print(f"395 get_positions3(), An error occurred: {e}, exiting positions processing")
        get_positions_success_flag = False
        return get_positions_success_flag, short_positions, long_positions
    

    try:
    

        if 'positions' in account_details[0]['securitiesAccount']:
            my_positions = account_details[0]['securitiesAccount']['positions']

            with client_lock:
                update_positions(my_positions)

            short_positions = short_options()
            long_positions = long_options()

            return get_positions_success_flag, short_positions, long_positions


        else:
            # Handle the case where 'positions' key is not present
            # print("200 No positions in account data, resetting positions table")
            reset_positions()
            return get_positions_success_flag, short_positions, long_positions

    except Exception as e:
            print(f"get_positions3() 8, An error occurred: {e}, exiting positions processing")
            get_positions_success_flag = False
            return get_positions_success_flag, short_positions, long_positions
    



# # test get_positions3()
# positions_success_flag, short_legs, long_legs = get_positions3()
# print(f'get_possitions3():\nsuccess flag:{positions_success_flag} \nshort legs:{short_legs} \nlong legs:{long_legs}\n')






