import json
import requests
from datetime import datetime, timezone, timedelta



access_token_issue_date = None
refresh_token_issue_date = None
expires_time = None
token_type= None
scope = None
refresh_token = None
access_token = None
id_token = None
account_number = None
account_hash = None



tokens_file_path = r"C:\MEIC\cred\tokens_mri.json"
acct_file_path = r"C:\MEIC\cred\acct_mri.json"






def get_tokens():
    global access_token_issue_date 
    global refresh_token_issue_date
    global expires_time
    global token_type
    global scope
    global refresh_token
    global access_token
    global id_token

    access_token_issue_date = None
    refresh_token_issue_date = None
    expires_time = None
    token_type= None
    scope = None
    refresh_token = None
    access_token = None
    id_token = None


    # print(f'reading  {tokens_file_path} ')



    try:
        # Open and read the JSON file
        with open(tokens_file_path, "r") as f:
            token_data = json.load(f)

    except Exception as e:
        print(f"1150 error with {tokens_file_path} file: {e}")
        return None, None, None, None, None, None, None, None
    
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

        # print(f'access_token_issue_date type:{type(access_token_issue_date)}, value:{access_token_issue_date}')
        # Convert to a datetime object
        issue_time = datetime.fromisoformat(access_token_issue_date)

        # Get current UTC time
        current_time = datetime.now(timezone.utc)

        # Calculate elapsed time in minutes
        elapsed_minutes = (current_time - issue_time).total_seconds() / 60

        # print(f"\nMinutes since access token was issued: {elapsed_minutes:.2f}")
        if elapsed_minutes > 29:
            print(f"!!! WARNING: The access token has expired.  Elapsed minutes:{elapsed_minutes:.2f}. Make sure that tokens.py is running")





    except Exception as e:
        print(f"1140 error extracting values: {e}")
        return None, None, None, None, None, None, None, None
    
    return (
        access_token_issue_date, 
        refresh_token_issue_date, 
        expires_time, 
        token_type, 
        scope, 
        refresh_token, 
        access_token, 
        id_token)
    


def get_account():
    global account_number
    global account_hash

    account_number = None
    account_hash = None

    # print(f'reading  {acct_file_path} ')



    try:
        # Open and read the JSON file
        with open(acct_file_path, "r") as f:
            account_data = json.load(f)

    except Exception as e:
        print(f"2150 error with {acct_file_path} file: {e}")
        return None, None
    
    try:

        # Extract values
        account_number = account_data.get("account_number")
        account_hash = account_data.get("account_hash")

        # print(f'got account_number:{account_number}, account_hash:{account_hash}')


    except Exception as e:
        print(f"2140 error extracting values: {e}")
        return None, None
    
    return account_number, account_hash





def get_orders(from_entered_time, to_entered_time):

    # print(f'004 account_hash:{account_hash}')

    get_tokens()
    get_account()
    # print(f'account_hash:{account_hash}')

    """Retrieve orders for a specific account from Schwab API."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/orders"
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    params = {
        "fromEnteredTime": from_entered_time.isoformat() if isinstance(from_entered_time, datetime) else from_entered_time,
        "toEnteredTime": to_entered_time.isoformat() if isinstance(to_entered_time, datetime) else to_entered_time
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()  # Return parsed JSON response
    else:
        print(f"930 Error: {response.status_code}, {response.text}")
        return None




def get_linked_accounts():
    """Retrieve linked account numbers and hashes from Schwab API."""

    get_tokens()
    print(f'access_token:{access_token}')

    linked_accounts = None

    url = "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "*/*"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        linked_accounts = response.json()

    else:
        print(f"934 Error: {response.status_code}, {response.text}")
        

    return linked_accounts

def get_access_token():
    get_tokens()
    return access_token



def get_transactions(start_date, end_date):

    get_tokens()
    get_account()

    # print(f'access_token:{access_token}')

    """Retrieve transactions for a specific account from Schwab API."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/transactions"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    params = {
        "startDate": start_date.isoformat() if isinstance(start_date, datetime) else start_date,
        "endDate": end_date.isoformat() if isinstance(end_date, datetime) else end_date,
        "types": "TRADE"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()  # Return parsed JSON response
    else:
        print(f"739 Error: {response.status_code}, {response.text}")
        return None

    

    

def display_all():
    print(f'access_token_issue_date:{access_token_issue_date}')
    print(f'refresh_token_issue_date:{refresh_token_issue_date}')
    print(f'expires_time:{expires_time}')
    print(f'token_type:{token_type}')
    print(f'scope:{scope}')
    print(f'refresh_token:{refresh_token}')
    print(f'id_token:{id_token}')
    print(f'account_number:{account_number}')
    print(f'account_hash:{account_hash}')

    pass


def get_account_details():

        # return self._session.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}',
        #                     headers={'Authorization': f'Bearer {self.tokens.access_token}'},
        #                     params=self._params_parser({'fields': fields}),
        #                     timeout=self.timeout)


    get_tokens()

    account_details = None

    # Define the API URL
    url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}"

    # Set headers for the request
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # Make the API request
    response = requests.get(url, headers=headers)

    # Handle the response
    if response.status_code == 200:
        # positions_data = response.json()
        account_details = response.json()

    return account_details


REQUESTS_GET_TIMEOUT = 10

def get_opt_quote_last(sym):

    get_tokens()

    sym_last = None
    

    success_flag = False

    rx_accessToken = access_token

    if rx_accessToken == None:
        print(f'unable to get opt quote, rx_accessToken is None')
        return sym_last

    # quotes_response_json = None
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
        # get_quote_fail_count += 1
        success_flag = False

        error_message = str(e)

        if "Failed to resolve 'api.schwabapi.com'" in error_message:
            info_str = f"3100 Detected network error. Failed to resolve 'api.schwabapi.com'. returning"
            print(info_str)
            # logging.error(info_str)

        else:
            info_str = f'3101A exception requesting quote for {sym} :{e} at {current_time_str}, returning'
            # logging.error(info_str)
            print(info_str)
            

            # try:
            #     code = response.status_code

            #     info_str = f'3301B failure quote failure code:{code}'
            #     logging.error(info_str)
            #     print(info_str)


            # except Exception as e:
            #     info_str = f'3101C failure getting response code afer get exception :{e}'
            #     logging.error(info_str)
            #     print(info_str)

        return sym_last
    
    
    try:

        if response.status_code != 200:
            info_str = f'1460 get_opt_quotes request failed:{response.status_code} at {current_time_str}, returning'
            print(info_str)
            # logging.error(info_str)
            return sym_last
        

        
    except Exception as e:
        # get_quote_fail_count += 1
        info_str = f'1470 exception requesting quotes :{e} at {current_time_str}, returning'
        print(info_str)
        # logging.error(info_str)
        return sym_last

    try:

        quote_response_json = response.json()
        # print(f'single quote quote_response_json json type:{type(quote_response_json)}:\n{quote_response_json}') 
        pretty_json = json.dumps(quote_response_json, indent=2)
        print(f'single quote requests.get pretty_json type:{type(pretty_json)}:\n{pretty_json}') 

        # # Extract lastPrice from the nested JSON
        sym_last = float(quote_response_json[sym]["quote"]["lastPrice"])

        # # Display with 2 decimal places
        # print(f"SPX last price: {spx_last:.2f}")

    except Exception as e:
        # get_quote_fail_count += 1
        info_str = f'734 exception requesting quotes :{e} at {current_time_str}, returning' 
        print(info_str)
        # logging.error(info_str)
        return sym_last


    return sym_last





def get_strike_value_from_sym(sym):
    strike_val = None
    # Extract strike price (characters 15 to 18)
    strike_str = sym[14:18]
    if len(strike_str) == 4:
        strike_val = int(float(strike_str))

    return strike_val

def get_opt_type_from_sym(sym):
    type_str = None
    if "P0" in sym:
        type_str = "PUT"

    elif "C0" in sym:
        type_str = "CALL"


    return type_str




def get_positions():
    

    long_positions = []
    short_positions = []
    get_positions_success_flag = False

    get_tokens()



    # Define the API URL
    url = "https://api.schwabapi.com/trader/v1/accounts?fields=positions"

    # Set headers for the request
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # Make the API request
    response = requests.get(url, headers=headers)




    # Handle the response
    if response.status_code == 200:
        # positions_data = response.json()
        account_details = response.json()
        # print(f'account_details type:{type(account_details)}, data:\n{account_details}')

        for account in account_details:
            account_number = account['securitiesAccount']['accountNumber']
            # print(f"\nAccount: {account_number}, Type: {account['securitiesAccount']['type']}")


            if 'positions' in account['securitiesAccount']:

                get_positions_success_flag = True

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



                    # Add positions based on quantities
                    if position['shortQuantity'] > 0:
                        short_positions.append({"symbol": my_symbol, "quantity": position['shortQuantity']})
                    
                    if position['longQuantity'] > 0:
                        long_positions.append({"symbol": my_symbol, "quantity": position['longQuantity']})




            else:
                print(" No Positions ")



    else:
        print(f"280 Error: {response.status_code}, {response.text}")


    return short_positions, long_positions












def get_option_buying_power():

    buyingPowerFl = None
    currentFundsForTrading = None

    try:

        
        access_token = get_access_token()
        # print(f'access_token type{type(access_token)}, data:\n{access_token}')



        # Define the API URL
        url = "https://api.schwabapi.com/trader/v1/accounts?"

        # Set headers for the request
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        # Make the API request
        response = requests.get(url, headers=headers)

        # print(f'100 response.status_code:{response.status_code}')

        if response.status_code == 200:
            response_json = response.json()
            # print(f'response_json:\n{response_json}')
            # print("Formatted response_json:")
            # print(json.dumps(response_json, indent=2))

            
            # Navigate to the nested field
            buyingPowerFl = float(
                response_json[0]['securitiesAccount']['currentBalances']['buyingPowerNonMarginableTrade']
            )
            # print(f"My Buying Power: {myBuyingPowerFl}")

            initialBalanceFl = float(
                response_json[0]['securitiesAccount']['initialBalances']['cashBalance']
            )
            # print(f"initial balance: {initialBalanceFl}")

             

            currentFundsForTrading = float(
                response_json[0]['securitiesAccount']['currentBalances']['availableFundsNonMarginableTrade']

            )
            # print(f"currentFundsForTrading: {currentFundsForTrading}")

            



            # balances = data['securitiesAccount']['currentBalances']
            # print("Buying Power:", balances.get('buyingPower'))
            # print("Cash Available:", balances.get('cashAvailableForTrading'))
            # print("Margin Balance:", balances.get('marginBalance'))
        else:
            print("buying power response Error:", response.status_code, response.text)


    except Exception as e:
        print(f'error attempting to get buying power:{e}')
        buyingPowerFl = None
        currentFundsForTrading = None



    return buyingPowerFl, currentFundsForTrading









def get_today_in_epoch():
    global todays_epoch_time 

    # Calculate the time in milliseconds since the UNIX epoch
    now = datetime.now(timezone.utc)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    todays_epoch_time  = int((now - epoch).total_seconds() * 1000.0)
    return todays_epoch_time




# test

# (access_token_issue_date, refresh_token_issue_date,
# expires_time, token_type, scope, 
# refresh_token, access_token, 
# id_token) = get_tokens()
# print(f'access_token_issue_date:{access_token_issue_date}, refresh_token_issue_date:{refresh_token_issue_date}')
# print(f'expires_time:{expires_time}, token_type:{token_type}')
# print(f'expires_time:{expires_time}, token_type:{token_type}, scope:{scope}')
# print(f'refresh_token:{refresh_token}, access_token:{access_token}, id_token:{id_token}')

# my_account_number, my_account_hash = get_account()
# print(f'my_account_number:{my_account_number}, my_account_hash:{my_account_hash}')
# display_all()



# my_account_number, my_account_hash = get_account()

# # start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) # today
# start_of_date_dt = datetime.now(timezone.utc) - timedelta(days=1) # yesterday
# end_of_date_dt = datetime.now(timezone.utc)
# transactions = get_transactions(start_of_date_dt, end_of_date_dt)

# if transactions:
#     print(f'\n 2972 transactions:\n{transactions}\n')
# else:
#     print(f'\n 2973 no transactions')


# start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) # today
# start_of_date_dt = datetime.now(timezone.utc) - timedelta(days=1) # yesterday
# end_of_date_dt = datetime.now(timezone.utc)

# my_account_number, my_account_hash = get_account()
# orders = get_orders(start_of_date_dt, end_of_date_dt)

# if orders:
#     print(f'\n 2974 orders:\n{orders}\n')

# shorts, longs = get_positions()

# print(f'shorts type:{type(shorts)}, data:\n{shorts}')
# print(f'longs type:{type(longs)}, data:\n{longs}')

# linked_accounts = get_linked_accounts()
# print(f'linked_accounts type:{type(linked_accounts)}, data:{linked_accounts}')




