import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path


import time
import uuid
import threading

import paho.mqtt.client as mqtt




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



def get_qty_of_working_stops():

    qty_working_stops = 0
    success_flag = False



    start = datetime.now()

    start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date_dt = datetime.now(timezone.utc)

    # print(f'orders start:{start_of_date_dt}, end:{end_of_date_dt}')

    working_stops = []   # this will become your JSON‑serializable list


    success_flag, orders = get_orders(start_of_date_dt, end_of_date_dt)

    if success_flag is True:

        # print(f'orders type:{type(orders)}')
        # print(f'40573 TP orders, type:{type(orders)}, data:')
        # print(json.dumps(orders, indent=2))

        for idx, order in enumerate(orders, start=1):

            pass
                    
            # print(f"\n=== Iron Condor #{idx} ===")
            # print(f"Order ID: {order.get('orderId')}")
            # print(f"Status:   {order.get('status')}")
            # print(f"Price:    {order.get('price')}")
            # print(f"Quantity: {order.get('quantity')}")
            # print("Legs:")

            # ---- MAIN ORDER LEGS ----
            # for leg in order.get("orderLegCollection", []):
            #     instr = leg["instrument"]
            #     print(f"  Leg {leg['legId']}:")
            #     print(f"    Instruction:   {leg['instruction']}")
            #     print(f"    Put/Call:      {instr['putCall']}")
            #     print(f"    Symbol:        {instr['symbol']}")
            #     print(f"    Instrument ID: {instr['instrumentId']}")
            #     print(f"    Quantity:      {leg['quantity']}")

            # ---- CHILD ORDER STRATEGIES ----
            child_orders = order.get("childOrderStrategies", [])
            if child_orders:
                # print("\n  Child Order Strategies:")
                pass
            
            for child_idx, child in enumerate(child_orders, start=1):
                found_working_stop = False

                # print(f"\n    Child #{child_idx}:")
                # print(f"      orderType:          {child.get('orderType')}")
                # print(f"      quantity:           {child.get('quantity')}")
                # print(f"      orderStrategyType:  {child.get('orderStrategyType')}")
                # print(f"      orderId:            {child.get('orderId')}")
                # print(f"      status:             {child.get('status')}")
                # print(f"      stopPrice:          {child.get('stopPrice')}")
                pass

                
                # if child.get("orderType") == "STOP" and child.get("status") == "WORKING" and child.get("orderStrategyType") == "TRIGGER":

                if (
                    child.get("orderType") == "STOP"
                    and child.get("status") == "WORKING"
                    and child.get("orderStrategyType") == "TRIGGER"
                ):


                    # print("Found WORKING STOP order:")
                    # print(f"  orderId:     {child.get('orderId')}")
                    # print(f"  quantity1:   {child.get('quantity')}")
                    # print(f"  stopPrice:   {child.get('stopPrice')}")
                    qty_working_stops += 1
                    pass

    
    if success_flag is True:
        # print(f'gqws220 qty of working stop orders:{qty_working_stops}')
        pass

    else:
        print(f'gqws222 get orders was not successful') 
        pass

    return success_flag, qty_working_stops





def get_orders(from_entered_time, to_entered_time):

    success_flag = True
    orders_none = None

    try:

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
            success_flag = True
            return success_flag, response.json()  # Return parsed JSON response
        else:
            print(f"930 Error: {response.status_code}, {response.text}")
            success_flag = False
            return success_flag, orders_none
        
    except Exception as e:
        print(f"Error in mri_schwab_lib.get_orders(): {e}")
        success_flag = False

    return success_flag, orders_none






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

    print(f'access token:{access_token}')

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

    print(f'2059 get_opt_quote\n  url:{url}\n  params:{params}\n  headers:{headers}')

    try:

        # Make the GET request
        response = requests.get(url, params=params, headers=headers, timeout=REQUESTS_GET_TIMEOUT)

    except Exception as e:

        print(f'2946 exception in get opt quote, e:{e}')


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
        print(f'0485 single quote quote_response_json json type:{type(quote_response_json)}:\n{quote_response_json}') 

        pretty_json = json.dumps(quote_response_json, indent=2)
        print(f'0488 single quote requests.get pretty_json type:{type(pretty_json)}:\n{pretty_json}') 

        # # Extract lastPrice from the nested JSON
        sym_last = float(quote_response_json[sym]["quote"]["lastPrice"])

        print(f'0492 ')

        # # Display with 2 decimal places
        print(f"0494 SPX last price: {sym_last:.2f}")

    except Exception as e:
        # get_quote_fail_count += 1
        info_str = f'734 exception requesting quotes :{e} at {current_time_str}, returning' 
        print(info_str)
        # logging.error(info_str)
        return sym_last


    return sym_last


def get_opt_quote_bid(sym):

    get_tokens()

    sym_bid = None
    

    success_flag = False

    rx_accessToken = access_token

    # print(f'access token:{access_token}')

    if rx_accessToken == None:
        print(f'30056 unable to get opt quote, rx_accessToken is None')
        return sym_bid

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

    # print(f'1059 get_opt_quote\n  url:{url}\n  params:{params}\n  headers:{headers}')

    try:

        # Make the GET request
        response = requests.get(url, params=params, headers=headers, timeout=REQUESTS_GET_TIMEOUT)

    except Exception as e:

        # print(f'1946 exception in get opt quote, e:{e}')


        # get_quote_fail_count += 1
        success_flag = False

        error_message = str(e)

        if "Failed to resolve 'api.schwabapi.com'" in error_message:
            info_str = f"11100 Detected network error. Failed to resolve 'api.schwabapi.com'. returning"
            print(info_str)
            # logging.error(info_str)

        else:
            info_str = f'11101A exception requesting quote for {sym} :{e} at {current_time_str}, returning'
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

        return sym_bid
    
    
    try:

        if response.status_code != 200:
            info_str = f'0460 get_opt_quotes request failed:{response.status_code} at {current_time_str}, returning'
            print(info_str)
            # logging.error(info_str)
            return sym_bid
        

        
    except Exception as e:
        # get_quote_fail_count += 1
        info_str = f'0470 exception requesting quotes :{e} at {current_time_str}, returning'
        print(info_str)
        # logging.error(info_str)
        return sym_bid

    try:

        quote_response_json = response.json()
        # print(f'8485 single quote quote_response_json json type:{type(quote_response_json)}:\n{quote_response_json}') 

        # pretty_json = json.dumps(quote_response_json, indent=2)
        # print(f'8488 single quote requests.get pretty_json type:{type(pretty_json)}:\n{pretty_json}') 

        # # Extract lastPrice from the nested JSON
        sym_bid = float(quote_response_json[sym]["quote"]["bidPrice"])

        # print(f'8492 ')

        # # Display with 2 decimal places
        # print(f"8494 SPX bid price: {sym_bid:.2f}")

    except Exception as e:
        # get_quote_fail_count += 1
        info_str = f'734 exception requesting quotes :{e} at {current_time_str}, returning' 
        print(info_str)
        # logging.error(info_str)
        return sym_bid


    return sym_bid




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










def get_balances():

    initialBalance = None
    currentBalance = None

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



            initialBalanceFl = float(
                response_json[0]['securitiesAccount']['initialBalances']['cashBalance']
            )
            currentBalanceFl = float(
                response_json[0]['securitiesAccount']['currentBalances']['cashBalance']
            )

            initialBalance = initialBalanceFl
            currentBalance = currentBalanceFl

            # pnlFl = currentBalanceFl - initialBalanceFl
            # pnlPercent = (pnlFl / initialBalanceFl) * 100

            # print(f"initial balance today: {initialBalanceFl}, current balance: {currentBalanceFl}")
            # print(f"P/L: {pnlFl}, {pnlPercent}%")

    
        else:
            print("pnl response Error:", response.status_code, response.text)


    except Exception as e:
        print(f'error attempting to get pnl:{e}')
        buyingPowerFl = None
        currentFundsForTrading = None



    return initialBalance, currentBalance






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


            # initialBalanceFl = float(
            #     response_json[0]['securitiesAccount']['initialBalances']['cashBalance']
            # )
            # currentBalanceFl = float(
            #     response_json[0]['securitiesAccount']['currentBalances']['cashBalance']
            # )

            # pnlFl = currentBalanceFl - initialBalanceFl
            # pnlPercent = (pnlFl / initialBalanceFl) * 100

            # print(f"initial balance today: {initialBalanceFl}, current balance: {currentBalanceFl}")
            # print(f"P/L: {pnlFl}, {pnlPercent}%")


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



def get_liquid_balances():

    buyingPowerFl = None
    currentFundsForTrading = None
    initialLiquidationFl = None
    currentLiquidationFl = None


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

            initialLiquidationFl = float(
                response_json[0]['securitiesAccount']['initialBalances']['liquidationValue']
            )

            currentLiquidationFl = float(
                response_json[0]['securitiesAccount']['currentBalances']['liquidationValue']
            )




            # initialBalanceFl = float(
            #     response_json[0]['securitiesAccount']['initialBalances']['cashBalance']
            # )
            # currentBalanceFl = float(
            #     response_json[0]['securitiesAccount']['currentBalances']['cashBalance']
            # )

            # pnlFl = currentBalanceFl - initialBalanceFl
            # pnlPercent = (pnlFl / initialBalanceFl) * 100

            # print(f"initial balance today: {initialBalanceFl}, current balance: {currentBalanceFl}")
            # print(f"P/L: {pnlFl}, {pnlPercent}%")


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
        print(f'error attempting to get balances:{e}')
        buyingPowerFl = None
        currentFundsForTrading = None
        initialLiquidationFl = None
        currentLiquidationFl = None


    return buyingPowerFl, currentFundsForTrading, initialLiquidationFl, currentLiquidationFl




def delete_working_orders(orderIDs):

    print(f'in delete_working_stop_orders()')

    print(f'\nMRILIBDWO3150 delete_working_orders for orderIDs:{orderIDs}\n')

    get_tokens()
    get_account()


    order_cnt = 0
    orders_cancelled_cnt = 0


    # for working_order in orderIDs:
    for my_order_id in orderIDs:





        order_cnt += 1

        print(f'MRI8905 in delete_working_orders(), my_order_id type:{type(my_order_id)}, data:{my_order_id}')



        print(f'\ncancelling working order ID #{my_order_id}')
        # cancel_response = client.order_cancel(account_hash, my_order_id)
        # print (f'cancel_response type{type(cancel_response)}, data:\n{cancel_response}')

        # continue






    #    return self._session.delete(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders/{orderId}',
    #                            headers={'Authorization': f'Bearer {self.tokens.access_token}'},
    #                            timeout=self.timeout)


        url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/orders/{my_order_id}"
    
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        cancel_response = requests.delete(url, headers=headers)




        if cancel_response.status_code == 200:
            print(f'Cancellation of order ID {my_order_id} was successful')
            orders_cancelled_cnt +=1
        else:
            print(f'Cancellation attempt failed with status code {cancel_response.status_code}')

    return orders_cancelled_cnt








# MQTT Configuration
BROKER_ADDRESS = "localhost"
PORT_NUMBER = 1883
CREDS_REQUEST_TOPIC = "mri/creds/request/#"
CREDS_INFO_TOPIC = "mri/creds/info"
CREDS_REQUEST_PREFIX = "mri/creds/request"


def get_creds_mqtt():

    start_time = time.perf_counter()

    creds_received_event = threading.Event()
    creds_data = {}

    client = None

    try:

        request_id = str(uuid.uuid4())
        request_topic = f"{CREDS_REQUEST_PREFIX}/{request_id}"

        def on_connect(client, userdata, flags, rc, properties=None):
            print(f"MQTT connected, rc={rc}")
            client.subscribe(CREDS_INFO_TOPIC)

        def on_message(client, userdata, msg):
            nonlocal creds_data

            try:
                payload = msg.payload.decode("utf-8")
                creds_data = json.loads(payload)

                print(
                    f"MQTT credential response received on topic:{msg.topic}"
                )

                creds_received_event.set()

            except Exception as e:
                print(f"MQTT credential payload error: {e}")

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )

        client.on_connect = on_connect
        client.on_message = on_message

        client.connect(BROKER_ADDRESS, PORT_NUMBER, keepalive=30)

        client.loop_start()

        #
        # Give subscription a moment to establish
        #
        time.sleep(0.1)

        request_payload = json.dumps(
            {
                "requestId": request_id,
                "timestamp": time.time()
            }
        )

        print(f"Publishing credential request to {request_topic}")

        client.publish(
            request_topic,
            request_payload,
            qos=1
        )

        #
        # Wait for credential response
        #
        if not creds_received_event.wait(timeout=10.0):

            elapsed = time.perf_counter() - start_time

            print(
                f"get_creds_mqtt() timeout after "
                f"{elapsed:.3f} seconds"
            )

            return None, None, None, None, None

        elapsed = time.perf_counter() - start_time

        print(
            f"get_creds_mqtt() completed in "
            f"{elapsed:.3f} seconds"
        )

        refresh_token = creds_data.get("refreshToken")
        access_token = creds_data.get("accessToken")
        acct_hash = creds_data.get("acctHash")
        customer_id = creds_data.get("customerId")
        correl_id = creds_data.get("correlId")

        return (
            refresh_token,
            access_token,
            acct_hash,
            customer_id,
            correl_id
        )

    except Exception as e:

        elapsed = time.perf_counter() - start_time

        print(
            f"get_creds_mqtt() exception after "
            f"{elapsed:.3f} seconds: {e}"
        )

        return None, None, None, None, None

    finally:

        try:
            if client is not None:
                client.loop_stop()
                client.disconnect()
        except Exception:
            pass




def extract_stop_order_leg_statuses(response):
    """
    Takes a 'response' object from requests.get() and returns:
      - parent_status
      - child_status

    Returns (None, None) if parsing fails.
    """

    parent_status = None
    child_status = None

    try:
        data = response.json()
    except Exception:
        return None, None

    # Parent order status
    try:
        parent_status = data.get("status")

        # Child order status (only if present)
        child_status = None
        child_list = data.get("childOrderStrategies")

        if isinstance(child_list, list) and len(child_list) > 0:
            child_status = child_list[0].get("status")
    except:
        return None, None

    return parent_status, child_status


def get_order_details(orderID):

    account_hash, access_token

    success_flag = False
    order_detail_response = None

    try:
            

        # print(f'MRILGODO2050 get details for orderID:{orderID}')

        get_tokens()
        get_account()


        my_order_id = orderID


        # print(f'MRIGOD8805 in get_order_details(), my_order_id type:{type(my_order_id)}, data:{my_order_id}')



        url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/orders/{my_order_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        order_detail_response = requests.get(url, headers=headers)

        # print(f'MRILIGODB9402 cancel_response type:{type(order_detail_response)}, data:{order_detail_response}')


        if order_detail_response.status_code == 200 or order_detail_response.status_code == 201:
            # print(f'MRIGOD8912 request for details of order ID {my_order_id} was successful, code:{order_detail_response.status_code}')
            
            # try:
            #     response_json = order_detail_response.json()
            #     print(f'MRIGOD4950 good order_detail_response JSON, type:{type(response_json)}, data:\n{response_json}')
            # except Exception:
            #     print("MRIGOD4950 good order_detail_response -- No JSON body")
            
            
            success_flag = True

        else:
            print(f'MRIGOD8913 get order details attempt failed with status code {order_detail_response.status_code}')
            success_flag = False
            try:
                print("MRIGOD response URL:", order_detail_response.url)
                print("MRIGOD response Headers:", order_detail_response.headers)
                print("MRIGOD response Body:", order_detail_response.text)
            except:
                print("MRIGOD response exception while trying to display .url, .headers, .text")
                pass

            try:
                print("MRIGOD JSON:", order_detail_response.json())
            except:
                print("MRIGOD No JSON in response")



    except Exception as e:
        print(f"MRILIBGOD9225 exeption:{e}")

    return success_flag, order_detail_response





def delete_working_stop_order(orderID):

    global account_hash, access_token

    print(f'in delete_working_stop_order()')

    success_flag = False
    orders_cancelled_cnt = 0
    my_order_id = orderID

    try:
            

        print(f'\nMRILIBDWO2050 delete_working_order for orderID:{orderID}\n')

        time.sleep(0.1)

        success_flag, response = get_order_details(my_order_id)

            
        if success_flag is True:
            if response.status_code in (200, 201):
                print(f'MRI6912 request for details of order ID {my_order_id} was successful, code:{response.status_code}')

                parent_status, child_status = extract_stop_order_leg_statuses(response)

                # print(f'parent_status type:{type(parent_status)}, value:{parent_status}')
                # print(f'child_status type:{type(child_status)}, value:{child_status}')

                if parent_status == "CANCELED":
                    print(f'MRI69430 parent order {my_order_id} IS ALREADY cancelled, aborting the attempt to delete again')
                    return True, 1

                else:
                    print(f'MRI69431 parent is not already cancelled')

                if child_status == "CANCELED":
                    print(f'MRI69435 child IS ALREADY cancelled')

                else:
                    print(f'MRI69435 child is not alreadycancelled')


        get_tokens()
        get_account()

        # mqtt_refresh_token, mqtt_access_token, mqtt_account_hash, customer_id, correl_id = get_creds_mqtt()

        # if mqtt_access_token is None or mqtt_access_token is None:
        #     get_tokens()
        #     get_account()

        # else:
        #     account_hash = mqtt_account_hash
        #     access_token = mqtt_access_token
        #     print(f'MRILIB delete_working_order() using mqtt credentials')


        order_cnt = 0
        orders_cancelled_cnt = 0

        


        order_cnt += 1

        print(f'MRI8805 in delete_working_order(), my_order_id type:{type(my_order_id)}, data:{my_order_id}')



        print(f'\n MRI8907 cancelling working order ID #{my_order_id}')


        url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/orders/{my_order_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        cancel_response = requests.delete(url, headers=headers)

        print(f'MRILIB9402 cancel_response type:{type(cancel_response)}, data:{cancel_response}')


        if cancel_response.status_code == 200 or cancel_response.status_code == 201:
            print(f'MRI8912 Cancellation of order ID {my_order_id} was successful')
            success_flag = True
            orders_cancelled_cnt +=1
        else:
            print(f'MRI8913 Cancellation attempt failed with status code {cancel_response.status_code}')
            success_flag = False
            try:
                print("MRIFCO response URL:", cancel_response.url)
                print("MRIFCO response Headers:", cancel_response.headers)
                print("MRIFCO response Body:", cancel_response.text)
            except:
                print("MRIFCO response exception while trying to display .url, .headers, .text")
                pass

            try:
                print("MRIFCO JSON:", cancel_response.json())
            except:
                print("MRIFCO No JSON in response")



    except Exception as e:
        print(f"MRILIB9225 exeption:{e}")

    return success_flag, orders_cancelled_cnt


def prep_genlogs_dirs():
    # Build today's date string
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Build full directory path
    target_dir = fr"C:\MEIC\log\{today_str}\omeic\tos\recommendations"

    # Create the directory (including parents) if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)

    # Build full directory path
    target_dir = fr"C:\MEIC\log\{today_str}\omeic\tos\positions"

    # Create the directory (including parents) if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)

    target_dir = fr"C:\MEIC\log\{today_str}\transactions"
    os.makedirs(target_dir, exist_ok=True)


    # print(f"Directory ensured: {target_dir}")
    # pass



def persist_early_indicator(time_str):
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Build full directory path
    target_dir = fr"C:\MEIC\log\{today_str}\omeic\tos\positions"

    # Create the directory (including parents) if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)

        # Full path to the EARLY file (no extension)
    early_file_path = os.path.join(target_dir, "EARLY")

    # Write the time_str into the EARLY file
    with open(early_file_path, "w", encoding="utf-8") as f:
        f.write(time_str)


def persist_spx_candle(spx_day_ohlc):

    # Saves the SPX candle data to:
    # C:\\MEIC\\log\<today>\\transactions\\spx--<today>.json


    try:
            

        # Determine today's date in YYYY-MM-DD format
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Build folder paths
        # base_dir = f"C:\\MEIC\\SPX\\{today_str}"
        base_dir = f"C:\\MEIC\\LOG\\{today_str}"
        transactions_dir = os.path.join(base_dir, "transactions")

        # Ensure folders exist
        os.makedirs(transactions_dir, exist_ok=True)

        # Build full file path
        file_path = os.path.join(transactions_dir, f"spx-{today_str}.json")

        # Write JSON in human-readable format
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(spx_day_ohlc, f, indent=2)

    except Exception as e:
        info_str = f'MRIPSC 3020 persist spx candle error: {e}'
        print(info_str)
        spx_day_ohlc = None


def get_spx_today_ohlc():

    try:

        get_tokens()
        get_account()

        my_epoch_time = get_today_in_epoch()



        # global get_current_day_history_lock
        # global spx_open, spx_high, spx_low, spx_close
        # global ohlc_get_time, chain_strike_cnt
        # global todays_epoch_time, rx_accessToken

        spx_day_ohlc = None

        if my_epoch_time is None or access_token is None:
            print(f'gsctohlc unable to get ohlc, todays_epoch_time: {my_epoch_time}, access_token:{access_token}')
            return None
        
        my_epoch_start_time = my_epoch_time - 10000
        my_epoch_end_time = my_epoch_start_time + 9000
        
        # print(f'gsctohlc got todays_epoch_time type: {type(my_epoch_time)}, value:{my_epoch_time}, access_token:{access_token}')
    
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        params = {
            "symbol": "$SPX",
            "periodType": "month",
            "period": 1,
            "frequencyType": "daily",
            "frequency": 1,
            "startDate": my_epoch_start_time,
            "endDate": my_epoch_end_time,
            "needExtendedHoursData": "false",
            "needPreviousClose": "false"
        }

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.get(url, headers=headers, params=params, timeout=REQUESTS_GET_TIMEOUT)

        if response.status_code == 200:
            spx_day_ohlc = response.json()

            # print(f'39050 spx_day_ohlc type:{type(spx_day_ohlc)}, data:\n{spx_day_ohlc}')



            # if 'candles' in spx_day_ohlc and 'empty' in spx_day_ohlc and not spx_day_ohlc['empty']:
            #     first_candle = spx_day_ohlc['candles'][0]

            #     # with get_current_day_history_lock:
            #     #     spx_open = first_candle['open']
            #     #     spx_high = first_candle['high']
            #     #     spx_low = first_candle['low']
            #     #     spx_close = first_candle['close']
            #     #     ohlc_get_time = datetime.now()

            #     #     day_high_distance = abs(spx_close - spx_high)
            #     #     day_low_distance = abs(spx_close - spx_low)
            #     #     max_distance = max(day_high_distance, day_low_distance)
            #     #     chain_strike_cnt = int(max_distance / 5) + 50

            #     print(f'285 chain_strike_cnt: {chain_strike_cnt} at {ohlc_get_time.strftime("%H:%M:%S")}')
            # else:
            #     print("SPX data response is empty or malformed.")


        else:
            print(f"gstohlc Failed to fetch SPX data. Status code: {response.status_code}")


            print("gstohlc bad response Text:", response.text)

            try:
                print("gstohlc bad response JSON:", response.json())
            except Exception:
                print("gstohlc bad response No JSON body")




            return None

        return spx_day_ohlc

    except requests.exceptions.Timeout:
        info_str = f'3010 Request timed out while fetching SPX OHLC data'
        print(info_str)
        spx_day_ohlc = None
        # raise

    except Exception as e:
        info_str = f'3020 pricehistory error: {e}, could not get SPX o/h/l/c'
        print(info_str)
        spx_day_ohlc = None
        # raise

    return spx_day_ohlc








def get_today_in_epoch():
    global todays_epoch_time 

    # Calculate the time in milliseconds since the UNIX epoch
    now = datetime.now(timezone.utc)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    todays_epoch_time  = int((now - epoch).total_seconds() * 1000.0)
    return todays_epoch_time


def send_email(recipients,subject,body):

    try:

        load_dotenv()  # load environment variables from .env file

        my_gmail_user = os.getenv('GMAIL_USER')
        my_gmail_passcode = os.getenv('GMAIL_APP_PASSCODE')

        print(f'mrisl2 my_gmail_user: {my_gmail_user}')
        print(f'mrisl3 my_gmail_passcode: {my_gmail_passcode}')

        # receiver_email = "mri1700@gmail.com"
        # recipients = ["mri1700@gmail.com", "rudy.isaacson@gmail.com", "scottike@gmail.com"]

        today = datetime.today()
        subject_str = f"{subject} {today.strftime('%Y-%m-%d %H:%M:%S')}"

        # Create the email
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject_str
        msg['From'] = my_gmail_user
        msg['To'] = recipients

    except Exception as e:
        print(f"mris2 email setup exception: {e}")


    try:
        # Send the email using Gmail's SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(my_gmail_user, my_gmail_passcode)
            smtp.send_message(msg)
        print("mris4 Email sent successfully!")
    except Exception as e:
        print(f"mris5 Failed to send email: {e}")



    








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




