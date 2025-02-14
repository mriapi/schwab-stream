import json
import threading
import pandas as pd
import schwabdev
from dotenv import load_dotenv
import os
import time

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
    


# positions = [{'shortQuantity': 0.0, 'averagePrice': 0.1, 'currentDayProfitLoss': -10.0, 'currentDayProfitLossPercentage': -50.0, 'longQuantity': 1.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.A355995000', 'symbol': 'SPXW  250103C05995000', 'description': 'S & P 500 INDEX 01/03/2025 $5995 Call', 'netChange': -0.034, 'type': 'VANILLA', 'putCall': 'CALL', 'underlyingSymbol': '$SPX'}, 'marketValue': 10.0, 'maintenanceRequirement': 0.0, 'averageLongPrice': 0.1, 'taxLotAverageLongPrice': 0.1, 'longOpenProfitLoss': 0.0, 'previousSessionLongQuantity': 0.0, 'currentDayCost': 20.0}, {'shortQuantity': 1.0, 'averagePrice': 2.0, 'currentDayProfitLoss': -2.5, 'currentDayProfitLossPercentage': -1.25, 'longQuantity': 0.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.M355885000', 'symbol': 'SPXW  250103P05885000', 'description': 'S & P 500 INDEX 01/03/2025 $5885 Put', 'netChange': -30.6534, 'type': 'VANILLA', 'putCall': 'PUT', 'underlyingSymbol': '$SPX'}, 'marketValue': -202.5, 'maintenanceRequirement': 500.0, 'averageShortPrice': 2.0, 'taxLotAverageShortPrice': 2.0, 'shortOpenProfitLoss': -2.5, 'previousSessionShortQuantity': 0.0, 'currentDayCost': -200.0}, {'shortQuantity': 1.0, 'averagePrice': 2.5, 'currentDayProfitLoss': -7.5, 'currentDayProfitLossPercentage': -3.0, 'longQuantity': 0.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.A355950000', 'symbol': 'SPXW  250103C05950000', 'description': 'S & P 500 INDEX 01/03/2025 $5950 Call', 'netChange': 1.0929, 'type': 'VANILLA', 'putCall': 'CALL', 'underlyingSymbol': '$SPX'}, 'marketValue': -257.5, 'maintenanceRequirement': 4500.0, 'averageShortPrice': 2.5, 'taxLotAverageShortPrice': 2.5, 'shortOpenProfitLoss': -7.5, 'previousSessionShortQuantity': 0.0, 'currentDayCost': -250.0}, {'shortQuantity': 0.0, 'averagePrice': 0.35, 'currentDayProfitLoss': -2.5, 'currentDayProfitLossPercentage': -7.14, 'longQuantity': 1.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.M355835000', 'symbol': 'SPXW  250103P05835000', 'description': 'S & P 500 INDEX 01/03/2025 $5835 Put', 'netChange': -9.5, 'type': 'VANILLA', 'putCall': 'PUT', 'underlyingSymbol': '$SPX'}, 'marketValue': 32.5, 'maintenanceRequirement': 0.0, 'averageLongPrice': 0.35, 'taxLotAverageLongPrice': 0.35, 'longOpenProfitLoss': -2.5, 'previousSessionLongQuantity': 0.0, 'currentDayCost': 35.0}]

# print(f'{positions}')

positions_success_flag, short_legs, long_legs = get_positions2()
# print(f'get_possitions2():\nsuccess flag:{positions_success_flag} \nshort legs:{short_legs} \nlong legs:{long_legs}\n')


# account_data1 = {'securitiesAccount': {'type': 'MARGIN', 'accountNumber': '56922081', 'roundTrips': 4, 'isDayTrader': True, 'isClosingOnlyRestricted': False, 'pfcbFlag': False, 'positions': [{'shortQuantity': 0.0, 'averagePrice': 0.1, 'currentDayProfitLoss': -10.0, 'currentDayProfitLossPercentage': -50.0, 'longQuantity': 1.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.A355995000', 'symbol': 'SPXW  250103C05995000', 'description': 'S & P 500 INDEX 01/03/2025 $5995 Call', 'netChange': -0.034, 'type': 'VANILLA', 'putCall': 'CALL', 'underlyingSymbol': '$SPX'}, 'marketValue': 10.0, 'maintenanceRequirement': 0.0, 'averageLongPrice': 0.1, 'taxLotAverageLongPrice': 0.1, 'longOpenProfitLoss': 0.0, 'previousSessionLongQuantity': 0.0, 'currentDayCost': 20.0}, {'shortQuantity': 1.0, 'averagePrice': 2.0, 'currentDayProfitLoss': -2.5, 'currentDayProfitLossPercentage': -1.25, 'longQuantity': 0.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.M355885000', 'symbol': 'SPXW  250103P05885000', 'description': 'S & P 500 INDEX 01/03/2025 $5885 Put', 'netChange': -30.6534, 'type': 'VANILLA', 'putCall': 'PUT', 'underlyingSymbol': '$SPX'}, 'marketValue': -202.5, 'maintenanceRequirement': 500.0, 'averageShortPrice': 2.0, 'taxLotAverageShortPrice': 2.0, 'shortOpenProfitLoss': -2.5, 'previousSessionShortQuantity': 0.0, 'currentDayCost': -200.0}, {'shortQuantity': 1.0, 'averagePrice': 2.5, 'currentDayProfitLoss': -7.5, 'currentDayProfitLossPercentage': -3.0, 'longQuantity': 0.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.A355950000', 'symbol': 'SPXW  250103C05950000', 'description': 'S & P 500 INDEX 01/03/2025 $5950 Call', 'netChange': 1.0929, 'type': 'VANILLA', 'putCall': 'CALL', 'underlyingSymbol': '$SPX'}, 'marketValue': -257.5, 'maintenanceRequirement': 4500.0, 'averageShortPrice': 2.5, 'taxLotAverageShortPrice': 2.5, 'shortOpenProfitLoss': -7.5, 'previousSessionShortQuantity': 0.0, 'currentDayCost': -250.0}, {'shortQuantity': 0.0, 'averagePrice': 0.35, 'currentDayProfitLoss': -2.5, 'currentDayProfitLossPercentage': -7.14, 'longQuantity': 1.0, 'settledLongQuantity': 0.0, 'settledShortQuantity': 0.0, 'instrument': {'assetType': 'OPTION', 'cusip': '0SPXW.M355835000', 'symbol': 'SPXW  250103P05835000', 'description': 'S & P 500 INDEX 01/03/2025 $5835 Put', 'netChange': -9.5, 'type': 'VANILLA', 'putCall': 'PUT', 'underlyingSymbol': '$SPX'}, 'marketValue': 32.5, 'maintenanceRequirement': 0.0, 'averageLongPrice': 0.35, 'taxLotAverageLongPrice': 0.35, 'longOpenProfitLoss': -2.5, 'previousSessionLongQuantity': 0.0, 'currentDayCost': 35.0}], 'initialBalances': {'accruedInterest': 0.0, 'availableFundsNonMarginableTrade': 56710.0, 'bondValue': 226840.44, 'buyingPower': 114170.0, 'cashBalance': 56710.11, 'cashAvailableForTrading': 0.0, 'cashReceipts': 0.0, 'dayTradingBuyingPower': 226840.0, 'dayTradingBuyingPowerCall': 0.0, 'dayTradingEquityCall': 0.0, 'equity': 56710.11, 'equityPercentage': 100.0, 'liquidationValue': 56710.11, 'longMarginValue': 0.0, 'longOptionMarketValue': 0.0, 'longStockValue': 0.0, 'maintenanceCall': 0.0, 'maintenanceRequirement': 0.0, 'margin': 56710.11, 'marginEquity': 56710.11, 'moneyMarketFund': 0.0, 'mutualFundValue': 56710.0, 'regTCall': 0.0, 'shortMarginValue': 0.0, 'shortOptionMarketValue': 0.0, 'shortStockValue': 0.0, 'totalCash': 0.0, 'isInCall': False, 'pendingDeposits': 0.0, 'marginBalance': 0.0, 'shortBalance': 0.0, 'accountValue': 56710.11}, 'currentBalances': {'accruedInterest': 0.0, 'cashBalance': 57076.01, 'cashReceipts': 0.0, 'longOptionMarketValue': 42.5, 'liquidationValue': 56658.51, 'longMarketValue': 0.0, 'moneyMarketFund': 0.0, 'savings': 0.0, 'shortMarketValue': 0.0, 'pendingDeposits': 0.0, 'mutualFundValue': 0.0, 'bondValue': 0.0, 'shortOptionMarketValue': -460.0, 'availableFunds': 52076.01, 'availableFundsNonMarginableTrade': 52076.01, 'buyingPower': 104901.8, 'buyingPowerNonMarginableTrade': 51817.29, 'dayTradingBuyingPower': 189408.48, 'equity': 57076.01, 'equityPercentage': 100.0, 'longMarginValue': 0.0, 'maintenanceCall': 0.0, 'maintenanceRequirement': 5000.0, 'marginBalance': 0.0, 'regTCall': 0.0, 'shortBalance': 0.0, 'shortMarginValue': 0.0, 'sma': 52450.9}, 'projectedBalances': {'availableFunds': 51817.29, 'availableFundsNonMarginableTrade': 51817.29, 'buyingPower': 104384.36, 'dayTradingBuyingPower': 188373.6, 'dayTradingBuyingPowerCall': 0.0, 'maintenanceCall': 0.0, 'regTCall': 0.0, 'isInCall': False, 'stockBuyingPower': 104384.36}}, 'aggregatedBalance': {'currentLiquidationValue': 56658.51, 'liquidationValue': 56657.29}}

# longs = []
# shorts = []
# get_positions(account_data1)
# if len(positions_df) > 0:
#     print(f'1 positions_df:\n{positions_df}')
#     longs = long_options()
#     shorts = short_options()
#     print(f'1 longs:\n{longs}')
#     print(f'1 shorts:\n{shorts}')
# else:
#     print(f'1 positions_df is empty')


# account_data2 = {'securitiesAccount': 
#     {'type': 'MARGIN', 'accountNumber': '56922081', 'roundTrips': 4, 'isDayTrader': True, 'isClosingOnlyRestricted': False, 'pfcbFlag': False,
#     'initialBalances': {'accruedInterest': 0.0, 'availableFundsNonMarginableTrade': 56710.0, 'bondValue': 226840.44, 'buyingPower': 114170.0, 'cashBalance': 56710.11, 'cashAvailableForTrading': 0.0, 'cashReceipts': 0.0, 'dayTradingBuyingPower': 226840.0, 'dayTradingBuyingPowerCall': 0.0, 'dayTradingEquityCall': 0.0, 'equity': 56710.11, 'equityPercentage': 100.0, 'liquidationValue': 56710.11, 'longMarginValue': 0.0, 'longOptionMarketValue': 0.0, 'longStockValue': 0.0, 'maintenanceCall': 0.0, 'maintenanceRequirement': 0.0, 'margin': 56710.11, 'marginEquity': 56710.11, 'moneyMarketFund': 0.0, 'mutualFundValue': 56710.0, 'regTCall': 0.0, 'shortMarginValue': 0.0, 'shortOptionMarketValue': 0.0, 'shortStockValue': 0.0, 'totalCash': 0.0, 'isInCall': False, 'pendingDeposits': 0.0, 'marginBalance': 0.0, 'shortBalance': 0.0, 'accountValue': 56710.11}, 'currentBalances': {'accruedInterest': 0.0, 'cashBalance': 57076.01, 'cashReceipts': 0.0, 'longOptionMarketValue': 42.5, 'liquidationValue': 56658.51, 'longMarketValue': 0.0, 'moneyMarketFund': 0.0, 'savings': 0.0, 'shortMarketValue': 0.0, 'pendingDeposits': 0.0, 'mutualFundValue': 0.0, 'bondValue': 0.0, 'shortOptionMarketValue': -460.0, 'availableFunds': 52076.01, 'availableFundsNonMarginableTrade': 52076.01, 'buyingPower': 104901.8, 'buyingPowerNonMarginableTrade': 51817.29, 'dayTradingBuyingPower': 189408.48, 'equity': 57076.01, 'equityPercentage': 100.0, 'longMarginValue': 0.0, 'maintenanceCall': 0.0, 'maintenanceRequirement': 5000.0, 'marginBalance': 0.0, 'regTCall': 0.0, 'shortBalance': 0.0, 'shortMarginValue': 0.0, 'sma': 52450.9}, 'projectedBalances': {'availableFunds': 51817.29, 'availableFundsNonMarginableTrade': 51817.29, 'buyingPower': 104384.36, 'dayTradingBuyingPower': 188373.6, 'dayTradingBuyingPowerCall': 0.0, 'maintenanceCall': 0.0, 'regTCall': 0.0, 'isInCall': False, 'stockBuyingPower': 104384.36}}, 'aggregatedBalance': {'currentLiquidationValue': 56658.51, 'liquidationValue': 56657.29}}


# longs = []
# shorts = []
# get_positions(account_data2)
# if len(positions_df) > 0:
#     print(f'2 positions_df:\n{positions_df}')
#     longs = long_options()
#     shorts = short_options()
#     print(f'2 longs:\n{longs}')
#     print(f'2 shorts:\n{shorts}')
# else:
#     print(f'2 positions_df is empty')






