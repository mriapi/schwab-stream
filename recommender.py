# recommender.py
#
# Evaluates SPX put or call options chain/grid data and 
# generates a list of all vertical spread candidates that satisfy rules.
#
# From the list of candidates, selects the spread with optimal credit
#

# 


import os
import csv
import pandas as pd
import numpy as np
from io import StringIO
import recommend_config
import json
import math


# fetch the spread leg selection rules from the recommend_config.py file
grid_directory = recommend_config.GRID_FILES_DIRECTORY # location of option grid data files
strategy = recommend_config.PICKER_STRATEGY
strategy_desc = recommend_config.PICKER_STRATEGY_DESC
max_width = recommend_config.MAX_SPREAD_WIDTH      # max spread width
min_width = recommend_config.MIN_SPREAD_WIDTH      # min spread width
max_net_credit = recommend_config.MAX_NET_CREDIT   # max net credit
min_net_credit = recommend_config.MIN_NET_CREDIT   # min net credit
optimal_net_credit = recommend_config.OPTIMAL_NET_CREDIT   # optimal credit
max_long_val = recommend_config.MAX_LONG_VAL       # max ask value for long leg
min_long_val = recommend_config.MIN_LONG_VAL       # min ask value for long leg
min_spx_distance = recommend_config.MIN_SPX_DISTANCE   # minimum difference bewteen strike price of the long leg 
                                                    # and current price of SPX

max_short_target = recommend_config.MAX_SHORT_TARGET
min_short_target = recommend_config.MIN_SHORT_TARGET

EM_MAX = recommend_config.EM_MAX
EM_MIN = recommend_config.EM_MIN

# print(f'grid_directory type:{type(grid_directory)}, value: <{grid_directory}>')
# print(f'max_width type:{type(max_width)}, value: {max_width}')
# print(f'min_width type:{type(min_width)}, value: {min_width}')
# print(f'max_net_credit:{type(max_net_credit)}, value: {max_net_credit}')
# print(f'min_net_credit:{type(min_net_credit)}, value: {min_net_credit}')
# print(f'max_long_val:{type(max_long_val)}, value: {max_long_val}')
# print(f'min_long_val:{type(min_long_val)}, value: {min_long_val}')
# print(f'min_spx_distance:{type(min_spx_distance)}, value: {min_spx_distance}')
# print(f'strategy: {strategy} ({strategy_desc})')




def get_strike_int_from_sym(sym):

    # Find the index of "C0" or "P0"
    index = sym.find("C0")
    if index == -1:
        index = sym.find("P0")

    # Extract the next 4 characters
    if index != -1:
        strike_str = sym[index + 2:index + 6]
    else:
        print(f'Did not find C0 or P0 in sym:{sym}')
        return None

    strike_int = None
    
    try:
        strike_int = int(strike_str)
        # print(f'good integer string, strike_str:{strike_str}, strike_int:{strike_int}')
    except:
        print(f'bad integer string:{strike_str} from sym:{sym}')

    return strike_int








# Function to check if value is None or NaN
def is_valid(value):
    return value is not None and not math.isnan(value)   




def display_list(option_list):
    try:
        for option in option_list:
            print(f"Symbol: {option['symbol']}")
            print(f"  Bid: {option['bid']} (Time: {option['bid_time']})")
            print(f"  Ask: {option['ask']} (Time: {option['ask_time']})")
            print(f"  Last: {option['last']} (Time: {option['last_time']})")
            print(f"  Strike: {option['STRIKE']}")
            print("-" * 40)
    except KeyError as e:
        print(f"KeyError: Missing key {e}. Exiting function.")
        return
    except Exception as e:
        print(f"An error occurred: {e}. Exiting function.")
        return
    
def display_sym_bid_ask(option_list):
    try:
        for option in option_list:
            print(f"Symbol:{option['symbol']}, bid:{option['bid']}, ask:{option['ask']}")

    except KeyError as e:
        print(f"KeyError: Missing key {e}. Exiting function.")
        return
    except Exception as e:
        print(f"An error occurred: {e}. Exiting function.")
        return
    

def display_syms_only(option_list):
    try:
        for option in option_list:
            print(f"Symbol: {option['symbol']}")

    except KeyError as e:
        print(f"KeyError: Missing key {e}. Exiting function.")
        return
    except Exception as e:
        print(f"An error occurred: {e}. Exiting function.")
        print(f'option_list type:{type(option_list)}, data:\n{option_list}')
        return
    

    
# def display_syms_only(option_list):
#     try:
#         for option in option_list:
#             for item in option:  # Since option is a list containing dict(s)
#                 print(f"Symbol: {item['symbol']}")

#     except KeyError as e:
#         print(f"KeyError: Missing key {e}. Exiting function.")
#         return
#     except Exception as e:
#         print(f"An error occurred: {e}. Exiting function.")
#         print(f'option_list type:{type(option_list)}, data:\n{option_list}')
#         return
    

# def display_syms_only(option_list):
#     try:
#         for option in option_list:
#             print(f"Symbol: {option['symbol']}")
            
#     except KeyError as e:
#         print(f"KeyError: Missing key {e}. Exiting function.")
#         return
#     except Exception as e:
#         print(f"An error occurred: {e}. Exiting function.")
#         print(f'option_list type:{type(option_list)}, data:\n{option_list}')
#         return
    
  

def display_lists(opt_short, opt_long):

    # print(f'displaying both lists\n')

    # print(f'in display_lists opt_short\n  opt_short type:{type(opt_short)}, data:{opt_short}')
    # print(f'in display_lists opt_long\n  opt_long type:{type(opt_long)}, data:{opt_long}')
    # print(f'len of call_list:{len(call_list)}, len of put_list:{len(put_list)}')

    # print(f'call_list:\n{call_list}')


    print(f'short option:')

    try:
        item = opt_short[0]
        print(f"  Symbol: {item['symbol']}")
        print(f"  Bid: {item['bid']}")
        print(f"  Ask: {item['ask']}")
        print(f"  Last: {item['last']}")
        print(f"  Strike: {item['STRIKE']}")

    except Exception as e:
        print(f'An error occurred: {e} while displaying opt_short, copt_short data:\n{opt_short}')

    print(f'long option:')

    try:
        item = opt_long[0]
        print(f"Symbol: {item['symbol']}")
        print(f"  Bid: {item['bid']}")
        print(f"  Ask: {item['ask']}")
        print(f"  Last: {item['last']}")
        print(f"  Strike: {item['STRIKE']}")

    except Exception as e:
        print(f'An error occurred: {e} while displaying opt_long, opt_long data:\n{opt_long}')



def calc_short_target(current_EM):
    global max_short_target, min_short_target
    
    # Set the variables EM_MAX and EM_MIN
    # Note: the lower the EM_MAX, the higher the target price will 
    # EM_MAX = 27.00
    # EM_MIN = 7.00
    
    # Determine the value of adjusted_target
    if current_EM <= EM_MIN:
        adjusted_target = min_short_target
    elif current_EM >= EM_MAX:
        adjusted_target = max_short_target
    else:
        # Linearly interpolate between min_short_target and max_short_target
        proportion = (current_EM - EM_MIN) / (EM_MAX - EM_MIN)
        adjusted_target = min_short_target + proportion * (max_short_target - min_short_target)
    
    # Round adjusted_target to the nearest 0.05
    adjusted_target = round(adjusted_target / 0.05) * 0.05
    
    return adjusted_target


last_call_list = []
last_call_short_list = []
last_call_long_list = []
last_put_list = []
last_put_short_list = []
last_put_long_list = []


def get_last_short_long_lists():
    # print(f'last_call_list type{type(last_call_list)}, data:\n{last_call_list}')
    return last_call_list, last_call_short_list, last_call_long_list, last_put_list, last_put_short_list, last_put_long_list




def pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target):
    
    global last_call_list
    global last_call_short_list
    global last_call_long_list
    global last_put_list
    global last_put_short_list
    global last_put_long_list


    # print(f'pl spx price:{spx_price}')
    # print(f'300 option_list:')
    # display_sym_bid_ask(option_list)


    DEBUG_PICK_LEGS = False

    try:


        short_call_positions = [symbol for symbol in short_positions if "C0" in symbol]
        short_put_positions = [symbol for symbol in short_positions if "P0" in symbol]
        long_call_positions = [symbol for symbol in long_positions if "C0" in symbol]
        long_put_positions = [symbol for symbol in long_positions if "P0" in symbol]





        # Filter the out-of-the-money (OTM) options based on the type (CALL or PUT)
        if option_type == "CALL":
            otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] > (spx_price + 2)]
            short_otm_list = [item for item in otm_list if item['symbol'] not in long_call_positions]
            # print(f'320 call short_otm_list:')
            # display_sym_bid_ask(short_otm_list)
            

            last_call_short_list = short_otm_list

            long_otm_list = [item for item in otm_list if item['symbol'] not in short_call_positions]
            # print(f'330 call long_otm_list:')
            # display_sym_bid_ask(long_otm_list)

            last_call_long_list = long_otm_list

        elif option_type == "PUT":
            otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] < (spx_price - 2)]
            short_otm_list = [item for item in otm_list if item['symbol'] not in long_put_positions]
            # print(f'340 put short_otm_list:')
            # display_sym_bid_ask(short_otm_list)

            last_put_short_list = short_otm_list

            long_otm_list = [item for item in otm_list if item['symbol'] not in short_put_positions]
            # print(f'350 put long_otm_list:')
            # display_sym_bid_ask(long_otm_list)

            last_put_long_list = long_otm_list

        else:
            print("Invalid option_type:<{option_type}>. Must be 'CALL' or 'PUT'.")
            return [], []
        



        if not short_otm_list or not long_otm_list:
            print("No OTM options found.")
            return [], []
        

        if DEBUG_PICK_LEGS == True:
            print(f'my_short_target used:{my_short_target}')
        

        


        # # Select the short_leg based on the bid closest to my_short_target
        # short_leg = min(otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - 2.00))
        # Select the short_leg based on the bid closest to my_short_target
        short_leg = min(short_otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - my_short_target))
        # print(f'Short Leg Selected.  short_leg type:{type(short_leg)},\n  data:<{short_leg}>')

        if not short_leg:
            print("No valid short_leg found")
            return [], []
        

        # fifty_max_list_old = [
        #     opt for opt in option_list
        #     if opt.get('STRIKE') and 
        #     (opt['STRIKE'] > short_leg['STRIKE'] if option_type == "CALL" else opt['STRIKE'] < short_leg['STRIKE']) and 
        #     # 20 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50
        #     15 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50

        # ]


        # print(f'fifty_max_list_old len:{len(fifty_max_list_old)}, data:')
        # # display_list(fifty_max_list_old)
        # display_syms_only(fifty_max_list_old)
        # print()



        # Filter for the fifty_max_list: further OTM and 20 <= strike difference <= 50
        fifty_max_list = [
            opt for opt in long_otm_list
            if opt.get('STRIKE') and 
            (opt['STRIKE'] > short_leg['STRIKE'] if option_type == "CALL" else opt['STRIKE'] < short_leg['STRIKE']) and 
            # 20 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50
            15 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50

        ]


        
        # print(f'fifty_max_list len:{len(fifty_max_list)}, data:')
        # # display_list(fifty_max_list)
        # display_syms_only(fifty_max_list)
        # print()


        if not fifty_max_list:
            # print("No valid fifty_max options found.")
            return [], []


        # Select the long_leg based on the lowest 'bid' but at least 0.05
        # print(f'Selecting long leg from fifty_max_list type:{type(fifty_max_list)}, data:\n{fifty_max_list}')
        # long_leg = [opt for opt in fifty_max_list if opt.get('bid', 0) >= 0.05]

        # Filter the list to include items with 'bid' >= 0.05
        valid_bids = [item for item in fifty_max_list if item['bid'] >= 0.05]

        # Select the dictionary with the lowest 'bid' value, 
        #   breaking ties for PUTs by the highest 'STRIKE'
        #   breaking ties for CALLs by the lowest 'STRIKE'
        if valid_bids:
            if option_type == "PUT":
                long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], -x['STRIKE']))
            else:
                long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], +x['STRIKE']))

            long_leg = [long_leg_dict]  # Wrap the selected dictionary in a single-item list
        else:
            long_leg = []  # If no valid bids are found, return an empty list

        # print(f'Long Leg Selected.  long_leg type:{type(long_leg)},\n  data:<{long_leg}>')

        if not long_leg:
            # print("No valid long_leg found with bid >= 0.05")
            return [short_leg], []
        
        # long_leg is a list.  Convert short_leg to a list
        short_leg = [short_leg]

        return short_leg, long_leg



    except KeyError as e:
    
        print(f"S 455 Missing key: {e}. Ensure all options have 'symbol', 'bid', 'ask', and 'STRIKE' keys.")
    
    except Exception as e:
        print(f"S 456 An error occurred: {e}")

    return [], [] # Return empty lists on any exception or rule violation
    
    
def calc_net(short_leg, long_leg):
    # Check if either list is empty
    if not short_leg or not long_leg:
        return 0.00
    
    # Extract the first dictionary from each list
    short = short_leg[0]
    long = long_leg[0]
    
    # Check if 'bid' and 'ask' keys exist in both dictionaries
    if 'bid' not in short or 'ask' not in short or 'bid' not in long or 'ask' not in long:
        return 0.00
    
    # Calculate the net value
    net_value = short['bid'] - long['ask']
    
    return float(net_value)


def ten_max(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value):

    global last_call_list
    global last_call_short_list
    global last_call_long_list
    global last_put_list
    global last_put_short_list
    global last_put_long_list

  
    DEBUG_TEN_MAX = False

    if option_type == "CALL":
        last_call_list = option_list
        last_call_long_list = []
        last_call_short_list = []

    if option_type == "PUT":
        last_put_list = option_list
        last_put_long_list = []
        last_put_short_list = []


    my_short_target = calc_short_target(atm_straddle_value)



        
    # try:

    #     # Create lists for call and put options
    #     short_call_positions = [symbol for symbol in short_positions if "C0" in symbol]
    #     short_put_positions = [symbol for symbol in short_positions if "P0" in symbol]
    #     long_call_positions = [symbol for symbol in long_positions if "C0" in symbol]
    #     long_put_positions = [symbol for symbol in long_positions if "P0" in symbol]

   


    #     # print(f'prev my_short_target:{my_short_target:.2f}')
    #     my_short_target = calc_short_target(atm_straddle_value)
    #     # print(f'new calc_short_target() my_short_target:{my_short_target:.2f} for atm_straddle_value:{atm_straddle_value:.2f}')


    
    #     # Filter the out-of-the-money (OTM) options based on the type (CALL or PUT)
    #     if option_type == "CALL":
    #         otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] > (spx_price + 2)]
    #         short_otm_list = [item for item in otm_list if item['symbol'] not in long_call_positions]

    #         last_call_short_list = short_otm_list

    #         long_otm_list = [item for item in otm_list if item['symbol'] not in short_call_positions]

    #         last_call_long_list = long_otm_list

    #     elif option_type == "PUT":
    #         otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] < (spx_price - 2)]
    #         short_otm_list = [item for item in otm_list if item['symbol'] not in long_put_positions]

    #         last_put_short_list = short_otm_list

    #         long_otm_list = [item for item in otm_list if item['symbol'] not in short_put_positions]

    #         last_put_long_list = long_otm_list

    #     else:
    #         print("Invalid option_type:<{option_type}>. Must be 'CALL' or 'PUT'.")
    #         return [], [], my_short_target
        


    #     # print(f'TM type:{option_type}, spx:{spx_price:2f}')

    #     # print(f'TM original list symbols:\n')
    #     # display_sym_bid_ask(option_list)

    #     # print(f'TM otm_list symbols:\n')
    #     # display_sym_bid_ask(otm_list)

    #     # print(f'TM short_otm_list:\n')
    #     # display_sym_bid_ask(short_otm_list)

    #     # print(f'TM long_otm_list:\n')
    #     # display_syms_only(long_otm_list)
        








    #     # print(f'otm_list:')
    #     # display_syms_only(otm_list)
    #     # print()

    #     # print(f'short_otm_list:')
    #     # display_syms_only(short_otm_list)
    #     # print()

    #     # print(f'long_otm_list:')
    #     # display_syms_only(long_otm_list)
    #     # print()
        
 
    #     # print(f'short_otm_list type:{type(short_otm_list)}, data:\n{short_otm_list}')





    #     # print(f'20 otm_list len:{len(otm_list)}, data:')
    #     # display_list(otm_list)


    #     if not short_otm_list or not long_otm_list:
    #         print("No OTM options found.")
    #         return [], [], my_short_target
        

    #     if DEBUG_TEN_MAX == True:
    #         print(f'my_short_target used:{my_short_target}')
        

        


    #     # # Select the short_leg based on the bid closest to my_short_target
    #     # short_leg = min(otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - 2.00))
    #     # Select the short_leg based on the bid closest to my_short_target
    #     short_leg = min(short_otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - my_short_target))
    #     # print(f'Short Leg Selected.  short_leg type:{type(short_leg)},\n  data:<{short_leg}>')

    #     if not short_leg:
    #         print("No valid short_leg found")
    #         return [], [], my_short_target
        

    #     # fifty_max_list_old = [
    #     #     opt for opt in option_list
    #     #     if opt.get('STRIKE') and 
    #     #     (opt['STRIKE'] > short_leg['STRIKE'] if option_type == "CALL" else opt['STRIKE'] < short_leg['STRIKE']) and 
    #     #     # 20 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50
    #     #     15 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50

    #     # ]


    #     # print(f'fifty_max_list_old len:{len(fifty_max_list_old)}, data:')
    #     # # display_list(fifty_max_list_old)
    #     # display_syms_only(fifty_max_list_old)
    #     # print()



    #     # Filter for the fifty_max_list: further OTM and 20 <= strike difference <= 50
    #     fifty_max_list = [
    #         opt for opt in long_otm_list
    #         if opt.get('STRIKE') and 
    #         (opt['STRIKE'] > short_leg['STRIKE'] if option_type == "CALL" else opt['STRIKE'] < short_leg['STRIKE']) and 
    #         # 20 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50
    #         15 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50

    #     ]


        
    #     # print(f'fifty_max_list len:{len(fifty_max_list)}, data:')
    #     # # display_list(fifty_max_list)
    #     # display_syms_only(fifty_max_list)
    #     # print()


    #     if not fifty_max_list:
    #         print("No valid fifty_max options found.")
    #         return [], [], my_short_target


    #     # Select the long_leg based on the lowest 'bid' but at least 0.05
    #     # print(f'Selecting long leg from fifty_max_list type:{type(fifty_max_list)}, data:\n{fifty_max_list}')
    #     # long_leg = [opt for opt in fifty_max_list if opt.get('bid', 0) >= 0.05]

    #     # Filter the list to include items with 'bid' >= 0.05
    #     valid_bids = [item for item in fifty_max_list if item['bid'] >= 0.05]

    #     # Select the dictionary with the lowest 'bid' value, 
    #     #   breaking ties for PUTs by the highest 'STRIKE'
    #     #   breaking ties for CALLs by the lowest 'STRIKE'
    #     if valid_bids:
    #         if option_type == "PUT":
    #             long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], -x['STRIKE']))
    #         else:
    #             long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], +x['STRIKE']))

    #         long_leg = [long_leg_dict]  # Wrap the selected dictionary in a single-item list
    #     else:
    #         long_leg = []  # If no valid bids are found, return an empty list

    #     # print(f'Long Leg Selected.  long_leg type:{type(long_leg)},\n  data:<{long_leg}>')

    #     if not long_leg:
    #         # print("No valid long_leg found with bid >= 0.05")
    #         return [short_leg], [], my_short_target
        
    #     # long_leg is a list.  Convert short_leg to a list
    #     short_leg = [short_leg]






    #     # short_leg and long_leg are both type list, each with a single dictionary
    #     return short_leg, long_leg, my_short_target


    # except KeyError as e:
    #     print(f"Missing key: {e}. Ensure all options have 'symbol', 'bid', 'ask', and 'STRIKE' keys.")
    # except Exception as e:
    #     print(f"R 356 An error occurred: {e}")
    
    # return [], [], my_short_target  # Return empty lists on any exception or rule violation

    # short_leg1, long_leg1 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target - 0.60)
    # short_leg2, long_leg2 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target - 0.40)
    # short_leg3, long_leg3 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target - 0.20)
    # short_leg4, long_leg4 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target)
    # short_leg5, long_leg5 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target + 0.20)
    # short_leg6, long_leg6 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target + 0.40)
    # short_leg7, long_leg7 = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target + 0.60)



    # net_1 = calc_net(short_leg1, long_leg1) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net1:{net_1}')

    # net_2 = calc_net(short_leg2, long_leg2) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net2:{net_2}')

    # net_3 = calc_net(short_leg3, long_leg3) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net3:{net_3}')

    # net_4 = calc_net(short_leg4, long_leg4) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net4:{net_4}')

    # net_5 = calc_net(short_leg5, long_leg5) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net5:{net_5}')

    # net_6 = calc_net(short_leg6, long_leg6) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net5:{net_6}')

    # net_7 = calc_net(short_leg7, long_leg7) 
    # print(f'my_short_target:{my_short_target}, option type:{option_type}, net5:{net_7}')












    # Initialize lists to store short_leg and long_leg
    short_leg_list = []
    long_leg_list = []
    net_list = []

    # Define the target adjustments
    target_adjustments = [-0.60, -0.40, -0.20, 0.00, 0.20, 0.40, 0.60]

    # Call pick_legs in a loop
    for adjustment in target_adjustments:
        short_leg, long_leg = pick_legs(option_list, short_positions, long_positions, spx_price, option_type, atm_straddle_value, my_short_target + adjustment)
        new_net = calc_net(short_leg, long_leg) 
        short_leg_list.append(short_leg)
        long_leg_list.append(long_leg)
        net_list.append(new_net)
        

    # # Print the short_leg and long_leg lists
    # print(f"short leg list:\n{short_leg_list}")
    # print(f"long leg list:\n{long_leg_list}")
    # print(f'my_short_target:{my_short_target}\nNets:{net_list}')


    # Find the value closest to my_short_target in the net_list
    closest_value = min(net_list, key=lambda x: abs(x - my_short_target))

    # Get all indices with the closest value
    closest_indices = [i for i, value in enumerate(net_list) if abs(value - my_short_target) == abs(closest_value - my_short_target)]

    # Select the first (smallest index) occurrence
    closest_index = closest_indices[0]

    # Select the corresponding short_leg and long_leg
    best_short_leg = short_leg_list[closest_index]
    best_long_leg = long_leg_list[closest_index]

    # # Print the selected legs
    # print(f'index for bests:{closest_index}')
    # print("Best Short Leg:", best_short_leg)
    # print("Best Long Leg:", best_long_leg)










    # print(f'short_leg type:{type(short_leg1)}, data:{short_leg1}')
    # print(f'long_leg type:{type(long_leg1)}, data:{long_leg1}')
    
    
    
    # short_leg = short_leg3
    # long_leg = long_leg3

    short_leg = best_short_leg
    long_leg = best_long_leg

    return short_leg, long_leg, my_short_target





def calculate_atm_straddle_value(chain):

    DEBUG_ATM_VALUE = False

    try:

        # Extract the SPX last price
        spx_last = None
        for item in chain:
            if item['symbol'] == '$SPX':
                spx_last = item['last']
                break

        if spx_last is None:
            print("atm_straddle: SPX last price not found in the chain")
            return None

        # Initialize variables to hold the closest Call and Put options
        closest_call = None
        closest_put = None
        closest_strike_diff = float('inf')

        # Iterate over the chain to find the closest ATM Call option
        for item in chain:
            if 'C0' in item['symbol']:
                strike_price = int(item['symbol'].split('C0')[1][:4])
                strike_diff = abs(spx_last - strike_price)
                if strike_diff < closest_strike_diff:
                    closest_strike_diff = strike_diff
                    closest_call = item

        # Find the corresponding Put option with the same strike price
        for item in chain:
            if 'P0' in item['symbol']:
                strike_price = int(item['symbol'].split('P0')[1][:4])
                if closest_call and strike_price == int(closest_call['symbol'].split('C0')[1][:4]):
                    closest_put = item
                    break

        if closest_call is None or closest_put is None:
            print("atm_straddle: Matching Call or Put option not found in the chain")
            return None

        # Calculate the value of the ATM straddle
        straddle_value = closest_call['bid'] + closest_put['bid']


        if DEBUG_ATM_VALUE == True:

            # Display the results
            print(f"Call Option Symbol: {closest_call['symbol']}")
            print(f"Put Option Symbol: {closest_put['symbol']}")
            print(f"Call Bid Value: {closest_call['bid']}")
            print(f"Put Bid Value: {closest_put['bid']}")
            print(f"ATM Straddle Value: {straddle_value:.2f}")

        return straddle_value

    except Exception as e:
        print(f"atm_straddle: An error occurred: {e}")
        return None







# returns four type:<class 'list'> variables
#[['Short Strike', 'Long Strike', 'Short Bid', 'Long Ask', 'Net Credit', 'SPX Offset', 'Spread Width'], [6030.0, 5980.0, 1.6, 0.1, 1.5, 10.1899999999996, 50.0]]
    

def generate_recommendation(short_position_list, long_position_list, grid):


    my_call_short = []
    my_call_long = []
    my_put_short = []
    my_put_long = []
    spx_last_fl = None
    atm_straddle_fl = None
    call_target = None
    put_target = None

    
    # print(f'grid type:{type(grid)}, data:\n{grid}')

    atm_straddle_value = calculate_atm_straddle_value(grid)
    
    if atm_straddle_value == None:
        print(f'unable to calculate the ATM straddle')
        return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl, atm_straddle_fl, call_target
    
    if atm_straddle_value < 1:
        print(f'ATM straddle value too low: {atm_straddle_value}')
        return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl, atm_straddle_fl, call_target
    
    atm_straddle_fl = float(atm_straddle_value)
    

    # pretty_grid = json.dumps(grid, indent=4)
    # print(f'pretty_grid:\n{pretty_grid}')

    # Initialize empty lists
    put_list = []
    call_list = []
    call_candidates = []
    call_recommendation = []
    put_candidates = []
    put_recommendation = []

    

    # Iterate through each item in the grid list
    for item in grid:

        # print(f'385 processing item :\n{item}')

        if 'symbol' in item:

            # print(f'processing item with a symbol:\n{item}')

            my_sym = item['symbol']

            # print(f"489 symbol: {my_sym}")

            if my_sym == "$SPX":

                my_last = None

                # print(f"my_sym is $SPX")

                if 'last' in item:
                    spx_last_fl = float(item['last'])

            else:
                # print(f"my_sym is NOT $SPX")
                pass


            if 'bid' not in item:
                # print(f"bid not in item")
                continue
            else:
                # print(f"Bid: {item['bid']}")
                pass

            if 'ask' not in item:
                # print(f"ask not in item")
                continue
            else:
                # print(f"Ask: {item['ask']}")
                pass

            if 'last' not in item:
                # print(f"last not in item")
                continue
            else:
                # print(f"Last: {item['last']}")
                pass

            


            # Add item to either put_list or call_list
            

            if 'P0' in item['symbol'] and is_valid(item['bid']) and is_valid(item['ask']):
                # get the strike price from the symbol
                strike_int = get_strike_int_from_sym(item['symbol'])
                item['STRIKE'] = strike_int

                # print(f'adding item <{item}> to put_list')
                put_list.append(item)
            
            # Check for call options and valid bid/ask
            if 'C0' in item['symbol'] and is_valid(item['bid']) and is_valid(item['ask']):
                # get the strike price from the symbol
                strike_int = get_strike_int_from_sym(item['symbol'])
                item['STRIKE'] = strike_int

                # print(f'adding item <{item}> to call_list')
                call_list.append(item)



 

    if spx_last_fl != None:

        # print(f'\n10 call_list type:{type(call_list)}, data:\n{call_list}\n')
        # print(f'\n20 put_list type:{type(put_list)}, data:\n{put_list}\n')


        my_call_short, my_call_long, call_target = ten_max(call_list, short_position_list, long_position_list, spx_last_fl, "CALL", atm_straddle_value)
        # print(f'\n10 ten_max CALL returned\nshort:{my_call_short}\nlong:{my_call_long}\n')


        # print(f'\n20 ten_max CALL returned:')
        # print(f'Call Spread:')
        # display_lists(my_call_short, my_call_long)

        my_put_short, my_put_long, put_target = ten_max(put_list, short_position_list, long_position_list, spx_last_fl, "PUT", atm_straddle_value)
        # print(f'\n30 ten_max PUT returned\nshort:{my_put_short}\nlong:{my_put_long}\n')
        
        # print(f'\n40 ten_max PUT returned:')
        # print(f'Put Spread:')
        # display_lists(my_put_short, my_put_long)


        # call_candidates, call_recommendation = calls_grid_candidates_list(call_list, spx_last_fl)
        # print(f'094C call_candidates:\n{call_candidates}\ncall_recommendation type:{type(call_recommendation)}, data:\n{call_recommendation}')

        # put_candidates, put_recommendation = puts_grid_candidates_list(put_list, spx_last_fl)
        # print(f'094P put_candidates:\n{put_candidates}\nput_recommendation type:{type(put_recommendation)}, data:\n{put_recommendation}')

        # call_recommendation_len = len(call_recommendation)
        # put_recommendation_len = len(put_recommendation)

        # print(f'call len:{call_recommendation_len}, put_len:{put_recommendation_len}')


        # print(f'294C call_recommendation:\n{call_recommendation}')
        # pretty_json = json.dumps(call_candidates, indent=4)
        # print(f'794C call pretty_json:\n{pretty_json}')

    
    else:
        print(f'094 call spx_last_fl was None')
        pass


    return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl, atm_straddle_fl, call_target



    # return call_candidates, call_recommendation, put_candidates, put_recommendation

    









    














