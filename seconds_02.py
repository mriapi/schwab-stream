



import mri_schwab_lib
import meic_config
import recommend_config

import os
import json
import csv
import matplotlib.pyplot as plt

from datetime import datetime, time, timezone
import pytz
from time import sleep  # Use only for sleep, avoids conflict with datetime.time



import market_open
# import profit_take

import importlib





BASE_DIR = r"C:\MEIC\take_profit"

loop_count = 0
last_buying_power = 0
last_current_available = 0
total_get_buying_power_errors = 0
total_get_balances_errors = 0

# myBuyingPower, currentAvailble = mri_schwab_lib.get_option_buying_power()
myBuyingPower, currentAvailble, initialLiquid, currentLiquid = mri_schwab_lib.get_liquid_balances()
last_buying_power = myBuyingPower
last_current_available = currentAvailble






def _today_dir():
    """Return today's directory path as C:\\MEIC\\take_profit\\YYYY-MM-DD"""
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(BASE_DIR, today)


def save_liquid(current_liquid_value, time):
    """
    Append a time:value entry to today's liquid.json AND liquid.csv.
    - current_liquid_value: float
    - time: string (e.g., '10:32:15' or ISO timestamp)
    """
    day_dir = _today_dir()
    os.makedirs(day_dir, exist_ok=True)

    json_path = os.path.join(day_dir, "liquid.json")
    csv_path = os.path.join(day_dir, "liquid.csv")

    # -------------------------
    # JSON APPEND
    # -------------------------
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            if not isinstance(data, list):
                data = []
        except Exception:
            data = []
    else:
        data = []

    entry = {
        "time": time,
        "liquidation_value": current_liquid_value
    }

    data.append(entry)

    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    # -------------------------
    # CSV MIRROR APPEND
    # -------------------------
    write_header = not os.path.exists(csv_path)

    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["time", "liquidation_value"])
        writer.writerow([time, current_liquid_value])



def get_qty_of_working_stops_s2():

    qty_working_stops = 0
    success_flag = False



    start = datetime.now()

    start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date_dt = datetime.now(timezone.utc)

    # print(f'orders start:{start_of_date_dt}, end:{end_of_date_dt}')

    working_stops = []   # this will become your JSON‑serializable list


    success_flag, orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)

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
        print(f'gqws220 qty of working stop orders:{qty_working_stops}')

    else:
        print(f'gqws222 get orders was not successful') 

    return success_flag, qty_working_stops






def get_recent_orders():

    

    # start_of_date_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # end_of_date_dt = datetime.datetime.now(datetime.timezone.utc)

    start = datetime.now()

    start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date_dt = datetime.now(timezone.utc)

    # print(f'orders start:{start_of_date_dt}, end:{end_of_date_dt}')

    working_stops = []   # this will become your JSON‑serializable list

    get_stop_order_success, get_stop_order_qty = mri_schwab_lib.get_qty_of_working_stops()
    if get_stop_order_success is True:
        print(f'20503 get_stop_order_qty:{get_stop_order_qty}')


    success_flag, orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)
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
                    pass



                    

                    # stop_entry = {
                    #     "orderId": child.get("orderId"),
                    #     "orderType": child.get("orderType"),
                    #     "status": child.get("status"),
                    #     "stopPrice": child.get("stopPrice"),
                    #     "quantity": child.get("quantity"),
                    #     "legs": [],
                    #     "longLegOrder": None
                    # }

                    stop_entry = {
                        # "orderId": child.get("orderId"),
                        # "orderType": child.get("orderType"),
                        # "status": child.get("status"),
                        # "stopPrice": child.get("stopPrice"),
                        # "quantity": child.get("quantity"),
                        "shortLeg": None,      # <-- renamed
                        "longLeg": None
                    }

                    stopOrderId = child.get('orderId')
                    stopOrderQty = child.get('quantity')




                    found_working_stop = True

                else:
                    found_working_stop = False



                # Child order legs
                for leg in child.get("orderLegCollection", []):
                    instr = leg["instrument"]
                    # print(f"      Leg:")
                    # print(f"        symbol:          {instr.get('symbol')}")
                    # print(f"        instrumentId:    {instr.get('instrumentId')}")
                    # print(f"        putCall:         {instr.get('putCall')}")
                    # print(f"        instruction:     {leg.get('instruction')}")
                    # print(f"        positionEffect:  {leg.get('positionEffect')}")
                    # print(f"        quantity:        {leg.get('quantity')}")

                    if found_working_stop is True:
                        pass
                        # print(f"  symbol:       {instr.get('symbol')}")
                        # print(f"  instruction:  {leg.get('instruction')}")
                        # print(f"  quantity2:    {leg.get('quantity')}")

                        # stop_entry["legs"].append({
                        #     "symbol": instr.get("symbol"),
                        #     "instrumentId": instr.get("instrumentId"),
                        #     "putCall": instr.get("putCall"),
                        #     "instruction": leg.get("instruction"),
                        #     "positionEffect": leg.get("positionEffect"),
                        #     "quantity": leg.get("quantity")
                        # })


                        stop_entry["shortLeg"] = {
                            "orderId" : stopOrderId,
                            "symbol": instr.get("symbol"),
                            "instrumentId": instr.get("instrumentId"),
                            # "putCall": instr.get("putCall"),
                            # "instruction": leg.get("instruction"),
                            # "positionEffect": leg.get("positionEffect"),
                            # "quantity": leg.get("quantity")
                            "quantity": stopOrderQty
                        }




                # ---- NESTED CHILD ORDERS (MARKET ORDERS) ----
                nested = child.get("childOrderStrategies", [])
                for nested_idx, n in enumerate(nested, start=1):
                    # print(f"\n        Nested Child #{nested_idx}:")
                    # print(f"          orderType:          {n.get('orderType')}")
                    # print(f"          quantity:           {n.get('quantity')}")
                    # print(f"          orderStrategyType:  {n.get('orderStrategyType')}")
                    # print(f"          orderId:            {n.get('orderId')}")
                    # print(f"          status:             {n.get('status')}")
                    pass

                    


                    for leg in n.get("orderLegCollection", []):
                        instr = leg["instrument"]
                        # print(f"          Leg:")
                        # print(f"            symbol:          {instr.get('symbol')}")
                        # print(f"            instrumentId:    {instr.get('instrumentId')}")
                        # print(f"            putCall:         {instr.get('putCall')}")
                        # print(f"            instruction:     {leg.get('instruction')}")
                        # print(f"            positionEffect:  {leg.get('positionEffect')}")
                        # print(f"            quantity:        {leg.get('quantity')}")

                        if found_working_stop is True:
                            # print(f'found long leg of WORKING STOP')
                            # print(f"  symbol:         {instr.get('symbol')}")
                            # print(f"  instruction:    {leg.get('instruction')}")
                            # print(f"  positionEffect: {leg.get('positionEffect')}")
                            # print(f"  quantity:       {leg.get('quantity')}")
                            pass


                        # nested = child.get("childOrderStrategies", [])
                        # if nested:
                            n = nested[0]   # always one nested MARKET order
                            long_leg = n.get("orderLegCollection", [])[0]
                            instr = long_leg["instrument"]

                            stop_entry["longLeg"] = {
                                "orderId": n.get("orderId"),
                                # "orderType": n.get("orderType"),
                                # "status": n.get("status"),
                                "symbol": instr.get("symbol"),
                                "instrumentId": instr.get("instrumentId"),
                                # "putCall": instr.get("putCall"),
                                # "instruction": long_leg.get("instruction"),
                                # "positionEffect": long_leg.get("positionEffect"),
                                "quantity": long_leg.get("quantity"),
                                "bid": None
                            }

                if found_working_stop is True:

                # Add to final list
                    working_stops.append(stop_entry)

         

    else:
        print(f'3057 PT orders not successful')
        success_flag = False
        orders = None
        return success_flag, orders

    # print(f"4050 pre working_stops data:\n{json.dumps(working_stops, indent=2)}")
 
    for ws in working_stops:
        long_leg = ws.get("longLeg")
        if not long_leg:
            print(f'could not find longLeg')
            continue

        sym = long_leg.get("symbol")
        if not sym:
            print(f'could not find symbol')
            continue

        # Call your Schwab quote function
        bid_price = mri_schwab_lib.get_opt_quote_bid(sym)

        # print(f'9356 bid price:{bid_price}')

        # Only update if the returned value is not None
        if bid_price is not None:
            long_leg["bid"] = bid_price

    print(f"4050 post working_stops data:\n{json.dumps(working_stops, indent=2)}")

    print()


    
    BID_MIN = 0.10

    spread_count = 0
    for ws in working_stops:
        spread_count += 1
        short_leg = ws.get("shortLeg", {})
        long_leg  = ws.get("longLeg", {})

        short_sym = short_leg.get("symbol")
        qty = short_leg.get("quantity")
        long_sym  = long_leg.get("symbol")
        bid       = long_leg.get("bid")

        print(f'spread #{spread_count}, short:{short_sym}, long:{long_sym}, long bid:{bid}, qty:{qty}')

        # Case 1: bid is None / null
        if bid is None:
            print(f"bid is null for long leg {long_sym}, (short leg: {short_sym})")
            print()
            continue

        # Case 2: bid >= BID_MIN → exit spread
        if bid >= BID_MIN:
            print(f"exit spread: short {short_sym}  long {long_sym}, qty:{qty}")
            print()

            continue

        # Case 3: bid < BID_MIN → exit short leg only
        print(f"exit short leg only: {short_sym}, qty:{qty}")
        print()

    print()


    working_short_order_ids = [
        ws["shortLeg"]["orderId"]
        for ws in working_stops
        if "shortLeg" in ws and "orderId" in ws["shortLeg"]
    ]

    print("Short‑leg order IDs:", working_short_order_ids)

    num_cancelled = mri_schwab_lib.delete_working_orders(working_short_order_ids)
    print(f'numcancelled:{num_cancelled}')

    # Get end time
    end = datetime.now()

    # Compute delta
    delta = end - start

    # Convert to seconds.milliseconds
    elapsed_seconds = delta.total_seconds()

    print(f"early exit Elapsed time: {elapsed_seconds:.3f} seconds")
    print()

    # importlib.reload(profit_take)
    # take_profit_reached, current_pl, target_pl = profit_take.detect_profit_target()

    # print(f'take_profit_reached:{take_profit_reached}, current_pl:{current_pl:.2f}, target_pl:{target_pl}')
    

    return success_flag, orders

    pass



def check_for_PT():
    my_market_open, et, to_minute = market_open.is_market_open2(open_offset=5, close_offset=-5)
    if my_market_open is False:
        now_time = datetime.now()
        now_time_str = now_time.strftime('%m/%d/%y %H:%M:%S.%f')[:-3]
        # print(f'\naborting check_for_PT() because market is closed\n')
        return False
    
    





while True:

    sleep(1)
    loop_count += 1
    # Get buying power and current Pacific time
    
    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')
    current_secs_int = current_time.second
    print(f'buying power: {last_buying_power}, current available:{last_current_available}      Pacific time: {current_time_str}')

    

    # Every 60 seconds, check Eastern time status
    # if loop_count % 10 == 2:
    if current_secs_int % 10 == 7:
        print(f'\n')

        try:

            initialBalance, currentBalance = mri_schwab_lib.get_balances()
            pnlFl = float(currentBalance - initialBalance)
            pnlPercent = float((pnlFl / initialBalance) * 100)

            print(f"\nInitial balance today: {initialBalance}, current balance: {currentBalance}")
            print(f"P/L: {pnlFl:.2f}, {pnlPercent:.1f}%\n")

        except Exception as e:
            print(f"error getting balances: {e}")
            initialBalance = currentBalance = None

        if (initialBalance is None) or (currentBalance is None):
            total_get_balances_errors += 1


        

        blackout_dates = meic_config.config_no_trade_dates
        entry_times = meic_config.config_meic_times
        # print(f'entry_times type:{type(entry_times)}, data:{entry_times}')
        number_of_entries = len(entry_times)
        max_contracts = meic_config.MAX_CONTRACTS
        total_contracts = number_of_entries * max_contracts
        
        max_short_target = recommend_config.MAX_SHORT_TARGET




        



        # myBuyingPower, currentAvailble = mri_schwab_lib.get_option_buying_power()
        myBuyingPower, currentAvailble, initialLiquid, currentLiquid = mri_schwab_lib.get_liquid_balances()

        if myBuyingPower is None or currentAvailble is None:
            print(f'problem getting buying power, myBuyingPower:{myBuyingPower}, currentAvailable:{currentAvailble}')
            total_get_buying_power_errors  += 1
            continue

        if initialLiquid is None or currentLiquid is None:
            print(f'problem getting liquid balanced, initialLiquid :{initialLiquid }, currentLiquid:{currentLiquid}')
            total_get_buying_power_errors  += 1
            continue

        liquid_diff = currentLiquid - initialLiquid
        liquid_ratio = (liquid_diff/initialLiquid) * 100

        print(f'initial liquid:{initialLiquid}, current liquid:{currentLiquid}, diff:{liquid_diff:.1f}, %:{liquid_ratio:.2f}')
        
        importlib.reload(meic_config)
        my_take_profit_factor = meic_config.TAKE_PROFIT_FACTOR
        # my_take_profit_factor = my_take_profit_factor.TAKE_PROFIT_FACTOR


        # TAKE_PROFIT_FACTOR = 40  # Used net credit as a factor
        # take_profit_threshold = total_contracts * (max_short_target * 0.75) * TAKE_PROFIT_FACTOR
        # print(f'total contracts:{total_contracts}, target_credit:{max_short_target}, take_profit_threshold:{take_profit_threshold:.1f}')

        # TAKE_PROFIT_FACTOR = 85  # does not use net credit as a factor

        take_profit_threshold = total_contracts * my_take_profit_factor
        print(f'total contracts:{total_contracts}, take_profit_threshold:{take_profit_threshold:.1f}, my_take_profit_factor:{my_take_profit_factor}')

        print(f'\n\n ~~~~ current P/L: {liquid_diff:.2f},  take-profit threshold:{take_profit_threshold:.2f} ~~~~~\n')
        gqws_success, gqws_cnt = mri_schwab_lib.get_qty_of_working_stops()

        if gqws_success is True:
            print(f's2gqws_cnt:{gqws_cnt}')
        else:
            print(f's2gqws_success:{gqws_success}')


        if liquid_diff < take_profit_threshold:
            continue 


        # Note get_recent_orders() closes working orders.

        # success_flag, orders = get_recent_orders()
        # if success_flag == None or success_flag == False:
        #     print(f'49506 get orders was not successful')
        #     continue

        # # print(f'\nretiurned from get recent orders\n')

        # print(f'seconds_02 not calling do early exit()')
        # # profit_take.do_early_exit()
        # # print(f'\nreturned from do early exit\n')

        # if success_flag is True:
        #     pass

        last_buying_power = myBuyingPower
        last_current_available = currentAvailble


        meic_times = meic_config.config_meic_times  # List of "HH:MM" strings in Eastern Time

        # Convert strings to datetime.time objects
        eastern_times = [time.fromisoformat(t) for t in meic_times]

        # Get current time in Pacific Time
        pacific = pytz.timezone("US/Pacific")
        eastern = pytz.timezone("US/Eastern")
        now_pacific = datetime.now(pacific)

        # Convert to Eastern Time
        now_eastern = now_pacific.astimezone(eastern)
        current_eastern_time = now_eastern.time()

        # Count passed and remaining times
        passed = sum(1 for t in eastern_times if t < current_eastern_time)
        remaining = len(eastern_times) - passed

        if remaining > 0 and currentAvailble is not None:
            availble_per_remaining = currentAvailble / remaining
        else:
            availble_per_remaining = 0

        if remaining > 0 and myBuyingPower is not None:
            bp_per_remaining = myBuyingPower / remaining
        
        else:
            bp_per_remaining = 0
  

        # Output results
        print(f"Total times: {len(eastern_times)}")
        print(f"Passed times: {passed}")
        print(f"Remaining times: {remaining}")
        print(f'cash available:{currentAvailble}, buying power:{myBuyingPower}')
        print(f'cash availble per entry for remining entry times:{availble_per_remaining:.2f}')
        print(f'per entry buying power for remaining entries:{bp_per_remaining:.2f}')
        print(f'total errors getting buying power {total_get_buying_power_errors}')
        print(f'total errors getting balances {total_get_balances_errors}')
        print(f'\n')

    