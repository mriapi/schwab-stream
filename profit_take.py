


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
from datetime import date


import market_open
import order
import importlib





# TAKE_PROFIT_FACTOR = 10000 
# # TAKE_PROFIT_FACTOR = 300  # $3600 for 6 entries/2 contracts
# # TAKE_PROFIT_FACTOR = 200  # $2400 for 6/2

# # TAKE_PROFIT_FACTOR = 100  # $1200 for 6/2

# # TAKE_PROFIT_FACTOR = 85  # $1020 for 6/2

# # TAKE_PROFIT_FACTOR = 75  # $900 for 6/2
# # TAKE_PROFIT_FACTOR = 65  # $780 for 6/2
# # TAKE_PROFIT_FACTOR = 50  # $600 for 6/2
# # TAKE_PROFIT_FACTOR = 45  # $540 for 6/2
# # TAKE_PROFIT_FACTOR = 25  # $300 for 6/2
# # TAKE_PROFIT_FACTOR = 15  # $180 for 6/2
# # TAKE_PROFIT_FACTOR = 10  # $120 for 6/2
# # TAKE_PROFIT_FACTOR = 5  # $60 for 6/2
# # TAKE_PROFIT_FACTOR = 3  # $36 for 6/2
# # TAKE_PROFIT_FACTOR = 2  # $24 for 6/2
# # TAKE_PROFIT_FACTOR = 1  # $12 for 6/2

# # TAKE_PROFIT_FACTOR = -300  #



my_take_profit_factor = meic_config.TAKE_PROFIT_FACTOR



def test_pt_sleep():
    print(f'0001 testing PT sleep(1)')
    sleep(1)
    print(f'0002 testing PT time.sleep(1)')
    time.sleep(1)


    print(f'0220 end testing PT sleep')




def detect_profit_target():

    reached_target = False
    current_pl = 0
    target_pl = 999999

    # print(f"2045 in detect profit target")

    try:
    

        entry_times = meic_config.config_meic_times
        # print(f'entry_times type:{type(entry_times)}, data:{entry_times}')
        number_of_entries = len(entry_times)
        max_contracts = meic_config.MAX_CONTRACTS
        total_contracts = number_of_entries * max_contracts

        importlib.reload(meic_config)
        my_take_profit_factor = meic_config.TAKE_PROFIT_FACTOR
        # take_profit_threshold = total_contracts * TAKE_PROFIT_FACTOR
        take_profit_threshold = total_contracts * my_take_profit_factor
        target_pl = take_profit_threshold
        # print(f'total contracts:{total_contracts}, take_profit_threshold:{take_profit_threshold:.1f}')

        # myBuyingPower, currentAvailble = mri_schwab_lib.get_option_buying_power()
        myBuyingPower, currentAvailble, initialLiquid, currentLiquid = mri_schwab_lib.get_liquid_balances()

        if myBuyingPower is None or currentAvailble is None:
            print(f'PT3950 problem getting buying power, myBuyingPower:{myBuyingPower}, currentAvailable:{currentAvailble}')
            total_get_buying_power_errors  += 1
            return reached_target, current_pl, target_pl

        if initialLiquid is None or currentLiquid is None:
            print(f'PT3952 problem getting liquid balanced, initialLiquid :{initialLiquid }, currentLiquid:{currentLiquid}')
            total_get_buying_power_errors  += 1
            return reached_target, current_pl, target_pl

        liquid_diff = currentLiquid - initialLiquid
        current_pl = liquid_diff
        liquid_ratio = (liquid_diff/initialLiquid) * 100

        # print(f'initial liquid:{initialLiquid}, current liquid:{currentLiquid}, diff:{liquid_diff:.1f}, %:{liquid_ratio:.2f}')

        if liquid_diff >= take_profit_threshold:
            reached_target = True

        else:
            reached_target = False

    except Exception as e:
        print(f"PT3954 error in detect profit target: {e}")


    return reached_target, current_pl, target_pl
    # return True, current_pl, target_pl


def get_working_stops_qty():


    print(f'GWSQ9020 entered')


    success_flag = True
    working_stops = []   # this will become your JSON‑serializable list

    try:


        

        start = datetime.now()

        start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_date_dt = datetime.now(timezone.utc)

        print(f'GWSQ9305 orders start:{start_of_date_dt}, end:{end_of_date_dt}')

        


        success_flag, orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)

        if success_flag is not True:
            print(f'GWSQ9040 success_flag:{success_flag}')


        if success_flag is True:


            for idx, order_item in enumerate(orders, start=1):

                # dumps

                        
                print(f"\nGWSQ === Iron Condor #{idx} ===")

                # print(f'GWSQ40180 order_item:')
                # print(json.dumps(order_item, indent=4))
                # print()


                print(f"Order ID: {order_item.get('orderId')}")
                print(f"Status:   {order_item.get('status')}")
                # print(f"Price:    {order_item.get('price')}")
                # print(f"Quantity: {order_item.get('quantity')}")
                # print("Legs:")


                # ---- MAIN ORDER LEGS ----
                # for leg in order_item.get("orderLegCollection", []):
                #     instr = leg["instrument"]
                #     print(f"  Leg {leg['legId']}:")
                #     print(f"    Instruction:   {leg['instruction']}")
                #     print(f"    Put/Call:      {instr['putCall']}")
                #     print(f"    Symbol:        {instr['symbol']}")
                #     print(f"    Instrument ID: {instr['instrumentId']}")
                #     print(f"    Quantity:      {leg['quantity']}")

                # ---- CHILD ORDER STRATEGIES ----
                child_orders = order_item.get("childOrderStrategies", [])
                if child_orders:
                    print("\n  GWSQ Child Order Strategies:")
                    pass
                
                for child_idx, child in enumerate(child_orders, start=1):
                    found_working_stop = False

                    # print(f"\n    ETP Child #{child_idx}:")
                    # print(f"      orderType:          {child.get('orderType')}")
                    # print(f"      quantity:           {child.get('quantity')}")
                    # print(f"      orderStrategyType:  {child.get('orderStrategyType')}")
                    # print(f"      orderId:            {child.get('orderId')}")
                    # print(f"      status:             {child.get('status')}")
                    # print(f"      stopPrice:          {child.get('stopPrice')}")

                    
                    # if child.get("orderType") == "STOP" and child.get("status") == "WORKING" and child.get("orderStrategyType") == "TRIGGER":

                    if (
                        child.get("orderType") == "STOP"
                        and child.get("status") == "WORKING"
                        and child.get("orderStrategyType") == "TRIGGER"
                    ):


                        print("GWSQ9045  Found WORKING STOP order")
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
                        print("GWSQ9095  did not find WORKING STOP order")
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
                                pass
                                # print(f'found long leg of WORKING STOP')
                                # print(f"  symbol:         {instr.get('symbol')}")
                                # print(f"  instruction:    {leg.get('instruction')}")
                                # print(f"  positionEffect: {leg.get('positionEffect')}")
                                # print(f"  quantity:       {leg.get('quantity')}")
                                


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
                        print(f'\nGWSQ updated working_stops:{working_stops}\n')

            

        else:
            print(f'GWSQ3057 PT orders not successful')
            success_flag = False
            return success_flag, working_stops

        # print(f"ETP4050 pre working_stops data:\n{json.dumps(working_stops, indent=2)}")
    
        for ws in working_stops:
            long_leg = ws.get("longLeg")
            if not long_leg:
                print(f'GWSQ could not find longLeg')
                continue

            sym = long_leg.get("symbol")
            if not sym:
                print(f'GWSQ could not find long_leg symbol')
                continue

            # Call the Schwab quote function
            bid_price = mri_schwab_lib.get_opt_quote_bid(sym)

            # print(f'ETP 9356 bid price:{bid_price}')

            # Only update if the returned value is not None
            if bid_price is not None:
                long_leg["bid"] = bid_price

        # print(f"ETP 4050 post working_stops data:\n{json.dumps(working_stops, indent=2)}")

        print()

    except Exception as e:
        success_flag = False
        print(f"GWSQEX exception: {e}.")


    print(f'GWSQ success_flag:{success_flag}, working_stops type:{type(working_stops)}, data:\n{working_stops}')


    return success_flag, working_stops





    #     BID_MIN = 0.05
    #     CANCEL_RETRY_CNT = 5
    #     EXIT_RETRY_CNT = 20

    #     # --- Robust validation helper ---
    #     def gwsq_validate_leg(leg, leg_name, spread_count, required_fields):
    #         if not isinstance(leg, dict):
    #             print(f"GWSQ ERROR: {leg_name} is missing or not a dict in working stop #{spread_count}: {leg}")
    #             return False

    #         missing = [f for f in required_fields if leg.get(f) is None]
    #         if missing:
    #             print(f"GWSQ ERROR: Missing {leg_name} fields {missing} in working stop #{spread_count}: {leg}")
    #             return False

    #         return True
        

    #     spread_count = 0
    #     spreads_qty = len(working_stops)
    #     print(f'GWSQ33467 working_stops qty:{spreads_qty}')
    #     for ws in working_stops:
    #         spread_count += 1
    #         print(f'GWSQ30459 spread #{spread_count}')
    #         print(f'GWSQ40985 ws:')
    #         print(json.dumps(ws, indent=4))

    #         pass


        

    #     spread_count = 0
    #     for ws in working_stops:
    #         spread_count += 1
    #         short_leg = ws.get("shortLeg", {})
    #         long_leg  = ws.get("longLeg", {})

    #         short_sym = short_leg.get("symbol")
    #         short_order_id = short_leg.get("orderId")
    #         short_order_qty = short_leg.get("quantity")
    #         long_sym  = long_leg.get("symbol")
    #         bid       = long_leg.get("bid")

    #         print(f'GWSQ9082 spread cnt:{spread_count}, WORKING STOP id:{short_order_id}, {short_sym}/{long_sym}, long bid:{bid}, qty:{short_order_qty}')


    #         # --- Validate legs before using them ---
    #         if not gwsq_validate_leg(short_leg, "shortLeg", spread_count, ["symbol", "orderId", "quantity"]):
    #             print(f'GWSQ7082 EARLY EXIT SKIPPED, short leg did not validate for  {short_sym}/{long_sym} qty:{short_order_qty}')
    #             continue

    #         if not gwsq_validate_leg(long_leg, "longLeg", spread_count, ["symbol", "bid"]):
    #             print(f'GWSQ7083 EARLY EXIT SKIPPED, long leg did not validate for  {short_sym}/{long_sym} qty:{short_order_qty}')
    #             continue

    #         print(f'GWSQ working stop short and long legs were validated')


    #         # Case 1: bid is None / null
    #         if bid is None:
    #             print(f"GWSQ 7085 EARLY EXIT SKIPPED bid is null for long leg {long_sym}, (short leg: {short_sym})")
    #             print()
    #             continue

    #         # Cancel this working order
    #         cancel_succeeded_flag = False
    #         for i in range(CANCEL_RETRY_CNT):
    #             print(f'ETP9405 cancel loop {i}, calling delete working order {short_order_id}, order ID type:{type(short_order_id)}')
    #             orders_cancelled_cnt = mri_schwab_lib.delete_working_order(short_order_id)
    #             print(f'PT4920 orders_cancelled_cnt type:{type(orders_cancelled_cnt)}, value:{orders_cancelled_cnt}')
    #             if orders_cancelled_cnt is not None:
    #                 if orders_cancelled_cnt > 0:
    #                     print(f'PT4930 cancelled order qty:{orders_cancelled_cnt}')
    #                     cancel_succeeded_flag = True
    #                     break

    #             sleep(1)

    #         if cancel_succeeded_flag is False:
    #             print(f'PT7830 FAILED TO CANCEL THIS WORKING STOP:')
    #             print(json.dumps(ws, indent=4))
    #             print(f'PT7831 continuing')
    #             continue

    #         # Case 2: bid >= BID_MIN → exit spread
    #         if bid >= BID_MIN:
    #             print(f'ETP exit full spread (bid:{bid}), id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

    #             # exit the spread
    #             exit_succeeded_flag = False
    #             for i in range(EXIT_RETRY_CNT):
    #                 status_code, order_form, order_id, order_details = order.exit_spread(short_sym, long_sym, short_order_qty)
    #                 print(f'exit spread status_code type:{type(status_code)}, value:{status_code}')
    #                 if status_code == 201 or status_code == 200:
    #                     print(f'spread exit order id:{order_id}')
    #                     exit_succeeded_flag = True
    #                     break

    #                 sleep(1)

    #             if exit_succeeded_flag is False:
    #                 print(f'ETP9615 exit_spread failed -- id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

    #             print()

    #         # Case 3: bid < BID_MIN → exit short leg only
    #         else:
    #             print(f"ETP exit short leg only (bid:{bid}) -- id:{short_order_id}, {short_sym}, qty:{short_order_qty}")

    #             # exit the short leg
    #             exit_succeeded_flag = False
    #             for i in range(EXIT_RETRY_CNT):
    #                 status_code, order_form, order_id, order_details = order.exit_short(short_sym, short_order_qty)
    #                 print(f'exit short status_code type:{type(status_code)}, value:{status_code}')
    #                 if status_code == 201 or status_code == 200:
    #                     print(f'short leg exit order id:{order_id}')
    #                     exit_succeeded_flag = True
    #                     break

    #                 sleep(1)

    #             if exit_succeeded_flag is False:
    #                 print(f'ETP9625 exit_short failed -- id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

    #             print()

    #         print(f'processed working stop {spread_count}, {short_sym}/{long_sym}, qty:{short_order_qty}')

    #     print(f'\nprocessed all working stops\n\n')

    #     # Get end time
    #     end = datetime.now()

    #     # Compute delta
    #     delta = end - start

    #     # Convert to seconds.milliseconds
    #     elapsed_seconds = delta.total_seconds()

    #     print(f"ETP early exit Elapsed time: {elapsed_seconds:.3f} seconds")
    #     print()
    

    # except Exception as e:
    #     print(f"PTDEX exception: {e}.  Returning without checking")

    # pass



def do_early_exit():


    print(f'DEE9020 entered')

    try:
        

        start = datetime.now()

        start_of_date_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_date_dt = datetime.now(timezone.utc)

        print(f'ETP9305 orders start:{start_of_date_dt}, end:{end_of_date_dt}')

        working_stops = []   # this will become your JSON‑serializable list


        success_flag, orders = mri_schwab_lib.get_orders(start_of_date_dt, end_of_date_dt)

        if success_flag is not True:
            print(f'DEE9040 success_flag:{success_flag}')


        if success_flag is True:

            # print(f'orders type:{type(orders)}')
            # print(f'ETP40573 TP orders, type:{type(orders)}, data:')
            # print(json.dumps(orders, indent=2))

            for idx, order_item in enumerate(orders, start=1):

                # dumps

                        
                print(f"\nETP === Iron Condor #{idx} ===")

                # print(f'40180 order_item:')
                # print(json.dumps(order_item, indent=4))
                # print()


                print(f"Order ID: {order_item.get('orderId')}")
                print(f"Status:   {order_item.get('status')}")
                # print(f"Price:    {order_item.get('price')}")
                # print(f"Quantity: {order_item.get('quantity')}")
                # print("Legs:")


                # ---- MAIN ORDER LEGS ----
                # for leg in order_item.get("orderLegCollection", []):
                #     instr = leg["instrument"]
                #     print(f"  Leg {leg['legId']}:")
                #     print(f"    Instruction:   {leg['instruction']}")
                #     print(f"    Put/Call:      {instr['putCall']}")
                #     print(f"    Symbol:        {instr['symbol']}")
                #     print(f"    Instrument ID: {instr['instrumentId']}")
                #     print(f"    Quantity:      {leg['quantity']}")

                # ---- CHILD ORDER STRATEGIES ----
                child_orders = order_item.get("childOrderStrategies", [])
                if child_orders:
                    print("\n  ETP Child Order Strategies:")
                    pass
                
                for child_idx, child in enumerate(child_orders, start=1):
                    found_working_stop = False

                    print(f"\n    ETP Child #{child_idx}:")
                    print(f"      orderType:          {child.get('orderType')}")
                    print(f"      quantity:           {child.get('quantity')}")
                    print(f"      orderStrategyType:  {child.get('orderStrategyType')}")
                    print(f"      orderId:            {child.get('orderId')}")
                    print(f"      status:             {child.get('status')}")
                    print(f"      stopPrice:          {child.get('stopPrice')}")

                    
                    # if child.get("orderType") == "STOP" and child.get("status") == "WORKING" and child.get("orderStrategyType") == "TRIGGER":

                    if (
                        child.get("orderType") == "STOP"
                        and child.get("status") == "WORKING"
                        and child.get("orderStrategyType") == "TRIGGER"
                    ):


                        print("ETP9045  Found WORKING STOP order")
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
                        print("ETP9095  did not find WORKING STOP order")
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
                                pass
                                # print(f'found long leg of WORKING STOP')
                                # print(f"  symbol:         {instr.get('symbol')}")
                                # print(f"  instruction:    {leg.get('instruction')}")
                                # print(f"  positionEffect: {leg.get('positionEffect')}")
                                # print(f"  quantity:       {leg.get('quantity')}")
                                


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
                        print(f'\nupdated working_stops:{working_stops}\n')

            

        else:
            print(f'ETP3057 PT orders not successful')
            return

        # print(f"ETP4050 pre working_stops data:\n{json.dumps(working_stops, indent=2)}")
    
        for ws in working_stops:
            long_leg = ws.get("longLeg")
            if not long_leg:
                print(f'ETP could not find longLeg')
                continue

            sym = long_leg.get("symbol")
            if not sym:
                print(f'ETP could not find symbol')
                continue

            # Call the Schwab quote function
            bid_price = mri_schwab_lib.get_opt_quote_bid(sym)

            # print(f'ETP 9356 bid price:{bid_price}')

            # Only update if the returned value is not None
            if bid_price is not None:
                long_leg["bid"] = bid_price

        # print(f"ETP 4050 post working_stops data:\n{json.dumps(working_stops, indent=2)}")

        print()





        BID_MIN = 0.05
        CANCEL_RETRY_CNT = 8
        EXIT_RETRY_CNT = 20

        # --- Robust validation helper ---
        def validate_leg(leg, leg_name, spread_count, required_fields):
            if not isinstance(leg, dict):
                print(f"ETP ERROR: {leg_name} is missing or not a dict in working stop #{spread_count}: {leg}")
                return False

            missing = [f for f in required_fields if leg.get(f) is None]
            if missing:
                print(f"ETP ERROR: Missing {leg_name} fields {missing} in working stop #{spread_count}: {leg}")
                return False

            return True
        

        spread_count = 0
        spreads_qty = len(working_stops)
        print(f'33467 working_stops qty:{spreads_qty}')
        for ws in working_stops:
            spread_count += 1
            print(f'30459 spread #{spread_count}')
            print(f'40985 ws:')
            print(json.dumps(ws, indent=4))

            pass


        

        spread_count = 0
        for ws in working_stops:
            spread_count += 1
            short_leg = ws.get("shortLeg", {})
            long_leg  = ws.get("longLeg", {})

            short_sym = short_leg.get("symbol")
            short_order_id = short_leg.get("orderId")
            short_order_qty = short_leg.get("quantity")
            long_sym  = long_leg.get("symbol")
            bid       = long_leg.get("bid")

            print(f'ETP9082 spread cnt:{spread_count}, WORKING STOP id:{short_order_id}, {short_sym}/{long_sym}, long bid:{bid}, qty:{short_order_qty}')


            # --- Validate legs before using them ---
            if not validate_leg(short_leg, "shortLeg", spread_count, ["symbol", "orderId", "quantity"]):
                print(f'ETP7082 EARLY EXIT SKIPPED, short leg did not validate for  {short_sym}/{long_sym} qty:{short_order_qty}')
                continue

            if not validate_leg(long_leg, "longLeg", spread_count, ["symbol", "bid"]):
                print(f'ETP7083 EARLY EXIT SKIPPED, long leg did not validate for  {short_sym}/{long_sym} qty:{short_order_qty}')
                continue

            print(f'working stop short and long legs were validated')


            # Case 1: bid is None / null
            if bid is None:
                print(f"ETP7085 EARLY EXIT SKIPPED bid is null for long leg {long_sym}, (short leg: {short_sym})")
                print()
                continue

            # Cancel this working order
            for i in range(CANCEL_RETRY_CNT):
                print(f'ETP9405 cancel loop {i}, calling delete working order {short_order_id}, order ID type:{type(short_order_id)}')
                cancel_success_flag, orders_cancelled_cnt = mri_schwab_lib.delete_working_stop_order(short_order_id)
                print(f'PT4920 stop order deleted, orders_cancelled_cnt type:{type(orders_cancelled_cnt)}, value:{orders_cancelled_cnt}')
                if cancel_success_flag is True:
                    print(f'PT4930 cancelled order qty:{orders_cancelled_cnt}')
                    break

                else:
                    print(f'PT9920 stop order failed, trying secondary check')
                    sleep(1)
                    god_success, response = mri_schwab_lib.get_order_details(short_order_id)
                    sleep(0.1)
                    if god_success is True:
                        parent_status, child_status = mri_schwab_lib.extract_stop_order_leg_statuses(response)
                        if parent_status == "CANCELED":
                            print(f'PT40985 secondary check confirms that order {short_order_id} is CANCELED')
                            cancel_success_flag = True
                            break

                        else:
                            print(f'PT40986 secondary check cannot confirm that order {short_order_id} is CANCELED.  Status:{parent_status}')


                    else:
                        print(f'PT983333 could not get order details for secondary check')

                        
                    pass

                sleep(1)

            if cancel_success_flag is False:
                print(f'PT7830 FAILED TO CANCEL short leg order Id:{short_order_id}, THE WORKING STOP:')
                print(json.dumps(ws, indent=4))
                print(f'PT7832 will attempt to exit the spread or short leg anyway')
                # continue

            print(f'PT6047 closing this spread')


            # Case 2: bid >= BID_MIN → exit spread
            if bid >= BID_MIN:
                print(f'ETP exit full spread (bid:{bid}), id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

                # exit the spread
                exit_succeeded_flag = False
                for i in range(EXIT_RETRY_CNT):
                    status_code, order_form, order_id, order_details = order.exit_spread(short_sym, long_sym, short_order_qty)
                    print(f'ETP exit spread status_code type:{type(status_code)}, value:{status_code}')
                    if status_code == 201 or status_code == 200:
                        print(f'spread exit order id:{order_id}')
                        exit_succeeded_flag = True
                        break

                    sleep(1)

                if exit_succeeded_flag is False:
                    print(f'ETP9615 exit_spread failed -- id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

                print()

            # Case 3: bid < BID_MIN → exit short leg only
            else:
                print(f"ETP exit short leg only (bid:{bid}) -- id:{short_order_id}, {short_sym}, qty:{short_order_qty}")

                # exit the short leg
                exit_succeeded_flag = False
                for i in range(EXIT_RETRY_CNT):
                    status_code, order_form, order_id, order_details = order.exit_short(short_sym, short_order_qty)
                    print(f'ETP exit short status_code type:{type(status_code)}, value:{status_code}')
                    if status_code == 201 or status_code == 200:
                        print(f'short leg exit order id:{order_id}')
                        exit_succeeded_flag = True
                        break

                    sleep(1)

                if exit_succeeded_flag is False:
                    print(f'ETP9625 exit_short failed -- id:{short_order_id}, {short_sym}/{long_sym}, qty:{short_order_qty}')

                print()

            print(f'processed working stop {spread_count}, {short_sym}/{long_sym}, qty:{short_order_qty}')

        print(f'\nprocessed all working stops\n\n')

        # Get end time
        end = datetime.now()

        # Compute delta
        delta = end - start

        # Convert to seconds.milliseconds
        elapsed_seconds = delta.total_seconds()

        print(f"ETP early exit Elapsed time: {elapsed_seconds:.3f} seconds")
        print()
    

    except Exception as e:
        print(f"PTDEX exception: {e}.  Returning without checking")


    

def persist_profit_taken(flag_value: bool):
    """Persist today's profit_taken flag to C:\\MEIC\\take_profit\\<YYYY-MM-DD>\\profit_taken.json"""

    # Build today's directory path
    today_str = date.today().isoformat()  # e.g., "2026-05-27"
    base_dir = fr"C:\MEIC\take_profit\{today_str}"

    # Ensure directory exists
    os.makedirs(base_dir, exist_ok=True)

    # File path
    file_path = os.path.join(base_dir, "profit_taken.json")

    # JSON object to persist
    data = {
        "profit_already_taken": bool(flag_value)
    }

    # Write JSON
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Persisted profit_taken.json → {file_path}")



def read_profit_taken():
    """Read today's profit_taken.json and return True/False.  
       If file does not exist, return False.
    """

    today_str = date.today().isoformat()
    file_path = fr"C:\MEIC\take_profit\{today_str}\profit_taken.json"

    if not os.path.exists(file_path):
        # print(f'attempting to read profit taken, file {file_path} does not exist')
        return False

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return bool(data.get("profit_already_taken", False))
    except Exception:
        # If file is corrupted or unreadable, treat as False
        return False





