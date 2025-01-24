import json
import schwabdev
import meic



def enter_spread_with_triggers(real_flag, client, hash, opt_type, short_leg, long_leg, qty):
    # print(f'enter spread opt_type:{opt_type}, short:{short_leg}, long:{long_leg}, qty:{qty}')

    try:

        short_sym = short_leg[0]['symbol']
        short_bid = short_leg[0]['bid']
        long_sym = long_leg[0]['symbol']
        long_ask = long_leg[0]['ask']

        order_form = None
        order_id = None
        order_details = None

        order_form = generate_order_STO_spread_with_triggers(short_sym, short_bid, long_sym, long_ask, qty)

        # print(f'order_form type:{type(order_form)}')
        # print(f'order_form data:{json.dumps(order_form, indent=4)}')




        if real_flag == True:
            info_str = f'Real trading mode is True.  Order WILL be placed'
            print(info_str)
            meic.post_tranche_data(info_str)

            resp = client.order_place(hash, order_form)
            info_str = "\nPlaced order:"
            print(info_str)
            meic.post_tranche_data(info_str)

            info_str = f"Response code: {resp}"
            print(info_str)
            meic.post_tranche_data(info_str)


            # Print all response headers
            print("\nall Response header items:")

            for header, value in resp.headers.items():
                print(f"{header}: {value}")

            print()

            info_str = f" "
            print(info_str)
            meic.post_tranche_data(info_str)
            

            # get the order ID - if order is immediately filled then the id might not be returned
            order_id = resp.headers.get('location', '/').split('/')[-1]

            info_str = f"Order id: {order_id}"
            print(info_str)
            meic.post_tranche_data(info_str)

            print(f'\nGet specific {opt_type} spread order details')
            print(client.order_details(hash, order_id).json())
            order_details = client.order_details(hash, order_id).json()

        else:
            info_str = f'Read trading mode is False.  Order NOT placed'
            print(info_str)
            meic.post_tranche_data(info_str)

            return order_form, order_id, order_details





    except Exception as e:
        print(f"Error in enter_spread_with_triggers(): {e}, no order was placed")
        return
    
    return order_form, order_id, order_details
    

    


def generate_order_STO_spread_with_triggers(short_sym, short_bid, long_sym, long_ask, qty):
    # Calculate the price and stopPrice
    price = short_bid - long_ask
    original_price = price

    price -= 0.10

    stop_price = price * 2 + (long_ask * 1.2)



  

    # Apply conditional stop_price rounding based on SPX order increment rules
    if stop_price >= 3.00:
        stop_price = round(stop_price * 10) / 10  # Round to nearest 0.10
    else:
        stop_price = round(stop_price * 20) / 20  # Round to nearest 0.05

    info_str = f'\ngenerate_order {short_sym}/{long_sym}'
    print(info_str)
    meic.post_tranche_data(info_str)
    info_str = f'short_bid:{short_bid:.2f}, long_ask:{long_ask:.2f}'
    print(info_str)
    meic.post_tranche_data(info_str)
    info_str = f'original_price limit:{original_price:.2f}, adjusted price limit:{price:.2f}, stop_price:{stop_price:.2f}'
    print(info_str)
    meic.post_tranche_data(info_str)

    
    # Create the JSON order
    json_order = {
        "orderStrategyType": "TRIGGER",
        "orderType": "NET_CREDIT",
        "price": f"{price:.2f}",
        "duration": "DAY",
        "session": "NORMAL",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": short_sym
                }
            },
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": long_sym
                }
            }
        ],
        "childOrderStrategies": [
            {
                "orderType": "STOP",
                "session": "NORMAL",
                "stopPrice": f"{stop_price:.2f}",
                "duration": "DAY",
                "orderStrategyType": "TRIGGER",
                "orderLegCollection": [
                    {
                        "instruction": "BUY_TO_CLOSE",
                        "quantity": qty,
                        "instrument": {
                            "symbol": short_sym,
                            "assetType": "OPTION"
                        }
                    }
                ],
                "childOrderStrategies": [
                    {
                        "orderType": "MARKET",
                        "session": "NORMAL",
                        "duration": "DAY",
                        "orderStrategyType": "SINGLE",
                        "orderLegCollection": [
                            {
                                "instruction": "SELL_TO_CLOSE",
                                "quantity": qty,
                                "instrument": {
                                    "symbol": long_sym,
                                    "assetType": "OPTION"
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }



    if "C0" in short_sym:
        opt_type_str = "CALL"
    elif  "P0" in short_sym:
        opt_type_str = "PUT"
    else:
        opt_type_str = "UNKNOWN"

    

    stopOnStr = "SHORT"
    # stopOnStr = "SPREAD"

    auto_sell_long_string = "TRUE"
    # auto_sell_long_string = "FALSE"


    

    # json_summary = {
    #     "orderType": "VERTICAL_SPREAD",
    #     "optType" : opt_type_str,
    #     "shortSym" : short_sym,
    #     "shortBid" : f"{short_bid:.2f}",
    #     "longSym" : long_sym,
    #     "shortBid" : f"{long_ask:.2f}",
    #     "spreadLimit" : f"{price:.2f}",
    #     "stopOn" : stopOnStr,
    #     "stopPrice" : f"{stop_price:.2f}",
    #     "auotSellLong" : auto_sell_long_string
    # }


    # Convert to JSON string and print
    # json_order_str = json.dumps(json_order, indent=4)
    # print(json_order_str)
    
    return json_order




# # Example usage
# short_sym = "SPXW  241125P05960000"
# short_bid = 1.90
# long_sym = "SPXW  241125P05895000"
# long_ask = 0.05
# qty = 1

# my_order_data = place_order_STO_spread_with_triggers(short_sym, short_bid, long_sym, long_ask, qty)

# pretty_order = json.dumps(my_order_data, indent=4)

# print(f'my_order_data type:{type(my_order_data)}, data:\n{pretty_order}')






