import json
import schwabdev



def enter_spread_with_triggers(real_flag, client, hash, opt_type, short_leg, long_leg, qty):
    # print(f'enter spread opt_type:{opt_type}, short:{short_leg}, long:{long_leg}, qty:{qty}')

    try:

        short_sym = short_leg[0]['symbol']
        short_bid = short_leg[0]['bid']
        long_sym = long_leg[0]['symbol']
        long_ask = long_leg[0]['ask']

        order_form = generate_order_STO_spread_with_triggers(short_sym, short_bid, long_sym, long_ask, qty)

        # print(f'order_form type:{type(order_form)}')
        # print(f'order_form data:{json.dumps(order_form, indent=4)}')


        if real_flag == True:

            resp = client.order_place(hash, order_form)
            print("\nPlaced order:")
            print(f"Response code: {resp}")

            # Print all response headers
            print("\nall Response header items:")
            for header, value in resp.headers.items():
                print(f"{header}: {value}")

            print()

            

            # get the order ID - if order is immediately filled then the id might not be returned
            order_id = resp.headers.get('location', '/').split('/')[-1]
            print(f"Order id: {order_id}")

            print("\nGet specific order details")
            print(client.order_details(hash, order_id).json())
            order_details = client.order_details(hash, order_id).json()

        else:
            print(f'Paper trading mode.  Order not placed')





    except Exception as e:
        print(f"Error in enter_spread_with_triggers(): {e}, no order was placed")
        return
    

    


def generate_order_STO_spread_with_triggers(short_sym, short_bid, long_sym, long_ask, qty):
    # Calculate the price and stopPrice
    price = short_bid - long_ask
    original_price = price


    price -= 0.05


    stop_price = price * 2 + long_ask

    print(f'\ngenerate_order {short_sym}/{long_sym}')
    print(f'short_bid:{short_bid:.2f}, long_ask:{long_ask:.2f}')
    print(f'original_price limit:{original_price}, adjusted price limit:{price}, stop_price:{stop_price:.2f}')

  

    # Apply conditional stop_price rounding based on SPX order increment rules
    if stop_price >= 3.00:
        stop_price = round(stop_price * 10) / 10  # Round to nearest 0.10
    else:
        stop_price = round(stop_price * 20) / 20  # Round to nearest 0.05

    
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






