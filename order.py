import json
# import schwabdev
import meic
# import streamer
import requests
import mri_schwab_lib




def enter_spread_with_triggers(real_flag, schwab_client, hash, opt_type, short_leg, long_leg, qty):
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
            meic.persist_string(info_str)

            resp = schwab_client.order_place(hash, order_form)

            # resp = streamer.place_order(order_form)

            print(f'streamer.place_order resp:{resp}')



            info_str = "\nPlaced order:"
            print(info_str)
            meic.post_tranche_data(info_str)

            info_str = f"Response code: {resp}"
            # print(info_str)
            meic.post_tranche_data(info_str)


            # Print all response headers
            # print("\nall Response header items:")

            # for header, value in resp.headers.items():
            #     print(f"{header}: {value}")

            # print()

            info_str = f" "
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)
            

            # get the order ID - if order is immediately filled then the id might not be returned
            order_id = resp.headers.get('location', '/').split('/')[-1]

            info_str = f"Order id: {order_id}"
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)

            # print(f'\nGet specific {opt_type} spread order details')
            # print(client.order_details(hash, order_id).json())

            try:
                order_details = schwab_client.order_details(hash, order_id).json()

            except Exception as e:
                info_str = f'Error with client.order_details:{e}'
                print(info_str)
                meic.post_tranche_data(info_str)
                meic.persist_string(info_str)
                order_details = None
                return order_form, order_id, order_details

        else:
            info_str = f'Real (Live) trading mode is False.  Order NOT placed'
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)

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

    # TODO: FIX_ME: comment this out
    price += 5

    stop_price = original_price * 2 + (long_ask * 1.2) - 0.10



  

    # Apply conditional stop_price rounding based on SPX order increment rules
    if stop_price >= 3.00:
        stop_price = round(stop_price * 10) / 10  # Round to nearest 0.10
    else:
        stop_price = round(stop_price * 20) / 20  # Round to nearest 0.05

    info_str = f'\nGenerating order for short: {short_sym} and long: {long_sym}'
    print(info_str)
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    info_str = f'short_bid:{short_bid:.2f}, long_ask:{long_ask:.2f}'
    print(info_str) 
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)
    
    info_str = f'original_price:{original_price:.2f}, adjusted price:{price:.2f}, stop_price:{stop_price:.2f}'
    print(info_str)
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    
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


def place_order(order_form):
    success_flag = True

    my_account_number, my_account_hash = mri_schwab_lib.get_account()
    if my_account_hash == None:
        print(f'place order: my_account_hash is None, cannot place order')
        return
    
    (access_token_issue_date, refresh_token_issue_date,
    expires_time, token_type, scope, 
    refresh_token, access_token, 
    id_token) = mri_schwab_lib.get_tokens()

    if access_token == None:
        print(f'place order: access_token is None, cannot place order')
        return

    




    
    url = f"https://api.schwabapi.com/trader/v1/accounts/{my_account_hash}/orders"

    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


    try:
        resp = requests.post(url, headers=headers, json=order_form)

        resp_code = resp.status_code
        print(f'0472 place order resp_code:{resp_code}')  # Example output: 200 (OK) or 400, 403, etc.

        if resp_code != 201:

            success_flag = False

            resp_json = resp.json()  # Converts response body to a Python dictionary
            print(f'0474 place order was not successful, resp_json:{resp_json}')

    except Exception as e:
        print(f"0479E Error in place order: {e}, no order was placed")
        success_flag = False
        return

    return success_flag


def enter_ic_with_triggers(
        real_flag, 
        rx_accessToken,
        rx_acctHash, 
        call_short_leg, 
        call_long_leg, 
        put_short_leg,
        put_long_leg,
        qty):
    
    
    # print(f'enter ic 001 \ncall_short:{call_short_leg}\ncall_long:{call_long_leg}\nput_short:{put_short_leg}\nput_long:{put_long_leg}, qty:{qty}')
    # print(f'enter ic 002 \nrx_accessToken:{rx_accessToken}\nrx_acctHash:{rx_acctHash}')

    try:

        call_short_sym = call_short_leg[0]['symbol']
        call_short_bid = call_short_leg[0]['bid']
        call_long_sym = call_long_leg[0]['symbol']
        call_long_ask = call_long_leg[0]['ask']


        put_short_sym = put_short_leg[0]['symbol']
        put_short_bid = put_short_leg[0]['bid']
        put_long_sym = put_long_leg[0]['symbol']
        put_long_ask = put_long_leg[0]['ask']

        order_form = None
        order_id = None
        order_details = None

        order_form = generate_order_STO_IC_with_triggers(
            call_short_sym, 
            call_short_bid, 
            call_long_sym, 
            call_long_ask, 
            put_short_sym, 
            put_short_bid, 
            put_long_sym, 
            put_long_ask, 
            qty)

        # print(f'order_form type:{type(order_form)}')
        # print(f'order_form data:{json.dumps(order_form, indent=4)}')




        if real_flag == True:
            info_str = f'Real trading mode is True.  Order WILL be placed'
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)


            # print(f'303 rx_acctHas type:{type(rx_acctHash)}, value:<{rx_acctHash}>')
            url = f"https://api.schwabapi.com/trader/v1/accounts/{rx_acctHash}/orders"
            # print(f'304 url type:{type(url)}, value:<{url}>')


            # print(f'395 placing order, rx_account_hash:{rx_acctHash}, url:{url}, rx_accessToken:{rx_accessToken}')



            # Set up headers
            headers = {
                "accept": "*/*",
                "Authorization": f"Bearer {rx_accessToken}",
                "Content-Type": "application/json",

            }

            # print(f'027\n  url:<{url}>\n  headers:<{headers}>\n  order_form:<{order_form}>')




            # order_form = {
            #     "complexOrderStrategyType": "NONE",
            #     "orderType": "LIMIT",
            #     "session": "NORMAL",
            #     "price": "0.05",
            #     "duration": "DAY",
            #     "orderStrategyType": "SINGLE",
            #     "orderLegCollection": [
            #         {
            #             "instruction": "BUY_TO_OPEN",
            #             "quantity": 1,
            #             "instrument": {
            #                 "symbol": "SPXW  250520C05970000",
            #                 "assetType": "OPTION"
            #             }
            #         }
            #     ]
            # }


            # Make the POST request
            # resp = requests.post(url, headers=headers, data=order_form)


            resp = requests.post(url, headers=headers, json=order_form)



            # Print response details
            # print(f"Status Code: {resp.status_code}")
            # print(f"Response Body: {resp.text}")







            # print(f'910 streamer.place_order resp:{resp}')



            info_str = "\nPlaced order:"
            print(info_str)
            meic.post_tranche_data(info_str)

            info_str = f"Response code: {resp}"
            # print(info_str)
            meic.post_tranche_data(info_str)


            # Print all response headers
            # print("\nall Response header items:")

            # for header, value in resp.headers.items():
            #     print(f"{header}: {value}")

            # print()

            info_str = f" "
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)
            

            # get the order ID - if order is immediately filled then the id might not be returned
            order_id = resp.headers.get('location', '/').split('/')[-1]

            info_str = f"Order id: {order_id}"
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)

            # print(f'\nGet specific {opt_type} spread order details')
            # print(client.order_details(hash, order_id).json())

            try:
                # order_details = schwab_client.order_details(hash, order_id).json()
                url = f"https://api.schwabapi.com/trader/v1/accounts/{rx_acctHash}/orders/{order_id}"
                print(f'294-1 url:{url}')

                headers = {
                    "accept": "application/json",
                    "Authorization": f"Bearer {rx_accessToken}"
                }

                response = requests.get(url, headers=headers)
                # print(f'294-3 response:{response}')
                order_details = response.json()
                # print(f'294-8 order_details:{order_details}')

            except Exception as e:
                info_str = f'Error with client.order_details:{e}'
                print(info_str)
                meic.post_tranche_data(info_str)
                meic.persist_string(info_str)
                order_details = None
                return order_form, order_id, order_details

        else:
            info_str = f'Real (Live) trading mode is False.  Order NOT placed'
            print(info_str)
            meic.post_tranche_data(info_str)
            meic.persist_string(info_str)

            return order_form, order_id, order_details





    except Exception as e:
        print(f"Error in enter_ic_with_triggers(): {e}, no order was placed")
        return order_form, order_id, order_details
    
    return order_form, order_id, order_details
    

    


# for the full iron condor
def generate_order_STO_IC_with_triggers(
    call_short_sym,
    call_short_bid,
    call_long_sym,
    call_long_ask,
    put_short_sym,
    put_short_bid,
    put_long_sym,
    put_long_ask,
    qty
):
    # Calculate the price and stopPrice
    call_price = (call_short_bid - call_long_ask)
    call_original_price = call_price

    put_price = (put_short_bid - put_long_ask)
    put_original_price = call_price


    ic_price = call_price + put_price
    
    ic_original_price = ic_price

    ic_price -= 0.10

    # for test
    # ic_price += 30


    #calculate call spread stop price
    call_stop_price = call_original_price * 2 + (call_long_ask * 1.2) - 0.10

    # Apply conditional stop_price rounding based on SPX order increment rules
    if call_stop_price >= 3.00:
        call_stop_price = round(call_stop_price * 10) / 10  # Round to nearest 0.10
    else:
        call_stop_price = round(call_stop_price * 20) / 20  # Round to nearest 0.05


    #calculate put spread stop price
    put_stop_price = put_original_price * 2 + (put_long_ask * 1.2) - 0.10

    # Apply conditional stop_price rounding based on SPX order increment rules
    if put_stop_price >= 3.00:
        put_stop_price = round(put_stop_price * 10) / 10  # Round to nearest 0.10
    else:
        put_stop_price = round(put_stop_price * 20) / 20  # Round to nearest 0.05





    info_str = f'\nGenerating IC order for\n   call {call_short_sym}/{call_long_sym}\n   put {put_short_sym}/{put_long_sym}'
    print(info_str)
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    info_str = f'call short_bid:{call_short_bid:.2f}, long_ask:{call_long_ask:.2f}'
    print(info_str) 
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    info_str = f'put short_bid:{put_short_bid:.2f}, long_ask:{put_long_ask:.2f}'
    print(info_str) 
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    
    info_str = f'IC original_price:{ic_original_price:.2f}, adjusted price:{ic_price:.2f}, call stop_price:{call_stop_price:.2f}, put stop_price:{put_stop_price:.2f}'
    print(info_str)
    meic.post_tranche_data(info_str)
    meic.persist_string(info_str)

    
    # Create the JSON order
    json_order = {
        "orderStrategyType": "TRIGGER",
        "orderType": "NET_CREDIT",
        "price": f"{ic_price:.2f}",
        "duration": "DAY",
        "session": "NORMAL",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": call_short_sym
                }
            },
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": call_long_sym
                }
            },
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": put_short_sym
                }
            },
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": qty,
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": put_long_sym
                }
            }



        ],
        "childOrderStrategies": [

            {
                "orderType": "STOP",
                "session": "NORMAL",
                "stopPrice": f"{call_stop_price:.2f}",
                "duration": "DAY",
                "orderStrategyType": "TRIGGER",
                "orderLegCollection": [
                    {
                        "instruction": "BUY_TO_CLOSE",
                        "quantity": qty,
                        "instrument": {
                            "symbol": call_short_sym,
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
                                    "symbol": call_long_sym,
                                    "assetType": "OPTION"
                                }
                            }
                        ]
                    }
                ]
            },

            {
                "orderType": "STOP",
                "session": "NORMAL",
                "stopPrice": f"{put_stop_price:.2f}",
                "duration": "DAY",
                "orderStrategyType": "TRIGGER",
                "orderLegCollection": [
                    {
                        "instruction": "BUY_TO_CLOSE",
                        "quantity": qty,
                        "instrument": {
                            "symbol": put_short_sym,
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
                                    "symbol": put_long_sym,
                                    "assetType": "OPTION"
                                }
                            }
                        ]
                    }
                ]
            }




        ]
    }


    
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






