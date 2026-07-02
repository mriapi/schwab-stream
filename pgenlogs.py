
"""

genLogsTOS.py



Python port of genLogsTOS.js — parses TOS (ThinkOrSwim) order/transaction/SPX

JSON files for a given date and reports MEIC Iron Condor results, either as

human-readable text or as CSV.



Usage:

    python genLogsTOS.py <DATE> [TEXT|CSV] [ALL|ICS|SUMMARY|SPREADS]



Environment:

    SPX_HOME  base directory containing the per-date data folders (required)

    SPX_WHO   display name for the report (default: 'ikebot')

"""



import os
import re
import sys
import json
import copy
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path


try:
    # Python 3.9+ standard library
    from zoneinfo import ZoneInfo

except ImportError:  # pragma: no cover
    print(f'Error getting from zoneinfo import ZoneInfo')
    ZoneInfo = None





# ----------------------------------------------------------------------------

# Globals (mirroring the original module-level state)

# ----------------------------------------------------------------------------

SPX_HOME = SPX_WHO = None


def load_env_variables():

    global SPX_HOME, SPX_WHO

    SPX_HOME = SPX_WHO = None

    try:

        load_dotenv()  # load environment variables from .env file

        SPX_HOME = os.getenv('MY_SPX_HOME')
        SPX_WHO = os.getenv('MY_WHO')

    
    except Exception as e:
        print(f"Error loading pgenlogs enviornmental data, e:{e}")



load_env_variables()
     
# SPX_HOME = os.environ.get('SPX_HOME')
print(f'SPX_MOME:{SPX_HOME}')

# SPX_WHO = os.environ.get('SPX_WHO') or 'ikebot'
print(f'SPX_WHO:{SPX_WHO}')


DATE = None
OUTPUT_TYPE = None
SELECTION = None
SPX_CLOSE = None

transactions = []
left_over_transactions = []
ics = []
early_orders = []
itms = []




# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def us_currency(value):
    """Format a number as US currency, e.g. -$1,234.56 (mirrors Intl USD)."""
    try:
        n = float(value)
    except (TypeError, ValueError):
        n = 0.0
    sign = '-' if n < 0 else ''
    return '{}${:,.2f}'.format(sign, abs(n))


def to_number(value, default=0.0):
    """Coerce to float like JS Number(), returning default on failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def round2(value):
    """Round to 2 decimals as a float (mirrors Number(x.toFixed(2)))."""
    return round(to_number(value) + 0.0, 2)


# ----------------------------------------------------------------------------
# Main flow
# ----------------------------------------------------------------------------

def monitor():
    get_opts()
    if DATE is None:
        return

    get_orders()
    get_transactions()
    assign_earlies()
    # dump_data()
    match_transactions()
    finalize_orders()
    process_itms()

    if OUTPUT_TYPE == 'CSV':
        export_csv()
    else:
        print_results()


def get_orders():
    # get all orders
    # orders_file = SPX_HOME + '/' + DATE + '/transactions/orders-' + DATE + '.json'
    orders_file = Path(SPX_HOME) / DATE / "transactions" / f"orders-{DATE}.json"

    print(f'expected orders file:{orders_file}')



    try:
        with open(orders_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for _o in data:
            process_order(_o)
    except Exception as err:
        print(f'exception in get_orders: {err}')
        print("in get_orders, No Orders")


def get_transactions():
    # get all transactions
    tc = 0
    # trans_file = SPX_HOME + '/' + DATE + '/transactions/transactions-' + DATE + '.json'
    trans_file = Path(SPX_HOME) / DATE / "transactions" / f"transactions-{DATE}.json"

    print(f'expected transactions file:{trans_file}')

    try:
        with open(trans_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for _t in data:
            tc += 1
            process_transaction(_t)
    except Exception as err:
        print(err)
        print("No Transactions")





def process_itms():
    global SPX_CLOSE

    # get SPX closing
    # spx_file = SPX_HOME + '/' + DATE + '/transactions/spx-' + DATE + '.json'
    spx_file = Path(SPX_HOME) / DATE / "transactions" / f"spx-{DATE}.json"

    print(f'expected spx file:{spx_file}')

    try:
        with open(spx_file, 'r', encoding='utf-8') as f:
            file_data = f.read()
        data = None
        if file_data:
            data = json.loads(file_data)
        else:
            data = None
        if (not data or data.get('empty') or not data.get('candles')
                or not data['candles'] or data['candles'][0].get('close') is None):
            SPX_CLOSE = None
        else:
            SPX_CLOSE = data['candles'][0]['close']
    except Exception as err:
        print(err)
        print("No SPX CLOSE data")

    if SPX_CLOSE:
        for ic in ics:
            put_spread = ic['putSpread']
            call_spread = ic['callSpread']

            if SPX_CLOSE < put_spread['spread']['short'] and put_spread['filledShortBTC'] is False:
                itm = {
                    'putCall': 'PUT ',
                    'longShort': 'SHORT',
                    'symbol': put_spread['shortSymbol'],
                    'amount': (SPX_CLOSE - put_spread['spread']['short']) * 100,
                    'spx_close': SPX_CLOSE,
                }
                itms.append(itm)
            if SPX_CLOSE > call_spread['spread']['short'] and call_spread['filledShortBTC'] is False:
                itm = {
                    'putCall': 'CALL',
                    'longShort': 'SHORT',
                    'symbol': call_spread['shortSymbol'],
                    'amount': (call_spread['spread']['short'] - SPX_CLOSE) * 100,
                    'spx_close': SPX_CLOSE,
                }
                itms.append(itm)
            if SPX_CLOSE < put_spread['spread']['long'] and put_spread['filledLongSTC'] is False:
                itm = {
                    'putCall': 'PUT ',
                    'longShort': 'LONG',
                    'symbol': put_spread['longSymbol'],
                    'amount': (put_spread['spread']['long'] - SPX_CLOSE) * 100,
                    'spx_close': SPX_CLOSE,
                }
                itms.append(itm)
            if SPX_CLOSE > call_spread['spread']['long'] and call_spread['filledLongSTC'] is False:
                itm = {
                    'putCall': 'CALL',
                    'longShort': 'LONG',
                    'symbol': call_spread['longSymbol'],
                    'amount': (SPX_CLOSE - call_spread['spread']['long']) * 100,
                    'spx_close': SPX_CLOSE,
                }
                itms.append(itm)


def get_opts():
    global DATE, OUTPUT_TYPE, SELECTION

    args = sys.argv[1:]

    if len(args) >= 1 and args[0]:
        DATE = args[0]
    else:
        print('No Date', file=sys.stderr)
        return

    OUTPUT_TYPE = 'TEXT'
    if len(args) >= 2 and args[1]:
        if args[1] == 'CSV':
            OUTPUT_TYPE = 'CSV'

    SELECTION = 'ALL'
    if len(args) >= 3 and args[2]:
        if args[2] == 'ALL':
            SELECTION = 'ALL'
        if args[2] == 'ICS':
            SELECTION = 'ICS'
        if args[2] == 'SUMMARY':
            SELECTION = 'SUMMARY'
        if args[2] == 'SPREADS':
            SELECTION = 'SPREADS'


def dump_data():
    print('\nTransactions:')
    for _t in transactions:
        print('{},{},{},{},{},{},{}'.format(
            _t['orderId'], _t['time'], _t['putCall'], _t['strike'],
            _t['qty'], '{:.2f}'.format(to_number(_t['netAmount'])), _t['position']))

    print('\nICs:')
    for _o in ics:
        print(json.dumps(_o, indent=2))

    print('\nEarlyOrders:')
    for _eo in early_orders:
        print(json.dumps(_eo, indent=2))


def assign_earlies():
    for eo in early_orders:

        found = False
        for ic in ics:

            temp_eo = eo
            temp_ic = ic
            if not found and temp_eo['putCall'] == 'PUT' and temp_eo['instruction'] == 'BUY_TO_CLOSE':
                if (not has_order_id(temp_ic['putSpread']['shortStopOrderId'])
                        and is_after(temp_eo['time'], temp_ic['time'])):
                    if temp_eo['symbol'] == temp_ic['putSpread']['shortSymbol']:
                        found = True
                        temp_ic['putSpread']['shortStopOrderId'] = temp_eo['orderId']
                        temp_ic['putSpread']['isEarly'] = True
            if not found and temp_eo['putCall'] == 'CALL' and temp_eo['instruction'] == 'BUY_TO_CLOSE':
                if (not has_order_id(temp_ic['callSpread']['shortStopOrderId'])
                        and is_after(temp_eo['time'], temp_ic['time'])):
                    if temp_eo['symbol'] == temp_ic['callSpread']['shortSymbol']:
                        found = True
                        temp_ic['callSpread']['shortStopOrderId'] = temp_eo['orderId']
                        temp_ic['callSpread']['isEarly'] = True
            if not found and temp_eo['putCall'] == 'PUT' and temp_eo['instruction'] == 'SELL_TO_CLOSE':
                if (not has_order_id(temp_ic['putSpread']['longStopOrderId'])
                        and is_after(temp_eo['time'], temp_ic['time'])):
                    if temp_eo['symbol'] == temp_ic['putSpread']['longSymbol']:
                        found = True
                        temp_ic['putSpread']['longStopOrderId'] = temp_eo['orderId']
            if not found and temp_eo['putCall'] == 'CALL' and temp_eo['instruction'] == 'SELL_TO_CLOSE':
                if (not has_order_id(temp_ic['callSpread']['longStopOrderId'])
                        and is_after(temp_eo['time'], temp_ic['time'])):
                    if temp_eo['symbol'] == temp_ic['callSpread']['longSymbol']:
                        found = True
                        temp_ic['callSpread']['longStopOrderId'] = temp_eo['orderId']


def has_order_id(order_id):
    if order_id is None or order_id == -1:
        return False
    return True


def finalize_orders():
    for ic in ics:

        put_spread = ic['putSpread']
        call_spread = ic['callSpread']

        put_spread['spreadNetCredit'] = round2(put_spread['shortSTO'] - put_spread['longBTO'])
        if not put_spread['shortBTC']:
            put_spread['shortBTC'] = 0
            put_spread['shortBTCFees'] = 0
        if not put_spread['longSTC']:
            put_spread['longSTC'] = 0
            put_spread['longSTCFees'] = 0
        pl = (to_number(put_spread['shortSTO']) * 100
              - to_number(put_spread['shortSTOFees'])
              - to_number(put_spread['longBTO']) * 100
              - to_number(put_spread['longBTOFees'])
              - to_number(put_spread['shortBTC']) * 100
              - to_number(put_spread['shortBTCFees'])
              + to_number(put_spread['longSTC']) * 100
              - to_number(put_spread['longSTCFees']))
        pl = round2(pl)
        put_spread['pl'] = pl
        if not put_spread['shortStopped']:
            put_spread['stoppedTime'] = None
        if not put_spread['isEarly']:
            put_spread['earlyTime'] = None

        call_spread['spreadNetCredit'] = round2(call_spread['shortSTO'] - call_spread['longBTO'])
        if not call_spread['shortBTC']:
            call_spread['shortBTC'] = 0
            call_spread['shortBTCFees'] = 0
        if not call_spread['longSTC']:
            call_spread['longSTC'] = 0
            call_spread['longSTCFees'] = 0
        pl = (to_number(call_spread['shortSTO']) * 100
              - to_number(call_spread['shortSTOFees'])
              - to_number(call_spread['longBTO']) * 100
              - to_number(call_spread['longBTOFees'])
              - to_number(call_spread['shortBTC']) * 100
              - to_number(call_spread['shortBTCFees'])
              + to_number(call_spread['longSTC']) * 100
              - to_number(call_spread['longSTCFees']))
        pl = round2(pl)
        call_spread['pl'] = pl
        if not call_spread['shortStopped']:
            call_spread['stoppedTime'] = None
        if not call_spread['isEarly']:
            call_spread['earlyTime'] = None

        ic['pl'] = round2(put_spread['pl'] + call_spread['pl'])
        ic['netCredit'] = round2(put_spread['spreadNetCredit'] + call_spread['spreadNetCredit'])
        ic['stopRisk'] = ('{:.1f}'.format(to_number(ic['em']) / to_number(ic['netCredit']))
                          if ic['em'] else None)

        if put_spread['shortStopped'] and call_spread['shortStopped']:
            ic['status'] = 'LOSER'
        elif ((put_spread['shortStopped'] and not call_spread['shortStopped'])
              or (not put_spread['shortStopped'] and call_spread['shortStopped'])):
            ic['status'] = 'BE'
        elif put_spread['isEarly'] or call_spread['isEarly']:
            ic['status'] = 'EARLY'
        else:
            ic['status'] = 'WINNER'


def get_recommendation_info_at_time(time):
    recommendation_file_name = get_recommendation_for_order(time)
    recommendation = None
    recommendation_info = {
        'putSpreadLimit': None,
        'callSpreadLimit': None,
        'putShortStop': None,
        'callShortStop': None,
        'underlying': {
            'spxLast': None,
            'em': None,
        },
    }
    try:
        with open(recommendation_file_name, 'r', encoding='utf-8') as f:
            recommendation = json.load(f)
    except Exception:
        return recommendation_info
    if not recommendation:
        return recommendation_info

    recommendation_info = {
        'putSpreadLimit': round2(recommendation['ic']['putLimit']),
        'callSpreadLimit': round2(recommendation['ic']['callLimit']),
        'putShortStop': round2(recommendation['ic']['putShortStop']),
        'callShortStop': round2(recommendation['ic']['callShortStop']),
        'underlying': {
            'spxLast': round(to_number(recommendation['underlying']['last'])),
            'em': round(to_number(recommendation['underlying']['em']) + 0.0, 1),
        },
    }
    return recommendation_info


def has_subsequent_order(order_status):
    # orderStatus: FILLED, EXPIRED, REJECTED, CANCELED, AWAITING_PARENT_ORDER, REPLACED
    if order_status == 'CANCELED' or order_status == 'REPLACED':
        return True
    return False


def process_order(order):
    o = order

    # adjust the time
    adjusted_date = fix_time(o['enteredTime'])
    time = convert_time_time(adjusted_date)

    recommendation_info = get_recommendation_info_at_time(time)

    # We found an IC, process it and add it to the ICs, account for more than one contract
    if (o.get('orderType') == 'NET_CREDIT'
            and o.get('complexOrderStrategyType') == 'IRON_CONDOR'
            and o.get('status') == 'FILLED'
            and o.get('orderStrategyType') == 'TRIGGER'):

        child_strategies = o.get('childOrderStrategies')
        if (not child_strategies or len(child_strategies) < 1 or not child_strategies[0]
                or len(child_strategies) < 2 or not child_strategies[1]):
            print("no spreads in the IC order", file=sys.stderr)
            return

        for q in range(int(o['quantity'])):

            put_spread = {
                'spread': {
                    'putCall': 'PUT',
                    'short': 0,
                    'long': 0,
                },
                'shortStopOrderId': -1,
                'longStopOrderId': -1,
                'pl': 0,
                'shortStopped': False,
                'stopPrice': recommendation_info['putShortStop'],
                'shortBTC': 0,
                'longSTC': 0,
                'shortBTCFees': 0,
                'longSTCFees': 0,
                'filledShortBTC': False,
                'filledLongSTC': False,
                'stoppedTime': 'STOPPED TIME',
                'earlyTime': 'EARLY TIME',
                'status': 'EXPIRED',
                'isEarly': False,
                'limit': recommendation_info['putSpreadLimit'],
                'shortSTO': 0,
                'longBTO': 0,
                'shortSTOFees': 0,
                'longBTOFees': 0,
                'filledShortSTO': False,
                'filledLongBTO': False,
                'spreadNetCredit': 0,
                'shortSymbol': None,
                'longSymbol': None,
            }

            call_spread = {
                'spread': {
                    'putCall': 'CALL',
                    'short': 0,
                    'long': 0,
                },
                'shortStopOrderId': -1,
                'longStopOrderId': -1,
                'pl': 0,
                'shortStopped': False,
                'stopPrice': recommendation_info['callShortStop'],
                'shortBTC': 0,
                'longSTC': 0,
                'shortBTCFees': 0,
                'longSTCFees': 0,
                'filledShortBTC': False,
                'filledLongSTC': False,
                'stoppedTime': 'STOPPED TIME',
                'earlyTime': 'EARLY TIME',
                'status': 'EXPIRED',
                'isEarly': False,
                'limit': recommendation_info['callSpreadLimit'],
                'shortSTO': 0,
                'longBTO': 0,
                'shortSTOFees': 0,
                'longBTOFees': 0,
                'filledShortSTO': False,
                'filledLongBTO': False,
                'spreadNetCredit': 0,
                'shortSymbol': None,
                'longSymbol': None,
            }

            spx_last = recommendation_info['underlying']['spxLast']
            em = recommendation_info['underlying']['em']
            ic = {
                'orderId': o['orderId'],
                'putSpread': put_spread,
                'callSpread': call_spread,
                'time': time,
                'pl': 0,
                'spxLast': '{:.0f}'.format(to_number(spx_last)) if spx_last else None,
                'em': '{:.1f}'.format(to_number(em)) if em else None,
                'stopRisk': None,
                'netCredit': 0,
                'limit': o['price'],
                'status': "WINNER",
            }

            for leg in o['orderLegCollection']:

                put_call = leg['instrument']['putCall']
                instruction = leg['instruction']

                if put_call == 'PUT' and instruction == 'SELL_TO_OPEN':
                    option = symbol_to_option(leg['instrument']['symbol'])
                    ic['putSpread']['spread']['short'] = option['strike']
                    ic['putSpread']['shortSymbol'] = leg['instrument']['symbol']
                elif put_call == 'PUT' and instruction == 'BUY_TO_OPEN':
                    option = symbol_to_option(leg['instrument']['symbol'])
                    ic['putSpread']['spread']['long'] = option['strike']
                    ic['putSpread']['longSymbol'] = leg['instrument']['symbol']
                elif put_call == 'CALL' and instruction == 'SELL_TO_OPEN':
                    option = symbol_to_option(leg['instrument']['symbol'])
                    ic['callSpread']['spread']['short'] = option['strike']
                    ic['callSpread']['shortSymbol'] = leg['instrument']['symbol']
                elif put_call == 'CALL' and instruction == 'BUY_TO_OPEN':
                    option = symbol_to_option(leg['instrument']['symbol'])
                    ic['callSpread']['spread']['long'] = option['strike']
                    ic['callSpread']['longSymbol'] = leg['instrument']['symbol']

            for child in o['childOrderStrategies']:

                if child['status'] != "REJECTED":

                    child_short_stop = child['orderLegCollection'][0]
                    child_order_status = child['status']
                    child_child = child['childOrderStrategies'][0]

                    if (child_short_stop['instrument']['symbol'] == ic['putSpread']['shortSymbol']
                            and not has_subsequent_order(child_order_status)):
                        ic['putSpread']['shortStopOrderId'] = child['orderId']
                        ic['putSpread']['longStopOrderId'] = child_child['orderId']
                        ic['putSpread']['stopPrice'] = child['stopPrice']
                    if (child_short_stop['instrument']['symbol'] == ic['callSpread']['shortSymbol']
                            and not has_subsequent_order(child_order_status)):
                        ic['callSpread']['shortStopOrderId'] = child['orderId']
                        ic['callSpread']['longStopOrderId'] = child_child['orderId']
                        ic['callSpread']['stopPrice'] = child['stopPrice']

            ics.append(ic)

        return

    if ((o.get('orderType') in ('MARKET', 'LIMIT', 'MARKET_ON_CLOSE'))
            and (o.get('complexOrderStrategyType') in ('CUSTOM', 'NONE'))
            and o.get('status') == 'FILLED'):

        for leg in o['orderLegCollection']:
            for _l in range(int(leg['quantity'])):
                early_order = {
                    'symbol': leg['instrument']['symbol'],
                    'orderId': o['orderId'],
                    'instruction': leg['instruction'],
                    'time': time,
                    'putCall': leg['instrument']['putCall'],
                }
                early_orders.append(early_order)

    if (o.get('orderType') == 'MARKET'
            and o.get('complexOrderStrategyType') == 'VERTICAL'
            and o.get('status') == 'FILLED'):

        for leg in o['orderLegCollection']:
            for _l in range(int(leg['quantity'])):
                ois = 'S' if leg['instruction'] == 'BUY_TO_CLOSE' else 'L'
                early_order = {
                    'symbol': leg['instrument']['symbol'],
                    'instruction': leg['instruction'],
                    'orderId': str(o['orderId']) + ois,
                    'time': time,
                    'putCall': leg['instrument']['putCall'],
                }
                early_orders.append(early_order)

    return


def process_transaction(transaction):
    t = transaction
    if t.get('type') == 'TRADE':

        symbol = None
        net_amount = None
        buy_or_sell = None
        put_call = None
        strike = None
        position = None
        order_id = None
        amount = None
        time = None
        filled_price = None
        fees = None

        for item in t['transferItems']:

            if item['instrument']['assetType'] == 'OPTION':

                adjusted_time = fix_time(t['time'])
                formatted_time = convert_time_time(adjusted_time)

                symbol = item['instrument']['symbol']
                amount = abs(item['amount'])
                net_amount = abs(to_number(t['netAmount']) / amount) / 100
                buy_or_sell = 'SELL' if to_number(t['netAmount']) > 0 else 'BUY'
                put_call = item['instrument']['putCall']
                strike = symbol_to_strike(symbol)
                position = item['positionEffect']
                order_id = t['orderId']
                time = formatted_time
                filled_price = to_number(item['price'])
                fees = abs(abs(to_number(item['price']) * 100 * amount) - abs(to_number(t['netAmount'])))
                fees = fees / amount

        j = 1
        while j <= amount:
            entry = {
                'symbol': symbol,
                'netAmount': net_amount,
                'buyOrSell': buy_or_sell,
                'putCall': put_call,
                'strike': strike,
                'position': position,
                'orderId': order_id,
                'time': time,
                'filledPrice': filled_price,
                'fees': fees,
            }
            transactions.append(entry)
            j += 1


def match_transactions():
    # make a working deep copy
    w = copy.deepcopy(transactions)

    found = False
    while len(w) > 0:

        # take the first transaction
        _t = w[0]

        for _i in ics:

            ic_order_id = _i['orderId']

            put_short_stop_order_id = _i['putSpread']['shortStopOrderId']
            put_long_stop_order_id = _i['putSpread']['longStopOrderId']
            call_short_stop_order_id = _i['callSpread']['shortStopOrderId']
            call_long_stop_order_id = _i['callSpread']['longStopOrderId']

            if _t['orderId'] == ic_order_id and not found:

                if (_t['putCall'] == 'PUT' and _t['strike'] == _i['putSpread']['spread']['short']
                        and not _i['putSpread']['filledShortSTO']):
                    _i['putSpread']['shortSTO'] = round2(abs(_t['filledPrice']))
                    _i['putSpread']['shortSTOFees'] = to_number(_t['fees'])
                    _i['putSpread']['filledShortSTO'] = True
                    found = True
                if (_t['putCall'] == 'PUT' and _t['strike'] == _i['putSpread']['spread']['long']
                        and not _i['putSpread']['filledLongBTO']):
                    _i['putSpread']['longBTO'] = round2(abs(_t['filledPrice']))
                    _i['putSpread']['longBTOFees'] = to_number(_t['fees'])
                    _i['putSpread']['filledLongBTO'] = True
                    found = True
                if (_t['putCall'] == 'CALL' and _t['strike'] == _i['callSpread']['spread']['short']
                        and not _i['callSpread']['filledShortSTO']):
                    _i['callSpread']['shortSTO'] = round2(abs(_t['filledPrice']))
                    _i['callSpread']['shortSTOFees'] = to_number(_t['fees'])
                    _i['callSpread']['filledShortSTO'] = True
                    found = True
                if (_t['putCall'] == 'CALL' and _t['strike'] == _i['callSpread']['spread']['long']
                        and not _i['callSpread']['filledLongBTO']):
                    _i['callSpread']['longBTO'] = round2(abs(_t['filledPrice']))
                    _i['callSpread']['longBTOFees'] = to_number(_t['fees'])
                    _i['callSpread']['filledLongBTO'] = True
                    found = True

            if (((str(_t['orderId']) + 'S') == put_short_stop_order_id or (_t['orderId'] == put_short_stop_order_id))
                    and _t['buyOrSell'] == 'BUY' and not found and not _i['putSpread']['filledShortBTC']):
                _i['putSpread']['status'] = 'EARLY' if _i['putSpread']['isEarly'] else 'STOPPED'
                _i['putSpread']['shortBTC'] = round2(abs(_t['filledPrice']))
                _i['putSpread']['shortBTCFees'] = to_number(_t['fees'])
                _i['putSpread']['shortStopped'] = False if _i['putSpread']['isEarly'] else True
                _i['putSpread']['stoppedTime'] = _t['time']
                _i['putSpread']['earlyTime'] = _t['time']
                _i['putSpread']['filledShortBTC'] = True
                found = True

            if (((str(_t['orderId']) + 'L') == put_long_stop_order_id or (_t['orderId'] == put_long_stop_order_id))
                    and _t['buyOrSell'] == 'SELL' and not found and not _i['putSpread']['filledLongSTC']):
                _i['putSpread']['longSTC'] = round2(abs(_t['filledPrice']))
                _i['putSpread']['longSTCFees'] = '{:.2f}'.format(to_number(_t['fees']))
                _i['putSpread']['filledLongSTC'] = True
                found = True

            if (((str(_t['orderId']) + 'S') == call_short_stop_order_id or (_t['orderId'] == call_short_stop_order_id))
                    and _t['buyOrSell'] == 'BUY' and not found and not _i['callSpread']['filledShortBTC']):
                _i['callSpread']['status'] = 'EARLY' if _i['callSpread']['isEarly'] else 'STOPPED'
                _i['callSpread']['shortBTC'] = round2(abs(_t['filledPrice']))
                _i['callSpread']['shortBTCFees'] = to_number(_t['fees'])
                _i['callSpread']['shortStopped'] = False if _i['callSpread']['isEarly'] else True
                _i['callSpread']['stoppedTime'] = _t['time']
                _i['callSpread']['earlyTime'] = _t['time']
                _i['callSpread']['filledShortBTC'] = True
                found = True

            if (((str(_t['orderId']) + 'L') == call_long_stop_order_id or (_t['orderId'] == call_long_stop_order_id))
                    and _t['buyOrSell'] == 'SELL' and not found and not _i['callSpread']['filledLongSTC']):
                _i['callSpread']['longSTC'] = round2(abs(_t['filledPrice']))
                _i['callSpread']['longSTCFees'] = '{:.2f}'.format(to_number(_t['fees']))
                _i['callSpread']['filledLongSTC'] = True
                found = True

        # matched or not, remove this transaction from the list
        w.pop(0)

        # If there was no match, save the transaction away for reporting/debug
        if not found:
            left_over_transactions.append(_t)

        found = False


def _local_tz():
    """Return the system local timezone (aware)."""
    return datetime.now().astimezone().tzinfo


def _parse_timestamp(time_stamp):
    """Parse an ISO/epoch timestamp into a timezone-aware datetime."""
    # Numeric epoch (ms) like JS Date(number)
    if isinstance(time_stamp, (int, float)):
        return datetime.fromtimestamp(time_stamp / 1000.0, tz=_local_tz())

    s = str(time_stamp).strip()
    # Normalize trailing Z and +0000 style offsets for fromisoformat
    iso = s.replace('Z', '+00:00')
    iso = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', iso)
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        # Fallback: epoch milliseconds as string
        dt = datetime.fromtimestamp(float(s) / 1000.0, tz=_local_tz())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_local_tz())
    return dt


def is_before_market_open_et(date):
    """date is an aware datetime; check if it's before 9:30 AM Eastern."""
    if ZoneInfo is not None:
        date_et = date.astimezone(ZoneInfo("America/New_York"))
    else:  # pragma: no cover
        date_et = date

    hour_et = date_et.hour
    minute_et = date_et.minute

    cutoff_hour = 9
    cutoff_minute = 30

    return (hour_et < cutoff_hour
            or (hour_et == cutoff_hour and minute_et < cutoff_minute))


def fix_time(time_stamp):
    """Convert UTC timestamp to local time; roll back pre-open trades to prior day."""
    # Convert to local time since the timestamp is in UTC
    date = _parse_timestamp(time_stamp).astimezone(_local_tz())

    if is_before_market_open_et(date):
        date = date.replace(hour=23, minute=59, second=59, microsecond=0)
        date = date - timedelta(days=1)

    return date


def convert_time_date(date):
    if ZoneInfo is not None:
        mst = date.astimezone(ZoneInfo("America/Denver"))
    else:  # pragma: no cover
        mst = date
    return mst.strftime('%m/%d/%Y')


def convert_time_time(date):
    hours = str(date.hour).zfill(2)
    minutes = str(date.minute).zfill(2)
    seconds = str(date.second).zfill(2)
    return '{}:{}:{}'.format(hours, minutes, seconds)


def symbol_to_strike(symbol):
    matches = re.search(r'SPXW  2.......([0-9][0-9][0-9][0-9])000', symbol)
    strike = matches.group(1)
    return strike


def symbol_to_option(symbol):
    # 'SPXW  250411P05295000'
    matches = re.search(r'SPXW  2.....([CP]).([0-9][0-9][0-9][0-9])000', symbol)
    put_call = 'PUT' if matches.group(1) == 'P' else 'CALL'
    strike = matches.group(2)
    return {
        'putCall': put_call,
        'strike': strike,
    }


def within_minutes(minutes, time1, time2):
    t1 = time1.split(':')
    t2 = time2.split(':')
    t1_val = (int(t1[0]) * 3600) + (int(t1[1]) * 60) + int(t1[2])
    t2_val = (int(t2[0]) * 3600) + (int(t2[1]) * 60) + int(t2[2])
    diff = abs(t1_val - t2_val)
    return diff < (minutes * 60)


def is_after(time1, time2):
    t1 = time1.split(':')
    t2 = time2.split(':')
    t1_val = (int(t1[0]) * 3600) + (int(t1[1]) * 60) + int(t1[2])
    t2_val = (int(t2[0]) * 3600) + (int(t2[1]) * 60) + int(t2[2])
    return t1_val >= t2_val


def get_recommendation_for_order(time):
    files = get_files_in_recommendations_directory()

    found = None

    for file in files:
        file_name = file.split('/')[-1]
        file_time = file_name.split('.')[0]
        file_time = file_time.replace("recommendation-", "").replace("-", ":")
        if within_minutes(1, time, file_time):
            found = file

    return found


def get_files_in_recommendations_directory():
    try:
        # dir_path = SPX_HOME + '/' + DATE + '/omeic/tos/recommendations'
        dir_path = Path(SPX_HOME) / DATE / "omeic" / "tos" / "recommendations"

        print(f'expected dir path:{dir_path}')

        names = os.listdir(dir_path)

        file_list = []
        for name in names:
            full = os.path.join(dir_path, name)
            if not os.path.isfile(full):
                continue
            if full.endswith('recommendation.json'):
                continue
            if not full.endswith('.json'):
                continue
            file_list.append(full)
        return file_list
    except Exception as error:
        print('Error reading directory:', error, file=sys.stderr)
        return []


def print_results():
    total_pl = 0
    entries = 0
    winners = 0
    losers = 0
    bes = 0

    print('\n' + SPX_WHO + "'s TOS Live MEIC Results for " + DATE + ': ')
    for i in range(len(ics) - 1, -1, -1):

        ic = ics[i]

        total_pl += ic['pl']
        entries += 1
        if ic['status'] == 'WINNER' or ic['status'] == 'EARLY':
            winners += 1
        elif ic['status'] == 'BE':
            bes += 1
        elif ic['status'] == 'LOSER':
            losers += 1
        else:
            print("ERROR: unknown IC status")

    print('\nTotal P/L: ' + us_currency(total_pl))
    print('Entries: ' + '{:.0f}'.format(entries))
    print('Winners: ' + '{:.0f}'.format(winners))
    print('BreakEvens: ' + '{:.0f}'.format(bes))
    print('Losers: ' + '{:.0f}'.format(losers))

    # early_file = SPX_HOME + '/' + DATE + '/omeic/tos/positions/EARLY'
    early_file = Path(SPX_HOME) / DATE / "omeic" / "tos" / "positions" / "EARLY"
    print(f'expected early_file file:{early_file}')



    if os.path.exists(early_file):
        print('EarlyPT: YES')
    else:
        print('EarlyPT: No')

    if itms and len(itms) > 0:
        itm_total = 0
        for itm in itms:
            symbol_to_option(itm['symbol'])
            itm_total += itm['amount']
        print('\nITM losses: ' + us_currency(itm_total))
        print('Actual P/L: ' + us_currency(total_pl + itm_total))

    for i in range(len(ics) - 1, -1, -1):

        ic = ics[i]

        ls = '\n*** IC ' + ic['time'] + ((' (' + ic['stopRisk'] + ') ') if ic['stopRisk'] else ' ') + ic['status']
        ls = ls + ' ' + us_currency(ic['pl'])
        print(ls)

        spx_at_time = ic['spxLast']

        put_spread = ic['putSpread']

        if put_spread['shortStopped']:
            put_short_stop_price = ' ' + us_currency(put_spread['stopPrice'])
            put_filled_price = ' ' + us_currency(put_spread['shortBTC'])
            put_slippage = ' ' + us_currency(put_spread['shortBTC'] - put_spread['stopPrice'])
            put_long_stc_price = ' ' + us_currency(put_spread['longSTC'])
            put_stopped_time = ' ' + str(put_spread['stoppedTime'])
            put_early_time = ''
        else:
            put_short_stop_price = ' ' + us_currency(put_spread['stopPrice'])
            put_filled_price = ''
            put_slippage = ''
            put_long_stc_price = ''
            put_stopped_time = ''
            put_early_time = (' ' + str(put_spread['earlyTime'])) if put_spread['isEarly'] else ''

        ls = put_spread['spread']['putCall'] + ' '
        ls = ls + ' ' + str(put_spread['spread']['short']) + '/' + str(put_spread['spread']['long'])
        if spx_at_time:
            ls = ls + ' ' + str(spx_at_time)
        ls = ls + ' ' + us_currency(put_spread['spreadNetCredit'])
        ls = ls + ' ' + put_spread['status']
        ls = ls + '' + put_stopped_time
        ls = ls + '' + put_early_time
        ls = ls + '' + put_short_stop_price
        ls = ls + '' + put_filled_price
        ls = ls + '' + put_slippage
        ls = ls + '' + put_long_stc_price
        ls = ls + ' ' + us_currency(put_spread['pl'])
        print(ls)

        call_spread = ic['callSpread']

        if call_spread['shortStopped']:
            call_short_stop_price = ' ' + us_currency(call_spread['stopPrice'])
            call_filled_price = ' ' + us_currency(call_spread['shortBTC'])
            call_slippage = ' ' + us_currency(call_spread['shortBTC'] - call_spread['stopPrice'])
            call_long_stc_price = ' ' + us_currency(call_spread['longSTC'])
            call_stopped_time = ' ' + str(call_spread['stoppedTime'])
            call_early_time = ''
        else:
            call_short_stop_price = ' ' + us_currency(call_spread['stopPrice'])
            call_filled_price = ''
            call_slippage = ''
            call_long_stc_price = ''
            call_stopped_time = ''
            call_early_time = (' ' + str(call_spread['earlyTime'])) if call_spread['isEarly'] else ''

        ls = call_spread['spread']['putCall']
        ls = ls + ' ' + str(call_spread['spread']['short']) + '/' + str(call_spread['spread']['long'])
        if spx_at_time:
            ls = ls + ' ' + str(spx_at_time)
        ls = ls + ' ' + us_currency(call_spread['spreadNetCredit'])
        ls = ls + ' ' + call_spread['status']
        ls = ls + '' + call_stopped_time
        ls = ls + '' + call_early_time
        ls = ls + '' + call_short_stop_price
        ls = ls + '' + call_filled_price
        ls = ls + '' + call_slippage
        ls = ls + '' + call_long_stc_price
        ls = ls + ' ' + us_currency(call_spread['pl'])
        print(ls)

    if itms and len(itms) > 0:
        print('\nITM Option Results')
        itm_total = 0
        for itm in itms:
            _o = symbol_to_option(itm['symbol'])
            print('ITM ' + itm['longShort'] + ' ' + itm['putCall'] + ' ' + _o['strike']
                  + ' (' + str(itm['spx_close']) + ') ' + us_currency(itm['amount']))
            itm_total += itm['amount']


def export_csv():
    total_pl = 0
    entries = 0
    winners = 0
    losers = 0
    bes = 0

    if SELECTION == 'ALL' or SELECTION == 'SUMMARY':

        # count the winners and losers
        for i in range(len(ics) - 1, -1, -1):

            ic = ics[i]

            total_pl += ic['pl']
            entries += 1
            if ic['status'] == 'WINNER' or ic['status'] == 'EARLY':
                winners += 1
            elif ic['status'] == 'BE':
                bes += 1
            elif ic['status'] == 'LOSER':
                losers += 1
            else:
                print("ERROR: unknown IC status")

        # dump the summary info
        print('Entity,Date,What,Value')
        print('SUMMARY,' + DATE + ',Total P/L,' + us_currency(total_pl))
        print('SUMMARY,' + DATE + ',Entries,' + '{:.0f}'.format(entries))
        print('SUMMARY,' + DATE + ',Winners,' + '{:.0f}'.format(winners))
        print('SUMMARY,' + DATE + ',BreakEvens,' + '{:.0f}'.format(bes))
        print('SUMMARY,' + DATE + ',Losers,' + '{:.0f}'.format(losers))

        # early_file = SPX_HOME + '/' + DATE + '/omeic/tos/positions/EARLY'
        early_file = Path(SPX_HOME) / DATE / "omeic" / "tos" / "positions" / "EARLY"
        print(f'expected early_file file #2:{early_file}')
        

        if os.path.exists(early_file):
            print('SUMMARY,' + DATE + ',EarlyPT,Yes')
        else:
            print('SUMMARY,' + DATE + ',EarlyPT,No')

    if SELECTION == 'ALL' or SELECTION == 'ICS':

        # export the IC info
        print('Entity,Date,Time,EM,Limit,NetCredit,StopRisk,Status,Gross P/L,Fees,Net P/L')
        for i in range(len(ics) - 1, -1, -1):

            ic = ics[i]

            ic_status = ic['status']
            ic_net_pl = to_number(ic['putSpread']['pl']) + to_number(ic['callSpread']['pl'])
            ic_fees = (to_number(ic['putSpread']['shortSTOFees'])
                       + to_number(ic['putSpread']['longBTOFees'])
                       + to_number(ic['putSpread']['shortBTCFees'])
                       + to_number(ic['putSpread']['longSTCFees'])
                       + to_number(ic['callSpread']['shortSTOFees'])
                       + to_number(ic['callSpread']['longBTOFees'])
                       + to_number(ic['callSpread']['shortBTCFees'])
                       + to_number(ic['callSpread']['longSTCFees']))
            ic_gross_pl = ic_net_pl + ic_fees
            ls = 'IC,' + DATE + ',' + ic['time']
            if ic['em']:
                ls = ls + ',' + '{:.1f}'.format(to_number(ic['em']))
            else:
                ls = ls + ','
            ls = ls + ',' + us_currency(ic['limit'])
            ls = ls + ',' + us_currency(ic['netCredit'])
            if ic['stopRisk']:
                ls = ls + ',' + '{:.1f}'.format(to_number(ic['stopRisk']))
            else:
                ls = ls + ','
            ls = ls + ',' + ic_status
            ls = ls + ',' + us_currency(ic_gross_pl)
            ls = ls + ',' + us_currency(ic_fees)
            ls = ls + ',' + us_currency(ic_net_pl)
            print(ls)

    if SELECTION == 'ALL' or SELECTION == 'SPREADS':

        # export the spread info
        print('Entity,Date,Time,Type,Short,Long,SPX,Offset,Width,Limit,ShortSTO,LongBTO,'
              'NetCredit,Outcome,ExitTime,Stop,Filled,Slip,LongSTC,Gross P/L,Fees,Net P/L')
        for i in range(len(ics) - 1, -1, -1):

            ic = ics[i]

            spx_at_time = to_number(ic['spxLast'])

            spread = ic['putSpread']['spread']
            put_limit = us_currency(ic['putSpread']['limit']) if ic['putSpread']['limit'] else ''
            put_short_sto = us_currency(ic['putSpread']['shortSTO'])
            put_long_bto = us_currency(ic['putSpread']['longBTO'])
            put_actual_net_credit = us_currency(ic['putSpread']['shortSTO'] - ic['putSpread']['longBTO'])
            put_spread_net_pl = to_number(ic['putSpread']['pl'])
            put_spread_fees = (to_number(ic['putSpread']['shortSTOFees'])
                               + to_number(ic['putSpread']['longBTOFees'])
                               + to_number(ic['putSpread']['shortBTCFees'])
                               + to_number(ic['putSpread']['longSTCFees']))
            put_spread_gross_pl = put_spread_net_pl + put_spread_fees
            if ic['putSpread']['status'] == 'STOPPED' and ic['putSpread']['stoppedTime']:
                put_exit_time = ',' + str(ic['putSpread']['stoppedTime'])
                put_filled_price = ',' + us_currency(ic['putSpread']['shortBTC'])
                put_short_stop_price = ',' + us_currency(ic['putSpread']['stopPrice'])
                put_slippage = ',' + us_currency(ic['putSpread']['shortBTC'] - ic['putSpread']['stopPrice'])
                put_long_stc = ',' + us_currency(ic['putSpread']['longSTC'])
            else:
                if ic['putSpread']['isEarly']:
                    put_exit_time = ',' + str(ic['putSpread']['earlyTime'])
                else:
                    put_exit_time = ','
                put_filled_price = ','
                put_short_stop_price = ','
                put_slippage = ','
                put_long_stc = ','
            ls = 'SPREAD,' + DATE + ',' + ic['time'] + ',' + spread['putCall']
            ls = ls + ',' + str(spread['short']) + ',' + str(spread['long'])
            if spx_at_time:
                ls = ls + ',' + '{:.0f}'.format(spx_at_time)
                ls = ls + ',' + '{:.0f}'.format(abs(spx_at_time - to_number(spread['short'])))
            else:
                ls = ls + ','
                ls = ls + ','
            ls = ls + ',' + str(abs(to_number(spread['short']) - to_number(spread['long'])))
            ls = ls + ',' + put_limit + ',' + put_short_sto + ',' + put_long_bto + ',' + put_actual_net_credit
            ls = ls + ',' + ic['putSpread']['status'] + put_exit_time
            ls = ls + put_short_stop_price + put_filled_price + put_slippage + put_long_stc
            ls = ls + ',' + us_currency(put_spread_gross_pl)
            ls = ls + ',' + us_currency(put_spread_fees)
            ls = ls + ',' + us_currency(put_spread_net_pl)
            print(ls)

            spread = ic['callSpread']['spread']
            call_limit = us_currency(ic['callSpread']['limit']) if ic['callSpread']['limit'] else ''
            call_short_sto = us_currency(ic['callSpread']['shortSTO'])
            call_long_bto = us_currency(ic['callSpread']['longBTO'])
            call_actual_net_credit = us_currency(ic['callSpread']['shortSTO'] - ic['callSpread']['longBTO'])
            call_spread_net_pl = to_number(ic['callSpread']['pl'])
            call_spread_fees = (to_number(ic['callSpread']['shortSTOFees'])
                                + to_number(ic['callSpread']['longBTOFees'])
                                + to_number(ic['callSpread']['shortBTCFees'])
                                + to_number(ic['callSpread']['longSTCFees']))
            call_spread_gross_pl = call_spread_net_pl + call_spread_fees
            if ic['callSpread']['status'] == 'STOPPED' and ic['callSpread']['stoppedTime']:
                call_exit_time = ',' + str(ic['callSpread']['stoppedTime'])
                call_filled_price = ',' + us_currency(ic['callSpread']['shortBTC'])
                call_short_stop_price = ',' + us_currency(ic['callSpread']['stopPrice'])
                call_slippage = ',' + us_currency(ic['callSpread']['shortBTC'] - ic['callSpread']['stopPrice'])
                call_long_stc = ',' + us_currency(ic['callSpread']['longSTC'])
            else:
                if ic['callSpread']['isEarly']:
                    call_exit_time = ',' + str(ic['callSpread']['earlyTime'])
                else:
                    call_exit_time = ','
                call_filled_price = ','
                call_short_stop_price = ','
                call_slippage = ','
                call_long_stc = ','
            ls = 'SPREAD,' + DATE + ',' + ic['time'] + ',' + spread['putCall']
            ls = ls + ',' + str(spread['short']) + ',' + str(spread['long'])
            if spx_at_time:
                ls = ls + ',' + '{:.0f}'.format(spx_at_time)
                ls = ls + ',' + '{:.0f}'.format(abs(spx_at_time - to_number(spread['short'])))
            else:
                ls = ls + ','
                ls = ls + ','
            ls = ls + ',' + str(abs(to_number(spread['short']) - to_number(spread['long'])))
            ls = ls + ',' + call_limit + ',' + call_short_sto + ',' + call_long_bto + ',' + call_actual_net_credit
            ls = ls + ',' + ic['callSpread']['status'] + call_exit_time
            ls = ls + call_short_stop_price + call_filled_price + call_slippage + call_long_stc
            ls = ls + ',' + us_currency(call_spread_gross_pl)
            ls = ls + ',' + us_currency(call_spread_fees)
            ls = ls + ',' + us_currency(call_spread_net_pl)
            print(ls)


if __name__ == '__main__':
    monitor()



