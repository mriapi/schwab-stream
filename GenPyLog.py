import json
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import re
from copy import deepcopy
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

# ----------------------------------------------------------------------
# Globals
# ----------------------------------------------------------------------

SPX_HOME = Path("C:/MEIC/log")

DATE = None
OUTPUT_TYPE = None
SELECTION = None
SPX_CLOSE = None

transactions = []
leftover_transactions = []
ics = []
early_orders = []
itms = []

# ----------------------------------------------------------------------
# Utility
# ----------------------------------------------------------------------

def usd(x):
    return locale.currency(x, grouping=True)

def read_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        return None

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def monitor():
    get_opts()
    get_orders()
    get_transactions()
    assign_earlies()
    match_transactions()
    finalize_orders()
    process_itms()

    if OUTPUT_TYPE == "CSV":
        export_csv()
    else:
        print_results()

# ----------------------------------------------------------------------
# Options
# ----------------------------------------------------------------------

def get_opts():
    global DATE, OUTPUT_TYPE, SELECTION

    args = sys.argv[1:]

    if len(args) >= 1:
        DATE = args[0]
        print("Provided Date:", DATE)
    else:
        now = datetime.now()
        DATE = now.strftime("%Y-%m-%d")

    OUTPUT_TYPE = "CSV" if (len(args) >= 2 and args[1] == "CSV") else "TEXT"

    if len(args) >= 3:
        if args[2] in ("ALL", "ICS", "SUMMARY", "SPREADS"):
            SELECTION = args[2]
        else:
            SELECTION = "ALL"
    else:
        SELECTION = "ALL"

# ----------------------------------------------------------------------
# Orders
# ----------------------------------------------------------------------

def get_orders():
    orders_file = SPX_HOME / DATE / "orders.json"
    data = read_json(orders_file)
    if not data:
        print("No Orders")
        return

    for o in data:
        process_order(o)

# ----------------------------------------------------------------------
# Transactions
# ----------------------------------------------------------------------

def get_transactions():
    trans_file = SPX_HOME / DATE / "transactions.json"
    data = read_json(trans_file)
    if not data:
        print("No Transactions")
        return

    for t in data:
        process_transaction(t)

# ----------------------------------------------------------------------
# ITM Processing
# ----------------------------------------------------------------------

def process_itms():
    global SPX_CLOSE

    spx_file = SPX_HOME / DATE / "transactions" / f"spx-{DATE}.json"
    data = read_json(spx_file)

    if not data or "candles" not in data or not data["candles"]:
        SPX_CLOSE = None
    else:
        SPX_CLOSE = data["candles"][0].get("close")

    if not SPX_CLOSE:
        return

    for ic in ics:
        # PUT short
        if SPX_CLOSE < ic["put"]["spread"]["short"] and not ic["put"]["filledShortBTC"]:
            itms.append({
                "putCall": "PUT",
                "longShort": "SHORT",
                "symbol": ic["put"]["shortSymbol"],
                "amount": (SPX_CLOSE - ic["put"]["spread"]["short"]) * 100,
                "spx_close": SPX_CLOSE
            })

        # CALL short
        if SPX_CLOSE > ic["call"]["spread"]["short"] and not ic["call"]["filledShortBTC"]:
            itms.append({
                "putCall": "CALL",
                "longShort": "SHORT",
                "symbol": ic["call"]["shortSymbol"],
                "amount": (ic["call"]["spread"]["short"] - SPX_CLOSE) * 100,
                "spx_close": SPX_CLOSE
            })

        # PUT long
        if SPX_CLOSE < ic["put"]["spread"]["long"] and not ic["put"]["filledLongSTC"]:
            itms.append({
                "putCall": "PUT",
                "longShort": "LONG",
                "symbol": ic["put"]["longSymbol"],
                "amount": (ic["put"]["spread"]["long"] - SPX_CLOSE) * 100,
                "spx_close": SPX_CLOSE
            })

        # CALL long
        if SPX_CLOSE > ic["call"]["spread"]["long"] and not ic["call"]["filledLongSTC"]:
            itms.append({
                "putCall": "CALL",
                "longShort": "LONG",
                "symbol": ic["call"]["longSymbol"],
                "amount": (SPX_CLOSE - ic["call"]["spread"]["long"]) * 100,
                "spx_close": SPX_CLOSE
            })

# ----------------------------------------------------------------------
# Early Orders
# ----------------------------------------------------------------------

def assign_earlies():
    for eo in early_orders:
        for ic in ics:
            if eo["putCall"] == "PUT" and eo["instruction"] == "BUY_TO_CLOSE":
                if not has_order_id(ic["put"]["shortStopOrderId"]) and is_after(eo["time"], ic["time"]):
                    if eo["symbol"] == ic["put"]["shortSymbol"]:
                        ic["put"]["shortStopOrderId"] = eo["orderId"]
                        ic["put"]["isEarly"] = True

            if eo["putCall"] == "CALL" and eo["instruction"] == "BUY_TO_CLOSE":
                if not has_order_id(ic["call"]["shortStopOrderId"]) and is_after(eo["time"], ic["time"]):
                    if eo["symbol"] == ic["call"]["shortSymbol"]:
                        ic["call"]["shortStopOrderId"] = eo["orderId"]
                        ic["call"]["isEarly"] = True

            if eo["putCall"] == "PUT" and eo["instruction"] == "SELL_TO_CLOSE":
                if not has_order_id(ic["put"]["longStopOrderId"]) and is_after(eo["time"], ic["time"]):
                    if eo["symbol"] == ic["put"]["longSymbol"]:
                        ic["put"]["longStopOrderId"] = eo["orderId"]

            if eo["putCall"] == "CALL" and eo["instruction"] == "SELL_TO_CLOSE":
                if not has_order_id(ic["call"]["longStopOrderId"]) and is_after(eo["time"], ic["time"]):
                    if eo["symbol"] == ic["call"]["longSymbol"]:
                        ic["call"]["longStopOrderId"] = eo["orderId"]

def has_order_id(order_id):
    return order_id not in (None, -1)

# ----------------------------------------------------------------------
# Order Processing
# ----------------------------------------------------------------------

def process_order(o):
    time = convert_time_time(fix_time(o["enteredTime"]))
    rec = get_recommendation_info_at_time(time)

    # Iron Condor entry
    if (o["orderType"] == "NET_CREDIT" and
        o["complexOrderStrategyType"] == "IRON_CONDOR" and
        o["status"] == "FILLED" and
        o["orderStrategyType"] == "TRIGGER"):

        if not o.get("childOrderStrategies") or len(o["childOrderStrategies"]) < 2:
            print("No spreads in IC order")
            return

        for _ in range(o["quantity"]):
            put_spread = make_spread("PUT", rec["putSpreadLimit"])
            call_spread = make_spread("CALL", rec["callSpreadLimit"])

            ic = {
                "orderId": o["orderId"],
                "put": put_spread,
                "call": call_spread,
                "time": time,
                "pl": 0,
                "spxLast": rec["underlying"]["spxLast"],
                "em": rec["underlying"]["em"],
                "stopRisk": None,
                "netCredit": 0,
                "limit": o["price"],
                "status": "WINNER"
            }

            # Legs
            for leg in o["orderLegCollection"]:
                symbol = leg["instrument"]["symbol"]
                putCall = leg["instrument"]["putCall"]
                instr = leg["instruction"]

                if putCall == "PUT" and instr == "SELL_TO_OPEN":
                    opt = symbol_to_option(symbol)
                    ic["put"]["spread"]["short"] = opt["strike"]
                    ic["put"]["shortSymbol"] = symbol

                elif putCall == "PUT" and instr == "BUY_TO_OPEN":
                    opt = symbol_to_option(symbol)
                    ic["put"]["spread"]["long"] = opt["strike"]
                    ic["put"]["longSymbol"] = symbol

                elif putCall == "CALL" and instr == "SELL_TO_OPEN":
                    opt = symbol_to_option(symbol)
                    ic["call"]["spread"]["short"] = opt["strike"]
                    ic["call"]["shortSymbol"] = symbol

                elif putCall == "CALL" and instr == "BUY_TO_OPEN":
                    opt = symbol_to_option(symbol)
                    ic["call"]["spread"]["long"] = opt["strike"]
                    ic["call"]["longSymbol"] = symbol

            # Stop orders
            for child in o["childOrderStrategies"]:
                leg0 = child["orderLegCollection"][0]
                child_status = child["status"]
                child_child = child["childOrderStrategies"][0] if child.get("childOrderStrategies") else None

                if leg0["instrument"]["symbol"] == ic["put"]["shortSymbol"] and not has_subsequent_order(child_status):
                    ic["put"]["shortStopOrderId"] = child["orderId"]
                    ic["put"]["longStopOrderId"] = child_child["orderId"]
                    ic["put"]["stopPrice"] = child["stopPrice"]

                if leg0["instrument"]["symbol"] == ic["call"]["shortSymbol"] and not has_subsequent_order(child_status):
                    ic["call"]["shortStopOrderId"] = child["orderId"]
                    ic["call"]["longStopOrderId"] = child_child["orderId"]
                    ic["call"]["stopPrice"] = child["stopPrice"]

            ics.append(ic)

        return

    # Early orders
    if (o["orderType"] in ("MARKET", "LIMIT") and
        o["complexOrderStrategyType"] in ("CUSTOM", "NONE") and
        o["status"] == "FILLED"):

        for leg in o["orderLegCollection"]:
            for _ in range(leg["quantity"]):
                early_orders.append({
                    "symbol": leg["instrument"]["symbol"],
                    "instruction": leg["instruction"],
                    "orderId": o["orderId"],
                    "time": time,
                    "putCall": leg["instrument"]["putCall"]
                })

# ----------------------------------------------------------------------
# Spread Template
# ----------------------------------------------------------------------

def make_spread(putCall, limit):
    return {
        "spread": {"putCall": putCall, "short": 0, "long": 0},
        "shortStopOrderId": -1,
        "longStopOrderId": -1,
        "pl": 0,
        "shortStopped": False,
        "stopPrice": 0,
        "shortBTC": 0,
        "longSTC": 0,
        "shortBTCFees": 0,
        "longSTCFees": 0,
        "filledShortBTC": False,
        "filledLongSTC": False,
        "stoppedTime": None,
        "earlyTime": None,
        "status": "EXPIRED",
        "isEarly": False,
        "limit": limit,
        "shortSTO": 0,
        "longBTO": 0,
        "shortSTOFees": 0,
        "longBTOFees": 0,
        "filledShortSTO": False,
        "filledLongBTO": False,
        "spreadNetCredit": 0,
        "shortSymbol": None,
        "longSymbol": None
    }

# ----------------------------------------------------------------------
# Transaction Processing
# ----------------------------------------------------------------------

def process_transaction(t):
    if t["type"] != "TRADE":
        return

    symbol = None
    amount = None
    netAmount = None
    putCall = None
    strike = None
    position = None
    orderId = None
    time = None
    filledPrice = None
    fees = None

    for item in t["transferItems"]:
        if item["instrument"]["assetType"] == "OPTION":
            dt = fix_time(t["time"])
            time = convert_time_time(dt)

            symbol = item["instrument"]["symbol"]
            amount = abs(item["amount"])
            netAmount = abs(t["netAmount"]) / amount / 100
            putCall = item["instrument"]["putCall"]
            strike = symbol_to_strike(symbol)
            position = item["positionEffect"]
            orderId = t["orderId"]
            filledPrice = float(item["price"])
            fees = abs(abs(item["price"] * 100) * amount - abs(t["netAmount"])) / amount

    for _ in range(amount):
        transactions.append({
            "symbol": symbol,
            "netAmount": netAmount,
            "putCall": putCall,
            "strike": strike,
            "position": position,
            "orderId": orderId,
            "time": time,
            "filledPrice": filledPrice,
            "fees": fees
        })

# ----------------------------------------------------------------------
# Matching Transactions to IC Legs
# ----------------------------------------------------------------------

def match_transactions():
    w = deepcopy(transactions)
    found = False

    while w:
        t = w.pop(0)

        for ic in ics:
            ic_id = ic["orderId"]

            putShortStop = ic["put"]["shortStopOrderId"]
            putLongStop = ic["put"]["longStopOrderId"]
            callShortStop = ic["call"]["shortStopOrderId"]
            callLongStop = ic["call"]["longStopOrderId"]

            # Entry legs
            if t["orderId"] == ic_id and not found:
                if t["putCall"] == "PUT" and t["strike"] == ic["put"]["spread"]["short"] and not ic["put"]["filledShortSTO"]:
                    ic["put"]["shortSTO"] = abs(t["filledPrice"])
                    ic["put"]["shortSTOFees"] = t["fees"]
                    ic["put"]["filledShortSTO"] = True
                    found = True

                elif t["putCall"] == "PUT" and t["strike"] == ic["put"]["spread"]["long"] and not ic["put"]["filledLongBTO"]:
                    ic["put"]["longBTO"] = abs(t["filledPrice"])
                    ic["put"]["longBTOFees"] = t["fees"]
                    ic["put"]["filledLongBTO"] = True
                    found = True

                elif t["putCall"] == "CALL" and t["strike"] == ic["call"]["spread"]["short"] and not ic["call"]["filledShortSTO"]:
                    ic["call"]["shortSTO"] = abs(t["filledPrice"])
                    ic["call"]["shortSTOFees"] = t["fees"]
                    ic["call"]["filledShortSTO"] = True
                    found = True

                elif t["putCall"] == "CALL" and t["strike"] == ic["call"]["spread"]["long"] and not ic["call"]["filledLongBTO"]:
                    ic["call"]["longBTO"] = abs(t["filledPrice"])
                    ic["call"]["longBTOFees"] = t["fees"]
                    ic["call"]["filledLongBTO"] = True
                    found = True

            # Stop orders
            if t["orderId"] == putShortStop and not found and not ic["put"]["filledShortBTC"]:
                ic["put"]["status"] = "EARLY" if ic["put"]["isEarly"] else "STOPPED"
                ic["put"]["shortBTC"] = abs(t["filledPrice"])
                ic["put"]["shortBTCFees"] = t["fees"]
                ic["put"]["shortStopped"] = not ic["put"]["isEarly"]
                ic["put"]["stoppedTime"] = t["time"]
                ic["put"]["earlyTime"] = t["time"]
                ic["put"]["filledShortBTC"] = True
                found = True

            if t["orderId"] == putLongStop and not found and not ic["put"]["filledLongSTC"]:
                ic["put"]["longSTC"] = abs(t["filledPrice"])
                ic["put"]["longSTCFees"] = t["fees"]
                ic["put"]["filledLongSTC"] = True
                found = True

            if t["orderId"] == callShortStop and not found and not ic["call"]["filledShortBTC"]:
                ic["call"]["status"] = "EARLY" if ic["call"]["isEarly"] else "STOPPED"
                ic["call"]["shortBTC"] = abs(t["filledPrice"])
                ic["call"]["shortBTCFees"] = t["fees"]
                ic["call"]["shortStopped"] = not ic["call"]["isEarly"]
                ic["call"]["stoppedTime"] = t["time"]
                ic["call"]["earlyTime"] = t["time"]
                ic["call"]["filledShortBTC"] = True
                found = True

            if t["orderId"] == callLongStop and not found and not ic["call"]["filledLongSTC"]:
                ic["call"]["longSTC"] = abs(t["filledPrice"])
                ic["call"]["longSTCFees"] = t["fees"]
                ic["call"]["filledLongSTC"] = True
                found = True

        if not found:
            leftover_transactions.append(t)

        found = False

# ----------------------------------------------------------------------
# Finalizing ICs
# ----------------------------------------------------------------------

def finalize_orders():
    for ic in ics:
        # PUT
        p = ic["put"]
        p["spreadNetCredit"] = round(p["shortSTO"] - p["longBTO"], 2)

        if not p["shortBTC"]:
            p["shortBTC"] = 0