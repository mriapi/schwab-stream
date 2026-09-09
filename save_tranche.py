import json
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any


# ============================================================
# Global tranche dictionary
# ============================================================

tranche_dict = {}


# ============================================================
# Initialize / reset tranche_dict
# ============================================================

def initialize_tranche_dict():
    """
    Initialize/reset tranche_dict.

    All values are initialized to None.
    """
    
    global tranche_dict

    try:

        tranche_dict = {
            "timeEt": None,
            "spx": None,
            "atmStraddle": None,
            "targetCredit": None,

            "callShortSym": None,
            "callLongSym": None,
            "callShortBid": None,
            "callLongAsk": None,
            "callCalcCredit": None,
            "callStop": None,
            

            "putShortSym": None,
            "putLongSym": None,
            "putShortBid": None,
            "putLongAsk": None,
            "putCalcCredit": None,
            "putStop": None,

            "orderId": None
        }

    except Exception as e:
        print(f"initialize_tranche_dict error: {e} ")


# ============================================================
# Set a single tranche value
# ============================================================

def set_tranche_value(key: str, value: Any):
    """
    Set one value in tranche_dict.

    Example:
        set_tranche_value("spx", 7638.54)
        set_tranche_value("targetCredit", 1.95)
    """

    try:

        if key not in tranche_dict:
            # raise KeyError(f"Invalid tranche key: {key}")
            print(f"in set_tranche_value, Invalid tranche key: {key}")

        else:
            tranche_dict[key] = value

    except Exception as e:
        print(f"set_tranche_value error: {e} ")

    


# ============================================================
# Convenience functions for commonly used values
# ============================================================

def set_tranche_time():
    """
    Set the current Eastern Time in HH:MM:SS format.
    Uses the America/New_York time zone regardless of
    the computer's local time zone.
    """

    try:

        eastern_time = datetime.now(ZoneInfo("America/New_York"))
        tranche_dict["timeEt"] = eastern_time.strftime("%H:%M:%S")

    except Exception as e:
        print(f"set_tranche_time error: {e} ")


def set_tranche_spx(spx):

    try:
        tranche_dict["spx"] = spx

    except Exception as e:
        print(f"set_tranche_spx error: {e} ")


def set_tranche_atm_straddle(atm_straddle):
    try:
        tranche_dict["atmStraddle"] = atm_straddle

    except Exception as e:
        print(f"set_tranche_atm_straddle error: {e} ")
    


def set_tranche_target_credit(target_credit):
    try:
        tranche_dict["targetCredit"] = target_credit
    
    except Exception as e:
        print(f"set_tranche_target_credit error: {e} ")

def set_call_tranche(call_short_sym,
                      call_long_sym,
                      call_short_bid,
                      call_long_ask,
                      call_stop):
    
    try:

        tranche_dict["callShortSym"] = call_short_sym
        tranche_dict["callLongSym"] = call_long_sym
        tranche_dict["callShortBid"] = call_short_bid
        tranche_dict["callLongAsk"] = call_long_ask
        tranche_dict["callStop"] = call_stop

    except Exception as e:
        print(f"set_call_tranche error: {e} ")


def set_put_tranche(put_short_sym,
                    put_long_sym,
                    put_short_bid,
                    put_long_ask,
                    put_stop):

    try:

        tranche_dict["putShortSym"] = put_short_sym
        tranche_dict["putLongSym"] = put_long_sym
        tranche_dict["putShortBid"] = put_short_bid
        tranche_dict["putLongAsk"] = put_long_ask
        tranche_dict["putStop"] = put_stop

    except Exception as e:
        print(f"set_put_tranche error: {e} ")

    


def set_tranche_order_id(order_id):
    try:
        tranche_dict["orderId"] = order_id

    except Exception as e:
        print(f"set_tranche_order_id error: {e} ")


# ============================================================
# Get today's tranche file path
# ============================================================

def get_tranche_file_path():
    """
    Return:

        C:\\MEIC\\tranche\\jdata\\YYYY-MM-DD\\tranches.json
    """

    try:
            

        today = datetime.now().strftime("%Y-%m-%d")

        directory = Path(r"C:\MEIC\tranche\jdata") / today

    except Exception as e:
        print(f"get_tranche_file_path error: {e} ")

    return directory / "tranches.json"


# ============================================================
# Save current tranche to JSON file
# ============================================================

def save_tranche():
    """
    Add the current tranche_dict to today's tranches.json file.

    If the file doesn't exist, it is created containing:

        []

    The current tranche is then added to the list.

    After successfully saving, tranche_dict is reset to None
    values for the next transaction.
    """

    global tranche_dict
    file_path = r"C:\MEIC\err"

    try:

        file_path = get_tranche_file_path()

        # Create today's directory if necessary
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # --------------------------------------------------------
        # Read existing tranche data
        # --------------------------------------------------------

        if file_path.exists():

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    tranches = json.load(f)

            except json.JSONDecodeError:
                # File exists but does not contain valid JSON
                raise ValueError(
                    f"Invalid JSON found in tranche file: {file_path}"
                )

            if not isinstance(tranches, list):
                raise ValueError(
                    f"Tranche JSON file does not contain a list: {file_path}"
                )

        else:
            # New file
            tranches = []

        # --------------------------------------------------------
        # Add current tranche
        # --------------------------------------------------------

        tranches.append(tranche_dict.copy())

        # --------------------------------------------------------
        # Write complete JSON list back to file
        # --------------------------------------------------------

        with file_path.open("w", encoding="utf-8") as f:

            json.dump(
                tranches,
                f,
                indent=4
            )

        # --------------------------------------------------------
        # Prepare for next transaction
        # --------------------------------------------------------

        initialize_tranche_dict()

    except Exception as e:
        print(f"save_tranche error: {e} ")


    return file_path




# def test_tranche_1():
#     initialize_tranche_dict()
#     set_tranche_time()
#     set_tranche_value("spx", 7638.54)
#     set_tranche_value("atmStraddle", 15.10)
#     set_tranche_value("targetCredit", 1.95)
#     set_tranche_value("callShortSym", "SPXW  260901C07655000")
#     set_tranche_value("callLongSym", "SPXW  260901C07705000")
#     set_tranche_value("callShortBid", 2.00)
#     set_tranche_value("callLongAsk", 0.15)
#     set_tranche_value("callStop", 3.10)
#     set_tranche_value("putShortSym", "SPXW  260901P07615000")
#     set_tranche_value("putLongSym", "SPXW  260901P07565000")
#     set_tranche_value("putShortBid", 1.70)
#     set_tranche_value("putLongAsk", 0.25)
#     set_tranche_value("putStop", 2.55)
#     set_tranche_value("orderId", "1007787982001")

# def test_tranche_2():
#     initialize_tranche_dict()
#     set_tranche_time()
#     set_tranche_value("spx", 7629.65)
#     set_tranche_value("atmStraddle", 14.85)
#     set_tranche_value("targetCredit", 1.90)
#     set_tranche_value("callShortSym", "SPXW  260901C07660000")
#     set_tranche_value("callLongSym", "SPXW  260901C07710000")
#     set_tranche_value("callShortBid", 2.05)
#     set_tranche_value("callLongAsk", 0.10)
#     set_tranche_value("callStop", 3.00)
#     set_tranche_value("putShortSym", "SPXW  260901P07620000")
#     set_tranche_value("putLongSym", "SPXW  260901P07560000")
#     set_tranche_value("putShortBid", 1.75)
#     set_tranche_value("putLongAsk", 0.20)
#     set_tranche_value("putStop", 2.45)
#     set_tranche_value("orderId", "1007787982022")

# def test_tranche_3():
#     initialize_tranche_dict()
#     set_tranche_time()
#     set_tranche_value("spx", 7625.43)
#     set_tranche_value("atmStraddle", 14.55)
#     set_tranche_value("targetCredit", 2.00)
#     set_tranche_value("callShortSym", "SPXW  260901C07650000")
#     set_tranche_value("callLongSym", "SPXW  260901C07700000")
#     set_tranche_value("callShortBid", 2.05)
#     set_tranche_value("callLongAsk", 0.10)
#     set_tranche_value("callStop", 3.00)
#     set_tranche_value("putShortSym", "SPXW  260901P07610000")
#     set_tranche_value("putLongSym", "SPXW  260901P07555000")
#     set_tranche_value("putShortBid", 1.70)
#     set_tranche_value("putLongAsk", 0.15)
#     set_tranche_value("putStop", 2.35)
#     set_tranche_value("orderId", "1007787982058")



# def run_test():


#     initialize_tranche_dict()

#     test_tranche_1()
#     save_tranche()

#     time.sleep(2)
#     test_tranche_2()
#     save_tranche()

#     time.sleep(2)
#     test_tranche_3()
#     save_tranche()

#     print("DONE")


# run_test()











