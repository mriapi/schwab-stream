# bt__best_times_oo.py
#
# - Run OptionOmega backtest over any 6-month period of time using the OO's max of entry 21 times.
#   Those 21 times should the once you would most likely use in trading.  This normally
#   means omitting the earlist and latest possible times of the session.
# - export the log from OO and save as C:/MEIC/oo/trade-log.csv
# - bt__best_times_oo.py will created a folder in C:/MEIC/oo, named with today's current date and
#   will create and place these files in the folder 
#       - oo_log_parsed_A_6m.csv (ranked entry times over the full 6-month OO test)
#       - oo_log_parsed_B_3m.csv (ranked entry times over the last 3 months of the 6-month OO test)
#       - oo_log_parsed_C_1m.csv (ranked entry times over the last montt of the 6-month OO test)
#       - aggregate.csv (aggregated sum of oo_log_parsed_A_6m.csv, oo_log_parsed_B_3m.csv, and
#         oo_log_parsed_C_1m.csv).  This gives more weight to later part of the 6-month test.


import csv
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta, date
import os

import sys
import pandas as pd


ROOT_DIR = "C:\\MEIC\\oo"

results_dir = ""


def intialize_summary_directory():
    global summary_file, results_dir

    # Get today's date in YYYY-MM-DD format
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Construct the subdirectory path
    sub_dir = os.path.join(ROOT_DIR, today_str)
    results_dir = sub_dir
    
    # Create the subdirectory if it doesn't exist
    os.makedirs(sub_dir, exist_ok=True)
    
    # Construct the summary.csv file path
    summary_file = os.path.join(sub_dir, "summary.csv")

    
    # Delete the file if it exists
    if os.path.exists(summary_file):
        os.remove(summary_file)
        print(f"Deleted: {summary_file}")
    else:
        print(f"No summary.csv file found in {sub_dir}") 



def process_summary(df, output_filename):

    print(f'output file name:{output_filename}, df:\n{df}')



    time_slot_data = defaultdict(lambda: {"pl": 0.0, "count": 0})

    # Sort to ensure pairing is consistent
    df.sort_values(by=["Date Opened", "Time Opened"], inplace=True)

    # Iterate in pairs to find iron condors
    i = 0
    while i < len(df) - 1:
        row1 = df.iloc[i]
        row2 = df.iloc[i + 1]

        if row1["Date Opened"] == row2["Date Opened"] and row1["Time Opened"] == row2["Time Opened"]:
            time_slot = row1["Time Opened"]
            total_pl = float(row1["P/L"]) + float(row2["P/L"])
            time_slot_data[time_slot]["pl"] += total_pl
            time_slot_data[time_slot]["count"] += 1
            i += 2
        else:
            i += 1

    # Convert to DataFrame
    summary = []
    for time_slot, data in time_slot_data.items():
        summary.append({
            "time_slot": time_slot,
            "P/L": round(data["pl"], 2),
            "#_trades": data["count"]
        })

    summary_df = pd.DataFrame(summary)

    

    # # Rank top 10 by P/L
    # summary_df["rank"] = ""
    # top10 = summary_df.nlargest(10, "P/L").sort_values(by="P/L", ascending=False).reset_index(drop=True)
    # for idx, row in top10.iterrows():
    #     summary_df.loc[summary_df["time_slot"] == row["time_slot"], "rank"] = str(idx + 1)


    # Rank top 21 by P/L
    summary_df["rank"] = ""
    top21 = summary_df.nlargest(21, "P/L").sort_values(by="P/L", ascending=False).reset_index(drop=True)

    for idx, row in top21.iterrows():
        summary_df.loc[summary_df["time_slot"] == row["time_slot"], "rank"] = str(idx + 1)






    print(f'summary_df:{summary_df}')

    # Reorder and save
    summary_df = summary_df[["time_slot", "rank", "P/L", "#_trades"]]
    summary_df.sort_values(by="time_slot", inplace=True)
    summary_df.to_csv(output_filename, index=False)





# def calc_aggregate(results_dir: str):
#     """
#     Reads three CSV files from results_dir, sums rank and P/L by time_slot,
#     and saves the aggregate results to aggregate.csv.
#     """

#     # Expected filenames
#     files = [
#         "oo_log_parsed_A_6m.csv",
#         "oo_log_parsed_B_3m.csv",
#         "oo_log_parsed_C_1m.csv"
#     ]

#     # Build full paths and check existence
#     paths = [os.path.join(results_dir, f) for f in files]
#     for p in paths:
#         if not os.path.exists(p):
#             print(f"Error: Required file not found: {p}")
#             sys.exit(1)

#     # Read all three CSVs
#     dfs = [pd.read_csv(p) for p in paths]

#     # Merge on time_slot
#     merged = dfs[0][["time_slot", "rank", "P/L"]].copy()
#     for df in dfs[1:]:
#         merged = merged.merge(df[["time_slot", "rank", "P/L"]],
#                               on="time_slot",
#                               suffixes=("", "_dup"))

#         # Sum rank and P/L across duplicates
#         merged["rank"] = merged["rank"] + merged["rank_dup"]
#         merged["P/L"] = merged["P/L"] + merged["P/L_dup"]

#         # Drop temporary columns
#         merged = merged.drop(columns=["rank_dup", "P/L_dup"])

#     # Save aggregate results
#     output_path = os.path.join(results_dir, "aggregate.csv")
#     merged.to_csv(output_path, index=False)

#     print(f"Aggregate results saved to {output_path}")









def calc_aggregate(results_dir: str):
    """
    Reads three CSV files from results_dir, sums rank and P/L by time_slot,
    and saves the aggregate results to aggregate.csv with headers:
    time_slot, rank, P/L, rank_10

    - 'rank' is the summed rank values from the three files
    - 'P/L' is the summed P/L values from the three files
    - 'rank_10' is the top 10 ranking based on summed P/L values
    """

    # Expected filenames
    files = [
        "oo_log_parsed_A_6m.csv",
        "oo_log_parsed_B_3m.csv",
        "oo_log_parsed_C_1m.csv"
    ]

    # Build full paths and check existence
    paths = [os.path.join(results_dir, f) for f in files]
    for p in paths:
        if not os.path.exists(p):
            print(f"Error: Required file not found: {p}")
            sys.exit(1)

    # Read all three CSVs
    dfs = [pd.read_csv(p) for p in paths]

    # Merge on time_slot
    merged = dfs[0][["time_slot", "rank", "P/L"]].copy()
    for df in dfs[1:]:
        merged = merged.merge(df[["time_slot", "rank", "P/L"]],
                              on="time_slot",
                              suffixes=("", "_dup"))

        # Sum rank and P/L across duplicates
        merged["rank"] = merged["rank"] + merged["rank_dup"]
        merged["P/L"] = merged["P/L"] + merged["P/L_dup"]

        # Drop temporary columns
        merged = merged.drop(columns=["rank_dup", "P/L_dup"])

    # Add rank_10 column based on summed P/L
    merged["rank_10"] = ""
    top10 = merged.nlargest(10, "P/L").sort_values(by="P/L", ascending=False).reset_index(drop=True)
    for idx, row in top10.iterrows():
        merged.loc[merged["time_slot"] == row["time_slot"], "rank_10"] = str(idx + 1)

    # Save aggregate results
    output_path = os.path.join(results_dir, "aggregate.csv")
    merged.to_csv(output_path, index=False)

    print(f"Aggregate results saved to {output_path}")



def get_valid_date():
    while True:
        user_input = input("Enter a date (YYYY-MM-DD), or press Enter for today's date: ").strip()
        
        # Case 1: User pressed Enter → use today's date
        if user_input == "":
            return date.today().strftime("%Y-%m-%d")
        
        # Case 2: User entered something → validate format
        try:
            # Try parsing the input string into a datetime object
            parsed_date = datetime.strptime(user_input, "%Y-%m-%d")
            # If successful, return the string in correct format
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            print("Invalid format. Please enter the date as YYYY-MM-DD (e.g., 2025-11-28).")




# def find_last_date(source_file):
#     """
#     Reads a CSV file and returns the most recent date
#     from the 'Date Opened' column.
    
#     Parameters:
#         source_file (str): Path to the CSV file.
    
#     Returns:
#         str: Most recent 'Date Opened' in YYYY-MM-DD format.
#     """
#     last_date = None

#     row_cnt = 0
    
#     with open(source_file, newline='', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
        
#         for row in reader:
#             row_cnt += 1

#             if row_cnt < 5:
#                 print(f'row_cnt:{row_cnt}, data:\n{row}')

#             date_str = row.get("Date Opened")
#             if date_str:
#                 try:
#                     current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
#                     if last_date is None or current_date > last_date:
#                         last_date = current_date
#                 except ValueError:
#                     if row_cnt < 5:
#                         print(f'row has invalid format, date_str:{date_str}')
#                     # Skip rows with invalid date format
#                     continue

#             else:
#                 if row_cnt < 5:
#                     print(f'date_str type:{type(date_str)}, value:{date_str}')



#     print(f'2 row_cnt:{row_cnt}')
    
#     return last_date.strftime("%Y-%m-%d") if last_date else None



def find_last_date(source_file):
    last_date = None
    with open(source_file, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        # Normalize headers
        reader.fieldnames = [name.strip().strip('"').lstrip('\ufeff') for name in reader.fieldnames]

        for row in reader:
            date_str = row.get("Date Opened")
            if date_str:
                date_str = date_str.strip().strip('"')
                try:
                    current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if last_date is None or current_date > last_date:
                        last_date = current_date
                except ValueError:
                    continue

    return last_date.strftime("%Y-%m-%d") if last_date else None


def find_first_date(source_file):
    first_date = None
    with open(source_file, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        # Normalize headers
        reader.fieldnames = [name.strip().strip('"').lstrip('\ufeff') for name in reader.fieldnames]

        for row in reader:
            date_str = row.get("Date Opened")
            if date_str:
                date_str = date_str.strip().strip('"')
                try:
                    current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if first_date is None or current_date < first_date:
                        first_date = current_date
                except ValueError:
                    continue

    return first_date.strftime("%Y-%m-%d") if first_date else None















directory = "C:/MEIC/oo"
filename = "trade-log.csv"
file_spec = os.path.join(directory, filename)




# users_end_date = get_valid_date()
# print(f'users_end_date type:{type(users_end_date)}, value:{users_end_date}')


# get first and last trade dates as strings
first_trade_date = find_first_date(file_spec)
print(f'first_trade_date type:{type(first_trade_date)}, value:{first_trade_date}')
last_trade_date = find_last_date(file_spec)
print(f'last_trade_date type:{type(last_trade_date)}, value:{last_trade_date}')


# Convert date strings to datetime.date objects
first_trade_date_dt = datetime.strptime(first_trade_date, "%Y-%m-%d").date()
last_trade_date_dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
print(f'first_trade_date_dt:{type(first_trade_date_dt)}", value:{first_trade_date_dt}')
print(f'last_trade_date_dt:{type(last_trade_date_dt)}", value:{last_trade_date_dt}')



# Calculate inclusive day count
days_inclusive = (last_trade_date_dt - first_trade_date_dt).days + 1
print("Days inclusive:", days_inclusive)






intialize_summary_directory()

# # Load full dataset
# df = pd.read_csv("trade-log.csv")




df = pd.read_csv(file_spec)

# Convert "Date Opened" to datetime
df["Date Opened"] = pd.to_datetime(df["Date Opened"])

# Today's date
today = datetime.today()
today2 = datetime.today().date()

# Full dataset
out_filename = f"oo_log_parsed_A_6m.csv"
out_file_spec = os.path.join(results_dir, out_filename)
# process_summary(df.copy(), "oo_log_parsed_A_6m.csv")
process_summary(df.copy(), out_file_spec)

# Last 3 months
out_filename = f"oo_log_parsed_B_3m.csv"
out_file_spec = os.path.join(results_dir, out_filename)

# three_months_ago = today - timedelta(days=90)
# df_3m = df[df["Date Opened"] >= three_months_ago]


today_2 = datetime.today().date()
print(f'B today_2 type:{type(today_2)}, value:{today_2}')
# three_months_ago = today_2 - timedelta(days=90)
three_months_ago = last_trade_date_dt - timedelta(days=90)
print(f'three_months_ago type:{type(three_months_ago )}, value:{three_months_ago}')
df["Date Opened"] = pd.to_datetime(df["Date Opened"]).dt.date
df_3m = df[df["Date Opened"] >= three_months_ago]
print(f'df_3m type:{type(df_3m)}, data:\n{df_3m}')


process_summary(df_3m.copy(), out_file_spec)
# process_summary(df.copy(), out_file_spec)





# Last 3 weeks
out_filename = f"oo_log_parsed_C_1m.csv"
out_file_spec = os.path.join(results_dir, out_filename)


# one_month_ago = today - timedelta(days=30)
# df_1m = df[df["Date Opened"] >= one_month_ago]

today_2 = datetime.today().date()
print(f'C today_2 type:{type(today_2)}, value:{today_2}')
# one_month_ago = today_2 - timedelta(days=30)
one_month_ago = last_trade_date_dt - timedelta(days=30)
print(f'one_month_ago type:{type(one_month_ago)}, value:{one_month_ago}')
df["Date Opened"] = pd.to_datetime(df["Date Opened"]).dt.date
df_1m = df[df["Date Opened"] >= one_month_ago]
print(f'df_1m type:{type(df_1m)}, data:\n{df_1m}')





process_summary(df_1m.copy(), out_file_spec)
# process_summary(df.copy(), out_file_spec)


calc_aggregate(results_dir)


