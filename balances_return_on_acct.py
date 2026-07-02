# import csv
# from datetime import datetime

# CSV_PATH = r"C:\MEIC\account\gain\account_daily_balance.csv"

# def parse_number(s):
#     """Convert '94,613.70' → 94613.70"""
#     return float(s.replace(",", "").strip())

# def main():
#     records = []

#     # Read CSV
#     with open(CSV_PATH, "r", encoding="utf-8") as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             date = datetime.strptime(row["DATE"], "%m/%d/%Y")
#             adj = parse_number(row["ADJUSTED"])
#             records.append((date, adj))

#     # Sort by date
#     records.sort(key=lambda x: x[0])

#     # --- YTD RETURN ---
#     start_balance = records[0][1]
#     end_balance = records[-1][1]
#     ytd_return = (end_balance - start_balance) / start_balance * 100

#     print("========== YTD RETURN ==========")
#     print(f"Start Balance: {start_balance:,.2f}")
#     print(f"End Balance:   {end_balance:,.2f}")
#     print(f"YTD Return:    {ytd_return:.2f}%\n")

#     # --- MONTHLY RETURNS ---
#     monthly_first = {}
#     monthly_last = {}

#     for date, bal in records:
#         key = (date.year, date.month)
#         if key not in monthly_first:
#             monthly_first[key] = bal
#         monthly_last[key] = bal

#     print("========== MONTHLY RETURNS ==========")
#     for (year, month) in sorted(monthly_first.keys()):
#         start = monthly_first[(year, month)]
#         end = monthly_last[(year, month)]
#         ret = (end - start) / start * 100
#         month_name = datetime(year, month, 1).strftime("%B")
#         print(f"{month_name} {year}: {ret:.2f}%")
#     print()

#     # --- LAST DAY RETURN ---
#     if len(records) >= 2:
#         prev_day_bal = records[-2][1]
#         last_day_bal = records[-1][1]
#         last_day_ret = (last_day_bal - prev_day_bal) / prev_day_bal * 100

#         print("========== LAST DAY RETURN ==========")
#         print(f"Previous Day Balance: {prev_day_bal:,.2f}")
#         print(f"Last Day Balance:     {last_day_bal:,.2f}")
#         print(f"Last Day Return:      {last_day_ret:.2f}%")
#     else:
#         print("Not enough data to compute last-day return.")

# if __name__ == "__main__":
#     main()




import csv
from datetime import datetime

CSV_PATH = r"C:\MEIC\account\gain\account_daily_balance.csv"

def parse_number(s):
    """Convert '94,613.70' → 94613.70"""
    return float(s.replace(",", "").strip())

def main():
    records = []

    # Read CSV
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = datetime.strptime(row["DATE"], "%m/%d/%Y")
            # date = datetime.strptime(row["DATE"], "%m/%d/%y")
            adj = parse_number(row["ADJUSTED"])
            records.append((date, adj))

    # Sort by date
    records.sort(key=lambda x: x[0])

    # --- YTD RETURN ---
    start_balance = records[0][1]
    end_balance = records[-1][1]
    ytd_return = (end_balance - start_balance) / start_balance * 100

    print("========== YTD RETURN ==========")
    print(f"Start Balance: {start_balance:,.2f}")
    print(f"End Balance:   {end_balance:,.2f}")
    print(f"YTD Return:    {ytd_return:.2f}%\n")

    # --- MONTHLY RETURNS ---
    monthly_first = {}
    monthly_last = {}

    for date, bal in records:
        key = (date.year, date.month)
        if key not in monthly_first:
            monthly_first[key] = bal
        monthly_last[key] = bal

    print("========== MONTHLY RETURNS ==========")

    # Build list of (label, return_value)
    monthly_results = []
    for (year, month) in sorted(monthly_first.keys()):
        start = monthly_first[(year, month)]
        end = monthly_last[(year, month)]
        ret = (end - start) / start * 100
        month_name = datetime(year, month, 1).strftime("%B")
        label = f"{month_name} {year}"
        monthly_results.append((label, ret))

    # Determine padding for alignment
    max_label_len = max(len(label) for label, _ in monthly_results)
    max_ret_len = max(len(f"{ret:.2f}%") for _, ret in monthly_results)

    # Print aligned output
    for label, ret in monthly_results:
        label_str = label.rjust(max_label_len)
        ret_str = f"{ret:.2f}%".rjust(max_ret_len)
        print(f"{label_str}: {ret_str}")
    print()

    # --- LAST DAY RETURN ---
    if len(records) >= 2:
        prev_day_bal = records[-2][1]
        last_day_bal = records[-1][1]
        last_day_ret = (last_day_bal - prev_day_bal) / prev_day_bal * 100

        print("========== LAST DAY RETURN ==========")
        print(f"Previous Day Balance: {prev_day_bal:,.2f}")
        print(f"Last Day Balance:     {last_day_bal:,.2f}")
        print(f"Last Day Return:      {last_day_ret:.2f}%")
    else:
        print("Not enough data to compute last-day return.")

if __name__ == "__main__":
    main()    