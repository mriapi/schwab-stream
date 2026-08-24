# import csv
# from collections import defaultdict
# import re

# def normalize_header(h):
#     # Remove BOM
#     h = h.replace("\ufeff", "")
#     # Remove ALL quotes
#     h = h.replace('"', "")
#     # Collapse weird whitespace
#     h = re.sub(r"\s+", " ", h)
#     # Final normalize
#     return h.strip().lower()

# def build_daily_log(input_file="trade-log.csv", output_file="daily-log.csv"):
#     daily_pl = defaultdict(float)

#     with open(input_file, newline='', encoding='utf-8') as f:
#         reader = csv.reader(f)
#         raw_headers = next(reader)

#         headers = [normalize_header(h) for h in raw_headers]

#         # Build DictReader with normalized headers
#         reader = csv.DictReader(f, fieldnames=headers)

#         for row in reader:
#             date = row["date opened"]
#             pl_value = float(row["p/l"])
#             daily_pl[date] += pl_value

#     # Write daily summary
#     with open(output_file, "w", newline='', encoding='utf-8') as f:
#         writer = csv.DictWriter(f, fieldnames=["Date Opened", "P/L"])
#         writer.writeheader()

#         for date, total_pl in sorted(daily_pl.items()):
#             writer.writerow({
#                 "Date Opened": date,
#                 "P/L": round(total_pl, 2)
#             })

# if __name__ == "__main__":
#     build_daily_log()

import csv
import re
from collections import defaultdict

# -----------------------------
# Header Normalization
# -----------------------------
def normalize_header(h):
    h = h.replace("\ufeff", "")      # Remove BOM
    h = h.replace('"', "")           # Remove quotes
    h = re.sub(r"\s+", " ", h)       # Normalize whitespace
    return h.strip().lower()         # Final normalize


# -----------------------------
# Moving Average Helper
# -----------------------------
def compute_moving_average(values, window):
    if len(values) < window:
        return ""  # Not enough data yet
    return round(sum(values[-window:]) / window, 2)


# -----------------------------
# Main Program
# -----------------------------
def build_daily_log(trade_file="trade-log.csv", daily_file="daily-log.csv"):
    daily_pl = defaultdict(float)

    # --- Read trade-log.csv and normalize headers ---
    with open(trade_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        headers = [normalize_header(h) for h in raw_headers]

        reader = csv.DictReader(f, fieldnames=headers)

        for row in reader:
            date = row["date opened"]
            pl_value = float(row["p/l"])
            daily_pl[date] += pl_value

    # --- Write daily-log.csv (first stage) ---
    with open(daily_file, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Date Opened", "P/L"])
        writer.writeheader()

        for date in sorted(daily_pl.keys()):
            writer.writerow({
                "Date Opened": date,
                "P/L": round(daily_pl[date], 2)
            })


def enhance_daily_log(daily_file="daily-log.csv"):
    rows = []

    # --- Read daily-log.csv ---
    with open(daily_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "Date Opened": row["Date Opened"],
                "P/L": float(row["P/L"])
            })

    # --- Compute moving averages ---
    pl_history = []
    enhanced_rows = []

    for row in rows:
        pl_history.append(row["P/L"])

        ma3 = compute_moving_average(pl_history, 3)
        ma7 = compute_moving_average(pl_history, 7)
        ma21 = compute_moving_average(pl_history, 21)

        enhanced_rows.append({
            "Date Opened": row["Date Opened"],
            "P/L": row["P/L"],
            "MA-3": ma3,
            "MA-7": ma7,
            "MA-21": ma21
        })

    # --- Write enhanced daily-log.csv ---
    with open(daily_file, "w", newline='', encoding='utf-8') as f:
        fieldnames = ["Date Opened", "P/L", "MA-3", "MA-7", "MA-21"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in enhanced_rows:
            writer.writerow(row)


# -----------------------------
# Run Both Stages
# -----------------------------
if __name__ == "__main__":
    build_daily_log()      # Parse trade-log.csv → daily-log.csv
    enhance_daily_log()    # Add MA-3, MA-7, MA-21
