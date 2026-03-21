# import pandas as pd

# # Load the CSV file
# df = pd.read_csv("account.csv", dtype=str)

# # Parse DATE column flexibly
# df["ParsedDate"] = pd.to_datetime(df["DATE"], errors="coerce")

# # Filter only valid BAL rows
# bal_df = df[(df["TYPE"] == "BAL") & (df["ParsedDate"].notna())].copy()

# # Extract just the date portion for grouping
# bal_df["DateOnly"] = bal_df["ParsedDate"].dt.date

# # Keep only the last BAL row per day
# bal_df = bal_df.groupby("DateOnly", as_index=False).tail(1)

# # Sort by date
# bal_df.sort_values(by="ParsedDate", inplace=True)

# # Clean BALANCE column for comparison (remove commas, convert to float)
# bal_df["BALANCE_CLEAN"] = bal_df["BALANCE"].str.replace(",", "").astype(float)

# # Drop rows where BALANCE is same as previous day
# bal_df["BALANCE_SHIFTED"] = bal_df["BALANCE_CLEAN"].shift(1)
# bal_df = bal_df[bal_df["BALANCE_CLEAN"] != bal_df["BALANCE_SHIFTED"]]

# # Keep only DATE and BALANCE columns
# output_df = bal_df[["DATE", "BALANCE"]]

# # Save to CSV
# output_df.to_csv("account_daily_balance.csv", index=False)





import pandas as pd


# def strip_leading_lines(filename="account.csv", lines_to_strip=3):
#     """
#     Removes the first `lines_to_strip` lines from the given CSV file
#     and overwrites the file with the remaining content.
#     """

#     # Read all lines
#     with open(filename, "r", encoding="utf-8") as f:
#         lines = f.readlines()

#     # Skip the leading lines
#     remaining = lines[lines_to_strip:]

#     # Write the remainder back to the same file
#     with open(filename, "w", encoding="utf-8") as f:
#         f.writelines(remaining)

TOS_ACCT_DATA = r"C:\MEIC\account\gain\account.csv"

OUT_FILE = r"C:\MEIC\account\gain\account_daily_balance.csv"




# def strip_leading_lines(filename="account.csv"):
def strip_leading_lines(filename=TOS_ACCT_DATA):
    """
    Removes all leading lines until the first line that begins with 'DATE'.
    Preserves that header line and all subsequent lines.
    Overwrites the original file.
    """

    cleaned_lines = []
    keep = False

    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            # Once we hit the real header, start keeping lines
            if line.startswith("DATE"):
                keep = True
            if keep:
                cleaned_lines.append(line)

    # Write cleaned content back to the same file
    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(cleaned_lines)





# def strip_trailing_lines(filename="account.csv"):
def strip_trailing_lines(filename=TOS_ACCT_DATA):
    """
    Removes the summary line (which starts with ',,,,') and all lines after it.
    Overwrites the original file with the cleaned content.
    """

    cleaned_lines = []

    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            # Stop when we reach the summary line
            if line.startswith(",,,,"):
                break
            cleaned_lines.append(line)

    # Write the cleaned content back to the same file
    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(cleaned_lines)






strip_leading_lines()
strip_trailing_lines()





# Load the CSV file
# df = pd.read_csv("account.csv", dtype=str)
df = pd.read_csv(TOS_ACCT_DATA, dtype=str)


# Parse DATE column with explicit format to avoid warnings
df["ParsedDate"] = pd.to_datetime(df["DATE"], format="%m/%d/%y", errors="coerce")

# Filter only valid BAL rows
bal_df = df[(df["TYPE"] == "BAL") & (df["ParsedDate"].notna())].copy()

# Extract just the date portion for grouping
bal_df["DateOnly"] = bal_df["ParsedDate"].dt.date

# Keep only the last BAL row per day
bal_df = bal_df.groupby("DateOnly", as_index=False).tail(1)

# Sort by date
bal_df.sort_values(by="ParsedDate", inplace=True)

# Clean BALANCE column for comparison (remove commas, convert to float)
bal_df["BALANCE_CLEAN"] = bal_df["BALANCE"].str.replace(",", "").astype(float)

# Drop rows where BALANCE is same as previous day
bal_df["BALANCE_SHIFTED"] = bal_df["BALANCE_CLEAN"].shift(1)
bal_df = bal_df[bal_df["BALANCE_CLEAN"] != bal_df["BALANCE_SHIFTED"]]

# Keep only DATE and BALANCE columns
output_df = bal_df[["DATE", "BALANCE"]]

# Save to CSV
# output_df.to_csv("account_daily_balance.csv", index=False)
output_df.to_csv(OUT_FILE, index=False)

# Optional: print summary
# print("Filtered daily balance saved to account_daily_balance.csv")
print(f'Filtered daily balance saved to {OUT_FILE}')