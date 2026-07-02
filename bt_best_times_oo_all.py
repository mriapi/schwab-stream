# import pandas as pd

# # Corrected path to your backtest log
# file_path = r"C:\MEIC\Test\trade-log.csv"

# # Load CSV
# df = pd.read_csv(file_path)

# # Column names (adjust if your CSV uses different names)
# time_col = "Time Opened"
# pl_col = "P/L"

# # Convert P/L to numeric (handles commas, stray characters, blanks)
# df[pl_col] = pd.to_numeric(df[pl_col], errors="coerce")

# # Drop rows where P/L is missing
# df = df.dropna(subset=[pl_col])

# # Group by time slot and sum total P/L
# summary = (
#     df.groupby(time_col)[pl_col]
#       .sum()
#       .reset_index()
#       .sort_values(time_col)
# )

# # Display results
# print("\n=== TOTAL P/L BY TIME SLOT ===")
# for _, row in summary.iterrows():
#     print(f"{row[time_col]} , {row[pl_col]:.2f}")

import pandas as pd

# Input file (your original trade log)
input_path = r"C:\MEIC\Test\trade-log.csv"

# Output file (ranked results)
output_path = r"C:\MEIC\Test\trade-ranked.csv"

# Load CSV
df = pd.read_csv(input_path)

# Column names
time_col = "Time Opened"
pl_col = "P/L"

# Convert P/L to numeric
df[pl_col] = pd.to_numeric(df[pl_col], errors="coerce")

# Drop rows with missing P/L
df = df.dropna(subset=[pl_col])

# Group by time slot and sum total P/L
summary = (
    df.groupby(time_col)[pl_col]
      .sum()
      .reset_index()
      .sort_values(pl_col, ascending=False)
)

# Add ranking (1 = highest total P/L)
summary["rank#"] = summary[pl_col].rank(method="dense", ascending=False).astype(int)

# Rename columns to match your required output
summary = summary.rename(columns={
    time_col: "time_slot",
    pl_col: "total P/L"
})

# Sort by rank for clean output
summary = summary.sort_values("rank#")

# Save results to NEW CSV (does NOT overwrite your original file)
summary.to_csv(output_path, index=False)

print("\n=== RESULTS SAVED TO trade-ranked.csv ===")
print(summary)
