# util_byob_parse.py
# On the BYOB website, configure parameters and select all time slots and all days.  
# Set the start data 6 months previous to the current date.
# Run the test and Export the test data.  Copy or move the exported Trades.csv file to C:\MEIC\byob
# Run this program.  
# It provide results time slot by time slot 
#    - with summaries for all days for the full period and for the last 6 weeks
#    - and with day of week summaries for the full period and for the last 6 weeks




import csv
from collections import defaultdict

from datetime import datetime, timedelta
import os


def process_csv_1(file_path):
    summary = defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0})
    last_six_weeks_summary = defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0})
    week_summaries = {
        0: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        1: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        2: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        3: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        4: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
    }
    last_six_weeks_week_summaries = {
        0: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        1: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        2: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        3: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        4: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
    }
    
    today = datetime.today()
    six_weeks_ago = today - timedelta(weeks=6)

    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            entry_datetime = datetime.strptime(row['EntryTime'], '%m/%d/%Y %I:%M:%S %p')
            entry_time = entry_datetime.time()
            profit_loss = float(row['ProfitLoss'])
            outcome = row['Outcome']
            weekday = entry_datetime.weekday()
            
            # Full summary
            summary[entry_time]['profit_loss'] += profit_loss
            summary[entry_time]['total_count'] += 1
            if outcome == 'Expiration':
                summary[entry_time]['win_count'] += 1
                
            # Last six weeks summary
            if entry_datetime >= six_weeks_ago:
                last_six_weeks_summary[entry_time]['profit_loss'] += profit_loss
                last_six_weeks_summary[entry_time]['total_count'] += 1
                if outcome == 'Expiration':
                    last_six_weeks_summary[entry_time]['win_count'] += 1

            # Weekly summaries
            if weekday in week_summaries:
                week_summaries[weekday][entry_time]['profit_loss'] += profit_loss
                week_summaries[weekday][entry_time]['total_count'] += 1
                if outcome == 'Expiration':
                    week_summaries[weekday][entry_time]['win_count'] += 1
                
                # Weekly last six weeks summaries
                if entry_datetime >= six_weeks_ago and weekday in last_six_weeks_week_summaries:
                    last_six_weeks_week_summaries[weekday][entry_time]['profit_loss'] += profit_loss
                    last_six_weeks_week_summaries[weekday][entry_time]['total_count'] += 1
                    if outcome == 'Expiration':
                        last_six_weeks_week_summaries[weekday][entry_time]['win_count'] += 1

    return summary, last_six_weeks_summary, week_summaries, last_six_weeks_week_summaries



def process_csv(file_path):
    summary = defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0})
    last_six_weeks_summary = defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0})
    last_three_weeks_summary = defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0})
    week_summaries = {
        0: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        1: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        2: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        3: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        4: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
    }
    last_six_weeks_week_summaries = {
        0: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        1: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        2: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        3: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        4: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
    }
    last_three_weeks_week_summaries = {
        0: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        1: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        2: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        3: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
        4: defaultdict(lambda: {'profit_loss': 0.0, 'win_count': 0, 'total_count': 0}),
    }

    today = datetime.today()
    six_weeks_ago = today - timedelta(weeks=6)
    three_weeks_ago = today - timedelta(weeks=3)

    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            entry_datetime = datetime.strptime(row['EntryTime'], '%m/%d/%Y %I:%M:%S %p')
            entry_time = entry_datetime.time()
            profit_loss = float(row['ProfitLoss'])
            outcome = row['Outcome']
            weekday = entry_datetime.weekday()

            # Full summary
            summary[entry_time]['profit_loss'] += profit_loss
            summary[entry_time]['total_count'] += 1
            if outcome == 'Expiration':
                summary[entry_time]['win_count'] += 1

            # Last six weeks summary
            if entry_datetime >= six_weeks_ago:
                last_six_weeks_summary[entry_time]['profit_loss'] += profit_loss
                last_six_weeks_summary[entry_time]['total_count'] += 1
                if outcome == 'Expiration':
                    last_six_weeks_summary[entry_time]['win_count'] += 1

            # Last three weeks summary
            if entry_datetime >= three_weeks_ago:
                last_three_weeks_summary[entry_time]['profit_loss'] += profit_loss
                last_three_weeks_summary[entry_time]['total_count'] += 1
                if outcome == 'Expiration':
                    last_three_weeks_summary[entry_time]['win_count'] += 1

            # Weekly summaries
            if weekday in week_summaries:
                week_summaries[weekday][entry_time]['profit_loss'] += profit_loss
                week_summaries[weekday][entry_time]['total_count'] += 1
                if outcome == 'Expiration':
                    week_summaries[weekday][entry_time]['win_count'] += 1

                # Weekly last six weeks summaries
                if entry_datetime >= six_weeks_ago and weekday in last_six_weeks_week_summaries:
                    last_six_weeks_week_summaries[weekday][entry_time]['profit_loss'] += profit_loss
                    last_six_weeks_week_summaries[weekday][entry_time]['total_count'] += 1
                    if outcome == 'Expiration':
                        last_six_weeks_week_summaries[weekday][entry_time]['win_count'] += 1

                # Weekly last three weeks summaries
                if entry_datetime >= three_weeks_ago and weekday in last_three_weeks_week_summaries:
                    last_three_weeks_week_summaries[weekday][entry_time]['profit_loss'] += profit_loss
                    last_three_weeks_week_summaries[weekday][entry_time]['total_count'] += 1
                    if outcome == 'Expiration':
                        last_three_weeks_week_summaries[weekday][entry_time]['win_count'] += 1

    return summary, last_six_weeks_summary, last_three_weeks_summary, week_summaries, last_six_weeks_week_summaries, last_three_weeks_week_summaries

def get_top_10_rank(summary):
    profit_list = [(entry_time, stats['profit_loss']) for entry_time, stats in summary.items()]
    sorted_profit_list = sorted(profit_list, key=lambda x: x[1], reverse=True)
    top_10_profit = sorted_profit_list[:10]
    top_10_rank = {entry_time: rank + 1 for rank, (entry_time, _) in enumerate(top_10_profit)}
    return top_10_rank


ROOT_DIR = "C:\\MEIC\\byob"
summary_file = None


def append_to_summary(line):

    # print(f'writing <{line}> to summary file')

    with open(summary_file, "a") as file:  # Open file in append mode
        file.write(line + "\n")



def print_summary(summary, title, top_10_rank):
    print(title)
    append_to_summary(title)
    for entry_time, stats in sorted(summary.items()):
        formatted_entry_time = entry_time.strftime('%H:%M:%S')
        total_profit_loss = stats['profit_loss']
        win_count = stats['win_count']
        total_count = stats['total_count']
        win_percentage = (win_count / total_count) * 100 if total_count > 0 else 0
        rank = top_10_rank.get(entry_time, 0)
        str_rank = str(rank)
        if rank == 0:
            str_rank = ""
        # print(f'rank type:{type(str_rank)}, value:{str_rank}')

        out_str = f"{formatted_entry_time}, ${(total_profit_loss * 100):.2f}, {win_percentage:.0f}%, {total_count},  {str_rank}"
        print(out_str)
        append_to_summary(out_str)






      


def print_summary_new(summary, title, top_10_rank):
    # Get today's date in yyyy-mm-dd format
    # today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Create the subdirectory if it doesn’t exist
    summary_dir = os.path.join(ROOT_DIR, today_str)
    os.makedirs(summary_dir, exist_ok=True)
    
    # Define the file path
    summary_file = os.path.join(summary_dir, "summary.csv")

    # Print title
    print(title)

    with open(summary_file, "a") as file:  # Open file in append mode
        for entry_time, stats in sorted(summary.items()):
            formatted_entry_time = entry_time.strftime('%H:%M:%S')
            total_profit_loss = stats['profit_loss']
            win_count = stats['win_count']
            total_count = stats['total_count']
            win_percentage = (win_count / total_count) * 100 if total_count > 0 else 0
            rank = top_10_rank.get(entry_time, 0)
            str_rank = str(rank) if rank != 0 else ""

            # Format output string
            out_str = f"{formatted_entry_time}, {total_profit_loss:.2f}, {win_percentage:.0f}%, {total_count}, {str_rank}"
            
            # Print to console
            print(out_str)
            
            # Append to file
            file.write(out_str + "\n")




def intialize_summary_directory():
    global summary_file

    # Get today's date in YYYY-MM-DD format
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Construct the subdirectory path
    sub_dir = os.path.join(ROOT_DIR, today_str)
    
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




if __name__ == "__main__":
    # file_path = 'BYOB_sample1.csv'
    file_path = r'C:\MEIC\byob\Trades.csv'

    intialize_summary_directory()

    # full_summary, last_six_weeks_summary, week_summaries, last_six_weeks_week_summaries = process_csv(file_path)
    full_summary, last_six_weeks_summary, last_three_weeks_summary, week_summaries, last_six_weeks_week_summaries, last_three_weeks_week_summaries = process_csv(file_path)
    
    full_top_10_rank = get_top_10_rank(full_summary)
    last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_summary)
    last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_summary)

    mondays_top_10_rank = get_top_10_rank(week_summaries[0])
    mondays_last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_week_summaries[0])
    mondays_last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_week_summaries[0])

    tuesdays_top_10_rank = get_top_10_rank(week_summaries[1])
    tuesdays_last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_week_summaries[1])
    tuesdays_last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_week_summaries[1])

    wednesdays_top_10_rank = get_top_10_rank(week_summaries[2])
    wednesdays_last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_week_summaries[2])
    wednesdays_last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_week_summaries[2])

    thursdays_top_10_rank = get_top_10_rank(week_summaries[3])
    thursdays_last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_week_summaries[3])
    thursdays_last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_week_summaries[3])

    fridays_top_10_rank = get_top_10_rank(week_summaries[4])
    fridays_last_six_weeks_top_10_rank = get_top_10_rank(last_six_weeks_week_summaries[4])
    fridays_last_three_weeks_top_10_rank = get_top_10_rank(last_three_weeks_week_summaries[4])
    
    print()
    out_str = f'entry time,P&L,win %,number of trades, top ten'
    print(out_str)
    append_to_summary(out_str)
    print()
    print_summary(full_summary, "All days of the week. Full date range", full_top_10_rank)
    print()
    print_summary(last_six_weeks_summary, "All days of the week. Last six weeks", last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_summary, "All days of the week. Last three weeks", last_three_weeks_top_10_rank)

    print()
    print_summary(week_summaries[0], "Mondays. Full date range", mondays_top_10_rank)
    print()
    print_summary(last_six_weeks_week_summaries[0], "Mondays. Last six weeks", mondays_last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_week_summaries[0], "Mondays. Last three weeks", mondays_last_three_weeks_top_10_rank)

    print()
    print_summary(week_summaries[1], "Tuesdays. Full date range", tuesdays_top_10_rank)
    print()
    print_summary(last_six_weeks_week_summaries[1], "Tuesdays. Last six weeks", tuesdays_last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_week_summaries[1], "Tuesdays. Last three weeks", tuesdays_last_three_weeks_top_10_rank)

    print()
    print_summary(week_summaries[2], "Wednesdays. Full date range", wednesdays_top_10_rank)
    print()
    print_summary(last_six_weeks_week_summaries[2], "Wednesdays. Last six weeks", wednesdays_last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_week_summaries[2], "Wednesdays. Last three weeks", wednesdays_last_three_weeks_top_10_rank)

    print()
    print_summary(week_summaries[3], "Thursdays. Full date range", thursdays_top_10_rank)
    print()
    print_summary(last_six_weeks_week_summaries[3], "Thursdays. Last six weeks", thursdays_last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_week_summaries[3], "Thursdays. Last three weeks", thursdays_last_three_weeks_top_10_rank)

    print()
    print_summary(week_summaries[4], "Fridays. Full date range", fridays_top_10_rank)
    print()
    print_summary(last_six_weeks_week_summaries[4], "Fridays. Last six weeks", fridays_last_six_weeks_top_10_rank)
    print()
    print_summary(last_three_weeks_week_summaries[4], "Fridays. Last three weeks", fridays_last_three_weeks_top_10_rank)




