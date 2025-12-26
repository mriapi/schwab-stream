import subprocess
from dotenv import load_dotenv
import os
import smtplib
from email.message import EmailMessage
from datetime import date







def load_env_variables():


    load_dotenv()  # load environment variables from .env file

    my_gmail_user = os.getenv('GMAIL_USER')
    my_gmail_passcode = os.getenv('GMAIL_APP_PASSCODE')

    print(f'1 my_gmail_user: {my_gmail_user}')
    print(f'1 my_gmail_passcode: {my_gmail_passcode}')

    return my_gmail_user, my_gmail_passcode


gmail_user, gmail_passcode = load_env_variables()

print(f'2 gmail_user: {gmail_user}')
print(f'2 gmail_passcode: {gmail_passcode}')





result_getLogs = subprocess.run(
    ["python", "getLogs.py"],
    capture_output=True,
    text=True
)

print("getLogs output:", result_getLogs.stdout)
if len(result_getLogs.stderr) > 0:
    print("getLogs STDERR:", result_getLogs.stderr)



result_genLogs = subprocess.run(
    ["node", "genLogs.js"],
    capture_output=True,
    text=True
)

print("genLogs output:", result_genLogs.stdout)
if len(result_genLogs.stderr) > 0:
    print("genLogs STDERR:", result_genLogs.stderr)




# # Save STDOUT to a file
# with open("test_result_std.txt", "w", encoding="utf-8") as f:
#     f.write(result.stdout)


# receiver_email = "mri1700@gmail.com"
receiver_email = ["mri1700@gmail.com", "rudy.isaacson@gmail.com", "scottike@gmail.com"]

today = date.today()
subject_str = f"MarkBot MEIC results for {today.strftime('%Y-%m-%d')}"


# Create the email
msg = EmailMessage()
# msg.set_content("Daily results test")
msg.set_content(result_genLogs.stdout)
msg['Subject'] = subject_str
msg['From'] = gmail_user
msg['To'] = receiver_email

try:
    # Send the email using Gmail's SMTP server
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(gmail_user, gmail_passcode)
        smtp.send_message(msg)
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")




