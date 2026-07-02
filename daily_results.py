import subprocess
from dotenv import load_dotenv
import os
import smtplib
from email.message import EmailMessage
from email.utils import make_msgid
import mimetypes
from datetime import date
import mri_schwab_lib



def load_env_variables():


    load_dotenv()  # load environment variables from .env file

    my_gmail_user = os.getenv('GMAIL_USER')
    my_gmail_passcode = os.getenv('GMAIL_APP_PASSCODE')

    print(f'1 my_gmail_user: {my_gmail_user}')
    print(f'1 my_gmail_passcode: {my_gmail_passcode}')

    return my_gmail_user, my_gmail_passcode



def all():

    mri_schwab_lib.prep_genlogs_dirs()


    display_all = ""

    gmail_user, gmail_passcode = load_env_variables()

    # print(f'2 gmail_user: {gmail_user}')
    disp_str = f'2 gmail_user: {gmail_user}'
    print(disp_str)
    display_all = display_all + disp_str + "\n"

    # print(f'2 gmail_passcode: {gmail_passcode}')
    disp_str = f'2 gmail_passcode: {gmail_passcode}'
    print(disp_str)
    display_all = display_all + disp_str + "\n"





    result_getLogs = subprocess.run(
        ["python", "getLogs.py"],
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    # print("getLogs output:", result_getLogs.stdout)
    disp_str = f'getLogs output: {result_getLogs.stdout}'
    print(disp_str)
    display_all = display_all + disp_str + "\n"


    if len(result_getLogs.stderr) > 0:
        # print("getLogs STDERR:", result_getLogs.stderr)
        disp_str = f'getLogs STDERR: {result_getLogs.stderr}'
        print(disp_str)
        display_all = display_all + disp_str + "\n"



    # result_genLogs = subprocess.run(
    #     ["node", "genLogs.js"],
    #     capture_output=True,
    #     text=True
    # )

    # result_genLogs = subprocess.run(
    #     ["node", "genLogs.js"],
    #     capture_output=True,
    #     text=True,
    #     encoding="utf-8"
    # )


    result_genLogs = subprocess.run(
    ["node", "genLogs.js"],
    cwd=r"C:\Users\mri17\Documents\repos\schwab-stream",   # <-- FIX
    capture_output=True,
    text=True,
    encoding="utf-8"
    )


    # print("genLogs output:", result_genLogs.stdout, flush=True)
    disp_str = f'genLogs output: {result_genLogs.stdout}'
    print(disp_str)
    display_all = display_all + disp_str + "\n"


    if len(result_genLogs.stderr) > 0:
        # print("genLogs STDERR:", result_genLogs.stderr, flush=True)
        disp_str = f'350682 genLogs STDERR: {result_genLogs.stderr}'
        print(disp_str)
        display_all = display_all + disp_str + "\n"

    # print("end of genLogs output\n", flush=True)
    disp_str = f'end of genLogs output\n'
    print(disp_str)
    display_all = display_all + disp_str + "\n"




    # # Save STDOUT to a file
    # with open("test_result_std.txt", "w", encoding="utf-8") as f:
    #     f.write(result.stdout)




    receiver_email = ["mri1700@gmail.com"]
    # receiver_email = ["mri1700@gmail.com", "rudy.isaacson@gmail.com", "scottike@gmail.com"]



    # today = date.today()
    # subject_str = f"MarkBot MEIC results for {today.strftime('%Y-%m-%d')}"


    # # Create the email
    # msg = EmailMessage()
    # # msg.set_content("Daily results test")
    # msg.set_content(result_genLogs.stdout)
    # msg['Subject'] = subject_str
    # msg['From'] = gmail_user
    # msg['To'] = receiver_email

    # try:
    #     # Send the email using Gmail's SMTP server
    #     with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    #         smtp.login(gmail_user, gmail_passcode)
    #         smtp.send_message(msg)
    #     print("Email sent successfully!")
    # except Exception as e:
    #     print(f"Failed to send email: {e}")



    today = date.today()
    subject_str = f"MarkBot MEIC results for {today.strftime('%Y-%m-%d')}"

    gauge_path = "balances_gauge.png"
    png_path = os.path.join(os.getcwd(), "balances_gauge.png")

    msg = EmailMessage()

    # 1. Set headers FIRST
    msg['Subject'] = subject_str
    msg['From'] = gmail_user
    msg['To'] = ", ".join(receiver_email)

    # 2. Set plain-text body BEFORE message becomes multipart
    # msg.set_content("Your email client does not support HTML.")
    msg.set_content("")

    # 3. Build HTML body
    body_html = f"<pre>{result_genLogs.stdout}</pre>"

    # 4. Add HTML alternative (message becomes multipart/alternative)
    msg.add_alternative(body_html, subtype="html")

    # 5. If image exists, embed it inline
    if os.path.exists(png_path):
        cid = make_msgid()

        # Add image reference to the HTML part
        # html_with_image = body_html + f"<br><br><img src='cid:{cid[1:-1]}' alt='Gauge Image'>"
        html_with_image = (
            body_html
            + f"<br><br><img src='cid:{cid[1:-1]}' alt='Gauge Image' width='300'>"
        )

        # Replace the HTML alternative with the updated version
        msg.get_payload()[1].set_content(html_with_image, subtype="html")

        # Attach the image as related content
        with open(png_path, "rb") as img:
            img_data = img.read()
            maintype, subtype = mimetypes.guess_type(png_path)[0].split('/')
            msg.get_payload()[1].add_related(img_data, maintype=maintype, subtype=subtype, cid=cid)

    else:
        print("balances_gauge.png not found — sending email without image.")

    # 6. Send email
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(gmail_user, gmail_passcode)
            smtp.send_message(msg)
        print("3701 Email sent successfully!")
    except Exception as e:
        print(f"3702 Failed to send email: {e}")

    return display_all

    # End of all()


# spx_candle = mri_schwab_lib.get_spx_today_ohlc()
# # print(f'33050 spx_day_ohlc type:{type(spx_candle)}, data:\n{spx_candle}')
# mri_schwab_lib.prep_genlogs_dirs()
# mri_schwab_lib.persist_spx_candle(spx_candle)
# time_str = "09:14:49"
# mri_schwab_lib.persist_early_indicator(time_str)
# display = all()




