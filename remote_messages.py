
# One-time setup:
# - Install ngrok and run this once in PowerShell:
#       ngrok config add-authtoken <authtoken>
#   note: may need to cd to the directory were ngrok is installed
# - If waitress is not installed:
#       pip install waitress
#
# Each time you want to run this Flask app:
# 1. Open a PowerShell terminal and cd into the directory where remote_messages.py lives.
# 2. Start the Flask app using Waitress:
#       waitress-serve --host=0.0.0.0 --port=5000 remote_messages:app
# 3. Open a second PowerShell terminal and run:
#       ngrok http 5000
#     note: may need to cd to the directory were ngrok is installed
# 4. From iPhone Shortcuts, run MEIC commands or MEIC queries.





from flask import Flask, request
import datetime

app = Flask(__name__)

fake_pl = 234.67
fake_balance = 98765.43
fake_spx = 7482.42

@app.route("/message", methods=["POST", "GET"])
def message_handler():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -----------------------------
    # Handle GET commands from Shortcuts
    # -----------------------------
    if request.method == "GET":
        cmd = request.args.get("cmd", "").lower()

        print(f"[{timestamp}] Received GET command: {cmd}")

        if cmd == "getpl":
            print(f'got getpl request')
            fake_pl_str = str(fake_pl)
            # reply = "getpl_reply"
            reply = fake_pl_str
        elif cmd == "getbalance":
            print(f'got getbalance request')
            fake_balance_str = str(fake_balance)
            # reply = "getbalance_reply"
            reply = fake_balance_str
        elif cmd == "getspx":
            print(f'got getspx request')
            fake_spx_str = str(fake_spx)
            # reply = "getbalance_reply"
            reply = fake_balance_str
        else:
            reply = "345.67"

        return {"status": "ok", "reply": reply}



    # -----------------------------
    # Handle POST messages (your existing workflow)
    # -----------------------------



    # if request.method == "POST":
    #     data = request.json or {}
    #     message = data.get("value1", "")

    #     print(f"[{timestamp}] Received POST message: {message}")

    #     # Optional: write to a log file
    #     with open("ngrok_messages.log", "a") as f:
    #         f.write(f"[{timestamp}] {message}\n")

    #     return {"status": "ok"}
    


    if request.method == "POST":
        data = request.json or {}
        message = data.get("value1", "").lower()   # <-- case‑insensitive now

        print(f"[{timestamp}] Received POST message: {message}")

        # Optional: write to a log file
        with open("ngrok_messages.log", "a") as f:
            f.write(f"[{timestamp}] {message}\n")

            if message == "meicnow":
                print(f'got meicnow command')

            elif message == "exitnow":
                print(f'got exitnow command')

            else:
                print(f'unknown command:{message}')


        return {"status": "ok"}

    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

