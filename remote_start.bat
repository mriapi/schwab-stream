@echo off
cd C:\path\to\your\project

start cmd /k "waitress-serve --host=0.0.0.0 --port=5000 remote_messages:app"
start cmd /k "ngrok http 5000"
