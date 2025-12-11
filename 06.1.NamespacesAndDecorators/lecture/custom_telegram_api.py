import json
import requests
import time
import sys
import os

from dotenv import load_dotenv  # will not work on your machine unless downloaded by pip

load_dotenv()

BOT_SECRET = os.getenv("BOT_SECRET")
CHAT_ID = os.getenv("CHAT_ID")
# pyrefly: ignore  # unsupported-operation
LINK = "https://api.telegram.org/bot" + BOT_SECRET + "/sendMessage"

MAX_SENDING_ATTEMPTS = 5

def send(message="Sample text."):
    message_data = {"chat_id": CHAT_ID, "text": message}

    is_sended = False
    attempts = 0
    while not is_sended and attempts < MAX_SENDING_ATTEMPTS:
        try:
            r1 = requests.get(LINK, params=message_data)
            is_sended = json.loads(r1.text)["ok"]
        except Exception:
            pass
        if not is_sended:
            time.sleep(2**attempts)
            attempts += 1
    if not is_sended:
        sys.stderr.write("Can't send:\n" + message)
