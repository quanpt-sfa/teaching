import sys
import datetime

def log(msg, file=None):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out = f"[{timestamp}] {msg}"
    print(out)
    if file:
        with open(file, 'a', encoding='utf8') as f:
            print(out, file=f)
