import subprocess

# 1. Сбор данных
subprocess.run([r"C:\Users\danik\wolmar\venv_wolmar\Scripts\python.exe", "append_auctions.py"], check=True)

# 2. Поиск интересных лотов и отправка уведомлений
subprocess.run([r"C:\Users\danik\wolmar\venv_wolmar\Scripts\python.exe", "find_interesting_lots.py"], check=True)