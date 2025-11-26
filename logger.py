import sqlite3
import os
from datetime import datetime

class Logger:
    def __init__(self, module_name):
        self.name = module_name
        os.makedirs("databases", exist_ok=True)
        self.con = sqlite3.connect(f"databases/{self.name}.db")
        self.cur = self.con.cursor()

    def log(self, msg, timestamp = datetime.now()):
        print(f"[{timestamp}][{self.name}] {msg}")
    