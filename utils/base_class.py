import pandas as pd

class BaseClass:
    def log(self, text, error=False):
        if error:
            print(f"[{pd.Timestamp.now()}] ERROR: {text}")
        else:
            print(f"[{pd.Timestamp.now()}] {text}")

