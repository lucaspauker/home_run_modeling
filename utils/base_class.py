import os
import pandas as pd

class BaseClass:
    def log(self, text, error=False, log=True, verbose=True):
        msg = f"[{pd.Timestamp.now()}] {text}"
        if error:
            msg = f"[{pd.Timestamp.now()}] ERROR: {text}"
        if verbose:
            print(msg)
        if log:
            logfile = os.path.join("logs", pd.Timestamp.now().strftime("%Y%m%d") + ".log")
            with open(logfile, "a") as f:
                f.write(msg + "\n")

