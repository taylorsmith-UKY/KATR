from datetime import datetime as dt
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as rdelt


# Get current time since dates (tsd) in days and in months, with or without grace period
def get_tsd(dates: pd.Series, grace_period: int = 7):
    today = dt.today()
    if dates.dtype == 'O':
        dates = dates.astype("datetime64[ns]")
    dsd = (today - dates).dt.days
    msd = np.zeros(len(dates), dtype=int)
    msd_grace = np.zeros(len(dates), dtype=int)
    for i, date in enumerate(dates):
        msd[i] = ((today.year - date.year) * 12) + (today.month - date.month)
        if today.day < date.day and msd[i] > 0:  # only count if the same day if it has reached same day of the month
            msd[i] -= 1
        grace_date = date + rdelt(months=msd[i], days=grace_period)
        if today < grace_date:
            msd_grace[i] = max(msd[i] - 1, 0)
        else:
            msd_grace[i] = msd[i]
    return dsd, msd, msd_grace



