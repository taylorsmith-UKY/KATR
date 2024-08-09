# Estimate the total number of payments that will need to be sent out after new_date

from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt
import pandas as pd

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

new_date_str = "2024-08-15"
new_date = dt.strptime(new_date_str, date_format)

# Load data
df = project.export_records(format_type="df", events=["reporting_arm_1"],
                            fields=['dintake', 'last_idate', 'mo_last_cont', 'comp_status', 'next_idate'])
df.index = df.index.get_level_values(0)
df['dintake'] = df['dintake'].astype("datetime64[ns]")
df['last_idate'] = df['last_idate'].astype("datetime64[ns]")
df['next_idate'] = df['next_idate'].astype("datetime64[ns]")
# df['disch_idate'] = df['disch_idate'].astype("datetime64[ns]")

# Remove all clients who have been terminated/administratively discharged
df = df[df['comp_status'] != 10]

# Remove all clients whose intake was > 1 year prior to the new date since they should receive their
# final payment at ~ 1 year
df = df[df['dintake'] > (new_date - rdelt(years=1))]

dates = []
payment_types = []
cids = []

counts = [0, 0, 0, 0]
for uid in df.index:
    pred_disch = df['dintake'][uid] + rdelt(months=6)
    pred_3mo = df['dintake'][uid] + rdelt(months=9)
    pred_12mo = df['dintake'][uid] + rdelt(months=12)
    if df['comp_status'][uid] == 4:                                 # Client has already completed final interview
        continue
    elif df['comp_status'][uid] == 3:                               # Client has already completed 1st follow-up
        if df['next_idate'][uid] > new_date:
            dates.append(df['next_idate'][uid])
            counts[3] += 1
            payment_types.append("12mo Follow-Up")
            cids.append(uid)
    elif df['comp_status'][uid] == 2:                               # Client has already completed discharge
        if df['next_idate'][uid] > new_date:
            counts[2] += 1
            counts[3] += 1
            dates.append(df['next_idate'][uid])
            dates.append(pred_12mo)
            payment_types.append("3mo Follow-Up")
            payment_types.append("12mo Follow-Up")
            cids.append(uid)
            cids.append(uid)
        elif pred_12mo > new_date:
            counts[3] += 1
            dates.append(pred_12mo)
            payment_types.append("12mo Follow-Up")
            cids.append(uid)
    elif df['comp_status'][uid] == 1:                               # Client has only completed intake interview
        if df['next_idate'][uid] > new_date:
            counts[1] += 1
            counts[2] += 1
            counts[3] += 1
            dates.append(df['next_idate'][uid])
            dates.append(pred_3mo)
            dates.append(pred_12mo)
            payment_types.append("Discharge")
            payment_types.append("3mo Follow-Up")
            payment_types.append("12mo Follow-Up")
            cids.append(uid)
            cids.append(uid)
            cids.append(uid)
        elif (df['next_idate'][uid] + rdelt(months=3)) > new_date:
            counts[2] += 1
            counts[3] += 1
            dates.append(pred_3mo)
            dates.append(pred_12mo)
            payment_types.append("3mo Follow-Up")
            payment_types.append("12mo Follow-Up")
            cids.append(uid)
            cids.append(uid)
        elif (df['dintake'][uid] + rdelt(years=1)) > new_date:
            counts[3] += 1
            dates.append(pred_12mo)
            payment_types.append("12mo Follow-Up")
            cids.append(uid)

ddf = pd.DataFrame(data={"Client ID": cids, "Date": dates, "Payment Type": payment_types})
ddf.to_csv("payment_dates_6mo.csv")