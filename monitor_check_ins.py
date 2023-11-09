from redcap import Project
from datetime import datetime as dt
from datetime import timedelta as td
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta as rdelt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
gp_days = 7                         # Check-in grace period

fields = ['idate', 'chkdate']
events = ['intake_arm_1', 'monthy_checkin_arm_1']
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)
ids = data.index.get_level_values(0).unique()

event_name = 'reporting_arm_1'
index = pd.MultiIndex.from_product([ids, [event_name]], names=['cid', 'redcap_event_name'])
columns = ["dintake", "msi", "msi_grace", "chktot", "misschk"]
values = [[""] * len(columns)] * len(ids)
out = pd.DataFrame(data=values, index=index, columns=columns)

for cid in ids:
    intake = data['idate'][cid]['intake_arm_1']
    if type(intake) != str or intake == "":
        ValueError(f"Problem with client id {cid} intake: {intake}")
        out = out.drop(cid, level=0)
        continue
    intake = dt.strptime(intake, date_format)
    today = dt.today()
    msi = ((today.year - intake.year) * 12) + (today.month - intake.month)
    if today.day < intake.day:      # only count if the same day if it has reached same day of the month
        msi -= 1
    check_deadline = intake + rdelt(months=msi, days=gp_days)
    if today.day >= intake.day:
        if today <= check_deadline:
            msi_grace = msi - 1
        else:
            msi_grace = msi
    else:
        msi_grace = msi
    try:    # If no monthly checkins, this will throw a KeyError
        checks = data.loc[cid].loc["monthy_checkin_arm_1"]
        if len(checks.shape) == 2:
            chktot = len(data.loc[cid].loc["monthy_checkin_arm_1"])
        else:
            chktot = 1
    except KeyError:
        chktot = 0
    misschk = max(msi_grace - chktot, 0)

    out.loc[(cid, 'reporting_arm_1'), 'dintake'] = dt.strftime(intake, date_format)
    out.loc[(cid, 'reporting_arm_1'), 'msi'] = str(msi)
    out.loc[(cid, 'reporting_arm_1'), 'msi_grace'] = str(msi_grace)
    out.loc[(cid, 'reporting_arm_1'), 'chktot'] = str(chktot)
    out.loc[(cid, 'reporting_arm_1'), 'misschk'] = str(msi_grace - chktot)

project.import_records(to_import=out.reset_index(), import_format="df")
