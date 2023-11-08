from redcap import Project
from datetime import datetime as dt
from datetime import timedelta as td
import numpy as np
import pandas as pd

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

fields = ['idate', 'chkdate']
events = ['intake_arm_1', 'monthy_checkin_arm_1']
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)
data = data.reset_index()
ids = data['cid'].unique()

out = []

for cid in ids:
    out.append({'cid': str(cid), 'redcap_event_name': 'reporting_arm_1',
                'dintake': '', 'msi': '', 'chktot': '', 'misschk': ''})
    intake = data[np.logical_and(data['cid'] == cid, data['redcap_event_name'] == 'intake_arm_1')]['idate'].values[0]
    if type(intake) != str or intake == "":
        out = out[:-1]
        ValueError(f"Problem with client id {cid} intake: {intake}")
        continue
    out[-1]['dintake'] = intake
    intake = dt.strptime(intake, date_format)
    today = dt.today()
    msi = ((today.year - intake.year) * 12) + (today.month - intake.month)
    if today.day < intake.day:
        msi -= 1
    check_deadline = dt.strptime(f"{intake.year + (intake.month + msi) // 12}-{(intake.month + msi) % 12}-{intake.day}",
                                 date_format) + td(days=7)
    if today <= check_deadline:
        target_checks = msi - 1
    else:
        target_checks = msi
    chktot = len(data[np.logical_and(data['cid'] == cid, data['redcap_event_name'] == 'monthy_checkin_arm_1')])
    misschk = msi - chktot
    out[-1]['msi'] = str(msi)
    out[-1]['chktot'] = str(chktot)
    out[-1]['misschk'] = str(target_checks - chktot)

df = pd.DataFrame.from_records(out)

project.import_records(to_import=out, import_format="json")