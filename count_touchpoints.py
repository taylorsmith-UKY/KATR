import pandas as pd
from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt
import json
from utils import get_tsd

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

with open("linkages_08162023.json", 'r') as f:
    link = json.load(f)

fields = ['comp_status', 'first_idate', 'dcoord']
coords = ["dmhu241", "rcbr241", "ssgr233"]

coords = {"": "All Coordinators", "dmhu241": "Donna Hurst", "rcbr241": "Rachel Bradford", "ssgr233": "Samantha Grothaus"}

data = project.export_records(format_type="df", fields=fields,
                               raw_or_label="raw", events=["reporting_arm_1"])

adata = data.copy

for coord in coords.keys():
    if coord == "":
        adata = data.copy()
    else:
        adata = data[data['dcoord'] == coord]
    adata.index = adata.index.get_level_values(0)
    uids = adata.index

    dsd, msd, msd_grace = get_tsd(adata['first_idate'])
    adata.loc[:, 'msd'] = msd

    skip_term = True
    out = pd.DataFrame(columns=["ints", "chks"], dtype=int, index=uids)
    for cid in uids:
        if skip_term is True and adata['comp_status'][cid] == 10:
            continue
        out['chks'][cid] = max(12 - adata['msd'][cid], 0)
        if adata['comp_status'][cid] < 10:
            out['ints'][cid] = 4 - adata['comp_status'][cid]

    ints = int(out['ints'].sum())
    chks = int(out['chks'].sum())
    print(coords[coord])
    print(f"Total Interviews: {ints}\tTotal Check-ins: {chks}\tTotal Touchpoints: {ints + chks}")
    # out.to_csv(f"touchpoints_{dt.strftime(dt.today(), date_format)}.csv")
    #
    # skip_term = False
    # out = pd.DataFrame(index=uids, columns=["ints", "chks"], dtype=int)
    # for cid in uids:
    #     if skip_term is True and adata['comp_status'][cid] == 10:
    #         continue
    #     out['chks'][cid] = max(12 - adata['msd'][cid], 0)
    #     if adata['comp_status'][cid] < 10:
    #         out['ints'][cid] = 4 - adata['comp_status'][cid]
    #
    # ints = out['ints'].sum()
    # chks = out['chks'].sum()
    # print(f"Total Interviews: {ints}\tTotal Check-ins: {chks}\tTotal Touchpoints: {ints + chks}")
