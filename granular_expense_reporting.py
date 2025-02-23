import pandas as pd
from redcap import Project
import numpy as np

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)

intake = project.export_records(format_type="df", raw_or_label="raw", events=["intake_arm_1"], fields=["idate"])\
        .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

intake = pd.to_datetime(intake['idate'])
intake.index = intake.index.get_level_values(0)

regions = ["EKY", "NKY"]
for region in regions:
    gran = pd.read_excel("granular_expense_reporting.xlsx", sheet_name=region)
    gran['Date'] = pd.to_datetime(gran['Date'])
    gran = gran.replace("Bus Pass", "Transportation")
    gran = gran.replace("Transportation (bus pass)", "Transportation")

    cids = gran['Client ID'].unique()
    categories = gran['Category'].unique()
    vendors = gran['Notes'].unique()

    cdata = {x: pd.DataFrame(data=np.zeros([len(cids), 7], dtype=int),
                             index=cids,
                             columns=["mo1", "mo2", "mo3", "mo4", "mo5", "mo6", "tot"])
             for x in categories}
    vdata = pd.DataFrame(np.zeros(len(vendors)), index=vendors, columns=["total"])
    cadata = pd.DataFrame(np.zeros(len(categories)), index=categories, columns=["total"])
    for i in range(len(gran)):
        cid, tdate, tot, cat, vend = gran.iloc[i]
        idate = intake[cid].to_pydatetime()
        tdate = tdate.to_pydatetime()
        nmos = ((tdate.year - idate.year) * 12) + (tdate.month - idate.month)
        if tdate.day < idate.day and nmos > 0:
            nmos -= 1
        cdata[cat][f"mo{nmos+1}"][cid] += tot
        vdata['total'][vend] += tot
        cadata['total'][cat] += tot
    for cat in categories:
        cdata[cat]['tot'] = cdata[cat][["mo1", "mo2", "mo3", "mo4", "mo5", "mo6"]].sum(axis=1)

    cdata = pd.concat([cdata[x].add_prefix(x.lower().replace(" ", "_") + "_") for x in categories], axis=1)
    cdata.to_csv(f"client_totals_{region}.csv")
    vdata.to_csv(f"vendor_totals_{region}.csv")
    cadata.to_csv(f"category_totals_{region}.csv")


