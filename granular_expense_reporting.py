import pandas as pd
from redcap import Project
import numpy as np
import itertools

# REDCap project parameters
api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)

# How many months past intake to look
serv_window_months = 6
import_individual_transactions = True
category_values = {"Basic Needs": 1, "Transportation": 2, "Employment Support": 3, "Auto Repair": 4}

# Expense categories and their corresponding field names
categories = {"Basic Needs": "bneeds", "Transportation": "trans", "Employment Support": "emp", "Auto Repair": "car"}
columns = [[f"{cat}_mo1", f"{cat}_mo2", f"{cat}_mo3", f"{cat}_mo4", f"{cat}_mo5", f"{cat}_mo6", f"{cat}_tot"]
           for cat in categories.values()]
columns = list(itertools.chain.from_iterable(columns))

# Load intake interview dates
intake = project.export_records(format_type="df", raw_or_label="raw", events=["intake_arm_1"], fields=["idate"])\
        .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
intake = pd.to_datetime(intake['idate'])
intake.index = intake.index.get_level_values(0)


regions = ["EKY", "LOU", "NKY"]
gt6mo = []
out = []
records = []
for region in regions:
    # Load granular expense spreadsheet
    gran = pd.read_excel("granular_expense_reporting.xlsx", sheet_name=region)
    gran['Date'] = pd.to_datetime(gran['Date'])                         # Convert dates to datetime object
    gran = gran.replace("Bus Pass", "Transportation")                   # Consolidate transportation categories for EKY
    gran = gran.replace("Transportation (bus pass)", "Transportation")
    gran = gran.replace("Auto Repair", "Transportation")
    cids = gran['Client ID'].unique()

    # Output data for the current region
    cdata = pd.DataFrame(data=np.zeros([len(cids), len(categories) * serv_window_months + len(categories)]),
                         columns=columns, index=cids)
    cdata.index.name = "cid"

    for i in range(len(gran)):
        cid, tdate, tot, cat, vend = gran.iloc[i]
        idate = intake[cid].to_pydatetime()                                         # intake date
        tdate = tdate.to_pydatetime()                                               # transaction date
        nmos = ((tdate.year - idate.year) * 12) + (tdate.month - idate.month)       # Months since intake
        if tdate.day < idate.day and nmos > 0:
            nmos -= 1
        # Flag any rows where date is outside of service window
        if 0 <= nmos:#  < serv_window_months:
            # Add to corresponding monthly total
            cdata[f"{categories[cat]}_mo{min(6, nmos+1)}"][cid] += tot
        else:
            # Preserve record for error correction
            gt6mo += [(region, cid, idate.strftime("%Y-%m-%d"), tdate.strftime("%Y-%m-%d"), nmos + 1, tot, cat, vend)]
        if import_individual_transactions:
            records.append((cid, tdate, nmos, tot, cat, vend))
    for cat in categories.values():
        cdata[f"{cat}_tot"] = cdata[[f"{cat}_mo1", f"{cat}_mo2", f"{cat}_mo3", f"{cat}_mo4", f"{cat}_mo5", f"{cat}_mo6"]].sum(axis=1)
    #     cdata[cat]['tot'] = cdata[cat][["mo1", "mo2", "mo3", "mo4", "mo5", "mo6"]].sum(axis=1)
    # Save region data to individual spreadsheet
    cdata.to_csv(f"client_totals_{region}.csv")
    # Save results for subsequent REDCap import
    out.append(cdata)

out = pd.concat(out)
out.to_csv("monthly_totals.csv")

out['redcap_event_name'] = ["reporting_arm_1"] * len(out)                               # event name is required field
project.import_records(to_import=out.reset_index(names='cid'), import_format="df")

gt6mo = pd.DataFrame.from_records(gt6mo,
                                  columns=["region", "cid", "intake", "pay_date",
                                           "mo_of_service", "total", "category", "vendor"])
gt6mo.to_csv(f"pay_gt6mo.csv")

if import_individual_transactions:
    out = []
    for row in records:
        cid, tdate, nmos, tot, cat, vend = row
        instance = 1
        if len(out) > 0 and out[-1][0] == cid:
            instance = out[-1][1] + 1
        out.append((cid, instance, tdate, nmos, tot, category_values[cat], vend))

    out = pd.DataFrame.from_records(out, columns=["cid", "redcap_repeat_instance", "rdate", "nmos", "rtot", "rcat", "rvend"])
    out['redcap_repeat_instrument'] = ["expense_records"] * len(out)
    out['redcap_event_name'] = ["reporting_arm_1"] * len(out)
    out.to_csv("imported_transactions.csv")
    # project.import_records(to_import=out, import_format="df")
