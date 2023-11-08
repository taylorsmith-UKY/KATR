import pandas as pd
from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

# Fields to get from REDCap
fields = ['idate', 'name', 'phone', 'coord']
events = ['intake_arm_1', "discharge_arm_1", "3month_postdischar_arm_1", "12month_postintake_arm_1"]

data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events).drop(
    ['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

# Create new dataframe for new report values
ids = data.index.get_level_values(0).unique()
nids = len(ids)
new = pd.DataFrame(dict(dname=[""]*nids, dphone=[""]*nids, comp_status=[""]*nids, first_idate=[""]*nids,
                        last_idate=[""]*nids, next_idate=[""]*nids, dcoord=[""]*nids,
                        days_since_last=[""]*nids, days_to_next=[""]*nids),
                   index=pd.MultiIndex.from_product([ids, ['reporting_arm_1']], names=['cid', 'redcap_event_name']))

for uid in ids:
    # Exclude records with no valid intake data
    if events[0] not in data.loc[uid].index.values:
        new = new.drop(uid)
        continue
    elif data['idate'][uid][events[0]] == "":
        print(f"Client {uid} not being processed due to missing intake date.")
        new = new.drop(uid)
        continue

    intake = data['idate'][uid]['intake_arm_1']
    dname = data['name'][uid]['intake_arm_1']
    dcoord = data['coord'][uid]['intake_arm_1']

    comp_stat = len(data.loc[uid])
    last_event = events[len(data.loc[uid]) - 1]
    dphone = data['phone'][uid][last_event]

    last_idate = data['idate'][uid][last_event]
    ndlast = (dt.today() - last_idate).days

    if comp_stat == 1:                              # Completed Intake
        dnext = last_idate + rdelt(months=6)
        ndnext = (dnext - dt.today()).days
    elif comp_stat == 2:                            # Completed Discharge
        dnext = last_idate + rdelt(months=3)
        ndnext = (dnext - dt.today()).days
    elif comp_stat == 3:                            # Completed 3-month Post Discharge Follow-up
        dnext = intake + rdelt(months=12)
        ndnext = (dnext - dt.today()).days
    elif comp_stat == 4:                            # Completed 12-month Post Intake Follow-up
        dnext = ""
        ndnext = ""

    new['comp_status'][uid]['report_arm_1'] = comp_stat
    new['dname'][uid]['report_arm_1'] = dname
    new['dphone'][uid]['report_arm_1'] = dphone
    new['first_idate'][uid]['report_arm_1'] = dt.strftime(intake, date_format)
    new['last_idate'][uid]['report_arm_1'] = dt.strftime(last_idate, date_format)
    new['next_idate'][uid]['report_arm_1'] = dt.strftime(dnext, date_format)
    new['days_since_last'][uid]['report_arm_1'] = ndlast
    new['days_to_next'][uid]['report_arm_1'] = ndnext

project.import_records(to_import=new, import_format="df")
