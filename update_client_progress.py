import pandas as pd
from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

# Import interview data from REDCap
fields = ['idate', 'name', 'phone', 'coord', 'addr', 'dob', 'addr']
events = ['intake_arm_1', "discharge_arm_1", "3month_postdischar_arm_1", "12month_postintake_arm_1"]
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

# Inventory management data
fields_inv = ['cardmail', 'clicont', 'cloadint', 'cloaddis', 'cload3mo', 'cload12m']
inv_data = project.export_records(format_type="df", fields=fields_inv, raw_or_label="raw", events=["reporting_arm_1"])\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
inv_data.index = inv_data.index.get_level_values(0)

# New dataframe components for new report values
ids = data.index.get_level_values(0).unique()
nids = len(ids)
columns = ['dcoord', 'dname', 'comp_status', 'first_idate', 'disch_idate', 'mo3post', 'addr2'
           'last_idate', 'next_idate', 'days_since_last', 'days_to_next', 'needsend', 'needload',
           'dphone', 'addr2', 'dob_inv', 'bslncomp', 'last_int']
index = pd.MultiIndex.from_product([ids, [out_event]], names=['cid', 'redcap_event_name'])
values = [[""] * len(columns)] * nids
new = pd.DataFrame(data=values, columns=columns, index=index)

for uid in ids:
    # Exclude clients with no valid intake data
    if data['idate'][uid][events[0]] == "" or type(data['idate'][uid][events[0]]) != str:
        print(f"Client {uid} not being processed due to missing intake date.")
        new = new.drop(uid, level=0)
        continue

    intake = data['idate'][uid]['intake_arm_1']     # For use calculating next interview deadline
    comp_stat = 0
    for event in data.loc[uid].index:
        if type(data['idate'][uid][event]) == str and data['idate'][uid][event] != "":
            comp_stat += 1
    last_event = events[comp_stat - 1]

    last_idate = dt.strptime(data['idate'][uid][last_event], date_format)
    ndlast = (dt.today() - last_idate).days

    needsend = 0
    needload = 0
    if comp_stat == 1:                              # Completed Intake
        dnext = last_idate + rdelt(months=6)
        ndnext = (dnext - dt.today()).days
        if uid not in inv_data.index.values or inv_data.loc[uid, "cardmail"] == "" or \
                type(inv_data.loc[uid, "cardmail"]) == float:
            needsend = 1
            needload = 1
        elif type(inv_data.loc[uid, "clicont"]) == float or inv_data.loc[uid, "clicont"] == "" or \
                type(inv_data.loc[uid, "cloadint"]) == float or inv_data.loc[uid, "cloadint"] == "":
            needload = 1

    elif comp_stat == 2:                            # Completed Discharge
        dnext = last_idate + rdelt(months=3)
        ndnext = (dnext - dt.today()).days
        if inv_data.loc[uid, "cloaddis"] == "" or type(inv_data.loc[uid, "cloaddis"]) == float:
            needload = 1
    elif comp_stat == 3:                            # Completed 3-month Post Discharge Follow-up
        dnext = intake + rdelt(months=12)
        ndnext = (dnext - dt.today()).days
        if inv_data.loc[uid, "cload3mo"] == "" or type(inv_data.loc[uid, "cload3mo"]) == float:
            needload = 1
    else:                            # Completed 12-month Post Intake Follow-up
        dnext = ""
        ndnext = ""
        if inv_data.loc[uid, "cload12m"] == "" or type(inv_data.loc[uid, "cload12m"]) == float:
            needload = 1

    # Updates for Program Completion and Inventory Management Instruments in REDCap
    new.loc[(uid, out_event), 'comp_status'] = comp_stat
    new.loc[(uid, out_event), 'last_idate'] = dt.strftime(last_idate, date_format)
    new.loc[(uid, out_event), 'next_idate'] = dt.strftime(dnext, date_format)
    new.loc[(uid, out_event), 'days_since_last'] = ndlast
    new.loc[(uid, out_event), 'days_to_next'] = ndnext
    new.loc[(uid, out_event), 'needsend'] = needsend
    new.loc[(uid, out_event), 'needload'] = needload

    # Copy values to corresponding instruments for convenience when viewing in REDCap
    new.loc[(uid, out_event), 'dname'] = data['name'][uid]['intake_arm_1']          # Program Completion
    new.loc[(uid, out_event), 'dcoord'] = data['coord'][uid]['intake_arm_1']        # Program Completion
    new.loc[(uid, out_event), 'first_idate'] = data['idate'][uid]['intake_arm_1']   # Program Completion

    new.loc[(uid, out_event), 'dphone'] = data['phone'][uid][last_event]            # Inventory Management
    new.loc[(uid, out_event), 'addr2'] = data['addr'][uid][last_event]              # Inventory Management
    new.loc[(uid, out_event), 'dob_inv'] = data['dob'][uid]['intake_arm_1']         # Inventory Management
    new.loc[(uid, out_event), 'last_int'] = dt.strftime(last_idate, date_format)    # Inventory Management

project.import_records(to_import=new.reset_index(), import_format="df", date_format=date_format)
