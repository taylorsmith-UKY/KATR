import pandas as pd
from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt
import numpy as np
import json
from utils import build_checkbox_cols as bcc

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

with open("linkages_08162023.json", 'r') as f:
    link = json.load(f)

# Import inventory management data from REDCap
fields_inv = ['cardmail', 'clicont', 'cloadint', 'cloaddis', 'cload3mo', 'cload12m']
inv_data = project.export_records(format_type="df", fields=fields_inv, raw_or_label="raw", events=["reporting_arm_1"])\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
inv_data.index = inv_data.index.get_level_values(0)

# Import interview data from REDCap
fields = ['idate', 'name', 'phone', 'coord', 'addr', 'dob', 'addr', 'disch_st', 'cty', 'age', 'gender', 'email',
          'race', 'eth', 'almeds', 'opmeds', 'stints', 'tomeds', 'plnserv', 'bel_pop', 'preppmin', 'bneeds_b',
          'bneeds_v', 'sl_v', 'sobliv_b', 'sobliv_b_mo1_np', 'sobliv_b_mo2_np', 'sobliv_b_mo3_np', 'sobliv_b_mo4_np',
          'sobliv_b_mo5_np', 'sobliv_b_mo1_p', 'sobliv_b_mo2_p', 'sobliv_b_mo3_p', 'sobliv_b_mo4_p', 'sobliv_b_mo5_p',
          'trans_b', 'trans_v', 'trans_t', 'employ_b', 'employ_v', 'employ_t', 'gpra_complete',
          'sobliv_sp', 'trans_sp', 'employ_b_3', 'bneeds_sp']

events = ['intake_arm_1', "discharge_arm_1", "3month_postdischar_arm_1", "12month_postintake_arm_1"]
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

# Import monthly check-in data (for months since last contact)
chdata = project.export_records(format_type="df", fields=["chkdate"], events=["monthy_checkin_arm_1"])

# Generate list of column names for multiple-selection fields
bel_pop_cols = bcc("bel_pop", data.columns)
bel_pop_2_cols = [x.replace("bel_pop", "bel_pop_2") for x in bel_pop_cols]

race_cols = bcc("race", data.columns)
race_2_cols = [x.replace("race", "race_2") for x in bel_pop_cols]

eth_cols = bcc("eth", data.columns)
eth_2_cols = [x.replace("eth", "eth_2") for x in bel_pop_cols]

diag_cols = {'alcohol': bcc("almeds", data.columns)[-1],
             'opioids': bcc("opmeds", data.columns)[-1],
             'stimulants': bcc("stints", data.columns)[-1],
             'tobacco': bcc("tomeds", data.columns)[-1]}

sobliv_cols = ['sobliv_b_mo1', 'sobliv_b_mo2', 'sobliv_b_mo3', 'sobliv_b_mo4', 'sobliv_b_mo5']
sobliv_cols_np = [x + "_np" for x in sobliv_cols]
sobliv_cols_p = [x + "_p" for x in sobliv_cols]

# Added for new reporting field indicating which SUD diagnoses are selected
sud_cols = [f"sud_diag___{i+1}" for i in range(4)]

# New dataframe components for new report values
ids = data.index.get_level_values(0).unique()
nids = len(ids)
columns = ['dcoord', 'dname', 'comp_status', 'first_idate', 'disch_idate', 'mo3post', 'addr2',
           'last_idate', 'next_idate', 'days_since_last', 'days_to_next', 'needsend', 'needload',
           'dphone', 'dob_inv', 'bslncomp', 'last_int', 'serv_reg', 'gender_2'] + \
           bel_pop_2_cols + race_2_cols + eth_2_cols + sud_cols + sobliv_cols + \
          ['bneeds_b_2', 'bneeds_v_2', 'sobliv_b_2', 'sl_v_2', 'trans_b_2', 'trans_v_2', 'trans_t_2',
           'employ_b_2', 'employ_v_2', 'employ_t_2', 'age_2', 'mo_last_cont',
           'comp_int', 'comp_dis', 'comp_3mo', 'comp_12mo']

index = pd.MultiIndex.from_product([ids, [out_event]], names=['cid', 'redcap_event_name'])
new = pd.DataFrame(columns=columns, index=index)
new[bel_pop_2_cols] = new[bel_pop_2_cols].fillna(0).astype("int64")
new[race_2_cols] = new[race_2_cols].fillna(0).astype("int64")
new[eth_2_cols] = new[eth_2_cols].fillna(0).astype("int64")
new[sud_cols] = new[sud_cols].fillna(0).astype("int64")
new['sobliv_b_2'] = np.zeros(len(new), dtype=int)
new['age_2'] = np.zeros(len(new), dtype=int)

data[bel_pop_cols] = data[bel_pop_cols].fillna(0).astype("int64")
data[race_cols] = data[race_cols].fillna(0).astype("int64")
data[eth_cols] = data[eth_cols].fillna(0).astype("int64")
data[sobliv_cols_p] = data[sobliv_cols_p].fillna(0).astype("int64")
data[sobliv_cols_np] = data[sobliv_cols_np].fillna(0).astype("int64")
data["sobliv_b"] = data["sobliv_b"].fillna(0).astype("int64")
data["bneeds_b"] = data["bneeds_b"].fillna(0).astype("int64")
data["trans_b"] = data["trans_b"].fillna(0).astype("int64")
data["employ_b"] = data["employ_b"].fillna(0).astype("int64")
data["gender"] = data["gender"].fillna(-1).astype("int64")
data["age"] = data["age"].fillna(-1).astype("int64")



for uid in ids:
    # Exclude clients with no valid intake data
    if data['idate'][uid][events[0]] == "" or type(data['idate'][uid][events[0]]) != str:
        print(f"Client {uid} not being processed due to missing intake date.")
        new = new.drop(uid, level=0)
        continue

    client = data.loc[uid]                                  # Extract individual client data

    intake = client['idate'][events[0]]                     # For use calculating next interview deadline
    intake_dt = dt.strptime(intake, date_format)

    # Determine SUD substances
    if client.loc[events[0], diag_cols["alcohol"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___1'] = 1
    if client.loc[events[0], diag_cols["opioids"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___2'] = 1
    if client.loc[events[0], diag_cols["stimulants"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___3'] = 1

    # Copy priority population, race, and ethnicity columns for reporting event
    new.loc[(uid, out_event), bel_pop_2_cols] = client.loc[events[0], bel_pop_cols]
    new.loc[(uid, out_event), race_2_cols] = client.loc[events[0], race_cols]
    new.loc[(uid, out_event), eth_2_cols] = client.loc[events[0], eth_cols]

    # Determine intake interview completion and associated values for reporting purposes
    comp_stat = 0
    needsend = 0
    needload = 0
    idate_int = ""
    idate_dis = ""
    idate_3mo = ""
    idate_12mo = ""

    # 12-MO FOLLOW-UP
    if events[3] in client.index and client['gpra_complete'][events[3]] == 2:
        comp_12mo = 1
        last_event = events[3]
        idate_12mo = dt.strptime(client['idate'][last_event], date_format)
        last_idate = idate_12mo
        dnext = ""
        ndnext = ""
        comp_stat = 4
        if inv_data.loc[uid, "cload12m"] == "" or type(inv_data.loc[uid, "cload12m"]) == float:
            needload = 1
    else:
        comp_12mo = 0
        idate_12mo = ""

    # 3MO FOLLOW-UP
    if events[2] in client.index and client['gpra_complete'][events[2]] == 2:
        comp_3mo = 1
        idate_3mo = dt.strptime(client['idate'][events[2]], date_format)
        if inv_data.loc[uid, "cload3mo"] == "" or type(inv_data.loc[uid, "cload3mo"]) == float:
            needload = 1
        if comp_stat > 0:
            last_event = events[2]
            last_idate = idate_3mo
            dnext = intake_dt + rdelt(months=12)
            ndnext = (dnext - dt.today()).days
            comp_stat = 3
    else:
        comp_3mo = 0
        idate_3mo = ""

    # DISCHARGE
    if events[1] in client.index and client['gpra_complete'][events[1]] == 2:
        comp_dis = 1
        if inv_data.loc[uid, "cloaddis"] == "" or type(inv_data.loc[uid, "cloaddis"]) == float:
            needload = 1
        # if client['disch_st'][events[1]] == 2:
        #     comp_stat = 10
        # elif client['disch_st'][events[1]] == 3:
        #     comp_stat = 11
        if comp_stat > 0:
            comp_stat = 2
            idate_dis = dt.strptime(client['idate'][events[1]], date_format)
            last_event = events[1]
            last_idate = idate_dis
            dnext = intake_dt + rdelt(months=6)
            ndnext = (dnext - dt.today()).days

    else:
        comp_dis = 0
        idate_dis = ""

    # INTAKE
    if events[0] in client.index and client['gpra_complete'][events[0]] == 2:
        comp_int = 1
        idate_int = dt.strptime(client['idate'][events[0]], date_format)
        if comp_stat == 0:
            last_event = events[0]
            last_idate = idate_int
            dnext = last_idate + rdelt(months=6)
            ndnext = (dnext - dt.today())
            comp_stat = 1
            if uid not in inv_data.index.values or inv_data.loc[uid, "cardmail"] == "" or \
                    type(inv_data.loc[uid, "cardmail"]) == float:
                needsend = 1
                needload = 1
            elif type(inv_data.loc[uid, "clicont"]) == float or inv_data.loc[uid, "clicont"] == "" or \
                    type(inv_data.loc[uid, "cloadint"]) == float or inv_data.loc[uid, "cloadint"] == "":
                needload = 1
    else:
        comp_int = 0
        idate_int = ""

    # Termination status
    terminated = 0
    if events[1] in client.index:
        if client['disch_st'][events[1]] == 10:     # Coordinator marked discharge status as "Terminated"
            if comp_stat < 2:                       # If later interviews are completed, ignore disch_st
                terminated = 1

    # If never received any funds, terminate them from study
    spent_cols = ['bneeds_sp', 'sobliv_sp', 'trans_sp', 'employ_b_3', 'bneeds_sp']
    if comp_dis and client.loc[events[1]][spent_cols].sum() == 0:
        terminated = 1

    # Determine the number of days since last contact, including monthly check-ins
    ndlast = (dt.today() - last_idate).days
    lcont = last_idate
    if uid in chdata.index.get_level_values(0):
        for tdate in chdata['chkdate'][uid]:
            lcont = max(lcont, dt.strptime(tdate, date_format))

    # Months since last check-in
    mslc = (dt.today().year - lcont.year) * 12 + dt.today().month - lcont.month

    # Service region (serv_reg)
    serv_reg = 0
    for reg in link['region']['groups']:
        if client[link['region']['fieldname']][last_event] in link['region']['groups'][reg]['matches']:
            serv_reg = link['region']['groups'][reg]['value']

    # Sober living budget (select values based on whether pregnant/post-partum or has minors, for recruiting info)
    # Consolidate values so that the same field can express the correct quantity whether pregnant or not
    sobliv = []
    for i in range(1, 6):
        if data.loc[(uid, events[0]), 'preppmin'] == 1:
            try:
                sobliv.append(int(client.loc[events[0], f'sobliv_b_mo{i}_p']))
            except ValueError:
                sobliv.append(0)
        else:
            try:
                sobliv.append(int(client.loc[events[0], f'sobliv_b_mo{i}_np']))
            except ValueError:
                sobliv.append(0)

    # Updates for Program Completion and Inventory Management Instruments in REDCap
    new.loc[(uid, out_event), 'comp_status'] = comp_stat
    new.loc[(uid, out_event), 'last_idate'] = dt.strftime(last_idate, date_format)
    new.loc[(uid, out_event), 'serv_reg'] = serv_reg                                # Program Completion
    new.loc[(uid, out_event), 'mo_last_cont'] = mslc                                # Program Completion

    new.loc[(uid, out_event), 'idate_int'] = idate_int  # Program Completion
    new.loc[(uid, out_event), 'idate_dis'] = idate_dis  # Program Completion
    new.loc[(uid, out_event), 'idate_3mo'] = idate_3mo  # Program Completion
    new.loc[(uid, out_event), 'idate_12mo'] = idate_12mo  # Program Completion

    # Interviews completed
    new.loc[(uid, out_event), 'comp_int'] = idate_int  # Program Completion
    new.loc[(uid, out_event), 'comp_dis'] = idate_dis  # Program Completion
    new.loc[(uid, out_event), 'comp_3mo'] = idate_3mo  # Program Completion
    new.loc[(uid, out_event), 'comp_12mo'] = idate_12mo  # Program Completion
    new.loc[(uid, out_event), 'terminated'] = terminated  # Program Completion


    new.loc[(uid, out_event), 'sobliv_b_mo1'] = sobliv[0]                           # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo2'] = sobliv[1]                           # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo3'] = sobliv[2]                           # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo4'] = sobliv[3]                           # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo5'] = sobliv[4]                           # Recruitment Info
    try:
        new.loc[(uid, out_event), 'next_idate'] = dt.strftime(dnext, date_format)
        new.loc[(uid, out_event), 'days_to_next'] = ndnext
    except TypeError:
        new.loc[(uid, out_event), 'next_idate'] = ""
        new.loc[(uid, out_event), 'days_to_next'] = ""
    new.loc[(uid, out_event), 'days_since_last'] = ndlast

    new.loc[(uid, out_event), 'needsend'] = needsend
    new.loc[(uid, out_event), 'needload'] = needload

    # Copy values to corresponding instruments for convenience when viewing in REDCap
    new.loc[(uid, out_event), 'dname'] = client['name']['intake_arm_1']             # Program Completion
    new.loc[(uid, out_event), 'dcoord'] = client['coord']['intake_arm_1']           # Program Completion
    new.loc[(uid, out_event), 'first_idate'] = intake                               # Program Completion

    new.loc[(uid, out_event), 'rname'] = client['name']['intake_arm_1']             # Inventory Management
    new.loc[(uid, out_event), 'dphone'] = client['phone'][last_event]               # Inventory Management
    new.loc[(uid, out_event), 'addr2'] = client['addr'][last_event]                 # Inventory Management

    # Copy
    new.loc[(uid, out_event), 'demail'] = client['email'][last_event]               # Recruitment Info
    new.loc[(uid, out_event), 'age_2'] = client['age'][events[0]]                   # Recruitment Info
    new.loc[(uid, out_event), 'bneeds_b_2'] = client['bneeds_b'][events[0]]         # Recruitment Info
    new.loc[(uid, out_event), 'bneeds_v_2'] = client['bneeds_v'][events[0]]
    new.loc[(uid, out_event), 'sobliv_b_2'] = client['sobliv_b'][events[0]]         # Recruitment Info
    new.loc[(uid, out_event), 'sl_v_2'] = client['sl_v'][events[0]]                 # Recruitment Info
    new.loc[(uid, out_event), 'trans_b_2'] = client['trans_b'][events[0]]           # Recruitment Info
    new.loc[(uid, out_event), 'trans_v_2'] = client['trans_v'][events[0]]           # Recruitment Info
    new.loc[(uid, out_event), 'trans_t_2'] = client['trans_t'][events[0]]           # Recruitment Info
    new.loc[(uid, out_event), 'employ_b_2'] = client['employ_b'][events[0]]         # Recruitment Info
    new.loc[(uid, out_event), 'employ_v_2'] = client['employ_v'][events[0]]         # Recruitment Info
    new.loc[(uid, out_event), 'employ_t_2'] = client['employ_t'][events[0]]         # Recruitment Info
    new.loc[(uid, out_event), 'gender_2'] = client['gender'][events[0]]             # Recruitment Info

# new.to_csv("updated.csv")
# project.import_records(to_import=new.reset_index(), import_format="df", date_format="YMD")
