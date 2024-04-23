import pandas as pd
from redcap import Project
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt
import numpy as np

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

# Import interview data from REDCap
fields = ['idate', 'name', 'phone', 'coord', 'addr', 'dob', 'addr', 'disch_st', 'cty', 'age', 'gender', 'email',
          'race', 'eth', 'almeds', 'opmeds', 'stints', 'tomeds', 'plnserv', 'bel_pop', 'preppmin', 'bneeds_b',
          'bneeds_v', 'sl_v', 'sobliv_b', 'sobliv_b_mo1_np', 'sobliv_b_mo2_np', 'sobliv_b_mo3_np', 'sobliv_b_mo4_np',
          'sobliv_b_mo5_np', 'sobliv_b_mo1_p', 'sobliv_b_mo2_p', 'sobliv_b_mo3_p', 'sobliv_b_mo4_p', 'sobliv_b_mo5_p',
          'trans_b', 'trans_v', 'trans_t', 'employ_b', 'employ_v', 'employ_t']
events = ['intake_arm_1', "discharge_arm_1", "3month_postdischar_arm_1", "12month_postintake_arm_1"]
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

# Inventory management data
fields_inv = ['cardmail', 'clicont', 'cloadint', 'cloaddis', 'cload3mo', 'cload12m']
inv_data = project.export_records(format_type="df", fields=fields_inv, raw_or_label="raw", events=["reporting_arm_1"])\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
inv_data.index = inv_data.index.get_level_values(0)

# Generate list of column names for multiple-selection fields
plnserv_cols = [x for x in data.columns if "plnserv___" in x and "____" not in x]
plnserv_2_cols = ["plnserv_2".join(x.split("plnserv")) for x in plnserv_cols]

bel_pop_cols = [x for x in data.columns if "bel_pop___" in x and "____" not in x]
bel_pop_2_cols = ["bel_pop_2".join(x.split("bel_pop")) for x in bel_pop_cols]

race_cols = [x for x in data.columns if "race___" in x and "____" not in x]
race_2_cols = ["race_2".join(x.split("race")) for x in race_cols]

eth_cols = [x for x in data.columns if "eth___" in x and "____" not in x]
eth_2_cols = ["eth_2".join(x.split("eth")) for x in eth_cols]

diag_cols = {'alcohol': [x for x in data.columns if "almeds___" in x and "____" not in x][-1],
             'opioids': [x for x in data.columns if "opmeds___" in x and "____" not in x][-1],
             'stimulants': [x for x in data.columns if "stints___" in x and "____" not in x][-1],
             'tobacco': [x for x in data.columns if "tomeds___" in x and "____" not in x][-1]}

sobliv_cols = ['sobliv_b_mo1', 'sobliv_b_mo2', 'sobliv_b_mo3', 'sobliv_b_mo4', 'sobliv_b_mo5']

sud_cols = [f"sud_diag___{i+1}" for i in range(4)]
# New dataframe components for new report values
ids = data.index.get_level_values(0).unique()
nids = len(ids)
columns = ['dcoord', 'dname', 'comp_status', 'first_idate', 'disch_idate', 'mo3post', 'addr2',
           'last_idate', 'next_idate', 'days_since_last', 'days_to_next', 'needsend', 'needload',
           'dphone', 'dob_inv', 'bslncomp', 'last_int', 'serv_reg', 'gender_2'] + \
           bel_pop_2_cols + race_2_cols + eth_2_cols + sud_cols + sobliv_cols + \
          ['bneeds_b_2', 'bneeds_v_2', 'sobliv_b_2', 'sl_v_2', 'trans_b_2', 'trans_v_2', 'trans_t_2',
           'employ_b_2', 'employ_v_2', 'employ_t_2', 'age_2']

index = pd.MultiIndex.from_product([ids, [out_event]], names=['cid', 'redcap_event_name'])
values = [[-1] * len(columns)] * nids
new = pd.DataFrame(data=values, columns=columns, index=index)
# for col in plnserv_2_cols:
#     new[col] = np.zeros(len(new), dtype=int)
for col in bel_pop_2_cols:
    new[col] = np.zeros(len(new), dtype=int)
for col in race_2_cols:
    new[col] = np.zeros(len(new), dtype=int)
for col in eth_2_cols:
    new[col] = np.zeros(len(new), dtype=int)
for col in sud_cols:
    new[col] = np.zeros(len(new), dtype=int)
new['sobliv_b_2'] = np.zeros(len(new), dtype=int)
new['age_2'] = np.zeros(len(new), dtype=int)

for uid in ids:
    # Exclude clients with no valid intake data
    if data['idate'][uid][events[0]] == "" or type(data['idate'][uid][events[0]]) != str:
        print(f"Client {uid} not being processed due to missing intake date.")
        new = new.drop(uid, level=0)
        continue

    intake = data['idate'][uid]['intake_arm_1']     # For use calculating next interview deadline
    intake_dt = dt.strptime(intake, date_format)
    comp_stat = 0
    for event in data.loc[uid].index:
        if type(data['idate'][uid][event]) == str and data['idate'][uid][event] != "":
            comp_stat += 1
    last_event = events[comp_stat - 1]

    last_idate = dt.strptime(data['idate'][uid][last_event], date_format)
    ndlast = (dt.today() - last_idate).days

    if data.loc[(uid, events[0]), diag_cols["alcohol"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___1'] = 1
    if data.loc[(uid, events[0]), diag_cols["opioids"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___2'] = 1
    if data.loc[(uid, events[0]), diag_cols["stimulants"]] == 0:
        new.loc[(uid, out_event), 'sud_diag___3'] = 1

    # for i in range(len(plnserv_cols)):
    #     if data.loc[(uid, events[0]), plnserv_cols[i]] == 1:
    #         new.loc[(uid, out_event), plnserv_2_cols[i]] = 1
    for i in range(len(bel_pop_cols)):
        if data.loc[(uid, events[0]), bel_pop_cols[i]] == 1:
            new.loc[(uid, out_event), bel_pop_2_cols[i]] = 1
    for i in range(len(race_cols)):
        if data.loc[(uid, events[0]), race_cols[i]] == 1:
            new.loc[(uid, out_event), race_2_cols[i]] = 1
    for i in range(len(eth_cols)):
        if data.loc[(uid, events[0]), eth_cols[i]] == 1:
            new.loc[(uid, out_event), eth_2_cols[i]] = 1

    needsend = 0
    needload = 0
    serv_reg = 0
    if data['cty'][uid][last_event] in [5, 8, 12, 13, 14, 16, 18, 19, 22]:
        serv_reg = 1
    elif data['cty'][uid][last_event] in [2, 9, 10, 15, 20, 21]:
        serv_reg = 2
    elif data['cty'][uid][last_event] in [1, 3, 4, 6, 7, 11, 17]:
        serv_reg = 3

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
        dnext = intake_dt + rdelt(months=6)
        ndnext = (dnext - dt.today()).days
        if inv_data.loc[uid, "cloaddis"] == "" or type(inv_data.loc[uid, "cloaddis"]) == float:
            needload = 1
        if data['disch_st'][uid][events[1]] == 2:
            comp_stat = 10
    elif comp_stat == 3:                            # Completed 3-month Post Discharge Follow-up
        dnext = intake_dt + rdelt(months=12)
        ndnext = (dnext - dt.today()).days
        if inv_data.loc[uid, "cload3mo"] == "" or type(inv_data.loc[uid, "cload3mo"]) == float:
            needload = 1
    else:                            # Completed 12-month Post Intake Follow-up
        dnext = ""
        ndnext = ""
        if inv_data.loc[uid, "cload12m"] == "" or type(inv_data.loc[uid, "cload12m"]) == float:
            needload = 1

    # Sober living budget
    sobliv = []
    for i in range(1, 6):
        if data.loc[(uid, events[0]), 'preppmin'] == 1:
            try:
                sobliv.append(int(data.loc[(uid, events[0]), f'sobliv_b_mo{i}_p']))
            except ValueError:
                sobliv.append(0)
        else:
            try:
                sobliv.append(int(data.loc[(uid, events[0]), f'sobliv_b_mo{i}_np']))
            except ValueError:
                sobliv.append(0)

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
    new.loc[(uid, out_event), 'first_idate'] = intake                               # Program Completion
    new.loc[(uid, out_event), 'serv_reg'] = serv_reg                                # Program Completion

    new.loc[(uid, out_event), 'rname'] = data['name'][uid]['intake_arm_1']           # Inventory Management
    new.loc[(uid, out_event), 'dphone'] = data['phone'][uid][last_event]             # Inventory Management
    new.loc[(uid, out_event), 'addr2'] = data['addr'][uid][last_event]               # Inventory Management

    new.loc[(uid, out_event), 'demail'] = data['email'][uid][last_event]             # Recruitment Info
    try:
        new.loc[(uid, out_event), 'age_2'] = int(data['age'][uid][events[0]])                 # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'age_2'] = ""
    try:
        new.loc[(uid, out_event), 'bneeds_b_2'] = int(data['bneeds_b'][uid][events[0]])  # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'bneeds_b_2'] = 0
    new.loc[(uid, out_event), 'bneeds_v_2'] = data['bneeds_v'][uid][events[0]]
    try:
        new.loc[(uid, out_event), 'sobliv_b_2'] = int(data['sobliv_b'][uid][events[0]])      # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'sobliv_b_2'] = 0
    new.loc[(uid, out_event), 'sl_v_2'] = data['sl_v'][uid][events[0]]        # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo1'] = sobliv[0]                            # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo2'] = sobliv[1]                            # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo3'] = sobliv[2]                            # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo4'] = sobliv[3]                            # Recruitment Info
    new.loc[(uid, out_event), 'sobliv_b_mo5'] = sobliv[4]                            # Recruitment Info
    try:
        new.loc[(uid, out_event), 'trans_b_2'] = int(data['trans_b'][uid][events[0]])    # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'trans_b_2'] = 0
    new.loc[(uid, out_event), 'trans_v_2'] = data['trans_v'][uid][events[0]]    # Recruitment Info
    new.loc[(uid, out_event), 'trans_t_2'] = data['trans_t'][uid][events[0]]    # Recruitment Info
    try:
        new.loc[(uid, out_event), 'employ_b_2'] = int(data['employ_b'][uid][events[0]])  # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'employ_b_2'] = 0
    new.loc[(uid, out_event), 'employ_v_2'] = data['employ_v'][uid][events[0]]  # Recruitment Info
    new.loc[(uid, out_event), 'employ_t_2'] = data['employ_t'][uid][events[0]]  # Recruitment Info
    try:
        new.loc[(uid, out_event), 'gender_2'] = int(data['gender'][uid][events[0]])          # Recruitment Info
    except ValueError:
        new.loc[(uid, out_event), 'gender_2'] = -1

    new.loc[(uid, out_event), 'dob_inv'] = data['dob'][uid]['intake_arm_1']         # Inventory Management
    new.loc[(uid, out_event), 'last_int'] = dt.strftime(last_idate, date_format)    # Inventory Management

project.import_records(to_import=new.reset_index(), import_format="df", date_format="YMD")
