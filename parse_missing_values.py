import pandas as pd
from redcap import Project
import numpy as np
from utils import parse_logic
from datetime import datetime as dt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

events = ['intake_arm_1', 'discharge_arm_1', '3month_postdischar_arm_1', '12month_postintake_arm_1']
event_ids = {'intake_arm_1': 42354, 'discharge_arm_1': 42355,
             '3month_postdischar_arm_1': 42356, '12month_postintake_arm_1': 42357}

branch_text_rep = {"next-event-name": {events[i]: events[i+1] for i in range(3)},
                   "previous-event-name": {events[i]: events[i-1] for i in range(1, 4)}}
branch_text_rep["next-event-name"][events[-1]] = ""
branch_text_rep["previous-event-name"][events[0]] = ""

dic = pd.read_csv("KATRParticipantSurvey_DataDictionary_2025-04-06.csv")
insts = ["record_management", "contact_information", "recovery_capital", "substance_use_history",
         "recovery_support_and_history", "overdose_history", "medicationstreatments_prescribed_for_substance_use",
         "correctional_history", "health_history", "life_domain_assessment", "mental_health_history", "demographics",
         "demographics", "barc10", "rmp_questions", "discharge", "programmatic"]
dic.index = dic['Variable / Field Name']
# ignore_insts = ['contact_information', 'monthly_checkin', 'monthly_checkin_2', 'program_completion',
#                 'inventory_management', 'checkin_monitoring',
#                 'recruiting_info', 'monthly_expenditures', 'expense_records', 'gpra']
all_missing = []
results = {}
for event in events:
    results[event] = {}
    missing_records = []
    counts = {}
    data = project.export_records(format_type="df", raw_or_label="raw", events=[event], forms=insts)\
        .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
    cids = list(data.index.get_level_values(0).unique())
    gc = project.export_records(format_type="df", raw_or_label="raw", events=[event], records=cids,
                                           fields=['gpra_complete']).drop(['redcap_repeat_instrument',
                                                                           'redcap_repeat_instance'], axis=1)
    # tdata = project.export_records(format_type="df", raw_or_label="raw",
    #                                events=["reporting_arm_1"], fields=['terminated'], records=cids)\
    #     .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
    data['gpra_complete'] = gc['gpra_complete']
    data = data.fillna("")
    all_fields = data.columns
    counts = {}
    for i, field in enumerate(all_fields):
        multi = False
        ofield = field
        if "___" in field:
            multi = True
            field = field.split("___")[0]
            if field in counts.keys():
                continue
            cols = [x for x in all_fields if x[:len(field)] == field]
            cols = [x for x in cols if x[-4:] != "_oth" and x[-2:] != "_o"]
        if field[-4:] == "_ref" or field[-2:] == "_r":
            continue
        if "complete" in field:
            continue
        inst = dic['Form Name'][field]
        if inst == "discharge" and event != "discharge_arm_1":
            continue
        elif inst == "demographics" and event != "intake_arm_1":
            continue
        if not multi:
            data[field] = data[field].astype(str)
        # Consolidate different entries for NA
            idx = data[field].str.match("^[n N]\/?[a A]$")
            data.loc[idx, field] = ""   # exclude any additional NA values despite format
        # ignore fields that are extraneous indicators of refusal
        branch = dic['Branching Logic (Show field only if...)'][field]
        if pd.isna(branch) or branch == "":
            if not multi:
                missing = data[field] == ""
            else:
                nsels = data[cols].astype(int).sum(axis=1)
                missing = nsels == 0
        else:
            # replace branching logic with explicit event names
            branch = branch.replace("first-event-name", events[0])
            branch = branch.replace("previous-event-name", branch_text_rep["previous-event-name"][event])
            branch = branch.replace("next-event-name", branch_text_rep["next-event-name"][event])
            branch = branch.replace("last-event-name", events[-1])

            idx = parse_logic(data, branch, event)
            if not multi:
                missing = np.logical_and(idx, data[field] == "")
            else:
                nsels = data[cols].sum(axis=1)
                missing = nsels == 0
                missing = np.logical_and(idx, nsels == 0)
        missing = np.logical_and(missing, data['gpra_complete'] == 2)
        counts[field] = missing.sum()
        results[event][field] = missing[missing].index.get_level_values(0)
        if not multi:
            missing_records.append(data[field][missing])
        else:

            missing_records.append(data[ofield][missing])
            continue
    all_missing.append(missing_records)
total_missing = sum([sum([len(x) for x in mr]) for mr in all_missing])

cdata = project.export_records(format_type="df", raw_or_label="raw", events=['intake_arm_1'], fields=['cid', 'coord'])\
        .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
cdata.index = cdata.index.get_level_values(0)
out_fields = ["cid", "coord", "event", "instrument", "field", "field_label", "url"]
data_out = pd.DataFrame(columns=out_fields, index=range(total_missing))
data_ct = pd.DataFrame(data=np.zeros(len(all_fields) * len(events), dtype=int), columns=["missing"],
                       index=pd.MultiIndex.from_product([all_fields, events]))
data_ct.index = data_ct.index.set_names(["field", "event"])
st = 0
for event in events:
    for field in results[event].keys():
        missing = results[event][field]
        ct = len(missing)
        data_ct.loc[(field, event), "missing"] = ct
        urls = [f"https://redcap.uky.edu/redcap/redcap_v15.1.2/DataEntry/index.php?pid=18233&id={cid}&" \
                f"event_id={event_ids[event]}&page={dic['Form Name'][field]}&fldfocus={field}#{field}-tr"
                for cid in missing]
        data_out.loc[st:st + ct - 1, 'cid'] = missing
        data_out.loc[st:st + ct - 1, 'coord'] = cdata['coord'][missing].to_numpy()
        data_out.loc[st:st + ct - 1, 'event'] = event
        data_out.loc[st:st + ct - 1, 'field'] = field
        data_out.loc[st:st + ct - 1, 'instrument'] = dic['Form Name'][field]
        data_out.loc[st:st + ct - 1, 'field_label'] = dic['Field Label'][field]
        data_out.loc[st:st + ct - 1, 'url'] = urls
        st += ct

# for (event, emissing) in zip(events, all_missing):
#     ct = len(emissing)
#     data_out.iloc[np.arange(st, st + ct, dtype=int)][out_fields] = emissing[[out_fields]]
#     st += ct

data_ct.to_csv(f"missing_counts{dt.strftime(dt.today(), '%Y%m%d')}.csv")

data_out.to_csv(f"missing_not_hidden_{dt.strftime(dt.today(), '%Y%m%d')}.csv")
# data_out.to_excel(f"missing_not_hidden_{dt.strftime(dt.today(), '%Y%m%d')}.xlsx")
    #
    #
    # fields = np.array(fields)
    # counts = np.zeros(len(fields))
    # for i, field in enumerate(fields):
    #     if field in data.columns:
    #         counts[i] = sum(data[field].isna())
    #     else:
    #         matches = [x for x in data.columns if field in x and "____" not in x]
    #         if len(matches):
    #             count = sum([sum(data[x].isna()) for x in matches]) / len(matches)
    #         else:
    #             print(f"Field {field} has no matches in database")
    #
    # sel = np.where(counts)
    # fields = fields[sel]
    # counts = counts[sel]
    #
    # o = np.argsort(counts)[::-1]
    # df = pd.DataFrame(data={"fields": fields[o], "count": counts[o]})
    #
    # df.to_csv(f"missing_counts_{event}.csv")

# sub = pd.read_csv(f"missing_counts{dt.strftime(dt.today(), '%Y%m%d')}.csv")

doindex = pd.MultiIndex.from_arrays([data_out['field'], data_out['event']])
idx = np.logical_and((data_ct['missing'] > 0).to_numpy(), (data_ct['missing'] < 31).to_numpy())
data = data_ct[idx]
didx = np.array([x in data.index for x in doindex])
data_out[didx].to_csv(f"missing_not_hidden_{dt.strftime(dt.today(), '%Y%m%d')}_sub.csv", index=False)

# sub = pd.read_csv("missing_counts20250401_sub.csv")
# sub.index = pd.MultiIndex.from_arrays([sub['field'], sub['event']])
# dindex = pd.MultiIndex.from_arrays([data_out['field'], data_out['event']])
# didx = [x in sub.index for x in dindex]
# data_out[didx].to_csv(f"missing_not_hidden_{dt.strftime(dt.today(), '%Y%m%d')}_sub.csv", index=False)
