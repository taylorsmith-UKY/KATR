import pandas as pd
from redcap import Project
import numpy as np

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
out_event = 'reporting_arm_1'

# fields = ["disch_st_gpra", "foupst_gpra"]
# dtypes = [str]
events = ['intake_arm_1', 'discharge_arm_1', '3month_postdischar_arm_1', '12month_postintake_arm_1']

dic = pd.read_csv("KATRParticipantSurvey_DataDictionary_2025-02-11.csv")
ignore_insts = ['contact_information', 'monthly_checkin', 'monthly_checkin_2', 'program_completion',
                'inventory_management', 'checkin_monitoring', 'recruiting_info']
for event in events:
    data = project.export_records(format_type="df", raw_or_label="raw", events=[event])\
        .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)

    all_fields = data.columns
    fields = []
    branches = []
    for field in all_fields:
        field = field.split("___")[0]
        if field[-4:] == "_ref" or field[-2:] == "_r" or field[-8:] == "ref_gpra":
            continue
        try:
            idx = np.where(dic['Variable / Field Name'] == field)[0][0]
        except IndexError:
            print(f"Ignoring {field}")
            continue
        if dic['Form Name'][idx] in ignore_insts:
            continue
        branch = dic['Branching Logic (Show field only if...)'][idx]
        if pd.isna(branch):
            if field not in fields:
                branches.append(branch)
                fields.append(field)
        else:
            branch = branch.replace("first-event-name", events[0])
            if f"[event-name] <> \'{event}\'" in branch:                    # remove field based on event exclusion
                continue
            elif f"[event-name] = " in branch and event not in branch:      # remove field based on event inclusion
                continue
            if field not in fields:
                fields.append(field)
                branches.append(branch)

    fields = np.array(fields)
    counts = np.zeros(len(fields))
    for i, field in enumerate(fields):
        if field in data.columns:
            counts[i] = sum(data[field].isna())
        else:
            matches = [x for x in data.columns if field in x and "____" not in x]
            if len(matches):
                count = sum([sum(data[x].isna()) for x in matches]) / len(matches)
            else:
                print(f"Field {field} has no matches in database")

    sel = np.where(counts)
    fields = fields[sel]
    counts = counts[sel]

    o = np.argsort(counts)[::-1]
    df = pd.DataFrame(data={"fields": fields[o], "count": counts[o]})

    df.to_csv(f"missing_counts_{event}.csv")
