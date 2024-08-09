# This script is for mapping out the demographics data for the REDCap project the the race/ethnicity/gender categories required for the RPPR report and saving the reports.
# Input: KATR REDCap project
# Output: Printed Results
#         Text file "demographics_YYYY_MM_DD.csv"


import numpy as np
from redcap import Project
from datetime import datetime as dt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

# Import demographic data from REDCap
fields = ['cid', 'hisp', 'race', 'gender', 'eth']
events = ['intake_arm_1']
data = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
data.index = data.index.droplevel(1)

# Import discharge status from Program Completion
fields = ["comp_status"]
events = ['reporting_arm_1']
ddata = project.export_records(format_type="df", fields=fields, raw_or_label="raw", events=events)\
    .drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
ddata.index = ddata.index.droplevel(1)

gender_vals = {"Female": 2, "Male": 1, "Transgender (Male to Female)": 3, "Gender Non-conforming": 5, "Unknown/Not Reported": -1}
race_vals = {'American Indian/Alaska Native': [3, 4], 'Asian': [6, 7, 8, 9, 10, 11],
             'Native Hawaiian/Pacific Islander': [12, 13], 'Black/African American': [1], 'White': [2],
             'More than One Race': [],
             'Unknown or Not Reported': ['_1', '_2', '_3']}

# Hispanic
lines = ["Ethnic Groups", "Ethnicity,Female,Male,Unknown"]
data['hisp'].fillna(-1, inplace=True)
hisp_vals = {"Hispanic": 1, "Not Hispanic": 0, "Unknown": -1}
for hisp, val in hisp_vals.items():
    s = hisp
    for gend, gval in gender_vals.items():
        s += f", %i" % np.logical_and(data['hisp'] == val, data['gender'] == gval).sum()
    lines.append(s)
    # print(s)

# Race
race_cols = [x for x in data.columns if 'race___' in x and '____' not in x]
data['race_cts'] = data[race_cols].sum(axis=1)
mtor_idx = data['race_cts'] > 1
none_idx = data['race_cts'] == 0
lines += ["", "Races (total population)", "Race, Female, Male, Transgender (Male to Female), Gender Non-conforming, Unknown/Not Reported"]
for race, vals in race_vals.items():
    if race == 'More than One Race':
        idx = data["race_cts"] > 1
    else:
        idx = np.zeros(len(data))
        cols = [f"race___{val}" for val in vals]
        for col in cols:
            idx = np.logical_or(idx, data[col] == 1)
        idx[mtor_idx] = 0
    if race == "Unknown or Not Reported":
        idx[none_idx] = 1
    s = race
    for gend, gval in gender_vals.items():
        s += f", %i" % np.logical_and(idx, data['gender'] == gval).sum()
    lines.append(s)
    # print(s)


lines += ["", "Races (hispanic population)", "Race, Female, Male, Transgender (Male to Female), Gender Non-conforming, Unknown/Not Reported"]
hdata = data[data["hisp"] == 1]
mtor_idx = hdata['race_cts'] > 1
for race, vals in race_vals.items():
    idx = np.zeros(len(hdata))
    if race == 'More than One Race':
        idx = hdata["race_cts"] > 1
    else:
        cols = [f"race___{val}" for val in vals]
        for col in cols:
            idx = np.logical_or(idx, hdata[col] == 1)
        idx[mtor_idx] = 0
    s = race
    for gend, gval in gender_vals.items():
        s += f", %i" % np.logical_and(idx, hdata['gender'] == gval).sum()
    lines.append(s)

for line in lines:
    print(line)

with open(f"demographics_{dt.strftime(dt.now(), '%Y_%m_%d')}.csv", "w") as f:
    for line in lines:
        f.write(line + "\n")

