# Generate a diverse list of candidates for qualitative interviews and report all planned financial
# assistance more clearly (not separate fields for preg/non-preg)

import pandas as pd
from utils import condense_cols

fname = "KATRParticipantSurve-QualitativeInterview_DATA_2024-02-06_1542.csv"

data = pd.read_csv(fname, sep="|")

serv_reg_key = {1: 'Eastern Kentucky',
                2: 'Louisville Area',
                3: 'Northern Kentucky',
                0: 'Unknown'}

gender_key = {1: 'Male',
              2: 'Female',
              3: 'Transgender (Male to Female)',
              4: 'Transgender (Female to Male)',
              5: 'Gender Non-Conforming',
              50: 'Other',
              -1: 'REFUSED'}

race_key = {1: 'Black/African American',
            2: 'White',
            3: 'American Indian',
            4: 'Alaska Native',
            5: 'Asian Indian',
            6: 'Chinese',
            7: 'Filipino',
            8: 'Japanese',
            9: 'Korean',
            10: 'Vietnamese',
            11: 'Other Asian',
            12: 'Native Hawaiian',
            13: 'Guamanian/Chamorro',
            14: 'Samoan',
            15: 'Other Pacific Islander',
            50: 'Other',
            -1: 'Refused'}

eth_key = {1: 'Central American', 2: 'Cuban', 3: 'Dominican', 4: 'Mexican',
           5: 'Puerto Rican', 6: 'South American', 50: 'Other', -1: 'Refused'}

bel_pop_key = {1: 'Veteran',
           2: 'Recently incarcerated',
           3: 'Primary caregiver',
           4: 'Pregnant',
           5: 'Post-partum',
           6: 'Currently using Medications for Opioid Use Disorder (MOUD)',
           7: 'History of felony convictions',
           0: 'None',
           -1: 'Refused',
           -2: 'Not Applicable',
           -3: 'Don\'t Know'}

sud_diag_key = {1: 'Alcohol', 2: 'Opioids', 3: 'Stimulants'}

races = condense_cols(data, legend=race_key, fieldname="race_2", output="labels")
for i in races.index:
    if len(races[i].split(",")) > 1:
        races[i] = "More than one race"

eth = condense_cols(data, legend=eth_key, fieldname="eth_2")

sud_diag = condense_cols(data, legend=sud_diag_key, fieldname='sud_diag')

bel_pop = condense_cols(data, legend=bel_pop_key, fieldname='bel_pop_2')

df = pd.DataFrame(index=range(len(data)), columns=['cid', 'Name', 'Phone', 'Email', 'Service Region', 'Age', 'Gender',
                                                   'Race', 'Ethnicity', 'Priority Populations', 'SUD Diagnosis',
                                                   'Basic Needs Clothing', 'Vendor Name (Basic)',
                                                   "Sober Living Housing", "Vendor Name (Hous)",
                                                   "Month 1", "Month 2", "Month 3", "Month 4", "Month 5"
                                                   "Transportation Assistance", "Vendor Name (Trans)",
                                                   "Transportation Type", "Vocation/Employment Support",
                                                   "Vendor Name (Emp)", "Employment Support Type"], dtype=str)

df['cid'] = data['cid']
df['Name'] = data['rname']
df['Phone'] = data['dphone']
df['Email'] = data['demail']
df['Service Region'] = [serv_reg_key[x] for x in data['serv_reg']]
df['Age'] = data['age_2']
df['Gender'] = [gender_key[x] for x in data['gender_2']] #    gender_key[data['gender_2'][i]]
df['Race'] = races
df['Ethnicity'] = eth
df['Priority Populations'] = bel_pop
df['SUD Diagnosis'] = sud_diag
df['Basic Needs Clothing'] = data['bneeds_b_2']
df['Vendor Name (Basic)'] = data['bneeds_v_2']
df['Sober Living Housing'] = data['sobliv_b_2']
df['Vendor Name (Hous)'] = data['sl_v_2']
df['Month 1'] = data['sobliv_b_mo1']
df['Month 2'] = data['sobliv_b_mo2']
df['Month 3'] = data['sobliv_b_mo3']
df['Month 4'] = data['sobliv_b_mo4']
df['Month 5'] = data['sobliv_b_mo5']
df["Transportation Assistance"] = data['trans_b_2']
df["Vendor Name (Trans)"] = data['trans_v_2']
df["Transportation Type"] = data['trans_t_2']
df["Vocation/Employment Support"] = data['employ_b_2']
df["Vendor Name (Emp)"] = data['employ_v_2']
df["Employment Support Type"] = data['employ_t_2']

df.to_csv("recruitment_list.csv", sep="|")
df.to_excel("reruitment_list.xlsx")
