import pandas as pd
from redcap import Project
import numpy as np
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rdelt

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

# Fields to get from REDCap
fields = ['idate', 'name', 'phone', 'coord', 'programmatic_complete']
events = ['intake_arm_1', "discharge_arm_1", "3month_postdischar_arm_1", "12month_postintake_arm_1"]

data = project.export_records(format_type="json", fields=fields, raw_or_label="raw", events=events)
ids = [x['cid'] for x in data]
unique_ids = set(ids)
new = []
for uid in unique_ids:
    new.append({'cid': uid, 'redcap_event_name': 'reporting_arm_2', 'dname': '', 'dphone': '',
                'comp_status': '', 'first_idate': '', 'last_idate': '', 'next_idate': '',
                'days_since_last': '', 'days_to_next': ''})
    idxs = [i for i in range(len(data)) if data[i]['cid'] == uid]
    dates = []
    events2 = []
    phones = []
    dname = data[idxs[0]]['name']
    dphone = ''
    for idx in idxs:
        if data[idx]['idate'] == '':
            print(f"cid:{uid} - event:{data[idx]['redcap_event_name']}\tMissing date")
            # raise ValueError(f"cid:{uid} - event:{data[idx]['redcap_event_name']}\tMissing date")
        else:
            dates.append(dt.strptime(data[idx]['idate'], date_format))
            events2.append(data[idx]['redcap_event_name'])
            phones.append(data[idx]['phone'])
    #assert(len(dates) > 0), f"Client {uid} does not have any valid dates"
    if len(dates) > 0:
        dates = np.array(dates)
        first = dates.min()
        last = dates.max()
        dphone = phones[dates.argmax()]
    else:
        print(f"Client {uid} does not have any valid dates")
        # raise ValueError(f"Client {uid} does not have any valid dates")
        continue

    ndlast = (dt.today() - last).days
    if "12month_postintake_arm_1" in events2:
        comp_stat = 4
        dnext = ''
        ndnext = -1
    elif "3month_postdischar_arm_1" in events2:
        comp_stat = 3
        dnext = first + rdelt(years=1)
        ndnext = (dnext - dt.today()).days
        dnext = dt.strftime(dnext, date_format)
    elif "discharge_arm_1" in events2:
        comp_stat = 2
        dnext = last + rdelt(months=3)
        ndnext = (dnext - dt.today()).days
        dnext = dt.strftime(dnext, date_format)
    elif "intake_arm_1" in events2:
        comp_stat = 1
        dnext = last + rdelt(months=4, weeks=2)
        # dnext = last + rdelt(months=6)
        ndnext = (dnext - dt.today()).days
        dnext = dt.strftime(dnext, date_format)
    else:
        comp_stat = 0
        dnext = ''
        ndnext = 0
    new[-1]['comp_status'] = comp_stat
    new[-1]['dname'] = dname
    new[-1]['dphone'] = dphone
    new[-1]['first_idate'] = dt.strftime(first, date_format)
    new[-1]['last_idate'] = dt.strftime(last, date_format)
    new[-1]['next_idate'] = dnext
    new[-1]['days_since_last'] = ndlast
    new[-1]['days_to_next'] = ndnext

project.import_records(to_import=new, import_format="json")


# data = project.export_records(format_type="df", fields=fields, raw_or_label="label", events=events)
# data.reset_index(inplace=True)                          # Create client ID and event name columns from dataframe index
# data['idate'] = pd.to_datetime(data['idate'], date_format)
#
# client_ids = data["cid"].unique()
#
# nfields = {'cid': str, 'redcap_event_name': str, 'comp_status': int, 'first_idate': '<M8[ns]',
#           'last_idate': '<M8[ns]', 'next_idate': '<M8[ns]', 'days_since_last': int, 'days_to_next': int}
# new = pd.DataFrame(index=client_ids, columns=nfields.keys(), dtypes=nfields.values())
# new.loc['comp_status'] = 0
# for cid in client_ids:
#     cdf = data[data['cid'] == cid]
#     new['cid'] = cid
#     new['first_idate'][cid] = cdf['idate'].min()
#     new['last_idate'][cid] = cdf['idate'].max()
#     new['days_since_last'][cid] = (pd.to_datetime("now") - new['first_idate'][cid]).days
#     if "12-month Post-Intake FU" in cdf['redcap_event_name'].to_list():
#         new['comp_status'][cid] = 4
#         new['next_idate'] = pd.NaT
#         new['days_to_next'][cid] = -1
#     elif "3-month Post-Discharge FU" in data['redcap_event_name'][data['cid'] == cid].to_list():
#         new['comp_status'][cid] = 3
#         new['next_idate'][cid] = new['first_idate'][cid] + pd.DateOffset(years=1)
#         new['days_to_next'][cid] = (new['next_idate'][cid] - pd.to_datetime("now")).days
#     elif "Discharge" in data['redcap_event_name'][data['cid'] == cid].to_list():
#         new['comp_status'][cid] = 2
#         new['next_idate'][cid] = new['last_idate'][cid] + pd.DateOffset(months=3)
#         new['days_to_next'][cid] = (new['next_idate'][cid] - pd.to_datetime("now")).days
#     else:
#         new['comp_status'][cid] = 1
#         new['next_idate'][cid] = new['last_idate'][cid] + pd.DateOffset(months=3)
#         new['days_to_next'][cid] = (new['next_idate'][cid] - pd.to_datetime("now")).days
#
#
# project.import_records(to_import=new, import_format="df")
#
# #
# # new.replace(to_replace=pd.NaT, value=-2, inplace=True)
# # n2 = new.to_dict()
# #
# # def convert_times(obj):
# #     if type(obj) != dict:
# #         try:
# #             return dt.strftime(obj, "%Y-%m-%d")
# #         except:
# #             return obj
# #     else:
# #         for key in obj.keys():
# #             obj[key] = convert_times(obj[key])
# #     return obj
