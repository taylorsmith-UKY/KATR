from redcap import Project
from datetime import datetime as dt
import numpy as np
import pandas as pd
import inflect
p = inflect.engine()
import plotly.graph_objects as go

api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"
gp_days = 7                         # Check-in grace period

# Load intake data from REDCap
intake_data = project.export_records(format_type="df", fields=['idate'], raw_or_label="raw", events=['intake_arm_1'])
all_ids = intake_data.index.get_level_values(0)
intake_data.index = pd.Index(all_ids)

# Dataframe to store client-specific information, such as dates for their intakes and checkins
client_df = pd.DataFrame(data=intake_data['idate'].astype("datetime64[ns]").values, index=all_ids, columns=['intake'])
client_df = client_df.drop(client_df.index[client_df['intake'].isna()])
client_df['dsi'] = (dt.today() - client_df['intake']).dt.days.astype(int)       # current days since intake on running
max_days = int(client_df['dsi'].max())                                          # this script

# Dataframe to store aggregate data according to the total number of days post intake
dsi_df = pd.DataFrame(index=pd.Index(np.arange(max_days + 1, dtype=int), name='dsi'))
dsi_df['count'] = [(client_df['dsi'] >= i).sum() for i in range(max_days + 1)]  # number of clients who are >= each
#                                                                                 number of days since their intake
# Load monthly check-in data from REDCap
check_data = project.export_records(format_type="df", fields=['chkdate'],
                                    raw_or_label="raw", events=['monthy_checkin_arm_1'])
check_data.loc[:, 'chkdate'] = check_data['chkdate'].astype("datetime64[ns]")   # convert to datetime for calculations
check_data = check_data.sort_values(by=['cid', 'chkdate'])
max_check = int(check_data.index.get_level_values(0).value_counts().max())      # greatest number of checkins for all
for i in range(max_check):                                                      # clients
    dsi_df[f"checkin_{i + 1}"] = np.zeros(max_days + 1, dtype=int)
    dsi_df[f"checkin_{i + 1}_pct"] = np.zeros(max_days + 1, dtype=int)
    client_df[f"checkin_{i + 1}_dsi"] = np.repeat(np.nan, len(client_df))

for cid in check_data.index.get_level_values(0).unique():                    # Determine date and days since intake
    intake = client_df['intake'][cid]                                        # for each monthly check-in for each client
    for i, chkdate in enumerate(check_data.loc[cid, 'chkdate'].values):
        dsi = (chkdate - intake).days
        dsi_df.loc[dsi, f"checkin_{i + 1}"] += 1
        client_df.loc[cid, f"checkin_{i + 1}_dsi"] = dsi

for dsi in range(max_days + 1):
    sel = client_df['dsi'] >= dsi
    for i in range(max_check):
        dsi_df.loc[dsi, f"checkin_{i + 1}_ct"] = \                                  # total number of clients who have
            (client_df[f"checkin_{i + 1}_dsi"][sel] <= dsi).sum()                   # completed ith checkin
        dsi_df.loc[dsi, f"checkin_{i + 1}_pct"] = \                                 # pct of total number of clients who
            (client_df[f"checkin_{i + 1}_dsi"][sel] <= dsi).sum() / sum(sel) * 100  # are currently at least dsi days
#                                                                                   # days since intake

xticks = [30*i for i in range(1, (max_days // 30) + 1) if 30*i < max_days]
fig = go.Figure()
fig.add_trace(go.Scatter(x=dsi_df.index.values, y=dsi_df[f"checkin_{max_check}_pct"],
                         name=f"{p.ordinal(max_check)} Checkin", fill="tozeroy", customdata=dsi_df[f"checkin_{max_check}_ct"],  # customdata=df2['count'],
                         hovertemplate='%{y:.1f}% %{customdata}'))  # <br>Count: %{customdata}'))
for i in range(max_check-1)[::-1]:
    fig.add_trace(go.Scatter(x=dsi_df.index.values, y=dsi_df[f"checkin_{i + 1}_pct"],
                             name=f"{p.ordinal(i + 1)} Checkin", fill="tonexty", customdata=dsi_df[f"checkin_{i + 1}_ct"], hovertemplate='%{y:.1f}% %{customdata}'))
# fig.add_trace(go.Scatter(x=df2.index.values, y=df2["count"], name="Number of Clients", visible="legendonly"))
fig.update_layout(dict(title="Pct Checkins Over Time", xaxis_title="Days Since Intake", yaxis_title="Pct Completed",
                       hovermode="x unified", xaxis_tickprefix="Days Since Intake: "))
fig.update_xaxes(tickmode='array', tickvals=xticks)
fig.show()
fig.write_html("checkins_over_time.html")
fig.write_image("checkins_over_time.png")
