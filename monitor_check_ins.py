from redcap import Project
import numpy as np
import pandas as pd
import inflect
import plotly.graph_objects as go
from utils import get_tsd
p = inflect.engine()

# Params
gp_days = 7                         # Check-in grace period
show_plot = False
save_plot = False
send_update = True

# Connect to REDCap project
api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)

# Load intake data from REDCap
client_df = project.export_records(format_type="df", fields=['idate'], raw_or_label="raw", events=['intake_arm_1']).\
    drop(['redcap_repeat_instrument', 'redcap_repeat_instance'], axis=1)
client_df.index = client_df.index.droplevel(1)
client_df.columns = ['dintake']
client_df = client_df.drop(client_df.index[client_df['dintake'].isna()])
client_df['dintake'] = client_df['dintake'].astype('datetime64[ns]')

# Get time since intake as of running this script
dsi, msi, msi_grace = get_tsd(client_df['dintake'])
client_df['dsi'] = dsi
client_df['msi'] = msi
client_df['msi_grace'] = msi_grace

# Load checkin data from REDCap
check_df = project.export_records(format_type="df", fields=['chkdate'],
                                  raw_or_label="raw", events=['monthy_checkin_arm_1'])
check_df.loc[:, 'chkdate'] = check_df['chkdate'].astype("datetime64[ns]")   # convert to datetime for calculations
check_df = check_df.sort_values(by=['cid', 'chkdate'])

# Get total number of checkins for each client
check_cts = check_df.index.get_level_values(0).value_counts()
max_check = check_cts.max()      # greatest number of checkins for all
client_df["chktot"] = np.zeros(len(client_df), dtype=int)
client_df.loc[check_cts.index.values, "chktot"] = check_cts.values
client_df["misschk"] = client_df["msi_grace"] - client_df["chktot"]
client_df.loc[:, "misschk"] = client_df["misschk"].replace(-1, 0)

# Update monthly check-in monitoring instrument in REDCap
if send_update:
    update_df = client_df[['dintake', 'msi', 'msi_grace', 'chktot', 'misschk']].reset_index()
    update_df['redcap_event_name'] = ["reporting_arm_1"] * len(update_df)
    project.import_records(to_import=update_df, import_format="df")

if show_plot or save_plot:
    max_days = int(client_df['dsi'].max())
    # New dataframe for storing values for each number of days since intake
    dsi_df = pd.DataFrame(index=pd.Index(np.arange(max_days + 1, dtype=int), name='dsi'))
    dsi_df['count'] = np.zeros(len(dsi_df), dtype=int)
    for i in range(max_check):                                                      # clients
        dsi_df[f"checkin_{i + 1}"] = np.zeros(max_days + 1, dtype=int)
        dsi_df[f"checkin_{i + 1}_pct"] = np.zeros(max_days + 1, dtype=int)
        client_df[f"checkin_{i + 1}_dsi"] = np.repeat(np.nan, len(client_df))

    for cid in check_cts.index:                    # Determine date and days since intake
        intake = client_df['dintake'][cid]                                        # for each monthly check-in for each client
        for i, chkdate in enumerate(check_df.loc[cid, 'chkdate'].values):
            dsi = (chkdate - intake).days
            dsi_df.loc[dsi, f"checkin_{i + 1}"] += 1
            client_df.loc[cid, f"checkin_{i + 1}_dsi"] = dsi

    for dsi in range(max_days + 1):
        sel = client_df['dsi'] >= dsi
        dsi_df.loc[dsi, 'count'] = sel.sum()
        for i in range(max_check):
            dsi_df.loc[dsi, f"checkin_{i + 1}_ct"] = (client_df[f"checkin_{i + 1}_dsi"][sel] <= dsi).sum()
            dsi_df.loc[dsi, f"checkin_{i + 1}_pct"] = (client_df[f"checkin_{i + 1}_dsi"][sel] <= dsi).sum() / sum(sel) * 100

# Plot data
    xticks = [30*i for i in range(1, (max_days // 30) + 1) if 30*i < max_days]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dsi_df.index.values, y=dsi_df[f"checkin_{max_check}_pct"],
                             name=f"{p.ordinal(max_check)} Checkin", fill="tozeroy", customdata=np.stack([dsi_df[f"checkin_{max_check}_ct"], dsi_df['count']]),
                             hovertemplate='%{y:.1f}% %{customdata[0]}<br>Count: %{customdata[1]}'))
    for i in range(max_check-1)[::-1]:
        fig.add_trace(go.Scatter(x=dsi_df.index.values, y=dsi_df[f"checkin_{i + 1}_pct"],
                                 name=f"{p.ordinal(i + 1)} Checkin", fill="tonexty", customdata=dsi_df[f"checkin_{i + 1}_ct"], hovertemplate='%{y:.1f}% %{customdata}'))
    # fig.add_trace(go.Scatter(x=df2.index.values, y=df2["count"], name="Number of Clients", visible="legendonly"))
    fig.update_layout(dict(title="Pct Checkins Over Time", xaxis_title="Days Since Intake", yaxis_title="Pct Completed",
                           hovermode="x unified", xaxis_tickprefix="Days Since Intake: "))
    fig.update_xaxes(tickmode='array', tickvals=xticks)
    if show_plot:
        fig.show()
    if save_plot:
        fig.write_html("checkins_over_time.html")
        fig.write_image("checkins_over_time.png")
