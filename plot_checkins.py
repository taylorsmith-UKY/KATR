from redcap import Project
from datetime import datetime as dt
from datetime import timedelta as td
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import inflect
p = inflect.engine()

# Configure REDCap API
api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)

# Extract data
intakes = project.export_records(format_type="df", fields=['idate'], raw_or_label="raw",
                                 events=['intake_arm_1'])[['idate']]
intakes.reset_index(inplace=True)
intakes = intakes.drop("redcap_event_name", axis=1)
ids = intakes['cid']

checks = project.export_records(format_type="df", fields=['chkdate'], raw_or_label="raw",
                                events=['monthy_checkin_arm_1'])[['chkdate', 'redcap_repeat_instance']]
checks.reset_index(inplace=True)
checks = checks.drop("redcap_event_name", axis=1)

# Process intake data
intakes['idate'] = intakes['idate'].astype('datetime64[ns]')
intakes.sort_values('idate', inplace=True)
intakes.reset_index(inplace=True, drop=True)
first = intakes['idate'].min()
last_intake = intakes['idate'].max()

# Process checkin data
checks['chkdate'] = checks['chkdate'].astype('datetime64[ns]')
checks['redcap_repeat_instance'] = checks['redcap_repeat_instance'].astype(int)
checks.sort_values(['chkdate'], inplace=True)
checks.reset_index(inplace=True, drop=True)
max_check = checks['redcap_repeat_instance'].max()
last_check = checks['chkdate'].max()

# Create dataframe for counts over time
last = max(last_intake, last_check)
numdays = (last - first).days + 1
date_range = np.array([first + td(days=i) for i in range(numdays)])
plot_data = pd.DataFrame.from_dict({"dates": date_range})

# Count daily intakes
plot_data["intakes"] = np.zeros(numdays, dtype=int)
for intake in intakes['idate']:
    plot_data['intakes'][plot_data["dates"] == intake] += 1
plot_data['total_intakes'] = plot_data['intakes'].cumsum()      # Get rolling total number of intakes over time

# Create columns for each monthly check-in and count daily check-ins
for instance in range(1, max_check + 1):
    plot_data[f"checkin_{instance}"] = np.zeros(numdays, dtype=int)
for date, instance in checks[['chkdate', 'redcap_repeat_instance']].values:
    plot_data[f"checkin_{instance}"][plot_data["dates"] == date] += 1
for instance in range(1, max_check + 1):
    plot_data[f'total_{p.ordinal(instance)}_checkins'] = plot_data[f'checkin_{instance}'].cumsum()


# Express
# fig = px.area(plot_data, 'dates', ['total_intakes',
#                                    'total_first_checkins', 'total_second_checkins', 'total_third_checkins'])
# fig.show()
# with open("example_intakes_vs_checkins.html", "w", encoding="utf-8") as f:
#     f.write(fig.to_html())
#
# fig.write_html("example_intakes_vs_checkins.html")
# fig.write_image("example_intakes_vs_checkins.png")

# Graph Objects
fig = go.Figure()
fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data[f'total_{p.ordinal(max_check)}_checkins'],
                         fill='tozeroy', name=f"Total {p.ordinal(max_check)} Checkins"))
for checkin in range(1, max_check)[::-1]:
    fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data[f'total_{p.ordinal(checkin)}_checkins'],
                             fill='tonexty', name=f"Total {p.ordinal(checkin)} Checkins"))

# fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data["total_third_checkins"], fill='tozeroy',
#               name="Total Third Checkins")) # fill down to xaxis
# fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data["total_second_checkins"], fill='tonexty',
#               name="Total Second Checkins")) # fill to trace0 y
# fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data["total_first_checkins"], fill='tonexty',
#               name="Total First Checkins")) # fill to trace0 y

fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data["total_intakes"], fill='tonexty',
              name="Total Intakes")) # fill to trace0 y
fig.update_layout(dict(title="Intakes vs. Monthly Check-ins", xaxis_title="Date", yaxis_title="Total Count",
                       hovermode="x"))
fig.show()
fig.write_html("intakes_vs_checkins.html")
fig.write_image("intakes_vs_checkins.png")

totals = plot_data[['dates', 'total_intakes', 'total_first_checkins', 'total_second_checkins', 'total_third_checkins']]
totals = totals.replace(0, np.nan)
fig = go.Figure()
fig.add_trace(go.Scatter(x=totals["dates"], y=totals["total_intakes"],
                         fill='tozeroy', name="Total Intakes", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=totals["dates"],
                         y=totals["total_first_checkins"],
                         name="Total First Checkins", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=totals["dates"],
                         y=totals["total_first_checkins"],
                         name="Total Second Checkins", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=totals["dates"],
                         y=totals["total_third_checkins"],
                         name="Total Third Checkins", stackgroup='one')) # fill down to xaxis
fig.update_layout(dict(title="Intakes vs. Monthly Check-ins", xaxis_title="Date", yaxis_title="Total Count"))
fig.show()


intake1 = plot_data[['dates', 'total_intakes']]
intake1.columns = ['dates', 'count']
intake1['Interaction'] = ['Total Intakes']*len(intake1)
first = plot_data[['dates', 'total_first_checkins']][plot_data['total_first_checkins'] > 0]
first.columns = ['dates', 'count']
first['Interaction'] = ['Total First Checkins']*len(first)
second = plot_data[['dates', 'total_second_checkins']][plot_data['total_second_checkins'] > 0]
second.columns = ['dates', 'count']
second['Interaction'] = ['Total Second Checkins']*len(second)
third = plot_data[['dates', 'total_third_checkins']][plot_data['total_third_checkins'] > 0]
third.columns = ['dates', 'count']
third['Interaction'] = ['Total Third Checkins']*len(third)
df = pd.concat([intake1, first, second, third])
fig = px.area(df, "dates", "count", color="Interaction", line_group="Interaction")
fig.show()

df = pd.concat([third, second, first, intake1])
fig = px.area(df, "dates", "count", color="Interaction", line_group="Interaction")
fig.show()


fig = go.Figure()
fig.add_trace(go.Scatter(x=plot_data["dates"], y=plot_data["total_intakes"],
                         fill='tozeroy', name="Total Intakes", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=plot_data["dates"],
                         y=plot_data["total_first_checkins"],
                         name="Total First Checkins", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=plot_data["dates"],
                         y=plot_data["total_first_checkins"],
                         name="Total Second Checkins", stackgroup='one')) # fill to trace0 y
fig.add_trace(go.Scatter(x=plot_data["dates"],
                         y=plot_data["total_third_checkins"],
                         name="Total Third Checkins", stackgroup='one')) # fill down to xaxis
fig.update_layout(dict(title="Intakes vs. Monthly Check-ins", xaxis_title="Date", yaxis_title="Total Count"))
fig.show()
