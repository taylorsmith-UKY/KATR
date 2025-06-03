import pandas as pd

df = pd.read_csv("logs.csv")
df = df.sort_values(["Time / Date", "Record"], ascending=False)

skip_ref = True     # skip fields if a corresponding refused field is selected

fields = ["addr", "addr2"]
record_ids = df["Record"].unique()
data = {}
for rid in record_ids:
    field_vals = {field: [] for field in fields}
    idxs = []
    disp = []
    show = False
    df1 = df[df["Record"] == rid].reset_index()
    df1["disp_addr"] = "" * len(df1)
    df1["disp_act"] = "" * len(df1)
    df1["disp_ev"] = "" * len(df1)
    for i in range(len(df1)):
        if type(df1["List of Data Changes OR Fields Exported"][i]) == float:
            continue
        elif df1["Username"][i] == "tdsm222":
            continue
        for field in fields:
            if f"{field} =" in df1["List of Data Changes OR Fields Exported"][i]:
                changes = df1["List of Data Changes OR Fields Exported"][i].split(", ")
                save = False
                for change in changes:
                    if field in change:
                        if skip_ref and (change[-4:] == "_ref" or change[-2:] == "_r"):
                            continue
                        save = True
                        field_vals[field].append(change.split(" = ")[-1])
                        df1["disp_addr"][i] = "addr2 = " + addr2[-1]
                        df1["disp_act"][i] = "Update"
                        df1["disp_ev"][i] = "Reporting"
                if save:
                    idxs.append(i)

    if len(field_vals[fields[-1]]) == 0:
        continue
    elif field_vals[fields[0]][0] != field_vals[fields[1]][0]:
        if df1["disp_ev"][idxs[0]] == "Reporting":
            data[rid] = df1.iloc[idxs][["Record", "Time / Date", "disp_act", "disp_ev", "disp_addr"]]

keys = list(data)
