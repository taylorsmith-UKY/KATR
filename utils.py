from datetime import datetime as dt
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as rdelt


# Get current time since dates (tsd) in days and in months, with or without grace period
def get_tsd(dates: pd.Series, grace_period: int = 7):
    today = dt.today()
    if dates.dtype == 'O':
        dates = dates.astype("datetime64[ns]")
    dsd = (today - dates).dt.days
    msd = np.zeros(len(dates), dtype=int)
    msd_grace = np.zeros(len(dates), dtype=int)
    for i, date in enumerate(dates):
        msd[i] = ((today.year - date.year) * 12) + (today.month - date.month)
        if today.day < date.day and msd[i] > 0:  # only count if the same day if it has reached same day of the month
            msd[i] -= 1
        grace_date = date + rdelt(months=msd[i], days=grace_period)
        if today < grace_date:
            msd_grace[i] = max(msd[i] - 1, 0)
        else:
            msd_grace[i] = msd[i]
    return dsd, msd, msd_grace


def condense_cols(data: pd.DataFrame, legend: dict, fieldname: str = "",
                  output: str = "labels" in ["labels", "values"], sep: str = ","):
    """
    Condense multiple columns representing response values for individual values corresponding to the same field
    :param data: DataFrame containing the columns for each response option for a single field
    :param legend: Dict containing the response values and their corresponding label
    :param fieldname: Name of the original field before splitting
    :param output: String in ["labels", "values"]
    :param sep: Separator character/string for output
    """
    if fieldname == "":
        fieldname = data.columns[0].split("___")[0]
    out = pd.Series(index=data.index, dtype=str, name=fieldname)
    colnames = {val: f"{fieldname}___{val}".replace("-", "_") for val in legend.keys()}
    for idx in out.index:
        vals = []
        for val in legend.keys():
            if data[colnames[val]][idx] == 1:
                vals.append(val)
        if output == "values":
            out[idx] = f"{sep}".join(vals)
        else:
            out[idx] = f"{sep}".join([legend[val] for val in vals])
    return out


def get_codes(dictfname, fieldnames):
    ddict = pd.read_csv(dictfname)
    out = {}
    for fieldname in fieldnames:
        out[fieldname] = {}
        for choice in ddict[ddict["Variable / Field Name"] == fieldname]\
                ['Choices, Calculations, OR Slider Labels'].values[0].split(" | "):
            val, label = choice.split(", ")
            try:
                out[fieldname][int(val)] = label
            except ValueError:
                out[fieldname][val] = label
    return out


def build_checkbox_cols(base_name, all_cols):
    cols = [x for x in all_cols if f"{base_name}___" in x and "____" not in x]
    vals = [x[len(base_name) + 3:] for x in cols]
    return cols
