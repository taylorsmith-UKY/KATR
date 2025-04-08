from datetime import datetime as dt
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as rdelt
import re

# Get current time since dates (tsd) in days and in months, with or without grace period
def get_tsd(dates: pd.Series, grace_period: int = 7, today=dt.today()):
    # today = dt.today()
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


# Example filter_vals
# filter_vals = {'disch_st': {'operation': 'eq', 'value': 4}}
# For the provided event and filters, report NOT completed and no date is returned
def evaluate_event(client: pd.DataFrame, event: str, filter_vals: dict, date_format: str="%Y-%m-%d"):
    if event in client.index and (str(client['idate'][event]) != "" and
                                  str(client['idate'][event]) != "nan"):
        # if event != "discharge_arm_1":
        for key, value in filter_vals.items():
            if 'operation' in value.keys():
                if value['operation'] == 'eq':
                    if str(client[key][event]) == str(value['value']):
                        return 0, ""
                elif value['operation'] == 'neq':
                    if str(client[key][event]) != str(value['value']):
                        return 0, ""
                else:
                    raise ValueError("Incorrect value provided for \"operation\". "
                                     "Currently, the only options are \"eq\" \"neq\"")
        return 1, dt.strptime(client['idate'][event], date_format)
        #
        # else:           # Manually process discharge because the logic is more complicated.
        #     if int(client['gpra_complete'][event]) == 2:
        #         return 1, dt.strptime(client['idate'][event], date_format)
        #     else:
        #         return 0, ""

            # services_received = ['bneeds_sp', 'sobliv_sp', 'trans_sp', 'employ_b_3']
            # if np.all([client[x][event] == "" for x in services_received]):
            #     return 0, ""
            # else:
            #     return 1, dt.strptime(client['idate'][event], date_format)

    return 0, ""


def parse_logic(data, logic, event, index=None):
    logic = logic.replace(" or ", "OR")
    logic = logic.replace(" and ", "AND")
    logic = logic.replace(" ", "")
    logic = logic.replace("'", "")
    logic = logic.replace("!=", "<>")
    if index is None:
        return parse_logic(data, logic, event, np.zeros(len(data)))
    if "OR" in logic:
        for component in logic.split("OR"):
            if component[0] == "(":
                component = component[1:]
            if component[-1] == ")":
                component = component[:-1]
            index = np.logical_or(index, parse_logic(data, component, event, index))
        return index
    if "AND" in logic:
        components = logic.split("AND")
        index = parse_logic(data, components[0][1:], event, index)
        for component in components[1:]:
            if component[-1] == ")":
                component = component[:-1]
            index = np.logical_and(index, parse_logic(data, component, event, index))
        return index
    r = re.search("[=<>]{1,2}", logic)
    op = r.group(0)
    field = logic[:r.start()]
    value = logic[r.end():]
    if field[0] == "(":
        field = field[1:]
    if field[-1] == ")":
        field = field[:-1]
    if "][" in field:
        event, field = field[1:-1].split("][")
    if field[0] == "[":
        field = field[1:]
    if field[-1] == "]":
        field = field[:-1]
    if "(" in field:
        field, val = field.split("(")
        field = f"{field}___{val[:-1]}"

    field = field.replace("[", "")
    field = field.replace("]", "")
    value = value.replace("[", "")
    value = value.replace("]", "")
    field = field.replace("(", "")
    field = field.replace(")", "")
    value = value.replace("(", "")
    value = value.replace(")", "")
    if field == "event-name":
        if op == "=":
            return data.index.get_level_values(1) == value
        elif op == "<>":
            return data.index.get_level_values(1) != value
    if op == "=":
        try:
            return pd.to_numeric(data[field]) == pd.to_numeric(value)
        except:
            return data[field].astype(str) == value
    elif op == "<>":
        try:
            return pd.to_numeric(data[field]) != pd.to_numeric(value)
        except:
            return data[field].astype(str) != value
    elif op == ">":
        return pd.to_numeric(data[field], errors="coerce") > pd.to_numeric(value)
    elif op == "<":
        return pd.to_numeric(data[field], errors="coerce") < pd.to_numeric(value)
    elif op == ">=":
        return pd.to_numeric(data[field], errors="coerce") >= pd.to_numeric(value)
    elif op == "<=":
        return pd.to_numeric(data[field], errors="coerce") <= pd.to_numeric(value)
    else:
        raise SyntaxError(f"Input Logic: {logic}\nInvalid operation \'{op}\'")

