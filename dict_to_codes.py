# Generate a json text file containing the codebook for all fields in the KATR database

import pandas as pd
import json

ftypes_keep = ['dropdown', 'radio', 'checkbox']  # , 'yesno']
codebook = {}
d = pd.read_csv("KATRParticipantSurvey_DataDictionary_2024-02-07.csv")
for row in d.iterrows():
    item = row[1]
    name = item["Variable / Field Name"]
    ftype = item["Field Type"]
    if ftype in ftypes_keep:
        codebook[name] = {"map": {}, "type": ftype}
        maps = item["Choices, Calculations, OR Slider Labels"].split(" | ")
        for key in maps:
            value = key.split(", ")[0]
            label = key[len(value) + 2:]
            codebook[name]["map"][value] = label
json.dump(codebook, "codebook.json")

with open("codebook.json", "w") as f:
    json.dump(codebook, f)

with open("codebook.json", "r") as f:
    codebook = json.load(f)
