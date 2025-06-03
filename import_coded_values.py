from redcap import Project
import pandas as pd
import numpy as np

"""
Script for importing coded free-text fields into REDCap

Data Preparation
 - Each question and the corresponding values are presented in a single Excel spreadsheet.
 - The first 3 columns of the spreadsheet should be 'cid', 'Timepoint', and the third column name is the original
    question, with the free-text values below.
 - The options and their values in REDCap should be presented in the same order as the columns in the spreadsheet,
   with corresponding values of 1, 2, 3, etc.
   
    e.g. if the field is "field" and the options are {"Bike": "1", "Scooter": 2}. Every time there is a "Bike" selected,
        the new column "field_coded___1" gets a value of 1 and every time "Scooter" is selected, "field_coded___2" is
        assigned a value of 1.
        
 - For single-choice fields, the values of the corresponding values must be provided explicitly.
"""

# Connect to the project using the REDCap API
api_url = 'https://redcap.uky.edu/redcap/api/'
with open("api_key.txt", "r") as f:
    api_key = f.readline().strip()
project = Project(api_url, api_key)
date_format = "%Y-%m-%d"

# Account for different free text values corresponding to REDCap events
timepoint_map = {"Intake": "intake_arm_1", "Discharge": "discharge_arm_1",
                 "3 Months Post\nDischarge": "3month_postdischar_arm_1",
                 "12 Months Post\nIntake": "12month_postintake_arm_1",
                 "3 months\npost\ndischarge": "3month_postdischar_arm_1",
                 "3 Months\nPost\nDischarge": "3month_postdischar_arm_1",
                 "12 Months\nPost Intake": "12month_postintake_arm_1"}

# Files for each question and the corresponding new coded field where data should be imported
files_and_fields = {"Listing_HEALTH.xlsx": "mh_dia_exp_coded",
                    "Listing_MED_q1.xlsx": "d_subs_coded",
                    "Listing_MED_q2.xlsx": "doc_coded",
                    "Listing_MED_q3.xlsx": "puse_coded",
                    "Listing_PRG.xlsx": "trans_t_sp_coded"}

# Each single option field requires an explicit label -> value mapping
single_option_fields = {"trans_t_sp_coded": {'Vehicle Repairs': "1", 'Bus Passes': "2"}}

# For each field/question/file
for filename, fieldname in files_and_fields.items():
    # Load data file and translate all Timepoint values to the corresponding REDCap event ID
    df = pd.read_excel(filename)
    df['Timepoint'] = df['Timepoint'].map(lambda x: timepoint_map[x])

    # Create a new data frame and change column names to match REDCap syntax
    upload = df[['cid', 'Timepoint']]
    upload.columns = ['cid', "redcap_event_name"]

    # If new field is single choice, simply apply the label -> value mapping for importing into REDCap
    if fieldname in single_option_fields.keys():
        upload[fieldname] = df[df.columns[-1]].map(lambda x: single_option_fields[fieldname][x], na_action="ignore")

    # For multiple selection fields
    else:
        # First, get the list of option labels from the 4th column name to the last
        opts = df[df.columns[3:]].columns
        n_opts = len(opts)

        # Create the field names corresponding to options selected in REDCap
        # By default start the numbering with 1 for the first option, followed by 2 for the second option, etc.
        # e.g. field___1, field___2, field_3...
        # This is why it is SO IMPORTANT to make sure that the columns in the spreadsheet are the same order
        # as the options in REDCap
        opt_fields = {x: fieldname + "_" * 3 + str(i+1) for i, x in enumerate(opts)}
        for ofield, nfield in opt_fields.items():
            # Translate original column data of 1 vs. empty to 1s and 0s
            # and assign to the new field
            ndata = np.zeros(len(df), dtype=int)
            ndata[df[ofield] == 1] = 1
            upload[nfield] = ndata

    upload['cid'] = upload['cid'].map(lambda x: x.upper())          # Added as some IDs were provided in lower case
    upload.to_csv(f"{fieldname}.csv")                               # Save results to CSV file
    project.import_records(to_import=upload, import_format="df")    # Import data into REDCap
