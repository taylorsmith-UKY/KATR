The bulk of the contents within this project are standalone scripts whose individual purposes are documented below. All of these scripts rely on reading data from REDCap and then possibly uploading new information into REDCap. In order to do this, you will need an API token from your project admin with the appropriate permissions to access/edit the data you are querying. Save this key as plain text in the file "api_key.txt" and place it in the same directory as the scripts. All standalone scripts can be executed once this file has been placed and all requirements are satisfied.

Note: <a href="https://www.python.org/downloads/">Python 3</a> must first be installed. 

To set up virtual environment using venv and install requirements, execute the following:
<code>
python -m myenv
source myenv/bin/activate
pip install -r reqruirements.txt
</code>

To run standalone scripts (after api_key.txt has been placed)
<code>
source myenv/bin/activate
python update_client_progress.py    # can replace with any standalone scripts
deactivate
</code>

The only script that needs to be run with any regularity is "update_client_progress.py". This script should be run daily to update various reporting fields without requiring manual form resubmission in the REDCap GUI. This will populate most of the fields in the reporting instruments for the KATR project.

Home
-/ update_client_progress.py    - basic script to run daily for reporting purposes
-/ api_key.txt                  - plain text file containing the API key for the user requesting access to the REDCap project
-/ requirements.txt`            - text file listing the python libraries required by the various standalone scripts 
