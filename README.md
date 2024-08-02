The bulk of the contents within this project are standalone scripts whose individual purposes are documented below. All of these scripts rely on reading data from REDCap and then possibly uploading new information into REDCap. In order to do this, you will need an API token from your project admin with the appropriate permissions to access/edit the data you are querying. Save this key as plain text in the file "api_key.txt" and place it in the same directory as the scripts.

The only script that needs to be run with any regularity is "update_client_progress.py". This script should be run daily to update various reporting fields without requiring manual form resubmission in the REDCap GUI. This will populate most of the fields in the reporting instruments for the KATR project.

Home
-/ update_client_progress.py    - basic script to run daily for reporting purposes
-/ api_key.txt                  - plain text file containing the API key for the user requesting access to the REDCap project
-/ requirements.txt`            - text file listing the python libraries required by the various standalone scripts 
