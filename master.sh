#!/bin/bash

#
# master script to run the OMOP ETL and copy files from and to the correct places
# in the Azure file system.
#
# each step is in its own script(s) that is run from here.
#

# copy or save OMOP database to appropriate location

# clean out database to start fresh

# pull latest recap report data to local storage
python3 /home/azureuser/omop_etl_labs_and_moca/pull_latest_redcap_data.py

# pull latest labs and moca data to local storage
bash /home/azureuser/omop_etl_labs_and_moca/copy_source_data_to_local_storage.sh

# run redcap ETL into OMOP

# run moca ETL into OMOP
python3 /home/azureuser/omop_etl_labs_and_moca/moca_main.py

# run labs ETL into OMOP
python3 /home/azureuser/omop_etl_labs_and_moca/moca_etl.py

# generate OMOP csv output files

# copy OMOP csv files to azure storage

# run DQD analysis and generate json file

# copy DQD json analysis to azure storage

# copy lastest redcap data files to azure storage
bash /home/azureuser/omop_etl_labs_and_moca/copy_redcap_reports_to_azure_staging.sh

