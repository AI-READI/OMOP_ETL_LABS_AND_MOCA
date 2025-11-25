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
# Get this from Julie in Year 3, has two files for main data and labs
# python3 /home/azureuser/omop_etl_labs_and_moca/pull_latest_redcap_data.py

# pull latest labs and moca data to local storage,
# copy latest version of each data to file with 'latest' in the name
# Use the Moca only version for Year 3
#bash /home/azureuser/omop_etl_labs_and_moca/copy_source_data_to_local_storage.sh
bash /home/azureuser/omop_etl_labs_and_moca/copy_source_data_to_local_storage_no_labs.sh

# run redcap ETL into OMOP
# the new version also processes the lab redcap report for Year 3
# python3 '/home/azureuser/omop/python/OMOP Datamart Step3_Azure.py' Years 1 and 2
python3 '/home/azureuser/omop/python/OMOP Datamart Step3_Azure_Plus_Labs.py'

# run moca ETL into OMOP
python3 /home/azureuser/omop_etl_labs_and_moca/moca_main.py

# run labs ETL into OMOP, only done in years 1 and 2, Year 3 it was done as a Redcap report
# python3 /home/azureuser/omop_etl_labs_and_moca/labs_main.py

# generate OMOP csv output files
python3 '/home/azureuser/omop/python/OMOP Datamart Step5 Azure.py'

# run DQD analysis and generate json file
# steve we need a way to transfer the json file to the ETL machine
# i'm asking Eamon about this, do it manually for now
# steve, you can use the following scp command on the DQD machine to 
# copy the dqd json file to the correct place on the ETL machine:
# RUN THIS ON THE DQD machine once the JSON file is generated
# scp -i ~/.ssh/omop-etl.pem dqd_aireadi_omop.json azureuser@10.0.0.6:///home/azureuser/data/output


# transfer OMOP csv, DQD json, and REDCAP csv files to Azure storage
bash /home/azureuser/omop_etl_labs_and_moca/send_output_files_to_storage_container.sh

# 1) Download the CSV files for post-hoc processing and reloading
# 2) Run 'Remove MOCA Duplicates.R' locally to both create the mini versions of the css
#    and remove duplicates from the MOCA data
# 3) Then reupload them to the ETL VM and them move them both to Azure storage by the running 
#    the following

bash /home/azureuser/omop_etl_labs_and_moca/send_output_files_to_storage_container.sh
bash /home/azureuser/omop_etl_labs_and_moca/send_output_files_to_storage_container_mini.sh

# Go the the MINI DATASET Azure storage folder and delete the non mini files since this copies
# all of them


