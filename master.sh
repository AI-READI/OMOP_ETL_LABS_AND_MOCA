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

# pull latest labs and moca data to local storage,
# copy latest version of each data to file with 'latest' in the name
bash /home/azureuser/omop_etl_labs_and_moca/copy_source_data_to_local_storage.sh

# run redcap ETL into OMOP
python3 '/home/azureuser/omop/python/OMOP Datamart Step3_Azure.py'

# run moca ETL into OMOP
python3 /home/azureuser/omop_etl_labs_and_moca/moca_main.py

# run labs ETL into OMOP
python3 /home/azureuser/omop_etl_labs_and_moca/labs_main.py

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


