#! /bin/bash

# send redcap reports to Azure storage
REDCAP_DESTINATION_PATH="AI-READI/REDCap/"
azcopy copy "/home/azureuser/data/redcap/*.csv" "${RAW_URI}/${REDCAP_DESTINATION_PATH}${RAW_SAS}"

# send OMOP csv files to Azure storage
OMOP_DESTINATION_PATH="AI-READI/pooled-data/OMOP"
azcopy copy "/home/azureuser/data/output/*.csv" "${STAGE1_URI}/${OMOP_DESTINATION_PATH}${STAGE1_SAS}"

# send DQD report to Azure storage
azcopy copy "/home/azureuser/data/output/*.json" "${STAGE1_URI}/${OMOP_DESTINATION_PATH}${STAGE1_SAS}"


