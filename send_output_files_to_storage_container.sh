#! /bin/bash

# send redcap reports to Azure storage
REDCAP_DESTINATION_PATH="AI-READI/REDCap/"
azcopy copy "/home/azureuser/data/redcap/*.csv" "${RAW_URI}/${REDCAP_DESTINATION_PATH}${RAW_SAS}"

# send OMOP csv files to Azure RESTRICTED storage
OMOP_DESTINATION_PATH="AI-READI/pooled-data/OMOP/RESTRICTED"
azcopy copy "/home/azureuser/data/output/*.csv" "${STAGE1_URI}/${OMOP_DESTINATION_PATH}${STAGE1_SAS}"

# send OMOP csv files to Azure PUBLIC storage
# we are skipping the drug_exposure table here
# oddly, wildcards are required or the copy does not work
OMOP_PUBLIC_DESTINATION_PATH="AI-READI/pooled-data/OMOP/PUBLIC"
azcopy copy "/home/azureuser/data/output/condition_occurrence*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/measurement*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/observation*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/person*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/procedure_occurrence*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"

# send DQD report to Azure storage
DQD_DESTINATION_PATH="AI-READI/pooled-data/OMOP"
azcopy copy "/home/azureuser/data/output/*.json" "${STAGE1_URI}/${DQD_DESTINATION_PATH}${STAGE1_SAS}"


