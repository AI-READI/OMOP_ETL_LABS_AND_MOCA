#! /bin/bash

# send OMOP csv files to Azure RESTRICTED storage
# OMOP_DESTINATION_PATH="AI-READI/pooled-data/OMOP/RESTRICTED"
# azcopy copy "/home/azureuser/data/output/*.csv" "${STAGE1_URI}/${OMOP_DESTINATION_PATH}${STAGE1_SAS}"

# send Mini OMOP csv files to Azure PUBLIC storage
# we are skipping the drug_exposure table here
# oddly, wildcards are required or the copy does not work
OMOP_PUBLIC_DESTINATION_PATH="AI-READI/pooled-data/OMOP/MINI DATASET"
azcopy copy "/home/azureuser/data/output/condition_occurrence*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/measurement*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/observation*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/person*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/procedure_occurrence*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"
azcopy copy "/home/azureuser/data/output/visit_occurrence*.csv" "${STAGE1_URI}/${OMOP_PUBLIC_DESTINATION_PATH}${STAGE1_SAS}"

 


