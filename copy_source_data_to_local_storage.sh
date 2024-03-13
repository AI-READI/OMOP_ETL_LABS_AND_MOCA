#! /bin/bash

# copy lab data xlsx files to local storage
#LAB_SOURCE_PATH="/mnt/b2ai_stg_raw-storage-1/AI-READI/UW/Lab_NORC"
#rm -f ~/data/labs/*
#FILES=`sudo ls $LAB_SOURCE_PATH`
#for FILENAME in $FILES; do
#        if [[ $FILENAME == *.xlsx ]]; then
#                echo "Copying:" $FILENAME
#                sudo cp $LAB_SOURCE_PATH/$FILENAME ~/data/labs
#        fi
#	sudo chown azureuser:azureuser ~/data/labs/*.xlsx
#done

LAB_SOURCE_PATH="AI-READI/UW/Lab_NORC"
rm -f /home/azureuser/data/labs/*.xlsx
rm -f /home/azureuser/data/labs/Lab_NORC/*.xlsx
azcopy copy "${RAW_URI}/${LAB_SOURCE_PATH}${RAW_SAS}" '/home/azureuser/data/labs/' --recursive --include-pattern '*.xlsx'
python3 /home/azureuser/omop_etl_labs_and_moca/select_and_copy_most_recent_file.py -src "/home/azureuser/data/labs/Lab_NORC/*.xlsx" -dest /home/azureuser/data/labs/LAB-NORC-latest.xlsx



# copy moca data csv files to local storage
#MOCA_SOURCE_PATH="/mnt/b2ai_stg_raw-storage-1/AI-READI/UAB/MoCA"
#rm -f ~/data/moca/*
#FILES=`sudo ls $MOCA_SOURCE_PATH`
#for FILENAME in $FILES; do
#        if [[ $FILENAME == *.csv ]]; then
#                echo "Copying:" $FILENAME
#                sudo cp $MOCA_SOURCE_PATH/$FILENAME ~/data/moca
#        fi
#	sudo chown azureuser:azureuser ~/data/moca/*.csv
#done

MOCA_SOURCE_PATH="AI-READI/UAB/MoCA"
rm -f /home/azureuser/data/moca/*.csv
rm -f /home/azureuser/data/moca/MoCA/*.csv
azcopy copy "${RAW_URI}/${MOCA_SOURCE_PATH}${RAW_SAS}" '/home/azureuser/data/moca/' --recursive --include-pattern '*.csv'
python3 /home/azureuser/omop_etl_labs_and_moca/select_and_copy_most_recent_file.py -src "/home/azureuser/data/moca/MoCA/*.csv" -dest /home/azureuser/data/moca/MOCA-latest.csv




