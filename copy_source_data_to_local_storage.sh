#! /bin/bash

# copy lab data xlsx files to local storage
LAB_SOURCE_PATH="/mnt/b2ai_stg_raw-storage-1/AI-READI/UW/Lab_NORC"
rm -f ~/data/labs/*
FILES=`sudo ls $LAB_SOURCE_PATH`
for FILENAME in $FILES; do
        if [[ $FILENAME == *.xlsx ]]; then
                echo "Copying:" $FILENAME
                sudo cp $LAB_SOURCE_PATH/$FILENAME ~/data/labs
        fi
        sudo chown azureuser:azureuser ~/data/labs/*.xlsx
done

# copy moca data csv files to local storage
MOCA_SOURCE_PATH="/mnt/b2ai_stg_raw-storage-1/AI-READI/UAB/MoCA"
rm -f ~/data/moca/*
FILES=`sudo ls $MOCA_SOURCE_PATH`
for FILENAME in $FILES; do
        if [[ $FILENAME == *.csv ]]; then
                echo "Copying:" $FILENAME
                sudo cp $MOCA_SOURCE_PATH/$FILENAME ~/data/moca
        fi
        sudo chown azureuser:azureuser ~/data/moca/*.csv
done




