#
# moca_etl.py
# primary functions for reading MoCA data and writing to OMOP database.
#

# general imports
import sys
import os
import os.path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import glob
import time
import re

# utility imports
from omop_etl_utils import create_empty_measurement_record
from omop_etl_utils import create_empty_observation_record
from omop_etl_utils import moca_string_to_date, moca_string_to_datetime, moca_string_to_time
from omop_etl_utils import STANDARD_ALGORITHM_OMOP_CONCEPT_ID, EQUALS_OMOP_CONCEPT_ID
from omop_etl_utils import OMOPIDTracker
from omop_etl_utils import get_table_row_count

# configurable parameter imports
import moca_etl_parameters
from moca_etl_parameters import POSTGRES_CONN_STRING_KEY
from moca_etl_parameters import STANDARDS_MAPPING_CSV_PATH
from moca_etl_parameters import MOCA_SOURCE_DATA_GLOB

from moca_etl_parameters import POSTGRES_OMOP_READ_PERSON_TABLE_NAME

from moca_etl_parameters import POSTGRES_MOCA_READ_MEASUREMENT_TABLE_NAME
from moca_etl_parameters import POSTGRES_MOCA_READ_OBSERVATION_TABLE_NAME

from moca_etl_parameters import POSTGRES_MOCA_WRITE_SCHEMA_NAME
from moca_etl_parameters import POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME
from moca_etl_parameters import POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME

from moca_etl_parameters import MOCA_OMOP_WRITE_TO_DATABASE

##SRC Added 10-30-24 This is part of getting the phys assess date from redcap
from moca_etl_parameters import redcap_report

#define constants for data collection types
# these extension values are defined by the AIREADI Standards Team
# SRC_CODE = app_generated | manually_entered_from_paper
MOCA_AUTOMATED_observation_type_concept_id = None
MOCA_MANUAL_observation_type_concept_id = None
MOCA_AUTOMATED_measurement_type_concept_id = None
MOCA_MANUAL_measurement_type_concept_id = None


###SRC Added 10-30-24, read the redcap data to use the physical assessment date for MOCA
def initialize_redcap(filename):
    redcap=pd.read_csv(filename)
    rcvars=redcap.dtypes
    return redcap

def display_moca_configuration_parameters():
    sys.stderr.write("Configuration Parameters:\n")
    for name, value in vars(moca_etl_parameters).items():
        if name[0] != '_':
            sys.stderr.write(f"\t{name} = '{value}'\n")
                    
    
class MoCAValidityChecker(object):
    def __init__(self, tablename, idfieldname, engine):
        query = text(f"SELECT {idfieldname} FROM {tablename}")
        df_temp = pd.read_sql(query, engine)      
        self.ids = set(df_temp[idfieldname])
            
    def is_valid_measurement(self, m):
        return isinstance(m['person_id'], int) and (m['person_id'] in self.ids) 

    def is_valid_observation(self, o):
        return isinstance(o['person_id'], int) and (o['person_id'] in self.ids) 

# compile regular expression for extraction time from 
# minutes, seconds string
MINSEC_REGEX = re.compile(r'(\d+)\s*mins?\s*(\d+)secs?')
def convert_duration_string_to_seconds(s):
    mo = MINSEC_REGEX.match(s)
    return 60*int(mo.group(1)) + int(mo.group(2))

def read_moca_mappings():
    # set constants
    global MOCA_AUTOMATED_observation_type_concept_id
    global MOCA_MANUAL_observation_type_concept_id
    global MOCA_AUTOMATED_measurement_type_concept_id
    global MOCA_MANUAL_measurement_type_concept_id

    # read the mapping file
    df_mapping = pd.read_csv(STANDARDS_MAPPING_CSV_PATH)    

    # read and set the automated vs. manual concept ids constants
    # app_generated
    # manually_entered_from_paper
    MOCA_AUTOMATED_observation_type_concept_id = \
        int(df_mapping[lambda df: df['SRC_CODE'] == 'app_generated'].iloc[0]['TARGET_CONCEPT_ID'])
    MOCA_AUTOMATED_measurement_type_concept_id = MOCA_AUTOMATED_observation_type_concept_id

    MOCA_MANUAL_observation_type_concept_id = \
        int(df_mapping[lambda df: df['SRC_CODE'] == 'manually_entered_from_paper'].iloc[0]['TARGET_CONCEPT_ID'])
    MOCA_MANUAL_measurement_type_concept_id = MOCA_MANUAL_observation_type_concept_id

    # load mappings files that are completed and ready for mapping...
    MAPPING_COLUMNS_REQUIRED = [
        'SRC_CODE',
        'Data_Type',
        'Value_Range',
        'TARGET_CONCEPT_ID', 
        'TARGET_CONCEPT_NAME', 
        'TARGET_DOMAIN_ID',
    ]

    df_completed_mappings = df_mapping[lambda df: (df['Protected_or_open-source'] == 'open-source') & \
                                                    (df['Map_to_OMOP'] == 'Yes') & \
                                                    df.TARGET_CONCEPT_ID.notnull()][MAPPING_COLUMNS_REQUIRED]

    # correct data types...
    df_completed_mappings.TARGET_CONCEPT_ID = df_completed_mappings.TARGET_CONCEPT_ID.astype(int)

    # output for logging and debugging...
    sys.stderr.write("Completed valid MOCA OMOP mappings:\n")
    sys.stderr.write(str(df_completed_mappings.reset_index(drop=True)))
    sys.stderr.write('\n')

    return df_completed_mappings

def safe_integer_converstion(s):
    s = str(s) # ensure we are dealing with a string
    if s.isnumeric():
        return s # string is an integer
    elif s.replace('.', '').isnumeric():
        return str(int(float(s))) # string is a float, convert to an integer string
    else:
        # return string as is, we will catch it later
        return s

def load_raw_moca_data():
    # load the data files
    df_moca_data = None
    for filepattern in MOCA_SOURCE_DATA_GLOB.split(';'):
        for filename in glob.glob(filepattern):
            sys.stderr.write(f"Reading MoCA data file: {filename}\n")
            df_temp = pd.read_csv(filename)
            # add source simple filename to the moca data table
            df_temp['source_filename'] = os.path.split(filename)[1]
            # accumulate data
            if df_moca_data is None:
                df_moca_data = df_temp
            else:
                df_moca_data = pd.concat((df_moca_data, df_temp), axis=0)
    
    # remove any records that are blank, for our purposes, if the Institute File number
    # is NaN, then the line is blank...
    df_moca_data = df_moca_data[lambda df: df['Institute File number'].notna()].reset_index(drop=True).copy()

    # clean up Institute File number...
    df_moca_data['Institute File number'] = df_moca_data['Institute File number'].map(safe_integer_converstion)

    return df_moca_data    


def create_single_measurement_record(moca_record, mapping_row,redcap):
    # first check to ensure that the column value is not NaN
    if pd.isna(moca_record[mapping_row['SRC_CODE']]):
        # don't create a measurement record for this value
        return None

    # create empty new record
    m = create_empty_measurement_record()
    m.measurement_id = 0 # filled in later

    # set person id field
    if moca_record['Institute File number'].isnumeric():
        m.person_id = int(moca_record['Institute File number'])
    else:
        m.person_id = moca_record['Institute File number']
    
    # set boilerplate fields    
    m.measurement_concept_id = mapping_row.TARGET_CONCEPT_ID
    m.operator_concept_id = EQUALS_OMOP_CONCEPT_ID
    m.unit_concept_id = 0    
    m.provider_id = 0
    m.visit_occurrence_id = 0
    m.visit_detail_id = 0
    m.measurement_source_value = mapping_row['SRC_CODE']
    m.measurement_source_concept_id = 0
    m.unit_source_value = ''
    m.unit_source_concept_id = 0
    m.measurement_event_id = 0
    m.meas_event_field_concept_id = 0

    # compute measurement_type_concept_id
    if moca_record.source_filename.lower().find('paper') >= 0:
        m.measurement_type_concept_id = MOCA_MANUAL_measurement_type_concept_id
    else:
        m.measurement_type_concept_id = MOCA_AUTOMATED_measurement_type_concept_id

    # set date and time fields...
    #print(moca_record['test_upload_date'],moca_record['Institute File number'])
    
    ##SRC Added 10-30-24 get the physical assessment date from redcap
    #id=moca_record['Institute File number'])
    #print(moca_record['Institute File number'],redcap[(redcap["studyid"]==id)].shape[0])
    if redcap[(redcap["studyid"]==moca_record['Institute File number'])].shape[0] !=0:
       physical_assess_date=redcap.loc[(redcap["studyid"]==moca_record['Institute File number'])].pacmpdat.item()
    else:
        physical_assess_date='01/01/2001'
    
    #m.measurement_date = moca_string_to_date(moca_record['test_upload_date'])
    #m.measurement_datetime = moca_string_to_datetime(moca_record['test_upload_date'])
    #m.measurement_time = moca_string_to_time(moca_record['test_upload_date'])        
    
    ##SRC Added 10-30-24 Use the pa date
    #print(physical_assess_date)
    m.measurement_date = moca_string_to_date(physical_assess_date)
    m.measurement_datetime = moca_string_to_datetime(physical_assess_date)
    m.measurement_time = moca_string_to_time(physical_assess_date)      

    # DEBUGGING
    #print() 
    #print(mapping_row)
    #print(moca_record)
    #print() 

    # set computed value fields...
    raw_value_text = str(moca_record[mapping_row['SRC_CODE']])
    m.value_source_value = raw_value_text
    if mapping_row['Data_Type'] == 'Integer':
        m.value_as_number = float(raw_value_text)
        m.value_as_concept_id = 0
        value_range = mapping_row['Value_Range']
        if pd.isna(value_range):
            # no range given
            m.range_low = 0.0
            m.range_high = 0.0
        elif value_range and (value_range.find('-') >= 0):
            parts = value_range.split('-')
            m.range_low = float(parts[0])
            m.range_high = float(parts[1])
        else:
            # no range given
            m.range_low = 0.0
            m.range_high = 0.0

    # DEBUG CODE
    #print(m)
    #print()

    # return as dotdict
    return m


def create_measurement_records(moca_record, df_mappings,redcap):
    moca_measurements = []
    for index, r in df_mappings.iterrows():
        if r.TARGET_DOMAIN_ID == 'Measurement':
            m = create_single_measurement_record(moca_record, r,redcap)
            if m:
                moca_measurements.append(m)
    return moca_measurements


def create_single_observation_record(moca_record, mapping_row,redcap):
    # first check to ensure that the column value is not NaN
    if pd.isna(moca_record[mapping_row['SRC_CODE']]):
        # don't create an observation record for this value
        return None

    # create empty new record
    o = create_empty_observation_record()
    o.observation_id = 0 # filled in later

    # set person id field
    if moca_record['Institute File number'].isnumeric():
        o.person_id = int(moca_record['Institute File number'])
    else:
        o.person_id = moca_record['Institute File number']
        
    # set boilerplate fields
    o.observation_concept_id = mapping_row.TARGET_CONCEPT_ID
    o.qualifier_concept_id = 0
    o.unit_concept_id = 0  
    o.provider_id = 0
    o.visit_occurrence_id = 0
    o.visit_detail_id = 0
    o.observation_source_value = mapping_row['SRC_CODE']
    o.observation_source_concept_id = 0
    o.unit_source_value = ''
    o.qualifier_source_value = ''
    o.observation_event_id = 0
    o.obs_event_field_concept_id = 0
    
    # compute observation_type_concept_id
    if moca_record.source_filename.lower().find('paper') >= 0:
        o.observation_type_concept_id = MOCA_MANUAL_observation_type_concept_id
    else:
        o.observation_type_concept_id = MOCA_AUTOMATED_observation_type_concept_id

    # set date and time fields...
    
    ##SRC Added 10-30-24 get the physical assessment date from redcap
    if redcap[(redcap["studyid"]==moca_record['Institute File number'])].shape[0] !=0:
       physical_assess_date=redcap.loc[(redcap["studyid"]==moca_record['Institute File number'])].pacmpdat.item()
    else:
        physical_assess_date='01/01/2001'
    
    #o.observation_date = moca_string_to_date(moca_record['test_upload_date'])
    #o.observation_datetime = moca_string_to_datetime(moca_record['test_upload_date'])
    
    ##SRC Added 10-30-24 Use the pa date
    o.observation_date = moca_string_to_date(physical_assess_date)
    o.observation_datetime = moca_string_to_datetime(physical_assess_date)
    
    # set computed value fields...
    raw_value_text = moca_record[mapping_row['SRC_CODE']]
    o.value_source_value = str(raw_value_text)
    o.value_as_string = str(raw_value_text).strip()
    o.value_as_concept_id = 0 # nothing to put in here so far
    if mapping_row['Data_Type'] == 'Integer':
        o.value_as_number = float(raw_value_text)
    elif mapping_row['Data_Type'] == 'Time Duration':
        # special handling for moca_total_score_time or any other Time Duration 
        # data type must convert string MM mins SS secs to value_as_number_seconds
        o.value_as_number = float(convert_duration_string_to_seconds(raw_value_text))      
    else:
        o.value_as_number = 0.0
    
    # DEBUG CODE
    #print(o)
    #print()

    # return as dotdict
    return o
        

def create_observation_records(moca_record, df_mappings,redcap):
    moca_observations = []
    for index, r in df_mappings.iterrows():
        if r.TARGET_DOMAIN_ID == 'Observation':
            o = create_single_observation_record(moca_record, r,redcap)
            if o:
                moca_observations.append(o)
    return moca_observations


def process_moca_etl():
    # begin timing
    sys.stderr.write(f"Starting process_moca_etl().\n")
    display_moca_configuration_parameters()                        
    start = time.time()
    
    # connect to the OMOP database
    engine = create_engine(POSTGRES_CONN_STRING_KEY)
    connection = engine.connect()    
    
    # read and configure the mappings as well as the constant codes for manual vs. automated values...   
    df_completed_mappings = read_moca_mappings()

    # display the read in defined constant codes for manual vs. automated values...    
    sys.stderr.write('\n')
    sys.stderr.write(f'MOCA_AUTOMATED_observation_type_concept_id and MOCA_AUTOMATED_measurement_type_concept_id set to {MOCA_AUTOMATED_observation_type_concept_id},{MOCA_AUTOMATED_measurement_type_concept_id} from mapping file.\n')
    sys.stderr.write(f'MOCA_MANUAL_observation_type_concept_id and MOCA_MANUAL_measurement_type_concept_id set to {MOCA_MANUAL_observation_type_concept_id},{MOCA_MANUAL_measurement_type_concept_id} from mapping file.\n')
    sys.stderr.write('\n')

    ###SRC Added 10.30.24 Read in the redcap report to get the phys assess date
    redcap=initialize_redcap(redcap_report)
    redcap['studyid']=redcap['studyid'].astype(str)
    #print(redcap['studyid'].dtypes)

    # read the raw moca data
    df_moca_data = load_raw_moca_data()
    sys.stderr.write(f"Read {df_moca_data.shape[0]} raw MoCA records:\n")
    sys.stderr.write(str(df_moca_data))
    sys.stderr.write('\n')

    # process moca data into records...
    # these records do not have the measurement_id and observation_id filled in unti later!
    moca_measurements = []
    moca_observations = []
    ##SRC
    #df_moca_data['Institute File number'].astype(int)
    #print(df_moca_data.dtypes)
    for index, r in df_moca_data.iterrows():
        #print(redcap[(redcap["studyid"]==r['Institute File number'])] )
        mms = create_measurement_records(r, df_completed_mappings,redcap)
        moca_measurements.extend(mms)

        mos = create_observation_records(r, df_completed_mappings,redcap)
        moca_observations.extend(mos)    

    sys.stderr.write(f"Created {len(moca_measurements)} new MEASUREMENT records from MoCA data.\n")
    sys.stderr.write(f"Created {len(moca_observations)} new OBSERVATION records from MoCA data.\n")
            
    # remove any records that don't meet checking criteria
    # for now the person_ids must be integers, and the participant ids
    # must be present in the person table
    checker = MoCAValidityChecker(POSTGRES_OMOP_READ_PERSON_TABLE_NAME, 'person_id', engine)
    moca_measurements = [m for m in moca_measurements if checker.is_valid_measurement(m)]
    moca_observations = [o for o in moca_observations if checker.is_valid_observation(o)]    
    sys.stderr.write(f"Validity checking found {len(moca_measurements)} valid MEASUREMENT records.\n")
    sys.stderr.write(f"Validity checking found {len(moca_observations)} valid OBSERVATION records.\n")
    
    # initialize record id trackers...
    measurementIDTracker = OMOPIDTracker(POSTGRES_MOCA_READ_MEASUREMENT_TABLE_NAME, 'measurement_id', engine)
    observationIDTracker = OMOPIDTracker(POSTGRES_MOCA_READ_OBSERVATION_TABLE_NAME, 'observation_id', engine)
    
    # for the valid records, we need to add the measurement_id and observeration_ids...
    for m in moca_measurements:
        m.measurement_id = measurementIDTracker.get_next_id();
    sys.stderr.write(f"Filled in measurement_id for valid MEASUREMENT records.\n")
    for o in moca_observations:
        o.observation_id = observationIDTracker.get_next_id();
    sys.stderr.write(f"Filled in observation_id for valid OBSERVATION records.\n")
        
    # create new measurements and observations data frame in preparation to write to database
    df_new_measurements = pd.DataFrame([dict(m) for m in moca_measurements])
    sys.stderr.write(f"Created dataframe with {df_new_measurements.shape[0]} new MEASUREMENT records.\n")
    df_new_observations = pd.DataFrame([dict(o) for o in moca_observations])    
    sys.stderr.write(f"Created dataframe with {df_new_observations.shape[0]} new OBSERVATION records.\n")

    if MOCA_OMOP_WRITE_TO_DATABASE:    
        # write measurement records to table as append...
        # pd.to_sql does not return the total number of rows written,
        # so we have to compute this for ourselves...
        n_before = get_table_row_count(POSTGRES_MOCA_WRITE_SCHEMA_NAME, POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME, engine)    
        ignore = df_new_measurements.to_sql(POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME, schema=POSTGRES_MOCA_WRITE_SCHEMA_NAME, 
                               if_exists='append', index=False, con=engine)    
        n_wrote = get_table_row_count(POSTGRES_MOCA_WRITE_SCHEMA_NAME, POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME, engine) - n_before        
        sys.stderr.write(f"Appended {n_wrote} MEASUREMENT records to table '{POSTGRES_MOCA_WRITE_SCHEMA_NAME}.{POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME}'.\n")
    else:
        sys.stderr.write("*** Skipping writing MEASUREMENT records to database.***\n")
        sys.stderr.write("Set configuration option MOCA_OMOP_WRITE_TO_DATABASE to True to enable write.\n")
        
    if MOCA_OMOP_WRITE_TO_DATABASE:    
        # write observation records to table as append...
        n_before = get_table_row_count(POSTGRES_MOCA_WRITE_SCHEMA_NAME, POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME, engine)    
        ignore = df_new_observations.to_sql(POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME, schema=POSTGRES_MOCA_WRITE_SCHEMA_NAME,  
                               if_exists='append', index=False, con=engine)
        n_wrote = get_table_row_count(POSTGRES_MOCA_WRITE_SCHEMA_NAME, POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME, engine) - n_before        
        sys.stderr.write(f"Appended {n_wrote} OBSERVATION records to table '{POSTGRES_MOCA_WRITE_SCHEMA_NAME}.{POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME}'.\n")
    else:
        sys.stderr.write("*** Skipping writing OBSERVATION records to database.***\n")
        sys.stderr.write("Set configuration option MOCA_OMOP_WRITE_TO_DATABASE to True to enable write.\n")
        
    # close database connection
    connection.close()

    # end timing
    elapsed = time.time() - start    
    sys.stderr.write(f"Completed process_moca_etl() in {elapsed:0.2f} seconds.\n")
