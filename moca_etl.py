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

# utility imports
from omop_etl_utils import create_empty_measurement_record
from omop_etl_utils import create_empty_observation_record
from omop_etl_utils import moca_string_to_date, moca_string_to_datetime, moca_string_to_time
from omop_etl_utils import STANDARD_ALGORITHM_OMOP_CONCEPT_ID, EQUALS_OMOP_CONCEPT_ID
from omop_etl_utils import OMOPIDTracker

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

    
def read_moca_mappings():
    # read the mapping file
    df_mapping = pd.read_csv(STANDARDS_MAPPING_CSV_PATH)    

    # load mappings files that are completed and ready for mapping...
    MAPPING_COLUMNS_REQUIRED = [
        'MOCA File Fieldname',
        'Data Type',
        'Value Range',
        'TARGET_CONCEPT_ID', 
        'TARGET_CONCEPT_NAME', 
        'TARGET_DOMAIN_ID',
    ]

    df_completed_mappings = df_mapping[lambda df: (df['Map to OMOP?'] == 'Yes') & df.TARGET_CONCEPT_ID.notnull()] \
        [MAPPING_COLUMNS_REQUIRED]

    # correct data types...
    df_completed_mappings.TARGET_CONCEPT_ID = df_completed_mappings.TARGET_CONCEPT_ID.astype(int)

    return df_completed_mappings
    
    
def load_raw_moca_data():
    # load the data files
    df_moca_data = None
    for filename in glob.glob(MOCA_SOURCE_DATA_GLOB):
        df_temp = pd.read_csv(filename)
        if df_moca_data is None:
            df_moca_data = df_temp
        else:
            df_moca_data = pd.concat((df_moca_data, df_temp), axis=0)
    
    return df_moca_data    


def create_single_measurement_record(moca_record, mapping_row):
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
    m.measurement_type_concept_id = STANDARD_ALGORITHM_OMOP_CONCEPT_ID
    m.operator_concept_id = EQUALS_OMOP_CONCEPT_ID
    m.unit_concept_id = 0    
    m.provider_id = 0
    m.visit_occurrence_id = 0
    m.visit_detail_id = 0
    m.measurement_source_value = mapping_row['MOCA File Fieldname']
    m.measurement_source_concept_id = 0
    m.unit_source_value = ''
    m.unit_source_concept_id = 0
    m.measurement_event_id = 0
    m.meas_event_field_concept_id = 0

    # set date and time fields...
    m.measurement_date = moca_string_to_date(moca_record['Test Upload Date'])
    m.measurement_datetime = moca_string_to_datetime(moca_record['Test Upload Date'])
    m.measurement_time = moca_string_to_time(moca_record['Test Upload Date'])          
    
    # set computed value fields...
    raw_value_text = str(moca_record[mapping_row['MOCA File Fieldname']])
    m.value_source_value = raw_value_text
    if mapping_row['Data Type'] == 'Integer':
        m.value_as_number = float(raw_value_text)
        m.value_as_concept_id = 0
        value_range = mapping_row['Value Range']
        if value_range and (value_range.find('-') >= 0):
            parts = value_range.split('-')
            m.range_low = float(parts[0])
            m.range_high = float(parts[1])
            
    # return as dotdict
    return m


def create_measurement_records(moca_record, df_mappings):
    moca_measurements = []
    for index, r in df_mappings.iterrows():
        if r.TARGET_DOMAIN_ID == 'Measurement':
            moca_measurements.append(create_single_measurement_record(moca_record, r))
    return moca_measurements


def create_single_observation_record(moca_record, mapping_row):
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
    o.observation_type_concept_id = STANDARD_ALGORITHM_OMOP_CONCEPT_ID
    o.qualifier_concept_id = 0
    o.unit_concept_id = 0  
    o.provider_id = 0
    o.visit_occurrence_id = 0
    o.visit_detail_id = 0
    o.observation_source_value = mapping_row['MOCA File Fieldname']
    o.observation_source_concept_id = 0
    o.unit_source_value = ''
    o.qualifier_source_value = ''
    o.observation_event_id = 0
    o.obs_event_field_concept_id = 0
    
    # set date and time fields...
    o.observation_date = moca_string_to_date(moca_record['Test Upload Date'])
    o.observation_datetime = moca_string_to_datetime(moca_record['Test Upload Date'])
    
    # set computed value fields...
    raw_value_text = moca_record[mapping_row['MOCA File Fieldname']]
    o.value_source_value = str(raw_value_text)
    o.value_as_string = str(raw_value_text).strip()
    o.value_as_concept_id = 0 # nothing to put in here so far
    if mapping_row['Data Type'] == 'Integer':
        o.value_as_number = float(raw_value_text)
    else:
        o.value_as_number = 0.0
        
    # return as dotdict
    return o
        

def create_observation_records(moca_record, df_mappings):
    moca_observations = []
    for index, r in df_mappings.iterrows():
        if r.TARGET_DOMAIN_ID == 'Observation':
            moca_observations.append(create_single_observation_record(moca_record, r))
    return moca_observations


def process_moca_etl():
    # begin timing
    sys.stderr.write(f"Starting process_moca_etl().\n")
    display_moca_configuration_parameters()                        
    start = time.time()
    
    # connect to the OMOP database
    engine = create_engine(POSTGRES_CONN_STRING_KEY)
    connection = engine.connect()    
    
    # read and configure the mappings
    df_completed_mappings = read_moca_mappings()
    
    # read the raw moca data
    df_moca_data = load_raw_moca_data()
    sys.stderr.write(f"Read {df_moca_data.shape[0]} raw MoCA records.\n")
    
    # process moca data into records...
    # these records do not have the measurement_id and observation_id filled in unti later!
    moca_measurements = []
    moca_observations = []
    for index, r in df_moca_data.iterrows():
        mms = create_measurement_records(r, df_completed_mappings)
        moca_measurements.extend(mms)

        mos = create_observation_records(r, df_completed_mappings)
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
        
    # write measurement records to table as append...
    df_new_measurements = pd.DataFrame([dict(m) for m in moca_measurements])    
    n_wrote = df_new_measurements.to_sql(POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME, schema=POSTGRES_MOCA_WRITE_SCHEMA_NAME, 
                           if_exists='append', index=False, con=engine)    
    sys.stderr.write(f"Appended {n_wrote} MEASUREMENT records to table '{POSTGRES_MOCA_WRITE_SCHEMA_NAME}.{POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME}'.\n")
    
    # write observation records to table as append...
    df_new_observations = pd.DataFrame([dict(o) for o in moca_observations])    
    n_wrote = df_new_observations.to_sql(POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME, schema=POSTGRES_MOCA_WRITE_SCHEMA_NAME,  
                           if_exists='append', index=False, con=engine)
    sys.stderr.write(f"Appended {n_wrote} OBSERVATION records to table '{POSTGRES_MOCA_WRITE_SCHEMA_NAME}.{POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME}'.\n")
    
    # close database connection
    connection.close()

    # end timing
    elapsed = time.time() - start    
    sys.stderr.write(f"Completed process_moca_etl() in {elapsed:0.2f} seconds.\n")
