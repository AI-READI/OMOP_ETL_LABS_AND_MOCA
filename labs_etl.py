#
# labs_etl.py
# primary functions for reading LABS data and writing to OMOP database.
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
import datetime

# configuration imports
import labs_etl_parameters
from labs_etl_parameters import POSTGRES_CONN_STRING_KEY
from labs_etl_parameters import LABS_STANDARDS_MAPPING_CSV_PATH
from labs_etl_parameters import LABS_DATA_DICTIONARY_XLSX_PATH
from labs_etl_parameters import LABS_NT_PROBNP_RANGES_SHEETNAME
from labs_etl_parameters import LABS_ALKALINE_PHOSPHATASE_RANGES_SHEETNAME

from labs_etl_parameters import LABS_SOURCE_DATA_GLOB
from labs_etl_parameters import POSTGRES_OMOP_READ_PERSON_TABLE_NAME

from labs_etl_parameters import POSTGRES_LABS_READ_MEASUREMENT_TABLE_NAME
from labs_etl_parameters import POSTGRES_LABS_READ_OBSERVATION_TABLE_NAME
from labs_etl_parameters import POSTGRES_LABS_READ_VISIT_OCCURENCE_TABLE_NAME
from labs_etl_parameters import POSTGRES_LABS_READ_VISIT_OCCURENCE_CONCEPT_ID

from labs_etl_parameters import POSTGRES_LABS_WRITE_SCHEMA_NAME
from labs_etl_parameters import POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME

from labs_etl_parameters import LABS_OMOP_WRITE_TO_DATABASE

# omop etl utilities
from omop_etl_utils import create_empty_measurement_record
from omop_etl_utils import EQUALS_OMOP_CONCEPT_ID, LESS_THAN_OMOP_CONCEPT_ID
from omop_etl_utils import labs_string_to_date, labs_string_to_datetime, labs_string_to_time
from omop_etl_utils import LAB_OMOP_CONCEPT_ID
from omop_etl_utils import OMOPIDTracker
from omop_etl_utils import dotdict
from omop_etl_utils import get_table_row_count
from omop_etl_utils import OMOPVisitOccurrenceLookup

def normalize_lab_test_name(s):
    # convert to lowercase...
    s = s.lower()
    # split at any open parenthesis
    s = s.split('(')[0]
    # remove any spaces
    s = s.replace(' ', '')

    # special cases...
    # LDL Cholesterol Calculation or LDL Cholesterol (calculated field) convert to LDL Cholesterol 
    if s.startswith("ldlcholesterol"):
        s = "ldlcholesterol"

    # return the normalized part
    return s

def create_standards_completed_lab_mappings():
    MAPPING_COLUMNS_REQUIRED = [
        'Name',
        'Data Type',
        'Reference Interval',
        'Units',
        'TARGET_CONCEPT_ID', 
        'TARGET_CONCEPT_NAME', 
        'TARGET_DOMAIN_ID',
        'TARGET_VOCABULARY_ID',
        'TARGET_CONCEPT_CLASS_ID', 
        'TARGET_CONCEPT_CODE',
    ]

    sys.stderr.write("Reading completed standards lab mappings...")
    
    # load mappings files that are completed and ready for mapping...
    df_labs_mapping = pd.read_csv(LABS_STANDARDS_MAPPING_CSV_PATH)
    df_completed_labs_mappings = df_labs_mapping[lambda df: (df['Map to OMOP?'] == 'Yes') & \
        df.TARGET_CONCEPT_ID.notnull()][MAPPING_COLUMNS_REQUIRED]

    # correct data types...
    df_completed_labs_mappings.TARGET_CONCEPT_ID = df_completed_labs_mappings.TARGET_CONCEPT_ID.astype(int)

    # display mappings
    sys.stderr.write("Display Completed Mappings")
    for index, mapping_row in df_completed_labs_mappings.iterrows():
        sys.stderr.write(f"{index} {str(mapping_row)}\n")
    sys.stderr.write("\n")

    sys.stderr.write("OK.\n")
    
    return df_completed_labs_mappings

class OMOPMapPIDToAgeInYears(object):
    def __init__(self, engine):
        query = text(f"SELECT person_id, year_of_birth FROM {POSTGRES_OMOP_READ_PERSON_TABLE_NAME}")        
        df_temp = pd.read_sql(query, engine)      
        self.pid2age = {int(r.person_id):int(r.year_of_birth) for index, r in df_temp.iterrows()}            
        self.current_year = datetime.date.today().year
        
    def get_age_in_years(self, pid):
        if pid in self.pid2age:
            return self.current_year - self.pid2age[pid] 
        else:
            return None


def age_string_to_fractional_years(s):
    yrs = None
    if s[-1] == 'd':
        yrs = float(s.split('d')[0]) / 365.25
    elif s[-1] == 'm':
        yrs = float(s.split('m')[0]) / 12
    elif s[-1] == 'y':
        yrs = float(s.split('y')[0])
    return yrs        


class NormalRangeLookupTable():
    def __init__(self, xlsx_path, sheetname):
        df_ranges = pd.read_excel(xlsx_path, sheet_name=sheetname)
        df_ranges = df_ranges[lambda df: df.Sex.map(lambda s: s in ('M', 'F'))]
        df_ranges['Age_Low_Years'] = df_ranges.Age_Low.map(age_string_to_fractional_years)
        df_ranges['Age_High_Years'] = df_ranges.Age_High.map(age_string_to_fractional_years)
        self.df_ranges = df_ranges
        
    def lookup_normal_range(self, age_in_years, sex):
        df_temp = self.df_ranges[lambda df: (df.Sex == sex) \
                                 & (age_in_years >= df.Age_Low_Years) \
                                 & (age_in_years <= df.Age_High_Years)]
        if df_temp.shape[0] == 1:
            low = df_temp.iloc[0].Range_Low
            high = df_temp.iloc[0].Range_High
            return (low, high)
        else:
            # some kind of error
            return None

    
def extract_range_from_text(t):
    t = t.strip()
    if '-' in t:
        parts = t.split('-')
        return (float(parts[0]), float(parts[1]))
    elif t[0:2] == '<=':
        return (0.0, float(t[2:]))
    elif t[0:2] == '>=':
        return (float(t[2:]), 0.0)
    elif t[0] == '<':
        return (0.0, float(t[1:]))
    elif t[0] == '>':
        return (float(t[1:]), 0.0)

def compute_superinterval(interval1, interval2):
    low = min(interval1[0], interval2[0])
    high = max(interval1[1], interval2[1])
    return (low, high)


def create_measurement(data_row, data_column_name, mapping_row, utilities):
    # create the measurement record or return None if there is a mistake
    # or if there is no associated person_id
    m = create_empty_measurement_record()
    
    m.person_id = data_row['Participant ID']
    age_in_years = utilities.pid2age_mapper.get_age_in_years(m.person_id)    
    if age_in_years is None:
        # no person_id for this participant, so no lab measurements
        sys.stderr.write(f"Invalid measurement record, person_id = {m.person_id} has no valid age in PERSON table.\n")
        return None
    
    # process concept ids
    m.measurement_concept_id = mapping_row.TARGET_CONCEPT_ID
    m.measurement_type_concept_id = LAB_OMOP_CONCEPT_ID
    m.value_as_concept_id = 0
    m.unit_concept_id = 0 # these have not been mapped by standards yet
    m.provider_id = 0
    m.visit_occurrence_id = 0
    m.visit_detail_id = 0
    
    # process source values
    m.measurement_source_value = data_column_name
    m.measurement_source_concept_id = 0
    
    # process dates and times
    m.measurement_date = labs_string_to_date(data_row['Date of Collection'])
    m.measurement_datetime = labs_string_to_datetime(data_row['Date of Collection'])
    m.measurement_time = labs_string_to_time(data_row['Date of Collection'])
    
    # process unit values
    m.unit_source_value = mapping_row.Units
    m.unit_source_concept_id = 0 # these have not been mapped by standards yet
    
    # process event ids
    m.measurement_event_id = 0
    m.meas_event_field_concept_id = 0
    
    # process value    
    value = str(data_row[data_column_name]) # values can be a mix of floats and str
    m.value_source_value = value
    if value[0] == '<':
        m.value_as_number = float(value[1:])
        m.operator_concept_id = LESS_THAN_OMOP_CONCEPT_ID
    else:
        m.value_as_number = float(value)
        m.operator_concept_id = EQUALS_OMOP_CONCEPT_ID
        
    # process normal ranges
    # this is a little tricky because sometimes the normal range is in the 
    # mapping and sometimes we need to use tables loaded from elsewhere
    # also note that we don't know the sex of the participant, so 
    # assume always Female, since we have to pick something
    if pd.isna(mapping_row['Reference Interval']):
        # no reference range is given
        m.range_low = 0.0
        m.range_high = 0.0
    elif mapping_row.Name == 'NT-proBNP':
        # note: we don't have gender information, so to create the normal range, we use the
        # max/min of the Male and Female values for the appropriate age
        reference_interval_F = utilities.NT_PROBNP_NormalRangeLookup.lookup_normal_range(age_in_years, 'F')
        reference_interval_M = utilities.NT_PROBNP_NormalRangeLookup.lookup_normal_range(age_in_years, 'M')
        if reference_interval_F and reference_interval_M:
            m.range_low, m.range_high = compute_superinterval(reference_interval_F, reference_interval_M)
        else:
            # treat not having a reference interval as a fatal error for now
            return None
    elif mapping_row.Name == 'Alkaline Phosphatase':
        reference_interval_F = utilities.ALKALINE_PHOSPHATASE_NormalRangeLookup.lookup_normal_range(age_in_years, 'F')
        reference_interval_M = utilities.ALKALINE_PHOSPHATASE_NormalRangeLookup.lookup_normal_range(age_in_years, 'M')
        if reference_interval_F and reference_interval_M:
            m.range_low, m.range_high = compute_superinterval(reference_interval_F, reference_interval_M)
        else:
            # treat not having a reference interval as a fatal error for now
            return None
    elif mapping_row.Name == 'Troponin-T':
        # Female: <11; Male <16
        # handle special case for this lab, we need to take the larger of the Female and Male ranges
        fpart, mpart = mapping_row['Reference Interval'].split(';')
        fpart = fpart.split(' ')[-1]
        mpart = mpart.split(' ')[-1]
        ignore, fvalue = extract_range_from_text(fpart)
        ignore, mvalue = extract_range_from_text(mpart)
        m.range_high = max(fvalue, mvalue)
        m.range_low = 0.0
    elif mapping_row.Name == 'ALT (GPT)':
        # Female: 7-33, Male Age 0-49: 10-64, Male Age 50+: 10-48
        # handle special case for this lab, we need to take the larger of the Female and Male ranges
        # complex logic in string, hardcode this here
        # note that we know the age, but not the sex,
        reference_interval_F = (7, 33)
        if age_in_years <= 49:
            reference_interval_M = (10, 64)
        else:
            reference_interval_M = (10, 48)
        m.range_low, m.range_high = compute_superinterval(reference_interval_F, reference_interval_M)
    elif mapping_row.Name == 'Creatinine':
        # Female: 0.38-1.02, Male: 0.51-1.18
        # handle special case for this lab, we need to take the larger of the Female and Male ranges
        fpart, mpart = mapping_row['Reference Interval'].split(',')
        fpart = fpart.split(' ')[-1]
        mpart = mpart.split(' ')[-1]
        reference_interval_F = extract_range_from_text(fpart)
        reference_interval_M = extract_range_from_text(mpart)
        if reference_interval_F and reference_interval_M:
            m.range_low, m.range_high = compute_superinterval(reference_interval_F, reference_interval_M)
        else:
            # treat not having a reference interval as a fatal error for now
            return None
    elif ',' in mapping_row['Reference Interval']:
        parts = mapping_row['Reference Interval'].split(',')
        if 'Female:' in parts[0]:
            t = parts[0].split(':')[-1].strip()
            m.range_low, m.range_high = extract_range_from_text(t)
            pass
        elif 'Female:' in parts[1]:
            t = parts[1].split(':')[-1].strip()
            m.range_low, m.range_high = extract_range_from_text(t)
        elif '>' in parts[0] and '<' in parts[1]:
            low, ignore = extract_range_from_text(parts[0])
            ignore, high = extract_range_from_text(parts[1])
            m.range_low, m.range_high = low, high
        elif '<' in parts[0] and '>' in parts[1]:
            ignore, high = extract_range_from_text(parts[0])
            low, ignore = extract_range_from_text(parts[1])        
            m.range_low, m.range_high = low, high
    elif mapping_row['Reference Interval'][0] == '<':
        m.range_low, m.range_high = extract_range_from_text(mapping_row['Reference Interval'])
    elif mapping_row['Reference Interval'][0] == '>':
        m.range_low, m.range_high = extract_range_from_text(mapping_row['Reference Interval'])
    elif '-' in mapping_row['Reference Interval']:
        m.range_low, m.range_high = extract_range_from_text(mapping_row['Reference Interval'])        
    elif len(mapping_row['Reference Interval']) == 0:
        # no reference range is given
        m.range_low = 0.0
        m.range_high = 0.0
        
    # assign new measurement_id when we are sure that we have
    # a valid new record
    m.measurement_id = utilities.measurementIDTracker.get_next_id()

    # fix up the date of the lab blood draw and insert the visit_occurrence 
    # if we have one in the lookup table...
    visitinfo = utilities.pid2visit_mapper.get_earliest_visit_occurrence_id_and_start_date(m.person_id)
    if visitinfo:
        visit_id, visit_date = visitinfo
        m.visit_occurrence_id = visit_id 
        m.measurement_date = visit_date
        m.measurement_datetime = datetime.datetime(year=visit_date.year, month=visit_date.month, day=visit_date.day)
        m.measurement_time = m.measurement_datetime.time()
    else:
        sys.stderr.write(f"Unable to find blood draw visit_id for person_id {m.person_id}, using date from labs xlsx file.\n")


    # return the valid record
    return m


def process_lab_sheet_row(r, utilities):
    labs_measurements = []
    bad_record_count = 0
    for index, mapping_row in utilities.df_completed_labs_mappings.iterrows():
        # look for a match to create a record
        # just look for prefix matching, since the data has units after the
        # lab name. this is a nest loop, and not greatly fast, but we need
        # to check all the rows.
        # could be optimized by truncating the value names if necessary
        for k in r.keys():
            if normalize_lab_test_name(k) == normalize_lab_test_name(mapping_row.Name):
                # found a matching value, create the measurement
                m = create_measurement(r, k, mapping_row, utilities)
                if m:
                    labs_measurements.append(m)
                else:
                    bad_record_count += 1
                    
    return labs_measurements, bad_record_count


def process_lab_sheet(df_lab_sheet, utilities):
    labs_measurements = []
    bad_record_count = 0
    for index, r in df_lab_sheet.iterrows():
        ms, bad = process_lab_sheet_row(r, utilities)
        labs_measurements.extend(ms)
        bad_record_count += bad
    return labs_measurements, bad_record_count



def safe_participant_id_converstion(s):
    if pd.isna(s):
        # zero is an illegal particpant id and will never match
        # a record in the PERSON table
        return 0
    else:
        s = str(s) # ensure we are dealing with a string
        if s.isnumeric():
            return int(s) # string is an integer
        elif s.replace('.', '').isnumeric():
            return int(float(s)) # string is a float, convert to an integer string
        else:
            # non-numeric string, set it illegal particpant id
            return 0


def process_lab_source_file(filename, utilities):
    LABS_SHEET_NAMES_AND_SKIP_ROWS = {
        'EDTA Plasma' : [0, 1], 
        'Serum' : [0], 
        'Whole blood' : [0], 
        'Urine' : [0],        
    }
    
    labs_measurements = []
    bad_record_count = 0    
    # loop over the lab sheets, process each sheet into measurements
    for sn, sr in LABS_SHEET_NAMES_AND_SKIP_ROWS.items():
        sys.stderr.write(f"\t Processing Sheet = '{sn}'.\n")
        df_lab_sheet = pd.read_excel(filename, sheet_name=sn, skiprows=sr)

        # remove any blank rows, these are found by having an NA value in particpant ID...
        df_lab_sheet = df_lab_sheet[lambda df: df['Participant ID'].notna()].reset_index(drop=True).copy()

        # need to clean up Participant ID so that it is always integer
        # since sometimes Excel thinks that it is text that looks like a float
        df_lab_sheet['Participant ID'] = df_lab_sheet['Participant ID'].map(safe_participant_id_converstion)
        df_lab_sheet['Participant ID']  =  df_lab_sheet['Participant ID'].astype(int)

        ms, bad = process_lab_sheet(df_lab_sheet, utilities)
        labs_measurements.extend(ms)
        bad_record_count += bad
        
    return labs_measurements, bad_record_count


def display_labs_configuration_parameters():
    sys.stderr.write("Configuration Parameters:\n")
    for name, value in vars(labs_etl_parameters).items():
        if name[0] != '_':
            sys.stderr.write(f"\t{name} = '{value}'\n")


def process_labs_etl():
    # begin timing
    sys.stderr.write(f"Starting process_labs_etl().\n")
    display_labs_configuration_parameters()                        
    start = time.time()
    
    # connect to the omop database
    engine = create_engine(POSTGRES_CONN_STRING_KEY)
    connection = engine.connect()    

    # create utility objects
    utilities = dotdict()
    utilities.df_completed_labs_mappings = create_standards_completed_lab_mappings()
    utilities.measurementIDTracker = OMOPIDTracker(POSTGRES_LABS_READ_MEASUREMENT_TABLE_NAME, 'measurement_id', engine)
    utilities.ALKALINE_PHOSPHATASE_NormalRangeLookup = NormalRangeLookupTable(LABS_DATA_DICTIONARY_XLSX_PATH, LABS_ALKALINE_PHOSPHATASE_RANGES_SHEETNAME)
    utilities.NT_PROBNP_NormalRangeLookup = NormalRangeLookupTable(LABS_DATA_DICTIONARY_XLSX_PATH, LABS_NT_PROBNP_RANGES_SHEETNAME)
    utilities.pid2age_mapper = OMOPMapPIDToAgeInYears(engine)
    utilities.pid2visit_mapper = OMOPVisitOccurrenceLookup(POSTGRES_LABS_READ_VISIT_OCCURENCE_TABLE_NAME, POSTGRES_LABS_READ_VISIT_OCCURENCE_CONCEPT_ID, engine)

    # extract data measurement records
    labs_measurements = []
    bad_record_count = 0
    # loop over lab source data files
    for filename in glob.glob(LABS_SOURCE_DATA_GLOB):
        sys.stderr.write(f"Processing file '{filename}'.\n")
        ms, bad = process_lab_source_file(filename, utilities)
        labs_measurements.extend(ms)
        bad_record_count += bad
    sys.stderr.write(f"Found {len(labs_measurements)} valid records and rejected {bad_record_count} invalid records.\n")

    if LABS_OMOP_WRITE_TO_DATABASE:
        # write measurement records to OMOP database as append...
        n_before = get_table_row_count(POSTGRES_LABS_WRITE_SCHEMA_NAME, POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME, engine)    
        df_new_measurements = pd.DataFrame([dict(m) for m in labs_measurements])        
        ignore = df_new_measurements.to_sql(POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME, schema=POSTGRES_LABS_WRITE_SCHEMA_NAME, 
                                                if_exists='append', index=False, con=engine)
        n_wrote = get_table_row_count(POSTGRES_LABS_WRITE_SCHEMA_NAME, POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME, engine) - n_before        
        sys.stderr.write(f"Appended {n_wrote} MEASUREMENT records to table '{POSTGRES_LABS_WRITE_SCHEMA_NAME}.{POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME}'.\n")
    else:
        sys.stderr.write("*** Skipping writing records to database.***\n")
        sys.stderr.write("Set configuration option LABS_OMOP_WRITE_TO_DATABASE to True to enable write.\n")
        sys.stderr.write("*** Printing records to stdout for debugging.***\n")
        for index, row in enumerate(labs_measurements):
            sys.stdout.write(f"{index} {str(row)}\n")

    # close database connection
    connection.close()

    # end timing
    elapsed = time.time() - start    
    sys.stderr.write(f"Completed process_labs_etl() in {elapsed:0.2f} seconds.\n")


