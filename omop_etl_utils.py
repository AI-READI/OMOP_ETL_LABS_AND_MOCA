#
# omop_etl_utils.py
#
# constants, functions, and structures useful for
# several source data type ETL processes.
#
import pandas as pd
from sqlalchemy import text

# omop concepts used
STANDARD_ALGORITHM_OMOP_CONCEPT_ID = 32880
LAB_OMOP_CONCEPT_ID = 32856
EQUALS_OMOP_CONCEPT_ID = 4172703
LESS_THAN_OMOP_CONCEPT_ID = 4171756

# helpful classes

# dotdict() should be part of the standard library, but it is not.
# similar classes in the standard library, such as the namedtuple in the collections package
# are not drop in replacements and are not as convenient,
# this makes a lot of the ETL record code cleaner and simpler, so we just define it here,
# since it is so short and simple.
class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    
# omop structures
def create_empty_measurement_record():
    # based on OMOP CDM 5.4
    dx = {'measurement_id':0, 
          'person_id':0,
          'measurement_concept_id':0,
          'measurement_date':None, 
          'measurement_datetime':None, 
          'measurement_time':None,
          'measurement_type_concept_id':0, 
          'operator_concept_id':0, 
          'value_as_number':0.0,
          'value_as_concept_id':0, 
          'unit_concept_id':0, 
          'range_low':0.0, 
          'range_high':0.0,
          'provider_id':0, 
          'visit_occurrence_id':0, 
          'visit_detail_id':0,
          'measurement_source_value':'', 
          'measurement_source_concept_id':0,
          'unit_source_value':'', 
          'unit_source_concept_id':0, 
          'value_source_value':'',
          'measurement_event_id':0, 
          'meas_event_field_concept_id':0}
    # return a dotdict to save a lot of biolerplate typing
    # needs to be changed back into a dict when using in DataFrame
    return dotdict(dx)


def create_empty_observation_record():
    # based on OMOP CDM 5.4
    dx = {'observation_id':0, 
          'person_id':0, 
          'observation_concept_id':0,
          'observation_date':None,
          'observation_datetime':None,
          'observation_type_concept_id':0, 
          'value_as_number':0.0, 
          'value_as_string':'',
          'value_as_concept_id':0, 
          'qualifier_concept_id':0, 
          'unit_concept_id':0,
          'provider_id':0, 
          'visit_occurrence_id':0, 
          'visit_detail_id':0,
          'observation_source_value':'', 
          'observation_source_concept_id':0,
          'unit_source_value':'', 
          'qualifier_source_value':'', 
          'value_source_value':'',
          'observation_event_id':0, 
          'obs_event_field_concept_id':0}
    # return a dotdict to save a lot of biolerplate typing
    # needs to be changed back into a dict when using in DataFrame
    return dotdict(dx)

def moca_string_to_date(s):
    return pd.to_datetime(s, format='%d-%m-%Y').date()

def moca_string_to_datetime(s):
    return pd.to_datetime(s, format='%d-%m-%Y')    

def moca_string_to_time(s):
    return pd.to_datetime(s, format='%d-%m-%Y').time()

def labs_string_to_date(s):
    return pd.to_datetime(s, format='%m/%d/%Y').date()

def labs_string_to_datetime(s):
    return pd.to_datetime(s, format='%m/%d/%Y')    

def labs_string_to_time(s):
    return pd.to_datetime(s, format='%m/%d/%Y').time()


class OMOPIDTracker():
    def __init__(self, tablename, idfieldname, engine):
        self.next_id = None
        query = text(f"SELECT MAX({idfieldname}) FROM {tablename}")
        try:
            df_temp = pd.read_sql(query, engine)      
            self.next_id = df_temp.iloc[0]['max'] + 1
        except:
            self.next_id = 1
            
        
    def get_next_id(self):
        nid = self.next_id
        self.next_id += 1
        return nid
        
def get_table_row_count(schema_name, table_name, engine):
    query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    df_temp = pd.read_sql(query, engine)      
    return df_temp.iloc[0, 0]   
    

