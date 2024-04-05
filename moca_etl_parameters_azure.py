# postgres database connection parameters
#POSTGRES_CONN_STRING_KEY='postgresql://cohenaa@localhost:5432/omop'
#POSTGRES_CONN_STRING_KEY='postgresql://postgres:$postgres@10.0.0.4/omop?options=-csearch_path%3Daireadi_omop'
POSTGRES_CONN_STRING_KEY='postgresql://postgres:$postgres@10.1.0.6/omop?options=-csearch_path%3Daireadi_omop'

# postgres OMOP read and write table names within correct schema
POSTGRES_OMOP_READ_PERSON_TABLE_NAME='aireadi_omop.person'
POSTGRES_MOCA_READ_MEASUREMENT_TABLE_NAME='aireadi_omop.measurement'
POSTGRES_MOCA_READ_OBSERVATION_TABLE_NAME='aireadi_omop.observation'
POSTGRES_MOCA_WRITE_SCHEMA_NAME = 'aireadi_omop'
POSTGRES_MOCA_WRITE_MEASUREMENT_TABLE_NAME='measurement'
POSTGRES_MOCA_WRITE_OBSERVATION_TABLE_NAME='observation'

# path to the standard mapping csv file
STANDARDS_MAPPING_CSV_PATH='/home/azureuser/omop_etl_labs_and_moca/MOCA/STANDARDS_MAPPINGS/0102_2024-04-05_MOCA_Data_Dictionary_Mappings.csv'

# glob wildcard path to the raw MOCA source data files
MOCA_SOURCE_DATA_GLOB = '/home/azureuser/data/moca/MOCA-latest.csv'

# control writing to the OMOP database, for debugging
MOCA_OMOP_WRITE_TO_DATABASE = True







