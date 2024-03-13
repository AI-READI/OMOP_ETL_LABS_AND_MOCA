# postgres database connection parameters
#POSTGRES_CONN_STRING_KEY='postgresql://cohenaa@localhost:5432/omop'
#POSTGRES_CONN_STRING_KEY='postgresql://postgres:$postgres@10.0.0.4/omop?options=-csearch_path%3Daireadi_omop'
POSTGRES_CONN_STRING_KEY='postgresql://postgres:$postgres@10.1.0.6/omop?options=-csearch_path%3Daireadi_omop'

# postgres OMOP read and write table names within correct schema
POSTGRES_OMOP_READ_PERSON_TABLE_NAME='aireadi_omop.person'
POSTGRES_LABS_READ_MEASUREMENT_TABLE_NAME='aireadi_omop.measurement'
POSTGRES_LABS_READ_OBSERVATION_TABLE_NAME='aireadi_omop.observation'
POSTGRES_LABS_WRITE_SCHEMA_NAME = 'aireadi_omop'
POSTGRES_LABS_WRITE_MEASUREMENT_TABLE_NAME='measurement'
POSTGRES_LABS_WRITE_OBSERVATION_TABLE_NAME='observation'

# path to the standard mapping csv file
LABS_STANDARDS_MAPPING_CSV_PATH='/home/azureuser/omop_etl_labs_and_moca/LABS/STANDARDS_MAPPINGS/0100_2023-11-29_AIREADi_LABS_Data_Dictionary_Mappings.csv'

# path to the data dictionaru xlsx file that contains range mappings
LABS_DATA_DICTIONARY_XLSX_PATH = '/home/azureuser/omop_etl_labs_and_moca/LABS/DATA_DICTIONARY/DRAFT AIREADI LABS Data Dictionary v0.1 DM3 2023_11_02.xlsx'
LABS_NT_PROBNP_RANGES_SHEETNAME = 'NT-proBNP Ranges'
LABS_ALKALINE_PHOSPHATASE_RANGES_SHEETNAME = 'Alkaline Phosphatase Ranges'

# glob wildcard path to the raw LAB source data files
LABS_SOURCE_DATA_GLOB = '/home/azureuser/data/labs/LAB-NORC-latest.xlsx'

# control writing to the OMOP database, for debugging
LABS_OMOP_WRITE_TO_DATABASE = True



