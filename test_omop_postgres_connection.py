#
# test_omop_postgres_connection.py
# simple script that uses the labs_etl_parameters.py configuration file
# to test connection to the database and attempt a query on the person table.
#
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import glob

# labs configuration imports
import labs_etl_parameters
from labs_etl_parameters import POSTGRES_CONN_STRING_KEY
from labs_etl_parameters import POSTGRES_OMOP_READ_PERSON_TABLE_NAME
from labs_etl_parameters import LABS_SOURCE_DATA_GLOB

# moca configuration imports
import moca_etl_parameters
from moca_etl_parameters import MOCA_SOURCE_DATA_GLOB

# utility imports
from omop_etl_utils import get_table_row_count

def display_test_configuration_parameters(module):
    sys.stderr.write(f"Configuration Parameters in {module.__name__}:\n")
    for name, value in vars(module).items():
        if name[0] != '_':
            sys.stderr.write(f"\t{name} = '{value}'\n")


def count_records_in_person_table(schema_name, table_name, engine):
    return get_table_row_count(schema_name, table_name, engine)


if __name__ == '__main__':
    sys.stderr.write("\n*** Begin Configuration Testing ***\n")

    # show configurations
    display_test_configuration_parameters(labs_etl_parameters)
    sys.stderr.write("OK.\n\n")

    display_test_configuration_parameters(moca_etl_parameters)
    sys.stderr.write("OK.\n\n")

    # quick check that they point to the same places...
    sys.stderr.write("Checking labs_etl_parameters match moca_etl_parameters...")
    if POSTGRES_CONN_STRING_KEY != moca_etl_parameters.POSTGRES_CONN_STRING_KEY:
        sys.stderr.write("Error POSTGRES_CONN_STRING_KEY mismatch...")
    if POSTGRES_OMOP_READ_PERSON_TABLE_NAME != moca_etl_parameters.POSTGRES_OMOP_READ_PERSON_TABLE_NAME:
        sys.stderr.write("Error POSTGRES_OMOP_READ_PERSON_TABLE_NAME mismatch...")
    sys.stderr.write("OK.\n\n")

    # connect to the omop database
    sys.stderr.write(f"Connecting to the OMOP database using '{POSTGRES_CONN_STRING_KEY}'...")
    engine = create_engine(POSTGRES_CONN_STRING_KEY)
    connection = engine.connect()   
    sys.stderr.write("OK.\n\n")

    # perform a simple COUNT(*) query on the person database table
    sys.stderr.write(f"Querying count of records in the '{POSTGRES_OMOP_READ_PERSON_TABLE_NAME}' table...")
    schema_name, table_name = POSTGRES_OMOP_READ_PERSON_TABLE_NAME.split('.', 1)
    nrecords = count_records_in_person_table(schema_name, table_name, engine)
    sys.stderr.write(f"OK, Found {nrecords} records.\n\n")

    # done with database
    sys.stderr.write("Closing database connection...")
    connection.close()    
    sys.stderr.write("OK.\n\n")

    # test source file reading
    sys.stderr.write(f"Testing reading of LABS source files in {LABS_SOURCE_DATA_GLOB}:\n")
    for filename in glob.glob(LABS_SOURCE_DATA_GLOB):
        sys.stderr.write(f"\tReading {filename}...")
        with open(filename, 'rb') as f:
            data = f.read()
        sys.stderr.write("OK.\n")       
    sys.stderr.write("Done.\n\n")

    sys.stderr.write(f"Testing reading of MoCA source files in {MOCA_SOURCE_DATA_GLOB}:\n")
    for filename in glob.glob(MOCA_SOURCE_DATA_GLOB):
        sys.stderr.write(f"\tReading {filename}...")
        with open(filename, 'rb') as f:
            data = f.read()
        sys.stderr.write("OK.\n")       
    sys.stderr.write("Done.\n\n")

    sys.stderr.write("*** Configuration Testing Completed ***\n")

