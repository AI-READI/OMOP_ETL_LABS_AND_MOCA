#
# test_omop_postgres_connection.py
# simple script that uses the labs_etl_parameters.py configuration file
# to test connection to the database and attempt a query on the person table.
#
import sys
import pandas as pd
from sqlalchemy import create_engine, text


# configuration imports
import labs_etl_parameters
from labs_etl_parameters import POSTGRES_CONN_STRING_KEY
from labs_etl_parameters import POSTGRES_OMOP_READ_PERSON_TABLE_NAME
from omop_etl_utils import get_table_row_count

def display_test_configuration_parameters():
    sys.stderr.write("Configuration Parameters in labs_etl_parameters:\n")
    for name, value in vars(labs_etl_parameters).items():
        if name[0] != '_':
            sys.stderr.write(f"\t{name} = '{value}'\n")


def count_records_in_person_table(schema_name, table_name, engine):
	return get_table_row_count(schema_name, table_name, engine)


if __name__ == '__main__':
	# show configuration
    display_test_configuration_parameters()
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

    # done
    sys.stderr.write("Closing database connection...")
    connection.close()    
    sys.stderr.write("OK.\n\n")
