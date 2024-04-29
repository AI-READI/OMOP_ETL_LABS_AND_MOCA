import sys
import pandas as pd
from sqlalchemy import create_engine, text
import os

from labs_etl_parameters_local import POSTGRES_CONN_STRING_KEY


def get_query_row_count(query, engine):
    query = text(query)
    df_temp = pd.read_sql(query, engine)      
    return df_temp.iloc[0, 0]   


def execute_database_tests():
    engine = create_engine(POSTGRES_CONN_STRING_KEY)
    connection = engine.connect()   

    sql = """
        select COUNT(*) from aireadi_omop.observation;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected > 1000")

    sql = """
        select COUNT(*) from aireadi_omop.measurement;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected > 1000")

    sql = """
        select COUNT(*) from aireadi_omop.observation WHERE observation_concept_id = 36684973;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ~78")

    sql = """
        select COUNT(*) from aireadi_omop.observation WHERE observation_concept_id = 36684973;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ~78")

    sql = """
        select COUNT(*) from aireadi_omop.person WHERE person_id = 1028;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected == 1")

    sql = """
       select COUNT(*) from aireadi_omop.observation WHERE person_id = 1028;
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ~78")

    sql = """
       select COUNT(*) from aireadi_omop.observation WHERE observation_concept_id = 46235213; # MoCA data
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ZERO")

    sql = """
       select COUNT(*) from aireadi_omop.observation WHERE value_as_string = 'Male';
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ZERO")

    sql = """
       select COUNT(*) from aireadi_omop.observation WHERE value_as_string = 'Female'; 
    """
    n = get_query_row_count(sql, engine)
    print(f"{sql} got {n} expected ZERO")    


def echo_and_execute_system_command(cmd):
    print(cmd)
    os.system(cmd)
    print('\n')


def execute_csv_file_tests():
    echo_and_execute_system_command("grep -i -e sex *.csv")
    echo_and_execute_system_command("grep -i -e gender *.csv")
    echo_and_execute_system_command("grep -i -e race *.csv")
    echo_and_execute_system_command("grep -i -e ethnic *.csv")
    echo_and_execute_system_command("grep -i -e male *.csv")
    echo_and_execute_system_command("grep -i -e female *.csv")
    echo_and_execute_system_command("grep -e 36684973 *.csv") # MoCA data


if __name__ == '__main__':
    execute_database_tests()
    execute_csv_file_tests()




