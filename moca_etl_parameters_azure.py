##The following two parameters contain Postgres database connection information
ps_connect_key="host=10.0.0.4 dbname=omop user=postgres password='postgres'"

conn_string_key='postgresql://postgres:$postgres@10.0.0.4/omop?options=-csearch_path%3Daireadi_omop'

##This points to the Redcap extract file
initialize_redcap_file='/home/azureuser/omop/pilot-release-10.2.23/AIREADi_PilotData_forOMOPmapping_20231221.csv'

##This points to the questions mapping file
questions_to_json_file='/home/azureuser/omop/pilot-release-10.2.23/0101_2023-11-29_AIREADi_Pilot_Redcap_Data_Dictionary_Mappings_Questions.csv'

##This points to the answeres mapping file
answers_to_json_file='/home/azureuser/omop/pilot-release-10.2.23/0101_2023-11-29_AIREADi_Pilot_Redcap_Data_Dictionary_Mappings_Answers.csv'

##This points to the data dictionary for Redcap
dd_to_json_file='/home/azureuser/omop/pilot-release-10.2.23/AIREADiPilot_DataDictionary_2023-09-21.csv'

##This also points to the data dictionary for Redcap
create_datatype_dictionary_file='/home/azureuser/omop/pilot-release-10.2.23/AIREADiPilot_DataDictionary_2023-09-21.csv'

