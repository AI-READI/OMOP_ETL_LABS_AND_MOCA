# OMOP_ETL_LABS_AND_MOCA
This project contains the source code and data mapping and dictionary files for the AIREADI MoCA  and Labs ETl process.

Project is in development.

Documentation
The system is designed to be as easy to configure and run as possible.
For maximum flexibility, all ETL source and destination configuration parameters are set in python configuration files.

For both MoCA and Labs processing, it is assumed that the OMOP database is up to date with the latest RedCap data and that the RedCap-to-OMOP process has been run successfully.
The MoCA and Labs ETL process adds to the database created by the RedCap-to-OMOP process. See the AIREADI OMOP code repository (https://github.com/AI-READI/OMOP).

- Processing MocA result files into OMOP.
1. Set the configuration variables in moca_etal_parameters.py appropriately for the configuration of the system. In particular, the local of the source data files, as well as the destination OMOP database connection, schema, and table names will need to be set. See moca_etal_parameters_local.py and moca_etal_parameters_azure.py for example configurations.
2. Run the ETL script: % python moca_main.py

- Processing LAB result files into OMOP.
1. Set the configuration variables in labs_etal_parameters.py appropriately for the configuration of the system. In particular, the local of the source data files, as well as the destination OMOP database connection, schema, and table names will need to be set. See labs_etal_parameters_local.py and labs_etal_parameters_azure.py for example configurations.
2. Run the ETL script: % python labs_main.py


