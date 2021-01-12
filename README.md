# FhirToCdm
Conversion from FHIR HL7 to OMOP CDM

It is console (.net core) application. Results will be in app folder (in Result folder, by default CDM, see below -r). One .csv file (tab delimited with header) per chunk (default size 10k) and cdm table.

Command line syntax examples:
============

FHIRtoCDM.exe -f E:\output\fhir -s dbo -v "Driver={SQL Server Native Client 11.0};Server=your_server;Database=db_name;Uid=user;Pwd=pswd;"

Command-line options:
============

  * -f, --fhir          Required. Fhir files location.

  * -v, --vocabulary    Required. ODBC connection string to the vocabulary
                      database.

  * -s, --schema        Required. Vocabulary database schema.

  * -c, --cdm           (Default: V53) CDM version. (V52, V53, V6)

  * -r, --result        (Default: CDM) Result folder name.

  * -u, --chunk         (Default: 10000) Chunk size.

  * --help              Display this help screen.

  --version           Display version information.

ODBC templates (for the vocabular database):
============

* __PostgreSQL__ -  "Driver={PostgreSQL UNICODE};Server={server};Port=5432;Database={database};Uid={username};Pwd={password};sslmode=require;UseDeclareFetch=1;"
* __MySQL__ - "DRIVER={MySQL ODBC 8.0 UNICODE Driver};SERVER={server};DATABASE={database};USER={username};PASSWORD={password};OPTION=3;"
* __MS SQL__  - "Driver={SQL Server Native Client 11.0};Server={server};Database={database};Uid={username};Pwd={password};"
