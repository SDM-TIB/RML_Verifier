# Running the RML-Verifier
```
python3 run_verifier.py /path/to/config
```

# Example of Configuration File
```
[default]
main_directory: .

[datasets]
number_of_datasets: 1
endpoint: None
alternate_path: None
output_folder: ..
mode: mapping

[dataset1]
name: TEST
mapping: ${default:main_directory}/mapping.ttl
```

# Parameters of a Configuration File
* main_directory: Directory where the mapping files are located.
* number_of_datasets: Number of mappings to be verified.
* endpoint: The endpoint that contains the onthology, which the predicates are checked against. If there is no endpoint, this option must be set to None.
* alternate_path: This option tells the verifier to search the logical sources of the mappings in the path indicated in this option instead of the path in the mappings. If there is no alternative path, this option must be set to None.
* output_folder: The folder where the log files will be generated.
* mode: This option tells the verifier which mode to enter. 
	* Mapping mode: Verifies the corectness of the mappings.
	* Ontology mode: Verifies which predicates and classes are being used in the ontology and which are not being used. Does not require mappings to be inputed. It only requires one endpoint.
	* Ontology-mapping mode: This mode is similar to the ontology mode but requires mapping to be inputed. 
* name: Name of the data set.
* mapping: Location of the mapping.
* user: User for the database. (This option is only necessary if a MySQL or Postgres database is being used)
* password: Password for the database. (This option is only necessary if a MySQL or Postgres database is being used)
* host: Host for the database. (This option is only necessary if a MySQL or Postgres database is being used)
* port: Port for the database. (This option is only necessary if a MySQL database is being used)
* db: Postgres database is being used. (This option is only necessary if a MySQL or Postgres database is being used)