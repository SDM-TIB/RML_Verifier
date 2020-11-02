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
- main_directory: Directory where the mapping files are located.
- number_of_datasets: Number of mappings to be verified.
- endpoint: The endpoint that contains the onthology, which the predicates are checked against. If there is no endpoint, this option must be set to None.
- alternate_path: This option tells the verifier to search the logical sources of the mappings in the path indicated in this option instead of the path in the mappings. If there is no alternative path, this option must be set to None.
- output_folder: The folder where the log files will be generated.
- mode: This option tells the verifier which mode to enter. 
-- Mapping mode: Verifies the corectness of the mappings.
-- Onthology mode: Verifies which predicates and classes are being used in the mappings and which are not being used. 
- name: Name of the data set.
- mapping: Location of the mapping.