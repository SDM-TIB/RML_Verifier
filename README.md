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

[dataset1]
name: TEST
mapping: ${default:main_directory}/mapping.ttl
```

# Parameters of a Configuration File
- main_directory: Directory where the mapping files are located.
- number_of_datasets: Number of mappings to be verified.
- endpoint: The endpoint that contains the onthology, which the predicates are checked against. If there is no endpoint, this option must be set to None.
- alternate_path: This option tells the verifier to search the logical sources of the mappings in the path indicated in this option instead of the path in the mappings. If there is no alternative path, this option must be set to None.
- name: Name of the data set.
- mapping: Location of the mapping.