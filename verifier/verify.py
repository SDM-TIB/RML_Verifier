import os
import os.path
import re
import csv
import sys
import uuid
import rdflib
import getopt
import subprocess
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
import time
import json
import xml.etree.ElementTree as ET
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm


def string_separetion(string):
	if ("{" in string) and ("[" in string):
		prefix = string.split("{")[0]
		condition = string.split("{")[1].split("}")[0]
		postfix = string.split("{")[1].split("}")[1]
		field = prefix + "*" + postfix
	elif "[" in string:
		return string, string
	else:
		return string, ""
	return string, condition

def mapping_parser(mapping_file):

	"""
	(Private function, not accessible from outside this package)

	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)

	Parameters
	----------
	mapping_file : string
		Path to the mapping file

	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

	mapping_graph = rdflib.Graph()

	try:
		mapping_graph.load(mapping_file, format='n3')
	except Exception as n3_mapping_parse_exception:
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rr: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
		prefix fnml: <http://semweb.mmlab.be/ns/fnml#> 
		SELECT DISTINCT *
		WHERE {

	# Subject -------------------------------------------------------------------------
		
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{?_source rml:source ?data_source .}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rr:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}
			
			OPTIONAL {?triples_map_id rr:subjectMap ?_subject_map .}
			OPTIONAL {?_subject_map rr:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rr:constant ?subject_constant}
			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
			OPTIONAL { ?_subject_map rr:termType ?termtype . }
			OPTIONAL { ?_subject_map rr:graph ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?graph . }
		   	OPTIONAL {?_subject_map fnml:functionValue ?subject_function .}		   

	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
			 }
			

	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:constant ?object_constant .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:template ?object_template .
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rr:language ?language .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value.
				 	OPTIONAL{?parent_value fnml:functionValue ?parent_function.}
				 	OPTIONAL{?child_value fnml:functionValue ?child_function.}
				}
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value;
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:object ?object_constant_shortcut .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL{
				?_predicate_object_map rr:objectMap ?_object_map .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
				?_object_map fnml:functionValue ?function .
				OPTIONAL {?_object_map rr:termType ?term .}
				
			}
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
			
		} """

	mapping_query_results = mapping_graph.query(mapping_query)
	triples_map_list = []


	for result_triples_map in mapping_query_results:
		triples_map_exists = False
		for triples_map in triples_map_list:
			triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
		
		subject_map = None
		if not triples_map_exists:
			if result_triples_map.subject_template is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_reference is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_constant is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_function is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
				
			mapping_query_prepared = prepareQuery(mapping_query)


			mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})




			predicate_object_maps_list = []

			function = False
			for result_predicate_object_map in mapping_query_prepared_results:

				if result_predicate_object_map.predicate_constant is not None:
					predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
				elif result_predicate_object_map.predicate_constant_shortcut is not None:
					predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
				elif result_predicate_object_map.predicate_template is not None:
					template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
					predicate_map = tm.PredicateMap("template", template, condition)
				elif result_predicate_object_map.predicate_reference is not None:
					reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
					predicate_map = tm.PredicateMap("reference", reference, condition)
				else:
					predicate_map = tm.PredicateMap("None", "None", "None")

				if "execute" in predicate_map.value:
					function = True

				if result_predicate_object_map.object_constant is not None:
					object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_template is not None:
					object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_reference is not None:
					object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_parent_triples_map is not None:
					if (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map parent function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
						object_map = tm.ObjectMap("parent triples map child function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					else:
						object_map = tm.ObjectMap("parent triples map", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_constant_shortcut is not None:
					object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.function is not None:
					object_map = tm.ObjectMap("reference function", str(result_predicate_object_map.function),str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				else:
					object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")

				predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map)]

			if function:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), None, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=True)
			else:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=False)
			triples_map_list += [current_triples_map]

	return triples_map_list


def verify(config_path):

	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)

	with ThreadPoolExecutor(max_workers=10) as executor:
		for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
			dataset_i = "dataset" + str(int(dataset_number) + 1)
			triples_map_list = mapping_parser(config[dataset_i]["mapping"])

			print("Verifying {}...".format(config[dataset_i]["name"]))
			for triples_map in triples_map_list:
				if os.path.exists(triples_map.data_source):
					attributes = {}
					if triples_map.function:
						pass
					else:
						if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
							if "{" in triples_map.subject_map.value and "}" in triples_map.subject_map.value:
								subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
								attributes[subject_field] = "subject"
							else:
								print("In the triple map " + triples_map.triples_map_id + " subject value is missing { }.")

							if "none" not in str(config["datasets"]["endpoint"].lower()):
								sparql = SPARQLWrapper(config["datasets"]["endpoint"])
								sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
													PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
													SELECT ?s 
													WHERE { ?s rdf:type	owl:DatatypeProperty. }""")
								sparql.setReturnFormat(JSON)
								predicates = sparql.query().convert()

								sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
													PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
													SELECT ?s 
													WHERE { ?s rdf:type owl:Class. }""")
								sparql.setReturnFormat(JSON)
								types = sparql.query().convert()

							for po in triples_map.predicate_object_maps_list:
								if "none" not in str(config["datasets"]["endpoint"].lower()):
									if triples_map.subject_map.rdf_class is not None:
										no_class = True
										for c in types["results"]["bindings"]:
											if triples_map.subject_map.rdf_class in c["s"]["value"]:
												no_class = False
												break
										if no_class:
											print("In the triple map " + triples_map.triples_map_id + " the class " + triples_map.subject_map.rdf_class + " is not in the endpoint " + config["datasets"]["endpoint"] + ".")

									no_predicate = True
									for p in predicates["results"]["bindings"]:
										if po.predicate_map.value in p["s"]["value"]:
											no_predicate = False
											break
									if no_predicate:
										print("In the triple map " + triples_map.triples_map_id + " the predicate " + po.predicate_map.value + " is not in the endpoint " + config["datasets"]["endpoint"] + ".")
									else:
										query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
										query += "SELECT ?domain,?range \n"
										query += "WHERE { " + po.predicate_map.value + " rdfs:domain ?domain;\n"
										query += "rdfs:range ?range. }"
										sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
															PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
															PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
															SELECT ?s,?domain,?range
															WHERE { ?s rdf:type owl:DatatypeProperty; 
															         rdfs:domain ?domain;
															         rdfs:range ?range. }""")
										sparql.setReturnFormat(JSON)
										domain_range = sparql.query().convert()

										for dr in domain_range["results"]["bindings"]:
											if po.predicate_map.value in dr["s"]["value"]:
												if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
													print("In the triple map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " should be " + dr["domain"]["value"] + ".")

												if "Literal" in dr["range"]["value"]:
													if po.object_map.mapping_type != "reference":
														print("In the triple map " + triples_map.triples_map_id + " the range for " + po.predicate_map.value + " should be a reference.")	
												break
									
								if po.object_map.mapping_type == "reference":
									if "{" in po.object_map.value or "}" in po.object_map.value:
										print("In the triple map " + triples_map.triples_map_id + " reference object value should not have { }.")
									else:
										attributes[po.object_map.value] = "object"
								elif po.object_map.mapping_type == "template":
									if "{" in po.object_map.value and "}" in po.object_map.value:
										object_field = po.object_map.value.value.split("{")[1].split("}")[0]
										attributes[object_field] = "object"
									else:
										print("In the triple map " + triples_map.triples_map_id + " template object value is missing { }.")
								elif po.object_map.mapping_type == "parent triples map":
									attributes[po.object_map.child] = "object"

							with open(triples_map.data_source, "r") as input_file_descriptor:
								data = csv.DictReader(input_file_descriptor, delimiter=',')
								row = next(data)

								for attr in attributes.keys():
									if attr not in row.keys():
										print("The attribute " + attr + "is not in " + triples_map.data_source)
						else:
							print("Invalid reference formulation or format")
							print("Aborting...")
							sys.exit(1)
				else:
					print("In the triple map " + triples_map.triples_map_id + " the file " + triples_map.data_source + " does not exist.")
					if "{" in triples_map.subject_map.value and "}" in triples_map.subject_map.value:
						subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
					else:
						print("In the triple map " + triples_map.triples_map_id + " subject value is missing { }.")

					if "none" not in str(config["datasets"]["endpoint"].lower()):
						sparql = SPARQLWrapper(config["datasets"]["endpoint"])
						sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
											PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
											SELECT ?s 
											WHERE { ?s rdf:type	owl:DatatypeProperty. }""")
						sparql.setReturnFormat(JSON)
						predicates = sparql.query().convert()

						sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
											PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
											SELECT ?s 
											WHERE { ?s rdf:type owl:Class. }""")
						sparql.setReturnFormat(JSON)
						types = sparql.query().convert()

					for po in triples_map.predicate_object_maps_list:
						if "none" not in str(config["datasets"]["endpoint"].lower()):
							if triples_map.subject_map.rdf_class is not None:
								no_class = True
								for c in types["results"]["bindings"]:
									if triples_map.subject_map.rdf_class in c["s"]["value"]:
										no_class = False
										break
								if no_class:
									print("In the triple map " + triples_map.triples_map_id + " the class " + triples_map.subject_map.rdf_class + " is not in the endpoint " + config["datasets"]["endpoint"] + ".")

							no_predicate = True
							for p in predicates["results"]["bindings"]:
								if po.predicate_map.value in p["s"]["value"]:
									no_predicate = False
									break
							if no_predicate:
								print("In the triple map " + triples_map.triples_map_id + " the predicate " + po.predicate_map.value + " is not in the endpoint " + config["datasets"]["endpoint"] + ".")
							else:
								query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
								query += "SELECT ?domain,?range \n"
								query += "WHERE { " + po.predicate_map.value + " rdfs:domain ?domain;\n"
								query += "rdfs:range ?range. }"
								sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
													PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
													PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
													SELECT ?s,?domain,?range
													WHERE { ?s rdf:type owl:DatatypeProperty; 
													         rdfs:domain ?domain;
													         rdfs:range ?range. }""")
								sparql.setReturnFormat(JSON)
								domain_range = sparql.query().convert()

								for dr in domain_range["results"]["bindings"]:
									if po.predicate_map.value in dr["s"]["value"]:
										if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
											print("In the triple map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " should be " + dr["domain"]["value"] + ".")

										if "Literal" in dr["range"]["value"]:
											if po.object_map.mapping_type != "reference":
												print("In the triple map " + triples_map.triples_map_id + " the range for " + po.predicate_map.value + " should be a reference.")	
										break

						if po.object_map.mapping_type == "reference":
							if "{" in po.object_map.value or "}" in po.object_map.value:
								print("In the triple map " + triples_map.triples_map_id + " object value should not have { }.")
						elif po.object_map.mapping_type == "template":
							if "{" in po.object_map.value and "}" in po.object_map.value:
								object_field = po.object_map.value.value.split("{")[1].split("}")[0]

				

			print("Successfully verfiried {}\n".format(config[dataset_i]["name"]))

		

def main():

	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv, 'hc:', 'config_file=')
	except getopt.GetoptError:
		print('python3 translate.py -c <config_file>')
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-h':
			print('python3 translate.py -c <config_file>')
			sys.exit()
		elif opt == '-c' or opt == '--config_file':
			config_path = arg

	verify(config_path)

if __name__ == "__main__":
	main()
