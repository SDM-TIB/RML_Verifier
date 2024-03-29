import os
import os.path
import re
import csv
import sys
import rdflib
import getopt
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
from SPARQLWrapper import SPARQLWrapper, JSON
from mysql import connector
from .functions import *

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm


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
		mapping_graph.parse(mapping_file, format='n3')
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

	if config["datasets"]["mode"].lower() == "mapping":
		triples_map_id = {}
		with ThreadPoolExecutor(max_workers=10) as executor:
			if not os.path.exists(config["datasets"]["output_folder"]):
				os.mkdir(config["datasets"]["output_folder"])
			if config["datasets"]["output_folder"][len(config["datasets"]["output_folder"])-1] == "/":
				f = open(config["datasets"]["output_folder"] + config["datasets"]["name"] + "_log.txt","w+")
			else:
				f = open(config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + "_log.txt","w+")
			for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
				dataset_i = "dataset" + str(int(dataset_number) + 1)
				triples_map_list = mapping_parser(config[dataset_i]["mapping"])
				f.write("Verifying {}...\n".format(config[dataset_i]["name"]))
				for triples_map in triples_map_list:
					if "#" in triples_map.triples_map_id:
						triples_id = triples_map.triples_map_id.split("#")[1]
					else:
						triples_id = triples_map.triples_map_id.split("/")[len(triples_map.triples_map_id.split("/"))-1]
					if triples_id in triples_map_id:
						triples_map_id[triples_id].append(config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1])
					else:
						triples_map_id[triples_id] = [config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1]]
					if "none" not in str(config["datasets"]["alternate_path"].lower()):
						file = triples_map.data_source.split("/")[len(triples_map.data_source.split("/"))-1]
						if str(config["datasets"]["alternate_path"])[-1] == "/":
							source = str(config["datasets"]["alternate_path"]) + file
						else:
							source = str(config["datasets"]["alternate_path"]) + "/" + file
					else:
						source = triples_map.data_source
					if str(triples_map.file_format) == "None" and triples_map.query == "None" and triples_map.tablename == "None":
						f.write("In the triples map " + triples_map.triples_map_id + " file format is not defined.\n")
					if triples_map.query == "None" and triples_map.tablename == "None":
						if os.path.exists(source):
							attributes = {}
							if triples_map.function:
								pass
							else:
								if str(triples_map.file_format).lower() == "csv":
									if "{" in triples_map.subject_map.value :
										if "}" in triples_map.subject_map.value:
											subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
											attributes[subject_field] = "subject"
										else:
											f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing }.\n")
									elif "}" in triples_map.subject_map.value:
										if "{" in triples_map.subject_map.value:
											subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
											attributes[subject_field] = "subject"
										else:
											f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing {.\n")
										
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
															WHERE { ?s rdf:type owl:ObjectProperty. }""")
										sparql.setReturnFormat(JSON)
										obj_property = sparql.query().convert()

										sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
															PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
															SELECT ?s 
															WHERE { ?s rdf:type owl:Class. }""")
										sparql.setReturnFormat(JSON)
										types = sparql.query().convert()

										sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
															PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
															SELECT ?s 
															WHERE { ?s rdf:type owl:Property. }""")
										sparql.setReturnFormat(JSON)
										properties = sparql.query().convert()

									for po in triples_map.predicate_object_maps_list:
										if "none" not in str(config["datasets"]["endpoint"].lower()):
											if triples_map.subject_map.rdf_class is not None:
												no_class = True
												for c in types["results"]["bindings"]:
													if triples_map.subject_map.rdf_class in c["s"]["value"]:
														no_class = False
														break
												if no_class:
													f.write("In the triples map " + triples_map.triples_map_id + " the class " + triples_map.subject_map.rdf_class + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")

											no_predicate = True
											for p in predicates["results"]["bindings"]:
												if po.predicate_map.value == p["s"]["value"]:
													no_predicate = False
													break
											if no_predicate:
												for p in obj_property["results"]["bindings"]:
													if po.predicate_map.value == p["s"]["value"]:
														no_predicate = False
														break
											if no_predicate:
												for p in properties["results"]["bindings"]:
													if po.predicate_map.value == p["s"]["value"]:
														no_predicate = False
														break
											if no_predicate:
												f.write("In the triples map " + triples_map.triples_map_id + " the predicate " + po.predicate_map.value + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")
											else:
												sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
																	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
																	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
																	SELECT ?s,?domain,?range
																	WHERE { ?s rdf:type owl:DatatypeProperty; 
																	         rdfs:domain ?domain;
																	         rdfs:range ?range. }""")
												sparql.setReturnFormat(JSON)
												domain_range = sparql.query().convert()
												dr_execute = False

												sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
																	PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
																	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
																	SELECT ?s,?domain,?range
																	WHERE { ?s rdf:type owl:ObjectProperty; 
																	         rdfs:domain ?domain;
																	         rdfs:range ?range. }""")
												sparql.setReturnFormat(JSON)
												dr_op = sparql.query().convert()

												for dr in domain_range["results"]["bindings"]:
													if dr["s"]["value"] in po.predicate_map.value:
														if triples_map.subject_map.rdf_class is not None:
															if dr["domain"]["value"] is not None:
																if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
																	dr_execute = True
															else:
																f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " is not defined.\n")
														else:
															f.write("In the triples map " + triples_map.triples_map_id + " there is no class defined.\n")

														if "Literal" in dr["range"]["value"]:
															if po.object_map.mapping_type != "reference":
																f.write("In the triples map " + triples_map.triples_map_id + " the range for " + po.predicate_map.value + " should be a reference.\n")	
														break
												if dr_execute:
													for dr in dr_op["results"]["bindings"]:
														if dr["s"]["value"] in po.predicate_map.value:
															if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
																f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " should be " + dr["domain"]["value"] + ".\n")
											
										if po.object_map.mapping_type == "reference":
											if "{" in po.object_map.value or "}" in po.object_map.value:
												f.write("In the triples map " + triples_map.triples_map_id + " reference object value should not have { }.\n")
											else:
												attributes[po.object_map.value] = "object"
										elif po.object_map.mapping_type == "template":
											if "{" in po.object_map.value and "}" in po.object_map.value:
												object_field = po.object_map.value.split("{")[1].split("}")[0]
												attributes[object_field] = "object"
											elif "{" not in po.object_map.value and "}" in po.object_map.value:
												f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing {.\n")
											elif "{" in po.object_map.value and "}" not in po.object_map.value:
												f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing }.\n")
												
										elif po.object_map.mapping_type == "parent triples map":
											if po.object_map.child != None:
												attributes[po.object_map.child] = "object"
											if po.object_map.parent != None:
												for triples_map_element in triples_map_list:
													if triples_map_element.triples_map_id == po.object_map.value:
														if "none" not in str(config["datasets"]["alternate_path"].lower()):
															file = triples_map_element.data_source.split("/")[len(triples_map_element.data_source.split("/"))-1]
															if str(config["datasets"]["alternate_path"])[-1] == "/":
																parent_source = str(config["datasets"]["alternate_path"]) + file
															else:
																parent_source = str(config["datasets"]["alternate_path"]) + "/" + file
														else:
															parent_source = triples_map.data_source
														with open(parent_source, "r") as input_file_descriptor:
															data = csv.DictReader(input_file_descriptor, delimiter=',')
															row = next(data)
															if po.object_map.parent not in row:
																f.write("In the triples map " + triples_map_element.triples_map_id + " the attribute " + po.object_map.parent + " from the join condition is missing from the data source.\n")
											if po.object_map.child == None and po.object_map.parent != None:
												f.write("In the triples map " + triples_map.triples_map_id + " the child attribute is missing from the join condition.\n")
											elif po.object_map.child != None and po.object_map.parent == None:
												f.write("In the triples map " + triples_map.triples_map_id + " the parent attribute is missing from the join condition.\n")
											elif po.object_map.child == None and po.object_map.parent == None:
												for triples_map_element in triples_map_list:
													if triples_map_element.triples_map_id == po.object_map.value:
														if triples_map_element.data_source != triples_map.data_source:
															f.write("Triples map " + triples_map.triples_map_id + " and triples map " + triples_map_element.triples_map_id + " do not use the same data source.\n")

									with open(source, "r") as input_file_descriptor:
										data = csv.DictReader(input_file_descriptor, delimiter=',')
										row = next(data)

										if attributes:
											for attr in attributes:
												if attr not in row and attr is not None:
													f.write("The attribute " + attr + " is not in " + source + ".\n")
								else:
									print("Invalid reference formulation or format")
									print("Aborting...")
									sys.exit(1)
						
						else:
							f.write("In the triples map " + triples_map.triples_map_id + " the file " + source + " does not exist.\n")
							if "{" in triples_map.subject_map.value :
								if "}" in triples_map.subject_map.value:
									pass
								else:
									f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing }.\n")
							elif "}" in triples_map.subject_map.value:
								if "{" in triples_map.subject_map.value:
									pass
								else:
									f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing {.\n")

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
													WHERE { ?s rdf:type owl:ObjectProperty. }""")
								sparql.setReturnFormat(JSON)
								obj_property = sparql.query().convert()

								sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
													PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
													SELECT ?s 
													WHERE { ?s rdf:type owl:Class. }""")
								sparql.setReturnFormat(JSON)
								types = sparql.query().convert()

								sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
													PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
													SELECT ?s 
													WHERE { ?s rdf:type owl:Property. }""")
								sparql.setReturnFormat(JSON)
								properties = sparql.query().convert()

								for po in triples_map.predicate_object_maps_list:
									if "none" not in str(config["datasets"]["endpoint"].lower()):
										if triples_map.subject_map.rdf_class is not None:
											no_class = True
											for c in types["results"]["bindings"]:
												if triples_map.subject_map.rdf_class in c["s"]["value"]:
													no_class = False
													break
											if no_class:
												f.write("In the triples map " + triples_map.triples_map_id + " the class " + triples_map.subject_map.rdf_class + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")

										no_predicate = True
										for p in predicates["results"]["bindings"]:
											if po.predicate_map.value == p["s"]["value"]:
												no_predicate = False
												break
										if no_predicate:
											for p in obj_property["results"]["bindings"]:
												if po.predicate_map.value == p["s"]["value"]:
													no_predicate = False
													break
										if no_predicate:
											for p in properties["results"]["bindings"]:
												if po.predicate_map.value == p["s"]["value"]:
													no_predicate = False
													break
										if no_predicate:
											f.write("In the triples map " + triples_map.triples_map_id + " the predicate " + po.predicate_map.value + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")
										else:
											sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
																PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
																PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
																SELECT ?s,?domain,?range
																WHERE { ?s rdf:type owl:DatatypeProperty; 
																         rdfs:domain ?domain;
																         rdfs:range ?range. }""")
											sparql.setReturnFormat(JSON)
											domain_range = sparql.query().convert()
											dr_execute = False

											sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
																PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
																PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
																SELECT ?s,?domain,?range
																WHERE { ?s rdf:type owl:ObjectProperty; 
																         rdfs:domain ?domain;
																         rdfs:range ?range. }""")
											sparql.setReturnFormat(JSON)
											dr_op = sparql.query().convert()

											for dr in domain_range["results"]["bindings"]:
												if po.predicate_map.value in dr["s"]["value"]:
													if triples_map.subject_map.rdf_class is not None:
														if dr["domain"]["value"] is not None:
															if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
																dr_execute = True
														else:
															f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " is not defined.\n")
													else:
														f.write("In the triples map " + triples_map.triples_map_id + " there is no class defined.\n")

													if "Literal" in dr["range"]["value"]:
														if po.object_map.mapping_type != "reference":
															f.write("In the triples map " + triples_map.triples_map_id + " the range for " + po.predicate_map.value + " should be a reference.\n")	
													break
											if dr_execute:
												for dr in dr_op["results"]["bindings"]:
													if dr["s"]["value"] in po.predicate_map.value:
														if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
															f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " should be " + dr["domain"]["value"] + ".\n")

								if po.object_map.mapping_type == "reference":
									if "{" in po.object_map.value or "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " object value should not have { }.\n")
								elif po.object_map.mapping_type == "template":
									if "{" in po.object_map.value and "}" in po.object_map.value:
										object_field = po.object_map.value.split("{")[1].split("}")[0]
									elif "{" not in po.object_map.value and "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing {.\n")
									elif "{" in po.object_map.value and "}" not in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing }.\n")
						
					else:
						database, query_list = translate_sql(triples_map)
						db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
						cursor = db.cursor(buffered=True)
						if config[dataset_i]["db"].lower() != "none":
							cursor.execute("use " + config[dataset_i]["db"])
						else:
							if database != "None":
								cursor.execute("use " + database)
						if triples_map.query == "None":	
							for query in query_list:
								cursor.execute(query)
								row_headers=[x[0] for x in cursor.description]
						else:
							cursor.execute(triples_map.query)
							row_headers = [x[0] for x in cursor.description]
						if "{" in triples_map.subject_map.value :
							if "}" in triples_map.subject_map.value:
								subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
								if subject_field not in row_headers:
									f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + subject_field + " is missing from the query.\n")
							else:
								f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing }.\n")
						elif "}" in triples_map.subject_map.value:
							if "{" in triples_map.subject_map.value:
								subject_field = triples_map.subject_map.value.split("{")[1].split("}")[0]
								if subject_field not in row_headers:
									f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + subject_field + " is missing from the query.\n")
							else:
								f.write("In the triples map " + triples_map.triples_map_id + " subject value is missing {.\n")

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
												WHERE { ?s rdf:type owl:ObjectProperty. }""")
							sparql.setReturnFormat(JSON)
							obj_property = sparql.query().convert()

							sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
												PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
												SELECT ?s 
												WHERE { ?s rdf:type owl:Class. }""")
							sparql.setReturnFormat(JSON)
							types = sparql.query().convert()

							sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
												PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
												SELECT ?s 
												WHERE { ?s rdf:type owl:Property. }""")
							sparql.setReturnFormat(JSON)
							properties = sparql.query().convert()

							for po in triples_map.predicate_object_maps_list:
								if "none" not in str(config["datasets"]["endpoint"].lower()):
									if triples_map.subject_map.rdf_class is not None:
										no_class = True
										for c in types["results"]["bindings"]:
											if triples_map.subject_map.rdf_class in c["s"]["value"]:
												no_class = False
												break
										if no_class:
											f.write("In the triples map " + triples_map.triples_map_id + " the class " + triples_map.subject_map.rdf_class + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")

									no_predicate = True
									for p in predicates["results"]["bindings"]:
										if po.predicate_map.value == p["s"]["value"]:
											no_predicate = False
											break
									if no_predicate:
										for p in obj_property["results"]["bindings"]:
											if po.predicate_map.value == p["s"]["value"]:
												no_predicate = False
												break
									if no_predicate:
										for p in properties["results"]["bindings"]:
											if po.predicate_map.value == p["s"]["value"]:
												no_predicate = False
												break
									if no_predicate:
										f.write("In the triples map " + triples_map.triples_map_id + " the predicate " + po.predicate_map.value + " is not in the endpoint " + config["datasets"]["endpoint"] + ".\n")
									else:
										sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
															PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
															PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
															SELECT ?s,?domain,?range
															WHERE { ?s rdf:type owl:DatatypeProperty; 
															         rdfs:domain ?domain;
															         rdfs:range ?range. }""")
										sparql.setReturnFormat(JSON)
										domain_range = sparql.query().convert()
										dr_execute = False

										sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
															PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
															PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  
															SELECT ?s,?domain,?range
															WHERE { ?s rdf:type owl:ObjectProperty; 
															         rdfs:domain ?domain;
															         rdfs:range ?range. }""")
										sparql.setReturnFormat(JSON)
										dr_op = sparql.query().convert()

										for dr in domain_range["results"]["bindings"]:
											if po.predicate_map.value in dr["s"]["value"]:
												if triples_map.subject_map.rdf_class is not None:
													if dr["domain"]["value"] is not None:
														if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
															dr_execute = True
													else:
														f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " is not defined.\n")
												else:
													f.write("In the triples map " + triples_map.triples_map_id + " there is no class defined.\n")

												if "Literal" in dr["range"]["value"]:
													if po.object_map.mapping_type != "reference":
														f.write("In the triples map " + triples_map.triples_map_id + " the range for " + po.predicate_map.value + " should be a reference.\n")	
												break
										if dr_execute:
											for dr in dr_op["results"]["bindings"]:
												if dr["s"]["value"] in po.predicate_map.value:
													if triples_map.subject_map.rdf_class not in dr["domain"]["value"]:
														f.write("In the triples map " + triples_map.triples_map_id + " the domain for " + po.predicate_map.value + " should be " + dr["domain"]["value"] + ".\n")
								if po.object_map.mapping_type == "reference":
									if "{" in po.object_map.value or "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " object value should not have { }.\n")
									elif "{" not in po.object_map.value and "}" not in po.object_map.value:
										if po.object_map.value not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + po.object_map.value + " is missing from the query.\n")
								elif po.object_map.mapping_type == "template":
									if "{" in po.object_map.value and "}" in po.object_map.value:
										object_field = po.object_map.value.split("{")[1].split("}")[0]
										if object_field not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + object_field + " is missing from the query.\n")
									elif "{" not in po.object_map.value and "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing {.\n")
									elif "{" in po.object_map.value and "}" not in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing }.\n")
								elif po.object_map.mapping_type == "parent triples map":
									if po.object_map.child != None:
										if po.object_map.child not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + po.object_map.child + " is missing from the query.\n")
									if po.object_map.parent != None:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												database, query_list = translate_sql(triples_map_element)
												db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
												cursor = db.cursor(buffered=True)
												if config[dataset_i]["db"].lower() != "none":
													cursor.execute("use " + config[dataset_i]["db"])
												else:
													if database != "None":
														cursor.execute("use " + database)
												if triples_map_element.query == "None":	
													for query in query_list:
														cursor.execute(query)
														parent_row_headers=[x[0] for x in cursor.description]
												else:
													cursor.execute(triples_map_element.query)
													parent_row_headers = [x[0] for x in cursor.description]
												if po.object_map.parent not in parent_row_headers:
													f.write("In the triples map " + triples_map_element.triples_map_id + " the attribute " + po.object_map.parent + " from the join condition is missing from the query.\n")
									if po.object_map.child == None and po.object_map.parent != None:
										f.write("In the triples map " + triples_map.triples_map_id + " the child attribute is missing from the join condition.\n")
									elif po.object_map.child != None and po.object_map.parent == None:
										f.write("In the triples map " + triples_map.triples_map_id + " the parent attribute is missing from the join condition.\n")
									elif po.object_map.child == None and po.object_map.parent == None:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												if triples_map_element.tablename != triples_map.tablename and triples_map_element.data_source != triples_map.data_source:
													f.write("Triples map " + triples_map.triples_map_id + " and triples map " + triples_map_element.triples_map_id + " do not use the same table.\n")
						else:
							for po in triples_map.predicate_object_maps_list:
								if po.object_map.mapping_type == "reference":
									if "{" in po.object_map.value or "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " object value should not have { }.\n")
									elif "{" not in po.object_map.value and "}" not in po.object_map.value:
										if po.object_map.value not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + po.object_map.value + " is missing from the query.\n")
								elif po.object_map.mapping_type == "template":
									if "{" in po.object_map.value and "}" in po.object_map.value:
										object_field = po.object_map.value.split("{")[1].split("}")[0]
										if object_field not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + object_field + " is missing from the query.\n")
									elif "{" not in po.object_map.value and "}" in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing {.\n")
									elif "{" in po.object_map.value and "}" not in po.object_map.value:
										f.write("In the triples map " + triples_map.triples_map_id + " template object value is missing }.\n")
								elif po.object_map.mapping_type == "parent triples map":
									if po.object_map.child != None:
										if po.object_map.child not in row_headers:
											f.write("In the triples map " + triples_map.triples_map_id + " the attribute " + po.object_map.child + " is missing from the query.\n")
									if po.object_map.parent != None:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												database, query_list = translate_sql(triples_map_element)
												db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
												cursor = db.cursor(buffered=True)
												if config[dataset_i]["db"].lower() != "none":
													cursor.execute("use " + config[dataset_i]["db"])
												else:
													if database != "None":
														cursor.execute("use " + database)
												if triples_map_element.query == "None":	
													for query in query_list:
														cursor.execute(query)
														parent_row_headers=[x[0] for x in cursor.description]
												else:
													cursor.execute(triples_map_element.query)
													parent_row_headers = [x[0] for x in cursor.description]
												if po.object_map.parent not in parent_row_headers:
													f.write("In the triples map " + triples_map_element.triples_map_id + " the attribute " + po.object_map.parent + " from the join condition is missing from the query.\n")
									if po.object_map.child == None and po.object_map.parent != None:
										f.write("In the triples map " + triples_map.triples_map_id + " the child attribute is missing from the join condition.\n")
									elif po.object_map.child != None and po.object_map.parent == None:
										f.write("In the triples map " + triples_map.triples_map_id + " the parent attribute is missing from the join condition.\n")
									elif po.object_map.child == None and po.object_map.parent == None:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												if triples_map_element.tablename != triples_map.tablename and triples_map_element.data_source != triples_map.data_source:
													f.write("Triples map " + triples_map.triples_map_id + " and triples map " + triples_map_element.triples_map_id + " do not use the same table.\n")

				f.write("Successfully verified {}\n".format(config[dataset_i]["name"]))

			f.write("\n")
			for triples_id in triples_map_id:
				f.write("The triples map id " + triples_id + " is used by: " + str(triples_map_id[triples_id]) +".\n")

		f.close()

	elif config["datasets"]["mode"].lower() == "ontology-mapping":

		if  config["datasets"]["endpoint"].lower() != "none":

			print("Verifying onthology...")

			if not os.path.exists(config["datasets"]["output_folder"]):
				os.mkdir(config["datasets"]["output_folder"])
			if config["datasets"]["output_folder"][len(config["datasets"]["output_folder"])-1] == "/":
				f = open(config["datasets"]["output_folder"] + "mapping_log.txt","w+")
				fo = open(config["datasets"]["output_folder"] + "ontology_log.txt","w+")
			else:
				f = open(config["datasets"]["output_folder"] + "/mapping_log.txt","w+")
				fo = open(config["datasets"]["output_folder"] + "/ontology_log.txt","w+")
				
			used_types = {}
			used_predicates = {}

			with ThreadPoolExecutor(max_workers=10) as executor:
				for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
					dataset_i = "dataset" + str(int(dataset_number) + 1)
					triples_map_list = mapping_parser(config[dataset_i]["mapping"])

					for triples_map in triples_map_list:
						if triples_map.subject_map.rdf_class is not None:
							used_types[triples_map.subject_map.rdf_class] = config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1]
							f.write("The class " + triples_map.subject_map.rdf_class + " is being used in the mapping " + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1] + ".\n")
						for po in triples_map.predicate_object_maps_list:
							used_predicates[po.predicate_map.value] = config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1]
							f.write("The predicate " + po.predicate_map.value + " is being used in the mapping " + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1] + ".\n")
						f.write("\n")		

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
								WHERE { ?s rdf:type owl:ObjectProperty. }""")
			sparql.setReturnFormat(JSON)
			obj_property = sparql.query().convert()

			sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
								PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
								SELECT ?s 
								WHERE { ?s rdf:type owl:Class. }""")
			sparql.setReturnFormat(JSON)
			types = sparql.query().convert()

			sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
								PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
								SELECT ?s 
								WHERE { ?s rdf:type owl:Property. }""")
			sparql.setReturnFormat(JSON)
			properties = sparql.query().convert()

			fo.write("The following classes from the onthology are not being used in the mappings:\n")

			for c in types["results"]["bindings"]:
				if c["s"]["value"] not in used_types:
					fo.write(c["s"]["value"] + "\n")

			fo.write("The following predicates from the onthology are not being used in the mappings:\n")

			for p in predicates["results"]["bindings"]:
				if p["s"]["value"] not in used_predicates:
					fo.write(p["s"]["value"] + "\n")

			for o in obj_property["results"]["bindings"]:
				if o["s"]["value"] not in used_predicates:
					fo.write(o["s"]["value"] + "\n")

			for o in properties["results"]["bindings"]:
				if o["s"]["value"] not in used_predicates:
					fo.write(o["s"]["value"] + "\n")


			print("Succesfully verifiried onthology...")
		else:
			print("An endpoint must be given for this mode.")
			print('Aborting...')
			sys.exit(1)

	elif config["datasets"]["mode"].lower() == "ontology":

		print("Verifying onthology...")

		if not os.path.exists(config["datasets"]["output_folder"]):
			os.mkdir(config["datasets"]["output_folder"])
		if config["datasets"]["output_folder"][len(config["datasets"]["output_folder"])-1] == "/":
			f = open(config["datasets"]["output_folder"] + "ontology_log.txt","w+")
		else:
			f = open(config["datasets"]["output_folder"] + "/ontology_log.txt","w+")

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
							WHERE { ?s rdf:type owl:ObjectProperty. }""")
		sparql.setReturnFormat(JSON)
		obj_property = sparql.query().convert()

		sparql.setQuery("""PREFIX owl: <http://www.w3.org/2002/07/owl#>
							PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
							SELECT ?s 
							WHERE { ?s rdf:type owl:Class. }""")
		sparql.setReturnFormat(JSON)
		types = sparql.query().convert()

		sparql.setQuery("""select distinct ?class{
							{select distinct ?class
							where { ?triplesMap <http://www.w3.org/ns/r2rml#objectMap> ?node.
							?node <http://www.w3.org/ns/r2rml#template> ?template.
							FILTER(?template NOT IN (\"https://www.drugbank.ca/drugs/{DrugBankID}\",\"http://bio2rdf.org/drugbank:{DrugBankID}\",\"http://wifo5-04.informatik.uni-mannheim.de/drugbank/resource/drugs/{DrugBankID}\"))
							BIND(SUBSTR(STR( STRBEFORE(STR(?template),STR("/{")) ),25) AS ?class)
							}
							} 
							UNION
							{select distinct ?class
							where{
							?triplesMap <http://www.w3.org/ns/r2rml#subjectMap> ?node.
							?node <http://www.w3.org/ns/r2rml#class> ?template.
							BIND(SUBSTR(STR(?template),29) AS ?class)}
							}
							}""")
		sparql.setReturnFormat(JSON)
		mapping_types = sparql.query().convert()

		sparql.setQuery("""select distinct ?template
							where { 
							{{ ?template rdf:type owl:ObjectProperty. }
							UNION 
							{ ?template rdf:type owl:DatatypeProperty. }}
							FILTER ( !EXISTS 
							{?triplesMap <http://www.w3.org/ns/r2rml#predicateObjectMap> ?node.
							?node <http://www.w3.org/ns/r2rml#predicate> ?template.})
							}""")
		sparql.setReturnFormat(JSON)
		mapping_properties = sparql.query().convert()

		f.write("The following classes from the onthology are not being used in the mappings:\n")

		for c in types["results"]["bindings"]:
			if used_classes(c["s"]["value"],mapping_types):
				f.write(c["s"]["value"] + "\n")

		f.write("The following predicates from the onthology are not being used in the mappings:\n")

		for p in predicates["results"]["bindings"]:
			if used_properties(p["s"]["value"],mapping_properties):
				f.write(p["s"]["value"] + "\n")

		for o in obj_property["results"]["bindings"]:
			if used_properties(o["s"]["value"],mapping_properties):
				f.write(o["s"]["value"] + "\n")

		print("Succesfully verifiried onthology...")

	else:
		print("The mode chosen is not valid.")
		print('Aborting...')
		sys.exit(1)	

def main():

	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv, 'hc:', 'config_file=')
	except getopt.GetoptError:
		print('python3 verify.py -c <config_file>')
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-h':
			print('python3 verify.py -c <config_file>')
			sys.exit()
		elif opt == '-c' or opt == '--config_file':
			config_path = arg

	verify(config_path)

if __name__ == "__main__":
	main()
