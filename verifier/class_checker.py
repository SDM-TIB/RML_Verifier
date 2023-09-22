import os
import os.path
import re
import csv
import sys
import rdflib
import urllib
import getopt
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
from rdflib.plugins.sparql import prepareQuery
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm

global prefixes
prefixes = {}

def encode_char(string):
	encoded = ""
	valid_char = ["~","#","/",":"]
	for s in string:
		if s in valid_char:
			encoded += s
		elif s == "/":
			encoded += "%2F"
		else:
			encoded += urllib.parse.quote(s)
	return encoded

def string_substitution(string, pattern, row, term, ignore, iterator):
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None":
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp and tp in row:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
				elif  tp == "":
					if len(row.keys()) == 1:
						while list(row.keys())[0] not in temp_keys:
							row = row[list(row.keys())[0]]
							if isinstance(row,list):
								break
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			no_match = True
			if "]." in reference_match.group(1):
				temp = reference_match.group(1).split("].")
				match = temp[1]
				condition = temp[0].split("[")
				temp_value = row[condition[0]]
				if "==" in condition[1]:
					temp_condition = condition[1][2:-1].split("==")
					iterators = temp_condition[0].split(".")
					if isinstance(temp_value,list):
						for tv in temp_value:
							t_v = tv
							for cond in iterators[:-1]:
								if cond != "@":
									t_v = t_v[cond]
							if temp_condition[1][1:-1] == t_v[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] == temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				elif "!=" in condition[1]:
					temp_condition = condition[1][2:-1].split("!=")
					iterators = temp_condition[0].split(".")
					match = iterators[-1]
					if isinstance(temp_value,list):
						for tv in temp_value:
							for cond in iterators[-1]:
								if cond != "@":
									temp_value = temp_value[cond]
							if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				if no_match:
					return None
			else:
				match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if row[match] != None and row[match] != "nan" and row[match] != "N/A" and row[match] != "None":
					if (type(row[match]).__name__) != "str" and row[match] != None:
						if (type(row[match]).__name__) == "float":
							row[match] = repr(row[match])
						else:
							row[match] = str(row[match])
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', row[match]) is not None:
							row[match] = repr(float(row[match]))
					if isinstance(row[match],dict):
						print("The key " + match + " has a Json structure as a value.\n")
						print("The index needs to be indicated.\n")
						return None
					else:
						if re.search("^[\s|\t]*$", row[match]) is None:
							value = row[match]
							if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
								value = encode_char(value)
							new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(value) - (end - start)
							if "\\" in new_string:
								new_string = new_string.replace("\\", "")
								count = new_string.count("}")
								i = 0
								while i < count:
									new_string = "{" + new_string
									i += 1
								new_string = new_string.replace(" ", "")

						else:
							return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		elif pattern == ".+":
			match = reference_match.group(0)
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if (type(row[match]).__name__) != "str" and row[match] != None:
					if (type(row[match]).__name__) == "float":
						row[match] = repr(row[match])
					else:
						row[match] = str(row[match])
				if isinstance(row[match],dict):
					print("The key " + match + " has a Json structure as a value.\n")
					print("The index needs to be indicated.\n")
					return None
				else:
					if row[match] != None and row[match] != "nan" and row[match] != "N/A" and row[match] != "None":
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start] + row[match].strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
					else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)
	return new_string

def object_extraction(obj_list,obj_map,subj_map,data):
	for row in data:
		subj = string_substitution(subj_map.value, "{(.+?)}", row, "subject", "yes", "None")
		if obj_map.__class__.__name__ == "ObjectMap":
			if obj_map.mapping_type == "template":
				obj = string_substitution(obj_map.value, "{(.+?)}", row, "object", "yes", "None")
			elif obj_map.mapping_type == "reference":
				obj = string_substitution(obj_map.value, ".+", row, "object", "yes", "None")
			elif obj_map.mapping_type == "constant":
				obj = obj = "<{}>".format(obj_map.value)
		else:
			obj = string_substitution(obj_map.value, "{(.+?)}", row, "object", "yes", "None")

		if obj != None:
			if obj[1:-1] not in obj_list:
				obj_list[obj[1:-1]] = {subj:""}
			else:
				if subj not in obj_list[obj[1:-1]]:
					obj_list[obj[1:-1]][subj] = ""
	return obj_list

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

def attr_extraction(string):
	attributes = {}
	if "{" in string:
		for pseudo_attr in string.split("{"):
			if "}" in pseudo_attr:
				attr = pseudo_attr.split("}")[0]
				if attr not in attributes:
					attributes[attr] = ""
	elif "http" in string:
		pass
	else:
		attributes[string] = ""
	return attributes

def union(array1, array2):
	combined = array1
	for row in array2:
		if row not in combined:
			combined.append(row)
	return combined

def count_non_none(data):
	amount = 0
	for row in data:
		none_value = True
		for key in row:
			if type(row[key]).__name__ == "NoneType" or str(row[key]) == 'nan':
				none_value = False
		if none_value:
			amount += 1
	return amount

def main(config_path):
	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)
	triples_map_list = []
	print("Beginning Class Verification.\n")
	for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
		classes = {}
		dataset_i = "dataset" + str(int(dataset_number) + 1)
		triples_map_list = mapping_parser(config[dataset_i]["mapping"])
		for tp in triples_map_list:
			if tp.subject_map is not None:
				clss = ""
				attr = attr_extraction(tp.subject_map.value)
				reader = pd.read_csv(tp.data_source, usecols=attr.keys())
				reader = reader.where(pd.notnull(reader), None)
				reader = reader.drop_duplicates(keep='first')
				reader = reader.to_dict(orient='records')
				if tp.subject_map.rdf_class is not None:
					if not isinstance(tp.subject_map.rdf_class,list):
						clss = "<{}>".format(tp.subject_map.rdf_class)
						if clss not in classes:
							classes[clss] = {"value":reader}
						else:
							classes[clss]["value"] = union(classes[clss]["value"],reader)
					else:
						for rdf_class in tp.subject_map.rdf_class:
							clss = "<{}>".format(rdf_class)
							if clss not in classes:
								classes[clss] = {"value":reader}
							else:
								classes[clss]["value"] = union(classes[clss]["value"],reader)
				else:
					for po in tp.predicate_object_maps_list:
						if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" in po.predicate_map.value:
							clss = "<{}>".format(po.object_map.value)
							if clss not in classes:
								classes[clss] = {"value":reader}
							else:
								classes[clss]["value"] = union(classes[clss]["value"],reader)

				if clss != "":
					predicates = {}
					for po in tp.predicate_object_maps_list:
						object_values = {}
						if po.object_map.mapping_type == "constant":
							obj = "<{}>".format(po.predicate_map.value)
							if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" in obj:
								pass
							else:
								if obj not in predicates:
									predicates[obj] = {"value":reader}
								else:
									predicates[obj]["value"] = union(predicates[obj]["value"],reader)
								predicates[obj]["object_values"] = {po.object_map.value:count_non_none(reader)}
						if po.object_map.mapping_type == "template" or po.object_map.mapping_type == "reference":
							obj = "<{}>".format(po.predicate_map.value)
							if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" in obj:
								pass
							else:
								if po.object_map.mapping_type == "template":
									attr2 = attr_extraction(po.object_map.value)
									for a in attr2:
										if a not in attr:
											attr[a] = ""
								elif po.object_map.mapping_type == "reference":
									attr2 = po.object_map.value
									if attr2 not in attr:
										attr[attr2] = ""
								reader = pd.read_csv(tp.data_source, usecols=attr.keys())
								reader = reader.where(pd.notnull(reader), None)
								reader = reader.drop_duplicates(keep='first')
								reader = reader.to_dict(orient='records')
								if obj not in predicates:
									predicates[obj] = {"value":reader}
								else:
									predicates[obj]["value"] = union(predicates[obj]["value"],reader)
								object_values = object_extraction(object_values,po.object_map,tp.subject_map,reader)
								predicates[obj]["object_values"] = object_values
								attr = attr_extraction(tp.subject_map.value)
						elif po.object_map.mapping_type == "parent triples map":
							for tp_element in triples_map_list:
								if po.object_map.value == tp_element.triples_map_id:
									if tp_element.data_source == tp.data_source:
										attr2 = attr_extraction(tp_element.subject_map.value)
										for a in attr2:
											if a not in attr:
												attr[a] = ""
										reader = pd.read_csv(tp.data_source, usecols=attr.keys())
										reader = reader.where(pd.notnull(reader), None)
										reader = reader.drop_duplicates(keep='first')
										reader = reader.to_dict(orient='records')
										if po.object_map.child == None and po.object_map.parent == None:
											obj = "<{}>".format(po.predicate_map.value)
											if obj not in predicates:
												predicates[obj] = {"value":reader}
											else:
												predicates[obj]["value"] = union(predicates[obj]["value"],reader)
											object_values = object_extraction(object_values,tp_element.subject_map,tp.subject_map,reader)
											predicates[obj]["object_values"] = object_values
											attr = attr_extraction(tp.subject_map.value)
									else:
										pass
					classes[clss]["predicates"] = predicates
		print("Complete verification of mapping classes in  " + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1] + ".\n")

		mapping_file = open(config["datasets"]["output_folder"] + "/" + config[dataset_i]["name"] + "_class_verification.txt","w")
		i = 0
		for clss in classes:
			sparql = SPARQLWrapper(config[dataset_i]["endpoint"])
			query = "SELECT count(distinct ?s) as ?cardinality\n"
			query += "WHERE { ?s a " + clss + ".}"
			sparql.setQuery(query)
			sparql.setReturnFormat(JSON)
			types = sparql.query().convert()
			for c in types["results"]["bindings"]:
				mapping_file.write("Class " + clss + ": Number of Subjects from Source: " + str(count_non_none(classes[clss]["value"])) + " Number of Subjects from Ontology: " + c["cardinality"]["value"] + "\n")
				if count_non_none(classes[clss]["value"]) != int(c["cardinality"]["value"]):
					i += 1
				query = "SELECT distinct ?p\n"
				query += "WHERE {?s ?p ?o.\n"
				query += "	?s a " + clss + " .}\n"
				sparql.setQuery(query)
				sparql.setReturnFormat(JSON)
				ontology_predicates = sparql.query().convert()
				for predicate in ontology_predicates["results"]["bindings"]:
					query = "SELECT distinct count(distinct ?s) as ?cardinality\n"
					query += "WHERE {?s <" + predicate["p"]["value"] + "> ?o.\n"
					query += "	?s a " + clss + " .}\n"
					sparql.setQuery(query)
					sparql.setReturnFormat(JSON)
					predicate_values = sparql.query().convert()
					for p in predicate_values["results"]["bindings"]:
						if "<" + predicate["p"]["value"] + ">" in classes[clss]["predicates"]:
							mapping_file.write("Predicate " + predicate["p"]["value"] + ": Number of Subjects from Source: " + str(count_non_none(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["value"])) + " Number of Subjects from Ontology: " + p["cardinality"]["value"] + "\n")
							if count_non_none(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["value"]) != int(p["cardinality"]["value"]):
								i += 1
							query = "SELECT distinct ?o count(distinct ?s) as ?cardinality\n"
							query += "WHERE {?s <" + predicate["p"]["value"] + "> ?o.\n"
							query += "	?s a " + clss + " .}\n"
							query += "GROUP BY ?o"
							sparql.setQuery(query)
							sparql.setReturnFormat(JSON)
							object_list = sparql.query().convert()
							for o in object_list["results"]["bindings"]:
								if o["o"]["value"] in classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"]:
									if isinstance(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"][o["o"]["value"]],int):
										mapping_file.write("Object " + o["o"]["value"] + ": Number of Objects from Source: " + str(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"][o["o"]["value"]]) + " Number of Objects from Ontology: " + o["cardinality"]["value"] + "\n")
										if classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"][o["o"]["value"]] != int(o["cardinality"]["value"]):
											i += 1 
									else:
										mapping_file.write("Object " + o["o"]["value"] + ": Number of Objects from Source: " + str(len(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"][o["o"]["value"]])) + " Number of Objects from Ontology: " + o["cardinality"]["value"] + "\n")
										if len(classes[clss]["predicates"]["<" + predicate["p"]["value"] + ">"]["object_values"][o["o"]["value"]]) != int(o["cardinality"]["value"]):
											i += 1 
		mapping_file.write("\nNumber of Inconsistencies Found: " + str(i) + "\n")
		mapping_file.close()
		print("Complete verification of mapping classes in Endpoint\n")
		print("Ending Class Verification.\n")	

if __name__ == '__main__':
	main(str(sys.argv[1]))