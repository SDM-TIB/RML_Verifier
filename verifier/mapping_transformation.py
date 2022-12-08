import os
import os.path
import re
import csv
import sys
import rdflib
import getopt
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
from rdflib.plugins.sparql import prepareQuery

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm

global prefixes
prefixes = {}

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

def prefix_extraction(original, uri):
    url = ""
    value = ""
    if prefixes:
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    else:
        f = open(original,"r")
        original_mapping = f.readlines()
        for prefix in original_mapping:
            if ("prefix" in prefix) or ("base" in prefix):
               elements = prefix.split(" ")
               prefixes[elements[2][1:-1]] = elements[1][:-1]
            else:
                break
        f.close()
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    return prefixes[url], url, value

def update_mapping(triple_maps, original):
    mapping = ""
    for triples_map in triple_maps:

        if triples_map.function:
            if "#" in triples_map.triples_map_id:
                mapping += "<" + triples_map.triples_map_id.split("#")[1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">\n"
            else: 
                mapping += "<" + triples_map.triples_map_id.split("/")[len(triples_map.triples_map_id.split("/"))-1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">\n"
            mapping += "    fnml:functionValue [\n"
            mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
            if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None": 
                mapping += "                rml:referenceFormulation ql:CSV\n"
            else:
                mapping += "    rml:logicalSource [ rml:source <DB_source>;\n"
                mapping += "                        rr:tableName \"" + triples_map.tablename + "\";\n"
                if triples_map.query != "None": 
                    mapping += "                rml:query \"" + triples_map.query +"\"\n" 
            mapping += "                ];\n"
            po_exist = {}
            for predicate_object in triples_map.predicate_object_maps_list:
                if predicate_object.predicate_map.value not in po_exist:
                    mapping += "    rr:predicateObjectMap [\n"
                    if "constant" in predicate_object.predicate_map.mapping_type:
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "constant shortcut" in predicate_object.predicate_map.mapping_type:
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
    
                    mapping += "        rr:objectMap "
                    if "constant" in predicate_object.object_map.mapping_type:
                        if "execute" in predicate_object.predicate_map.value:
                            prefix, url, value = prefix_extraction(original, predicate_object.object_map.value)
                            mapping += "[\n"
                            mapping += "        rr:constant " + prefix + ":" + value + "\n"
                            mapping += "        ]\n"
                        else:
                           mapping += "[\n"
                           mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                           mapping += "        ]\n"
                    elif "template" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:template  \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "reference function" == predicate_object.object_map.mapping_type:
                        mapping += "<" + predicate_object.object_map.value + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">\n"
                    elif "reference" == predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rml:reference \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "parent triples map function" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        <" + predicate_object.object_map.value + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">;\n"
                        mapping += "        ]\n"
                    po_exist[predicate_object.predicate_map.value ] = ""
                    mapping += "	];\n"	
            mapping += "	].\n\n"
        else:
            if "#" in triples_map.triples_map_id:
                mapping += "<" + triples_map.triples_map_id.split("#")[1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">\n"
            else: 
                mapping += "<" + triples_map.triples_map_id.split("/")[len(triples_map.triples_map_id.split("/"))-1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">\n"

            mapping += "    a rr:TriplesMap;\n"
            mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
            if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None": 
                mapping += "                rml:referenceFormulation ql:CSV\n"
            else:
                mapping += "    rml:logicalSource [ rml:source <DB_source>;\n"
                mapping += "                        rr:tableName \"" + triples_map.tablename + "\";\n"
                if triples_map.query != "None": 
                    mapping += "                rml:query \"" + triples_map.query +"\"\n" 
            mapping += "                ];\n"

            
            mapping += "    rr:subjectMap "
            if triples_map.subject_map.subject_mapping_type is "template":
                mapping += "[\n"
                mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
            elif triples_map.subject_map.subject_mapping_type is "reference":
                mapping += "[\n"
                mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
                mapping += "        rr:termType rr:IRI\n"
            elif triples_map.subject_map.subject_mapping_type is "constant":
                mapping += "[\n"
                mapping += "        rr:constant \"" + triples_map.subject_map.value + "\";\n"
                mapping += "        rr:termType rr:IRI\n"
            elif triples_map.subject_map.subject_mapping_type is "function":
                mapping += "<" + triples_map.subject_map.value + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">;\n"
                mapping += "	rr:termType rr:IRI;\n"
            if triples_map.subject_map.rdf_class is not None:
                prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class)
                mapping += "        rr:class " + prefix + ":" + value  + "\n"
            if triples_map.subject_map.subject_mapping_type is not "function":
                mapping += "    ];\n"

            for predicate_object in triples_map.predicate_object_maps_list:
                if predicate_object.predicate_map.mapping_type is not "None":
                    mapping += "    rr:predicateObjectMap [\n"
                    if "constant" in predicate_object.predicate_map.mapping_type :
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "constant shortcut" in predicate_object.predicate_map.mapping_type:
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "template" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"  
                        mapping += "        ];\n"
                    elif "reference" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n" 
                        mapping += "        ];\n"

                    mapping += "        rr:objectMap "
                    if "constant" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "template" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:template  \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "reference" == predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rml:reference \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "parent triples map function" in predicate_object.object_map.mapping_type:
                        mapping += "<" + predicate_object.object_map.value + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0] + ">;\n"
                    elif "parent triples map" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        if "#" in predicate_object.object_map.value:
                            parent = predicate_object.object_map.value.split("#")[1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0]
                        else: 
                            parent =  predicate_object.object_map.value.split("/")[len(predicate_object.object_map.value.split("/"))-1] + "_" + original.split("/")[len(original.split("/"))-1].split(".")[0]
                        if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                            mapping += "        	rr:parentTriplesMap <" + parent + ">\n"
                            mapping = mapping[:-1]
                            mapping += ";\n"
                            mapping += "        rr:joinCondition [\n"
                            mapping += "            rr:child \"" + predicate_object.object_map.child + "\";\n"
                            mapping += "            rr:parent \"" + predicate_object.object_map.parent + "\";\n"
                            mapping += "        ]\n"
                        else:
                            mapping = mapping[:-1]
                            for tm in triple_maps:
                                if tm.triples_map_id == predicate_object.object_map.value:
                                    if tm.subject_map.subject_mapping_type == "constant":
                                        mapping += "\n            rr:constant \"" + tm.subject_map.value + "\";\n"
                                        mapping += "            rr:termType rr:IRI\n"
                                    else:
                                        mapping += "\n"
                                        mapping += "        rr:parentTriplesMap <" + parent + ">;\n" 
                        mapping += "        ]\n"
                    elif "constant shortcut" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    mapping += "    ];\n"
            if triples_map.function:
                pass
            else:
                mapping = mapping[:-2]
                mapping += ".\n\n"

    prefix_string = ""
    
    f = open(original,"r")
    original_mapping = f.readlines()
    db_source = ""
    for prefix in original_mapping:
        if "prefix;" in prefix or "d2rq:Database;" in prefix:
            pass
        elif ("prefix" in prefix) or ("base" in prefix):
           prefix_string += prefix
        elif "jdbcDSN" in prefix:
            db_source +=  prefix 
        elif "jdbcDriver" in prefix:
            db_source += prefix 
        elif "d2rq:username" in prefix:
            db_source += prefix 
        elif "d2rq:password" in prefix:
            db_source += prefix 
    f.close()  

    prefix_string += "\n"
    prefix_string += mapping
    if "" == db_source:
        prefix_string += db_source 
    mapping_file = open(original,"w")
    mapping_file.write(prefix_string)
    mapping_file.close()

def main(config_path):
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)
    for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
        dataset_i = "dataset" + str(int(dataset_number) + 1)
        print("Transforming mapping " + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1] + ".\n")
        triples_map_list = mapping_parser(config[dataset_i]["mapping"])
        update_mapping(triples_map_list,config[dataset_i]["mapping"])
        print("Complete transforming mapping " + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1] + ".\n")	

if __name__ == '__main__':
	main(str(sys.argv[1]))