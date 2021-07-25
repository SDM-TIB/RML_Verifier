import os
import os.path
import re
import csv
import sys
import rdflib
import getopt

def count_characters(string):
	count = 0
	for s in string:
		if s == "{":
			count += 1
	return count

def translate_sql(triples_map):

    query_list = []
    proyections = []
  
    if "{" in triples_map.subject_map.value:
        subject = triples_map.subject_map.value
        count = count_characters(subject)
        if (count == 1) and (subject.split("{")[1].split("}")[0] not in proyections):
            subject = subject.split("{")[1].split("}")[0]
            if "[" in subject:
                subject = subject.split("[")[0]
            proyections.append(subject)
        elif count > 1:
            subject_list = subject.split("{")
            for s in subject_list:
                if "}" in s:
                    subject = s.split("}")[0]
                    if "[" in subject:
                        subject = subject.split("[")
                    if subject not in proyections:
                        proyections.append(subject)
    else:
        if triples_map.subject_map.value not in proyections:
            proyections.append(triples_map.subject_map.value)

    for po in triples_map.predicate_object_maps_list:
        if "{" in po.object_map.value:
            count = count_characters(po.object_map.value)
            if 0 < count <= 1 :
                predicate = po.object_map.value.split("{")[1].split("}")[0]
                if "[" in predicate:
                    predicate = predicate.split("[")[0]
                if predicate not in proyections:
                    proyections.append(predicate)

            elif 1 < count:
                predicate = po.object_map.value.split("{")
                for po_e in predicate:
                    if "}" in po_e:
                        pre = po_e.split("}")[0]
                        if "[" in pre:
                            pre = pre.split("[")
                        if pre not in proyections:
                            proyections.append(pre)
        elif "#" in po.object_map.value:
            pass
        elif "/" in po.object_map.value:
            pass
        else:
            predicate = po.object_map.value 
            if "[" in predicate:
                predicate = predicate.split("[")[0]
            if predicate not in proyections:
                proyections.append(predicate)
        if po.object_map.child != None:
            for c in po.object_map.child:
                if c not in proyections:
                    proyections.append(c)

    temp_query = "SELECT DISTINCT "
    for p in proyections:
        if type(p) == str:
            if p != "None":
                temp_query += "`" + p + "`, "
        elif type(p) == list:
            for pr in p:
                temp_query += "`" + pr + "`, " 
    temp_query = temp_query[:-2] 
    if triples_map.tablename != "None":
        temp_query = temp_query + " FROM " + triples_map.tablename + ";"
    else:
        temp_query = temp_query + " FROM " + triples_map.data_source + ";"
    query_list.append(temp_query)

    return triples_map.iterator, query_list

def used_properties(value, values_list):
	for v in values_list["results"]["bindings"]:
		if v["predicate"]["value"] in value and v["predicate"]["value"] != "":
			return False
	return True

def used_classes(value, values_list):
	for v in values_list["results"]["bindings"]:
		if v["class"]["value"] in value and v["class"]["value"] != "":
			return False
	return True


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