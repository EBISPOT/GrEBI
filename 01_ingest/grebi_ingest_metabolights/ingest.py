#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import sys
import json

def parse_metabolights_xml(xml_content):
    root = ET.fromstring(xml_content)

    entries = []
    for entry in root.findall(".//entry"):
        entry_id = entry.get("id")
        entry_data = {
            "id": entry_id,
            "grebi:name": entry.find("name").text if entry.find("name") is not None else None,
            "grebi:description": entry.find("description").text if entry.find("description") is not None else None
        }

        cross_references = []
        for ref in entry.findall(".//ref"):
            cross_ref = ref.get("dbkey")
            cross_references.append(cross_ref)
        entry_data["metabolights:ref"] = cross_references

        for date in entry.findall(".//date"):
            date_type = date.get("type")
            date_value = date.get("value")
            entry_data[f"metabolights:{date_type}_date"] = date_value

        for field in entry.findall(".//field"):
            field_name = f"metabolights:{field.get('name')}"
            if field_name in entry_data:
                entry_data[field_name].append(field.text)
            else:
                entry_data[field_name] = [field.text]

        if entry_id.startswith("MTBLS"):
            entry_data["grebi:type"]="metabolights:Study"
        elif entry_id.startswith("MTBLC"):
            entry_data["grebi:type"]="metabolights:Chemical"
            entry_data["metabolights:chemical"]=entry_data["metabolights:ref"]
            if "metabolights:inchi" in entry_data:
                entry_data["metabolights:chemical"]=entry_data["metabolights:chemical"]+entry_data["metabolights:inchi"]
            if "metabolights:formula" in entry_data:
                entry_data["metabolights:chemical"]=entry_data["metabolights:chemical"]+entry_data["metabolights:formula"]
        else:
            assert False

        entries.append(entry_data)

    return entries

xml_content = sys.stdin.read()
entries = parse_metabolights_xml(xml_content)

for entry in entries:
    print(json.dumps(entry))
