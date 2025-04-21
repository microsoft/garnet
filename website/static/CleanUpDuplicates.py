import json

def process_chunk(chunk):
    cleaned_data = {}
    for formal_name, entries in chunk.items():
        if ".0 Release)" in formal_name:
            name_to_highest_value = {}
            for entry in entries:
                benches = entry.get("benches", [])
                for bench in benches:
                    name = bench["name"]
                    value = bench["value"]
                    if name in name_to_highest_value:
                        if value > name_to_highest_value[name]["value"]:
                            # Delete the previous bench
                            del name_to_highest_value[name]
                            # Update with the current bench
                            name_to_highest_value[name] = bench
                        else:
                            # Delete the current bench
                            continue
                    else:
                        name_to_highest_value[name] = bench
            cleaned_entries = []
            for entry in entries:
                benches = entry.get("benches", [])
                cleaned_benches = [bench for bench in benches if bench["name"] in name_to_highest_value]
                entry["benches"] = cleaned_benches
                cleaned_entries.append(entry)
            cleaned_data[formal_name] = cleaned_entries
    return cleaned_data

def process_large_json(file_path, chunk_size=10000):
    cleaned_data = {}
    with open(file_path, 'r') as file:
        buffer = ""
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            try:
                json_chunk = json.loads(buffer)
                cleaned_chunk = process_chunk(json_chunk)
                cleaned_data.update(cleaned_chunk)
                buffer = ""
            except json.JSONDecodeError:
                continue

    return cleaned_data

# Path to the large JSON file
file_path = 'large_data.json'

# Process the large JSON file and remove duplicates
cleaned_data = process_large_json(file_path)

# Save the cleaned data to a new JSON file with .out at the end of the name
output_file_path = file_path + '.out'
with open(output_file_path, 'w') as file:
    json.dump(cleaned_data, file, indent=4)

print(f"Duplicates removed and cleaned data saved to {output_file_path}.")
