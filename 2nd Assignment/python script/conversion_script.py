import csv
import json

def convert_csv_to_json(csv_file):
    json_data = []

    with open(csv_file, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            json_row = {}
            json_row['show_id'] = int(row['show_id'])
            json_row['type'] = row['type']
            json_row['title'] = row['title']
            json_row['director'] = row['director']
            
            # convert the cast field to a list
            cast = row['cast'].split(', ')
            json_row['cast'] = cast if cast[0] != '' else []

            # convert the country field to a list
            country = row['country'].split(', ')
            json_row['country'] = country if country[0] != '' else []

            json_row['date_added'] = row['date_added']
            json_row['release_year'] = int(row['release_year'])
            json_row['rating'] = row['rating']
            json_row['duration'] = row['duration']
            
            # convert the listed_in field to a list
            listed_in = row['listed_in'].split(', ')
            json_row['listed_in'] = listed_in if listed_in[0] != '' else []

            json_row['description'] = row['description']

            json_data.append(json_row)

    return json_data


# Add your path to the CSV file
csv_file = 'netflix_titles.csv'


json_data = convert_csv_to_json(csv_file)


with open('netflix_data.json', 'w', encoding='utf-8') as file:
    json.dump(json_data, file, indent=4, ensure_ascii=False)

print("Conversion completed. JSON file saved as netflix_data.json.")