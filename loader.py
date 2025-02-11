import csv
import json
import os
import ssl
import argparse
from elasticsearch import Elasticsearch, helpers

# Ensure SSL verification is handled properly
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

# Fields to extract and their types
FIELDS = {
    "cataloged_item_type": "keyword",
    "cat_num": "text",
    "institution_acronym": "keyword",
    "collection_cde": "keyword",
    "collectors": "keyword",  # Stored as a keyword list
    "continent_ocean": "keyword",
    "country": "keyword",
    "state_prov": "keyword",
    "county": "keyword",
    "dec_lat": "float",
    "dec_long": "float",
    "datum": "text",
    "coordinateuncertaintyinmeters": "float",
    "scientific_name": "text",
    "identifiedby": "text",
    "kingdom": "text",
    "phylum": "text",
    "family": "text",
    "genus": "text",
    "species": "text",
    "subspecies": "text",
    "relatedinformation": "text",  # URL link but stored as text
    "year": "integer",
    "month": "integer",
    "day": "integer",
    "taxon_rank": "keyword"
}

class ESLoader:
    def __init__(self, csv_file, index_name, es_host='http://localhost:9200'):
        self.csv_file = csv_file
        self.index_name = index_name
        self.es_host = es_host
        #self.es = Elasticsearch([es_host])
        self.es = Elasticsearch( [{'host': self.es_host, 'port': 80, 'scheme': 'http'}],timeout=60)

    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            print(f"Index '{self.index_name}' already exists. Deleting and recreating.")
            self.es.indices.delete(index=self.index_name)
        
        mappings = {
            "mappings": {
                "properties": {field: {"type": FIELDS[field]} for field in FIELDS}
            }
        }
        self.es.indices.create(index=self.index_name, body=mappings)

    def load_data(self):
        print(f"Loading data from {self.csv_file} into index {self.index_name}")
        with open(self.csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = []
            for row in reader:
                doc = {}
                for field in FIELDS:
                    if field in row:
                        value = row[field].strip()
                        if FIELDS[field] == "integer":
                            doc[field] = int(value) if value.isdigit() else None
                        elif FIELDS[field] == "float":
                            try:
                                doc[field] = float(value)
                            except ValueError:
                                doc[field] = None
                        elif field == "collectors":  # Convert to keyword list
                            doc[field] = [c.strip() for c in value.split(",") if c.strip()]
                        else:
                            doc[field] = value
                data.append({"_index": self.index_name, "_source": doc})
            helpers.bulk(self.es, data)
        print(f"Finished indexing data into {self.index_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load data into Elasticsearch')
    parser.add_argument('csv_file', help='Path to CSV file')
    args = parser.parse_args()

    host =  '149.165.170.158'
    index = 'arctos'

    loader = ESLoader(args.csv_file, index, host)
    loader.create_index()
    loader.load_data()
