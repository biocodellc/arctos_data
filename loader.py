# -*- coding: utf-8 -*-
import csv
import json
import os
import ssl
import argparse
from glob import glob
from collections import Counter
from elasticsearch import Elasticsearch, helpers

# Increase max field size for large CSV entries
csv.field_size_limit(10**7)

# If SSL cert checking needs to be disabled
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

# Fields and their types for mapping
FIELDS = {
    "guid_prefix": "keyword",
    "type": "keyword",  # NEW derived field from guid_prefix via lookup
    "cataloged_item_type": "keyword",
    "cat_num": "text",
    "institution_acronym": "keyword",
    "collection_cde": "keyword",
    "collectors": "keyword",
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
    "kingdom": "keyword",
    "phylum": "keyword",
    "family": "keyword",
    "genus": "keyword",
    "species": "keyword",
    "subspecies": "keyword",
    "relatedinformation": "text",
    "year": "integer",
    "month": "integer",
    "day": "integer",
    "taxon_rank": "keyword",
    "parts": "keyword",
    "has_tissue": "keyword",
}

DEFAULT_CHUNK_SIZE = 100000

def load_type_lookup(lookup_path: str) -> dict:
    """
    Load a CSV mapping with headers: guid_prefix,type
    Returns dict {guid_prefix: type_string}
    """
    mapping = {}
    if not os.path.exists(lookup_path):
        print(f"⚠️  Lookup file '{lookup_path}' not found. 'type' will default to guid_prefix.")
        return mapping
    with open(lookup_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing_cols = {"guid_prefix", "type"} - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(
                f"Lookup file '{lookup_path}' is missing required columns: {', '.join(sorted(missing_cols))}"
            )
        for row in reader:
            gp = (row.get("guid_prefix") or "").strip()
            tp = (row.get("type") or "").strip()
            if gp:
                mapping[gp] = tp or gp
    print(f"🔎 Loaded {len(mapping):,} guid_prefix → type mappings from '{lookup_path}'.")
    return mapping

def create_index(es: Elasticsearch, index_name: str):
    if es.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists. Deleting and recreating.")
        es.indices.delete(index=index_name)
    mappings = {
        "mappings": {
            "properties": {field: {"type": FIELDS[field]} for field in FIELDS}
        }
    }
    es.indices.create(index=index_name, body=mappings)
    print(f"✅ Created index '{index_name}'.")

def transform_row(row: dict, type_map: dict) -> dict:
    doc = {}
    for field, ftype in FIELDS.items():
        if field == "type":
            # filled below after guid_prefix is read
            continue
        if field in row:
            value = (row[field] or "").strip()
            if ftype == "integer":
                try:
                    doc[field] = int(value) if value != "" else None
                except Exception:
                    doc[field] = None
            elif ftype == "float":
                try:
                    doc[field] = float(value) if value != "" else None
                except Exception:
                    doc[field] = None
            elif field == "collectors":
                doc[field] = [c.strip() for c in value.split(",") if c.strip()]
            else:
                doc[field] = value
    # derive 'type' from guid_prefix via lookup (fallback to guid_prefix if unmapped)
    gp = doc.get("guid_prefix", "")
    doc["type"] = type_map.get(gp, gp)
    return doc

def preview_file(csv_path: str, type_map: dict, max_preview: int = 5) -> dict:
    """
    Print up to max_preview transformed docs; return summary counts.
    """
    total_rows = 0
    unknowns = Counter()
    print(f"\n📄 Preview: {os.path.basename(csv_path)}")
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            total_rows += 1
            doc = transform_row(row, type_map)
            if i <= max_preview:
                print(json.dumps(doc, ensure_ascii=False))
            if doc.get("type", "") == doc.get("guid_prefix", ""):
                # unmapped (fallback used)
                gp = doc.get("guid_prefix") or ""
                unknowns[gp] += 1
    print(f"   Rows (total): {total_rows:,}")
    if unknowns:
        top5 = ", ".join([f"{k or '(empty)'}×{v}" for k, v in unknowns.most_common(5)])
        print(f"   Unmapped guid_prefix (top 5): {top5}")
    else:
        print("   Unmapped guid_prefix: none 🎉")
    return {"rows": total_rows, "unknowns": unknowns}

def index_file(es: Elasticsearch, index_name: str, csv_path: str, type_map: dict, chunk_size: int):
    print(f"\n🚚 Loading: {os.path.basename(csv_path)} → index '{index_name}'")
    total = 0
    chunk = []
    chunk_num = 0
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            doc = transform_row(row, type_map)
            chunk.append({"_index": index_name, "_source": doc})
            if len(chunk) >= chunk_size:
                chunk_num += 1
                _bulk(es, chunk, chunk_num)
                chunk.clear()
        if chunk:
            chunk_num += 1
            _bulk(es, chunk, chunk_num)
    print(f"✅ Done: {os.path.basename(csv_path)} ({total:,} rows)")

def _bulk(es: Elasticsearch, actions, chunk_num: int):
    print(f"   Processing chunk #{chunk_num} with {len(actions)} rows ...", end="", flush=True)
    try:
        helpers.bulk(es, actions)
        print(" done.")
    except Exception as e:
        print(f"\n   ❌ Error indexing chunk #{chunk_num}: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Load CSV files from a directory into Elasticsearch (with --test preview)."
    )
    parser.add_argument("--data-dir", "-d", required=True, help="Directory containing CSV files to load (non-recursive).")
    parser.add_argument("--test", "-t", action="store_true", help="Preview transformed docs instead of indexing.")
    parser.add_argument("--lookup-file", "-l", default="type_lookup.csv",
                        help="CSV mapping file in current directory with headers: guid_prefix,type (default: type_lookup.csv)")
    parser.add_argument("--host", default="149.165.170.158", help="Elasticsearch host (default: 149.165.170.158)")
    parser.add_argument("--port", type=int, default=8081, help="Elasticsearch port (default: 8081)")
    parser.add_argument("--index", "-i", default="arctos", help="Target index name (default: arctos)")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help=f"Bulk chunk size (default: {DEFAULT_CHUNK_SIZE})")
    parser.add_argument("--max-preview", type=int, default=5, help="Max preview rows per file in --test (default: 5)")
    args = parser.parse_args()

    # Collect CSVs
    if not os.path.isdir(args.data_dir):
        raise SystemExit(f"Data directory not found: {args.data_dir}")
    all_csvs = sorted([p for p in glob(os.path.join(args.data_dir, "*.csv")) if os.path.basename(p) != os.path.basename(args.lookup_file)])
    if not all_csvs:
        raise SystemExit(f"No CSV files found in {args.data_dir}")

    # Load mapping
    # Lookup path is relative to current working directory (as requested)
    type_map = load_type_lookup(args.lookup_file)

    if args.test:
        print(f"\n🧪 TEST MODE — no data will be written to Elasticsearch.")
        print(f"Data dir: {args.data_dir}")
        print(f"Lookup file: {args.lookup_file}")
        print(f"Files found: {len(all_csvs)}")
        grand_total = 0
        grand_unknowns = Counter()
        for path in all_csvs:
            res = preview_file(path, type_map, max_preview=args.max_preview)
            grand_total += res["rows"]
            grand_unknowns.update(res["unknowns"])
        print(f"\n📊 Summary: {grand_total:,} total rows across {len(all_csvs)} file(s).")
        if grand_unknowns:
            print(f"   Distinct unmapped guid_prefix: {len(grand_unknowns)} (top 10)")
            for gp, cnt in grand_unknowns.most_common(10):
                print(f"     - {gp or '(empty)'}: {cnt}")
        else:
            print("   All guid_prefix values mapped. 🎉")
        return

    # LIVE load
    es = Elasticsearch(hosts=[{"host": args.host, "port": args.port, "scheme": "http"}], request_timeout=60)
    create_index(es, args.index)
    for path in all_csvs:
        index_file(es, args.index, path, type_map, args.chunk_size)
    print("\n🏁 All files loaded.")

if __name__ == "__main__":
    main()

