#!/usr/bin/env bash
set -euo pipefail
exec /usr/bin/python3 -u /home/exouser/code/arctos_data/load_lupinus.py --csv /home/exouser/code/arctos_data/data/filtered_flat.csv --env-file /home/exouser/code/arctos_data/.env --index arctos-20260925 --types /home/exouser/code/arctos_data/lupinus_type_lookup.json --state /home/exouser/code/arctos_data/lupinus_import/state.json --connect-url https://127.0.0.1:19200 --ca-cert /home/exouser/code/arctos_data/lupinus_import/http_ca.crt --workers 4
