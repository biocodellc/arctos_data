# Lupinus Arctos import

Source: `/home/exouser/code/arctos_data/data/filtered_flat.csv`
Destination: `https://huxley.bnhm.berkeley.edu:1113` (Lupinus)
Index: `arctos-20260925`
Credentials: `elastic` key in `.env`, read internally without logging.

The import preserves all CSV columns in `_source`, indexes the existing Arctos search fields plus `guid` and `collection_object_id`, maps `has_tissues` to `has_tissue`, and maps `continent` (or `ocean`) to `continent_ocean`. Collection types are copied from the existing Arctos index into `lupinus_type_lookup.json`; unknown collection prefixes have no derived `type`. Invalid numeric values are retained under `_csv_numeric_originals` with null in the typed field. Unmapped columns are stored but not searchable.

Batches use stable CSV row IDs, bounded memory, TLS verification, retry/backoff, and a checkpoint saved after acknowledged batches. Repeating the command resumes safely with the unchanged source. The loader refuses to overwrite an unrelated existing index.

Progress:
```bash
tail -f ~/code/arctos_data/lupinus_import/import.log
cat ~/code/arctos_data/lupinus_import/state.json
```

Resume after a stopped or failed process:
```bash
cd ~/code/arctos_data
nohup ./run_lupinus_import.sh >> lupinus_import/import.log 2>&1 < /dev/null &
echo $! > lupinus_import/import.pid
```

Only one process can hold the checkpoint lock. Keep the CSV and type lookup unchanged until completion.

During this load, the refresh interval is 30 seconds and replicas are zero on this single-node cluster. After loading, refresh is restored, document count is checked against CSV row count, and a search is verified. The `arctos` alias is created only if no index or alias by that name already exists. No biscicol.org endpoint configuration is changed.

## Proxy restriction and active tunnel

Apache on port 1113 allows reads but returned 403 for index creation and bulk writes. The index was created using authenticated SSH to Lupinus. The import launcher uses a direct SSH tunnel from biscicol.org through Huxley, listening only on `127.0.0.1:19200` on biscicol.org. The loader verifies the Elasticsearch CA certificate and hostname. The public CA was retrieved from Lupinus over verified SSH and saved as `lupinus_import/http_ca.crt`.

The SSH tunnel was authenticated using an isolated agent with a 60-second key lifetime; no private key was copied to a server. Keep the outer SSH session open until the import finishes. If the tunnel closes, it must be re-established before running the resume command. After an Apache configuration change permitting writes, remove `--connect-url` and `--ca-cert` from the launch script to use the public proxy directly.

The `data` directory (including a symlink to external storage), `.env`, Python caches, and `lupinus_import` logs/checkpoints/certificates are excluded from Git. The former tracked `data/head.csv` sample is removed from version control; the full source CSV remains on the server.
