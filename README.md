# Dataverse Incremental Extract (VS Code)

This workspace is configured to run a Python script that incrementally extracts
changes from Dataverse using your interactive Azure CLI login.

The recommended workflow is now JSONL-first:

1. Dataverse delta extract writes JSONL to `output/`
2. Content extraction writes transcript payloads to JSONL (not many small files)
3. Analytics reads extracted JSONL and writes Parquet for reporting

## 1) One-time setup

1. Open a terminal in this workspace.
2. Create a local virtual environment:

```powershell
python -m venv .venv
```

3. Activate the virtual environment:

PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Command Prompt:

```cmd
.venv\Scripts\activate.bat
```

4. Install dependencies:

```powershell
pip install -r requirements.txt
```

5. Sign in with your user account:

```powershell
az login
```

## 2) Configure environment

Create `.env` from the sample file (first time only):

```powershell
Copy-Item .env.sample .env
```

Then edit `.env` and set at least:

- `DATAVERSE_URL=https://your-org.crm.dynamics.com`
- `DATAVERSE_TABLE=ConversationTranscript`

If `ConversationTranscript` is not the entity set name, the script tries to
resolve it from metadata. If resolution fails, set `DATAVERSE_TABLE` to the
entity set name (usually plural), for example `conversationtranscripts`.

Optional values for Entra enrichment are documented in Section 5.

## 3) Run extractor

```powershell
python scripts/dataverse_incremental_extract.py
```

## What happens on each run

- First run: performs initial sync and stores a Dataverse delta link.
- Later runs: use the saved delta link to pull only new/changed records.

Output files are written to `output/` as JSONL.
Delta state is stored in `.state/dataverse_delta_state.json`.

## 4) Extract transcript content (JSONL-first)

Extract the `content` field from Dataverse extract JSONL into extracted-content JSONL:

```powershell
python scripts/extract_content_json.py output --latest-only --output-format jsonl --output-dir output/extracted_content.jsonl
```

Notes:

- `--latest-only` keeps the most recent row per transcript key.
- `--output-format jsonl` is the recommended default for scale.
- If you need individual files for debugging, use:

```powershell
python scripts/extract_content_json.py output --latest-only --output-format files --output-dir output/extracted_json
```

## 5) Run analytics and export Parquet

Run analytics directly from extracted-content JSONL:

```powershell
python scripts/transcript_analytics.py output/extracted_content.jsonl --parquet output/usage_analytics.parquet
```

You can also output a partitioned Parquet dataset by passing a directory:

```powershell
python scripts/transcript_analytics.py output/extracted_content.jsonl --parquet output/usage_analytics_dataset
```

### Optional: Entra user enrichment auth modes

When using `--enrich-entra`, the analytics script supports two auth modes for Microsoft Graph:

1. `az` (default): use current Azure CLI login (`az login`)
2. `app`: use Entra app registration (client credentials)

Set these in [.env](.env) for app mode:

- `ENTRA_AUTH_MODE=app`
- `ENTRA_TENANT_ID=<tenant-guid>`
- `ENTRA_CLIENT_ID=<app-client-id-guid>`
- `ENTRA_CLIENT_SECRET=<app-client-secret>`

Optional:

- `ENTRA_AUTH_MODE=az` to force Azure CLI mode

You can also override auth mode at runtime:

```powershell
python scripts/transcript_analytics.py output/extracted_content.jsonl --parquet output/usage_analytics.parquet --enrich-entra --entra-auth-mode app
```

For app mode, grant the app Microsoft Graph application permission `User.Read.All` and provide admin consent.

## 6) One-command pipeline runner (recommended)

Use the orchestrator script to run extraction, content extraction, analytics,
cleanup, and archiving in one command.

Full run (includes Dataverse pull):

```powershell
python scripts/run_usage_analytics_pipeline.py
```

Process existing extracts only (skip Dataverse pull):

```powershell
python scripts/run_usage_analytics_pipeline.py --skip-dataverse-extract
```

Process all pending extract JSONL files:

```powershell
python scripts/run_usage_analytics_pipeline.py --skip-dataverse-extract --process-all
```

Keep temporary extracted-content files for debugging:

```powershell
python scripts/run_usage_analytics_pipeline.py --skip-dataverse-extract --keep-temp
```

## Archiving and cleanup behavior

When the runner completes successfully for an extract file:

- Processed Dataverse extract JSONL is moved to `output/archive/`
- Temporary files in `output/_tmp/` are removed (unless `--keep-temp` is used)
- Analytics Parquet output remains in `output/`
