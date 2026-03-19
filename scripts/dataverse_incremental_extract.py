import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_access_token(dataverse_url: str) -> str:
    candidate_commands = [
        shutil.which("az"),
        shutil.which("az.cmd"),
        r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    ]
    az_executable = None
    for candidate in candidate_commands:
        if candidate and Path(candidate).exists():
            az_executable = candidate
            break

    if not az_executable:
        raise RuntimeError(
            "Azure CLI executable was not found in PATH for this Python process. "
            "Try running the script from the same terminal where 'az login' works, "
            "or add Azure CLI to PATH."
        )

    cmd = [
        az_executable,
        "account",
        "get-access-token",
        "--resource",
        dataverse_url,
        "--query",
        "accessToken",
        "-o",
        "tsv",
    ]

    try:
        token = subprocess.check_output(cmd, text=True).strip()
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Failed to get access token from Azure CLI. "
            "Run 'az login' in this terminal and retry."
        ) from exc
    if not token:
        raise RuntimeError("Empty access token from Azure CLI.")
    return token


def build_headers(token: str, include_annotations: bool) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-Version": "4.0",
        "OData-MaxVersion": "4.0",
    }
    if include_annotations:
        headers["Prefer"] = 'odata.include-annotations="*"'
    return headers


def request_json(url: str, token: str, include_annotations: bool = False) -> dict:
    headers = build_headers(token, include_annotations=include_annotations)
    response = requests.get(url, headers=headers, timeout=120)
    if response.status_code >= 400:
        msg = (
            f"Request failed: {response.status_code} {response.reason}\n"
            f"URL: {url}\n"
            f"Response: {response.text}"
        )
        raise RuntimeError(msg)
    return response.json()


def resolve_entity_set(base_url: str, token: str, table_name: str) -> str:
    # If caller already passed an entity set name, using it directly is fine.
    quick_test_url = f"{base_url}/{table_name}?$top=1"
    try:
        request_json(quick_test_url, token)
        return table_name
    except RuntimeError:
        pass

    encoded_name = quote(table_name, safe="")
    metadata_url = (
        f"{base_url}/EntityDefinitions"
        f"?$select=LogicalName,SchemaName,EntitySetName"
        f"&$filter=LogicalName eq '{encoded_name}' or SchemaName eq '{encoded_name}'"
    )
    data = request_json(metadata_url, token)
    rows = data.get("value", [])
    if rows:
        entity_set = rows[0].get("EntitySetName")
        if entity_set:
            return entity_set

    guessed = f"{table_name.lower()}s"
    quick_test_url = f"{base_url}/{guessed}?$top=1"
    try:
        request_json(quick_test_url, token)
        return guessed
    except RuntimeError as exc:
        raise RuntimeError(
            "Could not resolve table name to an entity set name. "
            "Set DATAVERSE_TABLE to the entity set name (usually plural)."
        ) from exc


def read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")


def build_initial_url(base_url: str, entity_set: str, select: str) -> str:
    params = []
    if select:
        params.append(f"$select={select}")
    if not params:
        return f"{base_url}/{entity_set}"
    query = "&".join(params)
    return f"{base_url}/{entity_set}?{query}"


def extract_changes(
    base_url: str,
    token: str,
    entity_set: str,
    state_file: Path,
    output_file: Path,
    select: str,
    include_deletes: bool,
) -> None:
    state = read_state(state_file)
    state_key = f"{base_url}|{entity_set}|{select}|{include_deletes}"
    next_url = state.get(state_key)

    if not next_url:
        next_url = build_initial_url(base_url, entity_set, select)
        print("No previous delta state found. Starting initial sync...")
        print("This may return current rows before incremental tracking begins.")
    else:
        print("Previous delta state found. Pulling incremental changes...")

    total_records = 0
    pages = 0
    latest_delta_link = None

    while next_url:
        headers = build_headers(token, include_annotations=include_deletes)
        headers["Prefer"] = "odata.track-changes"

        response = requests.get(next_url, headers=headers, timeout=120)
        if response.status_code >= 400:
            msg = (
                f"Request failed: {response.status_code} {response.reason}\n"
                f"URL: {next_url}\n"
                f"Response: {response.text}"
            )
            raise RuntimeError(msg)

        payload = response.json()
        records = payload.get("value", [])
        append_jsonl(output_file, records)

        total_records += len(records)
        pages += 1
        print(f"Page {pages}: wrote {len(records)} records")

        if "@odata.nextLink" in payload:
            next_url = payload["@odata.nextLink"]
        else:
            next_url = None
            latest_delta_link = payload.get("@odata.deltaLink")

    if latest_delta_link:
        state[state_key] = latest_delta_link
        write_state(state_file, state)
        print("Saved latest delta link for next incremental run.")
    else:
        print("No delta link returned; state was not updated.")

    print(f"Done. Pages: {pages}, records written: {total_records}")
    print(f"Output: {output_file}")
    print(f"State:  {state_file}")


def main() -> int:
    load_dotenv(Path(".env"))

    dataverse_url = require_env("DATAVERSE_URL").rstrip("/")
    table_name = require_env("DATAVERSE_TABLE")
    select = os.getenv("DATAVERSE_SELECT", "").strip()
    output_dir = Path(os.getenv("OUTPUT_DIR", "output")).resolve()
    state_file = Path(os.getenv("DELTA_STATE_FILE", ".state/dataverse_delta_state.json")).resolve()
    include_deletes = bool_env("INCLUDE_DELETES", default=False)

    if not dataverse_url.startswith("https://"):
        dataverse_url = f"https://{dataverse_url}"

    base_url = f"{dataverse_url}/api/data/v9.2"
    token = get_access_token(dataverse_url)

    entity_set = resolve_entity_set(base_url, token, table_name)
    if entity_set != table_name:
        print(f"Resolved DATAVERSE_TABLE '{table_name}' -> entity set '{entity_set}'")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = output_dir / f"{entity_set}_changes_{timestamp}.jsonl"

    extract_changes(
        base_url=base_url,
        token=token,
        entity_set=entity_set,
        state_file=state_file,
        output_file=output_file,
        select=select,
        include_deletes=include_deletes,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)