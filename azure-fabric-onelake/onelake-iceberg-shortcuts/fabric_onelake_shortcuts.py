"""
Atlan Lakehouse -> Microsoft Fabric OneLake shortcut sync.

Creates OneLake shortcuts to the Atlan Lakehouse Iceberg tables that live on an
Atlan-hosted ADLS Gen2 storage account, inside a lakehouse in the customer's own
Fabric tenant. OneLake virtualizes each Iceberg table folder as a Delta Lake
table, so the tables are usable across every Fabric workload without copying
data (see the Iceberg and ADLS shortcut docs linked in the README).

Two phases, deliberately separate:

  setup  One-time per lakehouse. Creates the schema-enabled lakehouse data item,
         the ADLS Gen2 cloud connection that carries the Atlan-provided service
         principal, and a Files/_latest shortcut over Atlan's pointer files.
         Idempotent: existing objects are reused, never recreated.

  sync   Repeatable. Reads Atlan's pointer index through the Files/_latest
         shortcut and creates one table shortcut per Iceberg table under
         Tables/<schema>, where <schema> is the Iceberg namespace. Run it as
         often as you like, for any subset of namespaces.

The Atlan service principal secret is needed only when the connection is first
created; afterwards Fabric holds it and the script never asks again. It is read
from a console prompt or ATLAN_SP_CLIENT_SECRET, is never written to the profile
file, and is never logged.
"""

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests

# ============================================================================
# Constants
# ============================================================================

FABRIC_API = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"

DEFAULT_LAKEHOUSE_NAME = "atlan_context_store"
DEFAULT_CONNECTION_NAME = "atlan_context_store_adls_connection"
DEFAULT_POINTER_SHORTCUT = "_latest"
DEFAULT_NAMESPACES = ["entity_metadata", "gold", "atlan-ns"]

# Pointer file contract; source of truth is atlanhq/mdlh
# com.atlan.mdlh.destination.iceberg.pointer.ObjectStoreMetadataPointerConstants
POINTER_DIR = "_latest"
INDEX_FILE = "_index.json"
INDEX_TYPE = "atlan-lh-object-store-metadata-index"
POINTER_TYPE = "atlan-lh-object-store-metadata-pointer"
FORMAT_VERSION = 1

PROFILE_DIR = Path.home() / ".atlan" / "fabric-onelake-shortcuts"

# Fabric lakehouse schema and table names allow letters, digits and underscore.
SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")

logger = logging.getLogger("fabric-onelake-shortcuts")


class SyncError(Exception):
    """Fatal, user-actionable error. Reported without a traceback."""


# ============================================================================
# Identity
# ============================================================================

def build_credential(mode: str):
    """Credential for the CUSTOMER's Fabric tenant.

    Distinct from the Atlan-provided service principal, which is never used to
    call Fabric -- it only ever travels into the connection as the delegated
    ADLS credential.
    """
    try:
        from azure.identity import AzureCliCredential, ClientSecretCredential
    except ImportError as e:
        raise SyncError(
            "azure-identity is required. Install it with: pip install azure-identity requests"
        ) from e

    if mode == "azure_cli":
        # Honour FABRIC_TENANT_ID so an az login that holds several tenants
        # mints the token for the right one instead of the CLI's default.
        cred = AzureCliCredential(tenant_id=os.getenv("FABRIC_TENANT_ID") or "")
        try:
            cred.get_token(FABRIC_SCOPE)
        except Exception as e:
            raise SyncError(
                "no usable Azure CLI login. Install the Azure CLI and run 'az login' "
                "(add --tenant <id>, or set FABRIC_TENANT_ID, if you belong to several "
                f"tenants), then re-run. Details: {e}"
            ) from e
        return cred

    if mode == "service_principal":
        tenant = os.getenv("FABRIC_TENANT_ID")
        client = os.getenv("FABRIC_CLIENT_ID")
        secret = os.getenv("FABRIC_CLIENT_SECRET")
        missing = [n for n, v in (("FABRIC_TENANT_ID", tenant),
                                  ("FABRIC_CLIENT_ID", client),
                                  ("FABRIC_CLIENT_SECRET", secret)) if not v]
        if missing:
            raise SyncError(
                "--fabric-auth service_principal needs these environment variables: "
                + ", ".join(missing)
            )
        return ClientSecretCredential(tenant_id=tenant, client_id=client,
                                      client_secret=secret)

    raise SyncError(f"unknown --fabric-auth value: {mode}")


class TokenCache:
    """Caches one token per scope until shortly before it expires."""

    def __init__(self, credential):
        self._credential = credential
        self._tokens: Dict[str, Tuple[str, float]] = {}

    def token(self, scope: str) -> str:
        cached = self._tokens.get(scope)
        if cached and cached[1] - 120 > time.time():
            return cached[0]
        try:
            fresh = self._credential.get_token(scope)
        except Exception as e:
            raise SyncError(f"could not acquire a token for {scope}: {e}") from e
        self._tokens[scope] = (fresh.token, fresh.expires_on)
        return fresh.token


# ============================================================================
# HTTP
# ============================================================================

class RestClient:
    """Shared retry, throttle and error handling for both Fabric endpoints."""

    def __init__(self, tokens: TokenCache, scope: str, max_attempts: int = 5):
        self._tokens = tokens
        self._scope = scope
        self._max_attempts = max_attempts
        self._session = requests.Session()

    def request(self, method: str, url: str, *, ok: Tuple[int, ...] = (200, 201),
                **kwargs) -> requests.Response:
        # Pulled out of the loop: popping inside it would drop the caller's
        # headers on every retry after the first.
        caller_headers = dict(kwargs.pop("headers", {}) or {})
        for attempt in range(1, self._max_attempts + 1):
            headers = dict(caller_headers)
            headers["Authorization"] = f"Bearer {self._tokens.token(self._scope)}"
            response = self._session.request(method, url, headers=headers,
                                             timeout=120, **kwargs)
            if response.status_code in ok:
                return response
            retriable = response.status_code == 429 or response.status_code >= 500
            if retriable and attempt < self._max_attempts:
                delay = self._retry_after(response, attempt)
                logger.warning("%s %s returned %s, retrying in %ss (attempt %s/%s)",
                               method, _redact(url), response.status_code, delay,
                               attempt, self._max_attempts)
                time.sleep(delay)
                continue
            return response
        raise SyncError("unreachable")

    @staticmethod
    def _retry_after(response: requests.Response, attempt: int) -> int:
        header = response.headers.get("Retry-After")
        if header:
            try:
                return min(int(header), 120)
            except ValueError:
                pass
        return min(2 ** attempt, 60)


def _redact(url: str) -> str:
    """Drops the query string so a SAS-style token can never reach a log line."""
    return url.split("?", 1)[0]


def describe_error(response: requests.Response) -> str:
    """Fabric error responses carry errorCode/message; fall back to the body."""
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:400]}"
    if isinstance(body, dict):
        code = body.get("errorCode") or ""
        message = body.get("message") or ""
        details = body.get("moreDetails") or []
        extra = "; ".join(
            d.get("message", "") for d in details if isinstance(d, dict) and d.get("message")
        )
        parts = [p for p in (code, message, extra) if p]
        if parts:
            return f"HTTP {response.status_code}: " + " - ".join(parts)
    return f"HTTP {response.status_code}: {response.text[:400]}"


# ============================================================================
# Fabric control plane
# ============================================================================

class FabricClient:
    def __init__(self, tokens: TokenCache):
        self._rest = RestClient(tokens, FABRIC_SCOPE)

    # -- plumbing ----------------------------------------------------------

    def _get(self, path: str, **kwargs) -> requests.Response:
        return self._rest.request("GET", f"{FABRIC_API}{path}", **kwargs)

    def _post(self, path: str, payload: dict, *, ok=(200, 201, 202)) -> requests.Response:
        return self._rest.request("POST", f"{FABRIC_API}{path}", ok=ok, json=payload)

    def _paged(self, path: str, what: str) -> List[dict]:
        """Walks a Fabric continuationToken listing."""
        items: List[dict] = []
        separator = "&" if "?" in path else "?"
        next_path = path
        while True:
            response = self._get(next_path, ok=(200,))
            if response.status_code != 200:
                raise SyncError(f"could not list {what}: {describe_error(response)}")
            body = response.json()
            items.extend(body.get("value", []))
            token = body.get("continuationToken")
            if not token:
                return items
            next_path = f"{path}{separator}continuationToken={quote(token)}"

    def _await_operation(self, response: requests.Response, what: str) -> dict:
        """Resolves a 202 long-running operation to its result."""
        if response.status_code != 202:
            if not response.content:
                raise SyncError(f"{what} returned no body to read the new item from")
            return response.json()
        operation = response.headers.get("x-ms-operation-id")
        if not operation:
            raise SyncError(f"{what} was accepted but returned no operation id")
        deadline = time.time() + 600
        try:
            delay = int(response.headers.get("Retry-After") or 5)
        except ValueError:
            delay = 5
        while time.time() < deadline:
            time.sleep(delay)
            status_response = self._get(f"/operations/{operation}", ok=(200,))
            if status_response.status_code != 200:
                raise SyncError(f"could not poll {what}: {describe_error(status_response)}")
            body = status_response.json()
            state = body.get("status")
            if state == "Succeeded":
                result = self._get(f"/operations/{operation}/result", ok=(200,))
                if result.status_code != 200:
                    raise SyncError(f"could not read the {what} result: {describe_error(result)}")
                return result.json()
            if state == "Failed":
                error = body.get("error") or {}
                raise SyncError(f"{what} failed: {error.get('message', json.dumps(error)[:400])}")
            delay = min(max(delay, 5), 30)
        raise SyncError(f"{what} did not finish within 10 minutes")

    # -- workspaces --------------------------------------------------------

    def resolve_workspace(self, workspace: str) -> Tuple[str, str]:
        """Accepts a workspace id or display name; returns (id, displayName)."""
        if _looks_like_uuid(workspace):
            response = self._get(f"/workspaces/{workspace}", ok=(200,))
            if response.status_code != 200:
                raise SyncError(f"workspace {workspace} is not reachable: {describe_error(response)}")
            body = response.json()
            return body["id"], body.get("displayName", workspace)

        matches = [w for w in self._paged("/workspaces", "workspaces")
                   if w.get("displayName") == workspace]
        if not matches:
            raise SyncError(
                f"no workspace named {workspace!r} is visible to this identity. "
                "Pass the workspace id instead, or check that the identity has "
                "at least a Contributor role on it."
            )
        if len(matches) > 1:
            ids = ", ".join(m["id"] for m in matches)
            raise SyncError(f"several workspaces are named {workspace!r} ({ids}); pass the id")
        return matches[0]["id"], matches[0].get("displayName", workspace)

    # -- lakehouse ---------------------------------------------------------

    def find_lakehouse(self, workspace_id: str, name: str) -> Optional[dict]:
        for item in self._paged(f"/workspaces/{workspace_id}/lakehouses", "lakehouses"):
            if item.get("displayName") == name:
                return item
        return None

    def create_lakehouse(self, workspace_id: str, name: str) -> dict:
        payload = {
            "displayName": name,
            "description": "Atlan Lakehouse context store (Iceberg shortcuts)",
            "creationPayload": {"enableSchemas": True},
        }
        response = self._post(f"/workspaces/{workspace_id}/lakehouses", payload)
        if response.status_code not in (200, 201, 202):
            raise SyncError(f"could not create lakehouse {name!r}: {describe_error(response)}")
        return self._await_operation(response, f"lakehouse {name!r} creation")

    def get_lakehouse(self, workspace_id: str, lakehouse_id: str) -> dict:
        response = self._get(f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}", ok=(200,))
        if response.status_code != 200:
            raise SyncError(f"could not read lakehouse {lakehouse_id}: {describe_error(response)}")
        return response.json()

    # -- connections -------------------------------------------------------

    def find_connection(self, name: str) -> Optional[dict]:
        for connection in self._paged("/connections", "connections"):
            if connection.get("displayName") == name:
                return connection
        return None

    def adls_creation_method(self) -> Tuple[str, str, List[dict]]:
        """Discovers the ADLS Gen2 connection type from the tenant itself.

        The creation method name and its parameter names have changed across
        Fabric releases, so they are read from the service rather than pinned
        here. The fallback only applies when discovery is unavailable.
        """
        fallback = ("AzureDataLakeStorage", "AzureDataLakeStorage.Contents",
                    [{"name": "server", "required": True},
                     {"name": "path", "required": False}])
        try:
            types = self._paged(
                "/connections/supportedConnectionTypes?showAllCreationMethods=true",
                "supported connection types")
        except SyncError as e:
            logger.warning("Could not list supported connection types (%s); "
                           "assuming the default ADLS Gen2 shape", e)
            return fallback

        for entry in types:
            if str(entry.get("type", "")).lower() != "azuredatalakestorage":
                continue
            methods = entry.get("creationMethods") or []
            if not methods:
                break
            # Prefer a method that takes a server parameter; that is the
            # account-URL shape the shortcut target expects.
            chosen = next(
                (m for m in methods
                 if any(p.get("name") == "server" for p in (m.get("parameters") or []))),
                methods[0])
            supported = [str(c).lower() for c in (entry.get("supportedCredentialTypes") or [])]
            if supported and "serviceprincipal" not in supported:
                raise SyncError(
                    "this tenant's Azure Data Lake Storage connection type does not accept "
                    "service principal credentials, which cross-tenant ADLS shortcuts require"
                )
            return entry["type"], chosen["name"], chosen.get("parameters") or []

        logger.warning("Azure Data Lake Storage was not listed as a supported connection "
                       "type; assuming the default shape")
        return fallback

    def create_connection(self, name: str, account_url: str, container: str,
                          tenant_id: str, client_id: str, client_secret: str) -> dict:
        conn_type, creation_method, parameter_spec = self.adls_creation_method()

        # Fill only the parameters this tenant's creation method declares.
        values = {"server": account_url, "url": account_url, "path": container}
        parameters = []
        for spec in parameter_spec:
            pname = spec.get("name")
            if pname in values and values[pname]:
                parameters.append({"dataType": "Text", "name": pname, "value": values[pname]})
            elif spec.get("required"):
                raise SyncError(
                    f"the ADLS Gen2 connection requires a {pname!r} parameter that this "
                    "script does not know how to fill; create the connection in the Fabric "
                    f"portal and re-run with --connection-name {name}"
                )
        if not parameters:
            parameters = [{"dataType": "Text", "name": "server", "value": account_url}]

        payload = {
            "connectivityType": "ShareableCloud",
            "displayName": name,
            "privacyLevel": "Organizational",
            "connectionDetails": {
                "type": conn_type,
                "creationMethod": creation_method,
                "parameters": parameters,
            },
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": False,
                "credentials": {
                    "credentialType": "ServicePrincipal",
                    "tenantId": tenant_id,
                    "servicePrincipalClientId": client_id,
                    "servicePrincipalSecret": client_secret,
                },
            },
        }
        response = self._post("/connections", payload, ok=(200, 201))
        if response.status_code not in (200, 201):
            detail = describe_error(response)
            if "DuplicateConnectionName" in detail:
                detail += (
                    f"\nA connection named {name!r} already exists but is not visible to "
                    "this identity. Ask whoever created it to share it with you (Manage "
                    "connections and gateways in the Fabric portal), or pass a different "
                    "--connection-name."
                )
            if "IncorrectCredentials" in detail:
                detail += (
                    "\nCheck that the service principal Atlan gave you has the Storage Blob "
                    "Data Reader role (or Delegator plus ACLs) on the Atlan storage account, "
                    "and that the secret has not expired."
                )
            raise SyncError(f"could not create connection {name!r}: {detail}")
        return response.json()

    # -- shortcuts ---------------------------------------------------------

    def list_shortcuts(self, workspace_id: str, item_id: str) -> List[dict]:
        return self._paged(f"/workspaces/{workspace_id}/items/{item_id}/shortcuts",
                           "shortcuts")

    def create_shortcut(self, workspace_id: str, item_id: str, path: str, name: str,
                        location: str, subpath: str, connection_id: str,
                        conflict_policy: Optional[str] = None) -> dict:
        query = f"?shortcutConflictPolicy={conflict_policy}" if conflict_policy else ""
        payload = {
            "path": path,
            "name": name,
            "target": {
                "adlsGen2": {
                    "location": location,
                    "subpath": subpath,
                    "connectionId": connection_id,
                }
            },
        }
        response = self._post(
            f"/workspaces/{workspace_id}/items/{item_id}/shortcuts{query}", payload,
            ok=(200, 201))
        if response.status_code not in (200, 201):
            raise SyncError(
                f"could not create shortcut {path}/{name}: {describe_error(response)}")
        return response.json()

    def delete_shortcut(self, workspace_id: str, item_id: str, path: str, name: str) -> None:
        url = (f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}"
               f"/shortcuts/{quote(path)}/{quote(name)}")
        response = self._rest.request("DELETE", url, ok=(200, 204))
        if response.status_code not in (200, 204):
            raise SyncError(
                f"could not delete shortcut {path}/{name}: {describe_error(response)}")


# ============================================================================
# OneLake data plane
# ============================================================================

class OneLakeClient:
    """Reads the pointer files, and materializes lakehouse schema directories.

    A schema in a schema-enabled lakehouse is a directory under Tables, so
    creating one needs the OneLake DFS endpoint rather than the Fabric API.
    """

    def __init__(self, tokens: TokenCache, workspace_id: str, item_id: str):
        self._rest = RestClient(tokens, STORAGE_SCOPE)
        self._base = f"{ONELAKE_DFS}/{workspace_id}/{item_id}"

    def read_json(self, path: str) -> dict:
        url = f"{self._base}/{quote(path)}"
        response = self._rest.request("GET", url, ok=(200,))
        if response.status_code == 404:
            raise SyncError(
                f"{path} was not found in the lakehouse. Run 'setup' first so the pointer "
                "shortcut exists, and confirm with Atlan that the metadata pointer workflow "
                "is enabled for your tenant."
            )
        if response.status_code != 200:
            raise SyncError(f"could not read {path}: {describe_error(response)}")
        try:
            return response.json()
        except ValueError as e:
            raise SyncError(f"{path} is not valid JSON: {e}") from e

    def ensure_directory(self, path: str) -> bool:
        """Creates a directory if absent. Returns True when it created one."""
        head = self._rest.request("HEAD", f"{self._base}/{quote(path)}", ok=(200,))
        if head.status_code == 200:
            return False
        url = f"{self._base}/{quote(path)}?resource=directory"
        response = self._rest.request("PUT", url, ok=(200, 201, 409),
                                      headers={"Content-Length": "0"})
        # 409 means another run (or the Fabric UI) created it in between; the
        # postcondition still holds, so treat it as success rather than a failure.
        if response.status_code == 409:
            return False
        if response.status_code not in (200, 201):
            raise SyncError(f"could not create {path}: {describe_error(response)}")
        return True


# ============================================================================
# Storage URI handling
# ============================================================================

def split_adls_uri(uri: str) -> Tuple[str, str]:
    """Splits an ADLS URI into a shortcut (location, subpath) pair.

    Accepts the forms Atlan and Iceberg emit:
      abfss://container@account.dfs.core.windows.net/path
      abfs(s)/wasb(s) equivalents
      https://account.dfs.core.windows.net/container/path

    Returns ("https://account.dfs.core.windows.net", "/container/path").
    """
    parsed = urlparse(uri.strip())
    scheme = parsed.scheme.lower()
    if not scheme or not parsed.netloc:
        raise SyncError(f"{uri!r} is not an absolute storage URI")

    if scheme in ("abfss", "abfs", "wasbs", "wasb"):
        if "@" not in parsed.netloc:
            raise SyncError(f"{uri!r} is missing the container@account part")
        container, host = parsed.netloc.split("@", 1)
        account = host.split(".", 1)[0]
        path = parsed.path.strip("/")
    elif scheme == "https":
        host = parsed.netloc
        account = host.split(".", 1)[0]
        segments = [s for s in parsed.path.split("/") if s]
        if not segments:
            raise SyncError(
                f"{uri!r} names a storage account but no container. Include the container, "
                "for example https://account.dfs.core.windows.net/mycontainer/catalog-root"
            )
        container, path = segments[0], "/".join(segments[1:])
    else:
        raise SyncError(f"{uri!r} uses an unsupported scheme {scheme!r}")

    if not account:
        raise SyncError(f"could not read the storage account name out of {uri!r}")
    location = f"https://{account}.dfs.core.windows.net"
    subpath = "/" + "/".join(p for p in (container, path) if p)
    return location, subpath


def join_subpath(subpath: str, suffix: str) -> str:
    return subpath.rstrip("/") + "/" + suffix.strip("/")


# ============================================================================
# Naming
# ============================================================================

def schema_name(namespace: str) -> str:
    """Iceberg namespace -> Fabric lakehouse schema name."""
    return _safe(namespace.replace(".", "__"))


def table_name(table: str) -> str:
    return _safe(table)


def _safe(name: str) -> str:
    cleaned = SAFE_NAME.sub("_", name)
    if not cleaned:
        raise SyncError(f"{name!r} has no characters that are valid in a Fabric name")
    if cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned


# ============================================================================
# Profile
# ============================================================================

@dataclass
class Profile:
    """Everything except the secret, so a re-run needs no re-typing."""

    name: str = "default"
    workspace: str = ""
    workspace_id: str = ""
    lakehouse_name: str = DEFAULT_LAKEHOUSE_NAME
    lakehouse_id: str = ""
    connection_name: str = DEFAULT_CONNECTION_NAME
    connection_id: str = ""
    catalog_location: str = ""
    catalog_subpath: str = ""
    pointer_shortcut: str = DEFAULT_POINTER_SHORTCUT
    atlan_tenant_id: str = ""
    atlan_client_id: str = ""
    namespaces: List[str] = field(default_factory=lambda: list(DEFAULT_NAMESPACES))

    @property
    def path(self) -> Path:
        return PROFILE_DIR / f"{self.name}.json"

    @classmethod
    def load(cls, name: str) -> "Profile":
        path = PROFILE_DIR / f"{name}.json"
        if not path.exists():
            return cls(name=name)
        try:
            stored = json.loads(path.read_text())
        except ValueError as e:
            raise SyncError(f"profile {path} is corrupt: {e}") from e
        known = {f for f in cls.__dataclass_fields__}
        profile = cls(**{k: v for k, v in stored.items() if k in known})
        profile.name = name
        return profile

    def save(self) -> None:
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        stored = {k: v for k, v in self.__dict__.items() if k != "name"}
        self.path.write_text(json.dumps(stored, indent=2, sort_keys=True) + "\n")
        os.chmod(self.path, 0o600)


# ============================================================================
# Pointer files
# ============================================================================

@dataclass
class TablePointer:
    table: str
    table_location: str
    metadata_version: int
    total_records: Optional[int]


def read_index(onelake: OneLakeClient, profile: Profile) -> List[dict]:
    path = f"Files/{profile.pointer_shortcut}/{INDEX_FILE}"
    index = onelake.read_json(path)
    if index.get("type") != INDEX_TYPE:
        raise SyncError(f"{path} is not an Atlan pointer index (type={index.get('type')!r})")
    version = int(index.get("formatVersion", 0))
    if version > FORMAT_VERSION:
        raise SyncError(
            f"the pointer files use format version {version}, newer than this script "
            f"supports ({FORMAT_VERSION}). Update the script."
        )
    return index.get("namespaces") or []


def read_pointer(onelake: OneLakeClient, profile: Profile, pointer_file: str) -> List[TablePointer]:
    # pointerFile is relative to the catalog root (_latest/<namespace>.json) and
    # the shortcut is mounted at the _latest folder itself.
    relative = pointer_file.strip("/")
    prefix = POINTER_DIR + "/"
    if relative.startswith(prefix):
        relative = relative[len(prefix):]
    pointer = onelake.read_json(f"Files/{profile.pointer_shortcut}/{relative}")
    if pointer.get("type") != POINTER_TYPE:
        raise SyncError(f"{pointer_file} is not an Atlan pointer file "
                        f"(type={pointer.get('type')!r})")

    pointers = []
    for entry in pointer.get("tables") or []:
        location = entry.get("tableLocation")
        if not location:
            logger.warning("Table %s has no tableLocation in the pointer file; skipping",
                           entry.get("identifier") or entry.get("table"))
            continue
        total = entry.get("totalRecords")
        pointers.append(TablePointer(
            table=entry["table"],
            table_location=location,
            metadata_version=int(entry.get("metadataVersion", -1)),
            total_records=int(total) if total is not None else None,
        ))
    return pointers


# ============================================================================
# Planning
# ============================================================================

@dataclass
class Action:
    action: str          # CREATE | UP_TO_DATE | RETARGET | SKIP_EMPTY | PRUNE | ERROR
    namespace: str
    schema: str
    table: str
    detail: str = ""
    location: str = ""
    subpath: str = ""


def shortcut_key(shortcut: dict) -> Tuple[str, str]:
    return shortcut.get("path", "").strip("/"), shortcut.get("name", "")


def plan(profile: Profile, namespaces: List[dict], pointers: Dict[str, List[TablePointer]],
         existing: List[dict], non_empty_only: bool, prune: bool) -> List[Action]:
    """Compares the pointer files against the lakehouse. Read-only."""
    by_key = {shortcut_key(s): s for s in existing}
    actions: List[Action] = []

    for entry in namespaces:
        namespace = entry["namespace"]
        schema = schema_name(namespace)
        table_path = f"Tables/{schema}"
        seen = set()

        for pointer in pointers.get(namespace, []):
            name = table_name(pointer.table)
            if name in seen:
                actions.append(Action(
                    "ERROR", namespace, schema, name,
                    f"table {pointer.table!r} sanitizes to the same Fabric name as another "
                    "table in this namespace; one of them needs renaming on the Atlan side"))
                continue
            seen.add(name)

            if non_empty_only:
                if pointer.total_records is None:
                    logger.warning(
                        "%s.%s has no record count in the pointer file; including it rather "
                        "than guessing it is empty", namespace, pointer.table)
                elif pointer.total_records == 0:
                    actions.append(Action("SKIP_EMPTY", namespace, schema, name,
                                          "0 records in the current snapshot"))
                    continue

            try:
                location, subpath = split_adls_uri(pointer.table_location)
            except SyncError as e:
                actions.append(Action("ERROR", namespace, schema, name, str(e)))
                continue

            if location.lower() != profile.catalog_location.lower():
                actions.append(Action(
                    "ERROR", namespace, schema, name,
                    f"table sits on {location}, outside the configured storage account "
                    f"{profile.catalog_location}. Confirm the connector URL with Atlan."))
                continue

            current = by_key.get((table_path, name))
            if current is None:
                rows = "unknown" if pointer.total_records is None else pointer.total_records
                actions.append(Action("CREATE", namespace, schema, name,
                                      f"{rows} records, metadata v{pointer.metadata_version}",
                                      location, subpath))
                continue

            target = (current.get("target") or {}).get("adlsGen2") or {}
            if (target.get("subpath") or "").rstrip("/") != subpath.rstrip("/"):
                actions.append(Action(
                    "RETARGET", namespace, schema, name,
                    f"points at {target.get('subpath')!r}, pointer file says {subpath!r}",
                    location, subpath))
            elif (target.get("connectionId") or "").lower() != profile.connection_id.lower():
                actions.append(Action(
                    "RETARGET", namespace, schema, name,
                    f"bound to connection {target.get('connectionId')}, not the current "
                    f"{profile.connection_id}", location, subpath))
            else:
                actions.append(Action("UP_TO_DATE", namespace, schema, name))

        if prune:
            for (path, name), shortcut in sorted(by_key.items()):
                if path != table_path or name in seen:
                    continue
                target = (shortcut.get("target") or {}).get("adlsGen2") or {}
                if (target.get("connectionId") or "").lower() != profile.connection_id.lower():
                    # Not bound to the Atlan connection, so not something this
                    # script created: a customer's own shortcut stays put.
                    continue
                actions.append(Action("PRUNE", namespace, schema, name,
                                      "present in the lakehouse but gone from the pointer file"))

    return actions


def apply(fabric: FabricClient, onelake: OneLakeClient, profile: Profile,
          actions: List[Action], overwrite: bool) -> List[Action]:
    results: List[Action] = []
    ensured_schemas = set()

    for action in actions:
        if action.action in ("UP_TO_DATE", "SKIP_EMPTY", "ERROR"):
            results.append(action)
            continue

        table_path = f"Tables/{action.schema}"
        try:
            if action.action in ("CREATE", "RETARGET"):
                if action.schema not in ensured_schemas:
                    if onelake.ensure_directory(table_path):
                        logger.info("Created schema %s", action.schema)
                    ensured_schemas.add(action.schema)

            if action.action == "CREATE":
                fabric.create_shortcut(profile.workspace_id, profile.lakehouse_id,
                                       table_path, action.table, action.location,
                                       action.subpath, profile.connection_id)
                results.append(Action("CREATED", action.namespace, action.schema,
                                      action.table, action.detail))

            elif action.action == "RETARGET":
                if not overwrite:
                    results.append(Action("NEEDS_OVERWRITE", action.namespace, action.schema,
                                          action.table, action.detail + " (re-run with --overwrite)"))
                    continue
                fabric.create_shortcut(profile.workspace_id, profile.lakehouse_id,
                                       table_path, action.table, action.location,
                                       action.subpath, profile.connection_id,
                                       conflict_policy="CreateOrOverwrite")
                results.append(Action("RETARGETED", action.namespace, action.schema,
                                      action.table, action.detail))

            elif action.action == "PRUNE":
                fabric.delete_shortcut(profile.workspace_id, profile.lakehouse_id,
                                       table_path, action.table)
                results.append(Action("PRUNED", action.namespace, action.schema,
                                      action.table, action.detail))

        except SyncError as e:
            results.append(Action("FAILED", action.namespace, action.schema,
                                  action.table, str(e)))

    return results


# ============================================================================
# Reporting
# ============================================================================

def report(actions: List[Action], title: str) -> None:
    if not actions:
        print(f"\n{title}: nothing to do.")
        return

    counts: Dict[str, int] = {}
    for action in actions:
        counts[action.action] = counts.get(action.action, 0) + 1

    print(f"\n{title}")
    print("-" * len(title))
    width = max(len(f"{a.schema}.{a.table}") for a in actions)
    noteworthy = {"UP_TO_DATE", "SKIP_EMPTY"}
    for action in actions:
        if action.action in noteworthy and not logger.isEnabledFor(logging.DEBUG):
            continue
        qualified = f"{action.schema}.{action.table}".ljust(width)
        detail = f"  {action.detail}" if action.detail else ""
        print(f"  {action.action:<16} {qualified}{detail}")

    print("\n  " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if any(a.action in noteworthy for a in actions) and not logger.isEnabledFor(logging.DEBUG):
        print("  (run with --verbose to list up-to-date and skipped tables)")


# ============================================================================
# Commands
# ============================================================================

def cmd_setup(args) -> int:
    profile = Profile.load(args.profile)

    workspace = args.workspace or profile.workspace
    if not workspace:
        raise SyncError(
            f"--workspace is required: no saved profile {args.profile!r}. If you have run "
            "setup before and used --lakehouse-name or --connection-name, pass the same "
            "names again, or the script will create a second lakehouse and connection.")

    catalog_url = args.adls_url
    if catalog_url:
        location, subpath = split_adls_uri(catalog_url)
    elif profile.catalog_location:
        location, subpath = profile.catalog_location, profile.catalog_subpath
    else:
        raise SyncError(f"--adls-url is required: no saved profile {args.profile!r} "
                        "(Atlan provides the shortcut connector URL)")

    tokens = TokenCache(build_credential(args.fabric_auth))
    fabric = FabricClient(tokens)

    workspace_id, workspace_name = fabric.resolve_workspace(workspace)
    logger.info("Workspace: %s (%s)", workspace_name, workspace_id)

    profile.workspace = workspace
    profile.workspace_id = workspace_id
    profile.catalog_location = location
    profile.catalog_subpath = subpath
    profile.lakehouse_name = args.lakehouse_name or profile.lakehouse_name
    profile.connection_name = args.connection_name or profile.connection_name
    profile.pointer_shortcut = args.pointer_shortcut or profile.pointer_shortcut
    if args.namespaces:
        profile.namespaces = args.namespaces

    # -- lakehouse ---------------------------------------------------------
    lakehouse = fabric.find_lakehouse(workspace_id, profile.lakehouse_name)
    if lakehouse:
        logger.info("Lakehouse %s already exists (%s)", profile.lakehouse_name, lakehouse["id"])
    else:
        logger.info("Creating schema-enabled lakehouse %s", profile.lakehouse_name)
        lakehouse = fabric.create_lakehouse(workspace_id, profile.lakehouse_name)
        logger.info("Created lakehouse %s (%s)", profile.lakehouse_name, lakehouse["id"])
    profile.lakehouse_id = lakehouse["id"]

    details = fabric.get_lakehouse(workspace_id, profile.lakehouse_id)
    if not (details.get("properties") or {}).get("defaultSchema"):
        raise SyncError(
            f"lakehouse {profile.lakehouse_name!r} is not schema-enabled, so Iceberg "
            "namespaces cannot be mapped to schemas. Use a different --lakehouse-name so "
            "the script can create a schema-enabled one."
        )

    # -- connection --------------------------------------------------------
    connection = fabric.find_connection(profile.connection_name)
    if connection:
        logger.info("Connection %s already exists (%s); its credentials were left untouched",
                    profile.connection_name, connection["id"])
    else:
        tenant_id = args.atlan_tenant_id or profile.atlan_tenant_id
        client_id = args.atlan_client_id or profile.atlan_client_id
        missing = [n for n, v in (("--atlan-tenant-id", tenant_id),
                                  ("--atlan-client-id", client_id)) if not v]
        if missing:
            raise SyncError(
                "connection " + repr(profile.connection_name) + " does not exist yet, so these "
                "Atlan-provided values are required: " + ", ".join(missing)
            )
        secret = os.getenv("ATLAN_SP_CLIENT_SECRET") or getpass.getpass(
            "Atlan service principal secret (not stored, not echoed): ")
        if not secret.strip():
            raise SyncError("no service principal secret was entered")

        container = profile.catalog_subpath.strip("/").split("/", 1)[0]
        logger.info("Creating connection %s", profile.connection_name)
        connection = fabric.create_connection(profile.connection_name, location, container,
                                              tenant_id, client_id, secret)
        del secret
        logger.info("Created connection %s (%s)", profile.connection_name, connection["id"])
        profile.atlan_tenant_id = tenant_id
        profile.atlan_client_id = client_id
    profile.connection_id = connection["id"]

    # -- pointer shortcut --------------------------------------------------
    existing = fabric.list_shortcuts(workspace_id, profile.lakehouse_id)
    pointer_subpath = join_subpath(profile.catalog_subpath, POINTER_DIR)
    current = next((s for s in existing
                    if shortcut_key(s) == ("Files", profile.pointer_shortcut)), None)
    if current:
        target = (current.get("target") or {}).get("adlsGen2") or {}
        if (target.get("subpath") or "").rstrip("/") != pointer_subpath.rstrip("/"):
            logger.warning("Files/%s points at %s, not the configured %s. Delete it in the "
                           "Fabric portal and re-run setup to repoint it.",
                           profile.pointer_shortcut, target.get("subpath"), pointer_subpath)
        else:
            logger.info("Pointer shortcut Files/%s already exists", profile.pointer_shortcut)
    else:
        logger.info("Creating pointer shortcut Files/%s -> %s%s",
                    profile.pointer_shortcut, location, pointer_subpath)
        fabric.create_shortcut(workspace_id, profile.lakehouse_id, "Files",
                               profile.pointer_shortcut, location, pointer_subpath,
                               profile.connection_id)

    profile.save()

    # Prove the whole chain works before declaring success: this read goes
    # through the new shortcut, so it exercises the connection credentials too.
    onelake = OneLakeClient(tokens, workspace_id, profile.lakehouse_id)
    namespaces = read_index(onelake, profile)
    print(f"\nSetup complete. Profile saved to {profile.path}")
    print(f"  Workspace   {workspace_name} ({workspace_id})")
    print(f"  Lakehouse   {profile.lakehouse_name} ({profile.lakehouse_id})")
    print(f"  Connection  {profile.connection_name} ({profile.connection_id})")
    print(f"  Catalog     {location}{profile.catalog_subpath}")
    print(f"\nNamespaces Atlan publishes ({len(namespaces)}):")
    for entry in namespaces:
        print(f"  {entry['namespace']:<24} {entry.get('tableCount', '?')} tables"
              f"  -> schema {schema_name(entry['namespace'])}")
    print("\nNext: fabric_onelake_shortcuts.py sync")
    return 0


def cmd_sync(args) -> int:
    profile = Profile.load(args.profile)
    if not (profile.workspace_id and profile.lakehouse_id and profile.connection_id):
        raise SyncError(f"profile {args.profile!r} is incomplete; run setup first")
    # Per-run override only. Persisting it would shrink the saved list, so a later
    # plain `sync` would silently cover just the last subset.
    requested = args.namespaces or profile.namespaces

    tokens = TokenCache(build_credential(args.fabric_auth))
    fabric = FabricClient(tokens)
    onelake = OneLakeClient(tokens, profile.workspace_id, profile.lakehouse_id)

    published = read_index(onelake, profile)
    by_namespace = {e["namespace"]: e for e in published}

    selected = [by_namespace[n] for n in requested if n in by_namespace]
    absent = [n for n in requested if n not in by_namespace]
    if absent:
        logger.info("Not published by Atlan, skipping: %s", ", ".join(absent))
    if not selected:
        raise SyncError(
            "none of the requested namespaces are published. Atlan currently publishes: "
            + (", ".join(by_namespace) or "(none)")
        )

    pointers = {}
    for entry in selected:
        pointers[entry["namespace"]] = read_pointer(onelake, profile, entry["pointerFile"])
        logger.info("Namespace %s: %s tables in the pointer file",
                    entry["namespace"], len(pointers[entry["namespace"]]))

    existing = fabric.list_shortcuts(profile.workspace_id, profile.lakehouse_id)
    actions = plan(profile, selected, pointers, existing, args.non_empty_only, args.prune)

    if args.plan:
        report(actions, "Planned changes (dry run, nothing was applied)")
        return 0

    results = apply(fabric, onelake, profile, actions, args.overwrite)
    report(results, "Sync results")

    failures = [r for r in results if r.action in ("FAILED", "ERROR")]
    if failures:
        print(f"\n{len(failures)} table(s) failed. Fix the causes above and re-run; "
              "the sync is idempotent.")
        return 1
    print("\nOneLake needs up to two minutes to virtualize each Iceberg table as Delta. "
          "If a table does not appear, open its View files menu and read "
          "_delta_log/latest_conversion_log.txt.")
    return 0


def cmd_status(args) -> int:
    profile = Profile.load(args.profile)
    if not profile.workspace_id:
        raise SyncError(f"profile {args.profile!r} does not exist yet; run setup first")

    tokens = TokenCache(build_credential(args.fabric_auth))
    fabric = FabricClient(tokens)

    print(f"Profile {profile.path}")
    print(f"  Workspace   {profile.workspace} ({profile.workspace_id})")
    print(f"  Lakehouse   {profile.lakehouse_name} ({profile.lakehouse_id})")
    print(f"  Connection  {profile.connection_name} ({profile.connection_id})")
    print(f"  Catalog     {profile.catalog_location}{profile.catalog_subpath}")
    print(f"  Namespaces  {', '.join(profile.namespaces)}")

    shortcuts = fabric.list_shortcuts(profile.workspace_id, profile.lakehouse_id)
    tables: Dict[str, List[str]] = {}
    for shortcut in shortcuts:
        path, name = shortcut_key(shortcut)
        if path.startswith("Tables/"):
            tables.setdefault(path.split("/", 1)[1], []).append(name)

    total = sum(len(v) for v in tables.values())
    print(f"\nTable shortcuts ({total}):")
    for schema in sorted(tables):
        print(f"  {schema} ({len(tables[schema])})")
        for name in sorted(tables[schema]):
            print(f"    {name}")

    onelake = OneLakeClient(tokens, profile.workspace_id, profile.lakehouse_id)
    print("\nPublished by Atlan:")
    for entry in read_index(onelake, profile):
        print(f"  {entry['namespace']:<24} {entry.get('tableCount', '?')} tables")
    return 0


# ============================================================================
# Entry point
# ============================================================================

def _looks_like_uuid(value: str) -> bool:
    return bool(re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", value))


def _namespaces(value: str) -> List[str]:
    return [n.strip() for n in value.split(",") if n.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fabric_onelake_shortcuts.py",
        description="Create OneLake shortcuts to the Atlan Lakehouse Iceberg tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--profile", default="default",
                        help="named configuration under ~/.atlan/fabric-onelake-shortcuts "
                             "(default: default)")
    parser.add_argument("--fabric-auth", default="azure_cli",
                        choices=["azure_cli", "service_principal"],
                        help="how to authenticate to YOUR Fabric tenant (default: azure_cli, "
                             "the identity from 'az login'; service_principal reads "
                             "FABRIC_TENANT_ID, FABRIC_CLIENT_ID and FABRIC_CLIENT_SECRET)")
    parser.add_argument("--verbose", action="store_true", help="debug logging and full listings")

    sub = parser.add_subparsers(dest="command", required=True)

    setup = sub.add_parser("setup", help="create or verify the lakehouse, connection and "
                                         "pointer shortcut (idempotent)")
    setup.add_argument("--workspace", help="Fabric workspace id or display name")
    setup.add_argument("--adls-url", help="shortcut connector URL from Atlan, pointing at the "
                                          "catalog root, e.g. "
                                          "abfss://container@account.dfs.core.windows.net/root")
    setup.add_argument("--lakehouse-name", help=f"default: {DEFAULT_LAKEHOUSE_NAME}")
    setup.add_argument("--connection-name", help=f"default: {DEFAULT_CONNECTION_NAME}")
    setup.add_argument("--pointer-shortcut", help=f"default: {DEFAULT_POINTER_SHORTCUT}")
    setup.add_argument("--atlan-tenant-id", help="Entra tenant id of the Atlan service principal "
                                                 "(only needed when creating the connection)")
    setup.add_argument("--atlan-client-id", help="client id of the Atlan service principal "
                                                 "(only needed when creating the connection)")
    setup.add_argument("--namespaces", type=_namespaces,
                       help=f"comma-separated; default: {','.join(DEFAULT_NAMESPACES)}")
    setup.set_defaults(func=cmd_setup)

    sync = sub.add_parser("sync", help="create table shortcuts for the selected namespaces")
    sync.add_argument("--namespaces", type=_namespaces,
                      help="comma-separated; defaults to the profile's list. Namespaces Atlan "
                           "does not publish are skipped, not an error")
    sync.add_argument("--non-empty-only", action="store_true",
                      help="only shortcut tables whose current snapshot has more than 0 records")
    sync.add_argument("--plan", action="store_true", help="show what would change, apply nothing")
    sync.add_argument("--overwrite", action="store_true",
                      help="repoint shortcuts whose target no longer matches the pointer file")
    sync.add_argument("--prune", action="store_true",
                      help="delete table shortcuts that are gone from the pointer files")
    sync.set_defaults(func=cmd_sync)

    status = sub.add_parser("status", help="show the profile, existing shortcuts and what "
                                           "Atlan publishes")
    status.set_defaults(func=cmd_status)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        return args.func(args)
    except SyncError as e:
        logger.error("%s", e)
        return 1
    except KeyboardInterrupt:
        logger.error("Interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
