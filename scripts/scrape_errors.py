#!/usr/bin/env python3
"""Scrape Microsoft documentation repos for error codes across multiple products.

Clones specific paths from Microsoft doc repos on GitHub, parses markdown files
for error code tables and sections, and produces:
  - public/data/manifest.json   (lightweight index for all errors)
  - public/data/products/*.json (per-product detail with full descriptions/resolutions)

Usage:
    python3 scripts/scrape_errors.py [--workdir DIR]

The workdir defaults to a temporary 'docs_work' directory that is cleaned up after.
Uses only Python stdlib — no pip dependencies.
"""

import html as _html
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "public" / "data"
PRODUCTS_DIR = DATA_DIR / "products"

# ── Repo definitions ──────────────────────────────────────────────────────────
# Each entry: (product_name, repo_url, sparse_paths, parser_function_name)
SOURCES = [
    {
        "product": "Entra ID",
        "repo": "https://github.com/MicrosoftDocs/entra-docs.git",
        "sparse_paths": [
            "docs/identity-platform",
            "docs/identity",
        ],
        "parser": "parse_entra",
    },
    {
        "product": "Entra ID",
        "repo": "https://github.com/MicrosoftDocs/SupportArticles-docs.git",
        "sparse_paths": [
            "support/entra",
        ],
        "parser": "parse_entra",
        "tag": "support",
    },
    {
        "product": "Microsoft Graph",
        "repo": "https://github.com/microsoftgraph/microsoft-graph-docs-contrib.git",
        "sparse_paths": [
            "concepts",
            "api-reference",
        ],
        "parser": "parse_graph",
    },
    {
        "product": "Intune",
        "repo": "https://github.com/MicrosoftDocs/memdocs.git",
        "sparse_paths": [
            "intune",
        ],
        "parser": "parse_intune",
    },
    {
        "product": "Intune",
        "repo": "https://github.com/MicrosoftDocs/SupportArticles-docs.git",
        "sparse_paths": [
            "support/mem/intune",
        ],
        "parser": "parse_intune",
        "tag": "support",
    },
    {
        "product": "SCCM",
        "repo": "https://github.com/MicrosoftDocs/memdocs.git",
        "sparse_paths": [
            "intune/configmgr",
        ],
        "parser": "parse_sccm",
    },
    {
        "product": "SCCM",
        "repo": "https://github.com/MicrosoftDocs/SupportArticles-docs.git",
        "sparse_paths": [
            "support/mem/configmgr",
        ],
        "parser": "parse_sccm",
        "tag": "support",
    },
    {
        "product": "Exchange",
        "repo": "https://github.com/MicrosoftDocs/SupportArticles-docs.git",
        "sparse_paths": [
            "support/azure",
            "support/exchange-online",
            "support/exchange",
        ],
        "parser": "parse_exchange",
        "tag": "support-azure",
    },
    {
        "product": "Windows Installer",
        "repo": "https://github.com/MicrosoftDocs/win32.git",
        "sparse_paths": [
            "desktop-src/Msi",
        ],
        "parser": "parse_windows_installer",
    },
    {
        "product": "Windows Update",
        "url": "https://learn.microsoft.com/en-us/windows/deployment/update/windows-update-error-reference",
        "parser": "parse_windows_update",
    },
    {
        "product": "Intune",
        "url": "https://tplant.com.au/notes/microsoft/intune/error-codes",
        "parser": "parse_tplant_intune",
    },
]


# ── Git helpers ───────────────────────────────────────────────────────────────

def clone_sparse(repo_url: str, dest: Path, sparse_paths: list[str]) -> bool:
    """Shallow clone with sparse checkout. Returns True on success."""
    if dest.exists():
        subprocess.run(["rm", "-rf", str(dest)], check=True)
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--filter=blob:none", "--sparse", repo_url, str(dest)],
            check=True, capture_output=True, text=True, timeout=120,
        )
        subprocess.run(
            ["git", "sparse-checkout", "set"] + sparse_paths,
            cwd=str(dest), check=True, capture_output=True, text=True, timeout=60,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"  ⚠ Clone failed for {repo_url}: {e}", file=sys.stderr)
        return False


# ── Markdown parsing helpers ──────────────────────────────────────────────────

def read_md(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def extract_section(text: str, heading: str) -> str:
    """Extract content under a ## heading until the next ##."""
    pattern = re.compile(
        r"^##\s+" + re.escape(heading) + r"\s*\n(.*?)(?=^##\s|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    m = pattern.search(text)
    return m.group(1).strip() if m else ""


def parse_md_tables(text: str) -> list[list[dict]]:
    """Parse ALL markdown tables in text. Returns list of tables, each a list of row dicts."""
    tables = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith("|") and "|" in stripped[1:]:
            if i + 1 < len(lines) and re.match(r"^\|[\s\-:|]+\|$", lines[i + 1].strip()):
                headers = [h.strip().lower() for h in stripped.split("|")[1:-1]]
                rows = []
                j = i + 2
                while j < len(lines):
                    row_line = lines[j].strip()
                    if not row_line.startswith("|"):
                        break
                    cells = [c.strip() for c in row_line.split("|")[1:-1]]
                    if len(cells) >= len(headers):
                        row = {}
                        for k, h in enumerate(headers):
                            row[h] = clean_md_text(cells[k]) if k < len(cells) else ""
                        rows.append(row)
                    j += 1
                if rows:
                    tables.append(rows)
                i = j
                continue
        i += 1
    return tables


def parse_md_table(text: str) -> list[dict]:
    """Parse all markdown tables in text, returning all rows flattened."""
    tables = parse_md_tables(text)
    rows = []
    for table in tables:
        rows.extend(table)
    return rows


def find_md_files(base_dir: Path, pattern: str = "**/*.md") -> list[Path]:
    """Recursively find markdown files."""
    return sorted(base_dir.glob(pattern))


def clean_md_text(text: str) -> str:
    """Strip markdown formatting for plain-text output."""
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)  # links
    text = re.sub(r"\*\*`([^`]+)`\*\*", r"\1", text)  # bold+code combo
    text = re.sub(r"`([^`]+)`", r"\1", text)  # inline code
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)  # bold
    text = re.sub(r"__([^_]+)__", r"\1", text)  # bold (underscore)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)  # italic
    text = text.replace("\\_", "_")  # escaped underscores
    text = re.sub(r"<br\s*/?>", " ", text)  # line breaks
    text = re.sub(r"<[^>]+>", "", text)  # HTML tags
    text = re.sub(r"\s+", " ", text)  # collapse whitespace
    return text.strip()


_SYMBOLIC_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")


def merge_symbolic_name(symbolic: str, message: str) -> tuple[str, str]:
    """Prefix an ALL-CAPS symbolic name onto message.

    The UI renders ``description || message``, so an ALL-CAPS token like
    ``OM_S_REBOOT_REQUIRED`` must not stay in description or it hides the
    real error text. Returns ``(message, description)``:
    - if *symbolic* matches ``_SYMBOLIC_NAME_RE``, description is cleared
      and the token is prefixed onto message
    - otherwise the cleaned *symbolic* value is returned as description
      (non-token text is preserved for the caller)
    """
    symbolic = clean_md_text(symbolic)
    message = clean_md_text(message)
    if symbolic and _SYMBOLIC_NAME_RE.match(symbolic):
        if message and symbolic not in message:
            message = f"{symbolic} \u2014 {message}"
        elif not message:
            message = symbolic
        return message, ""
    return message, symbolic


def extract_error_blocks(text: str, code_pattern: str) -> list[dict]:
    """Extract error blocks that follow a pattern of heading with error code."""
    errors = []
    # Match headings like ### AADSTS50001 or ### Error 0x80070005
    heading_re = re.compile(
        r"^###?\s+.*?(" + code_pattern + r").*$",
        re.MULTILINE,
    )
    matches = list(heading_re.finditer(text))
    for i, match in enumerate(matches):
        code = match.group(1)
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        # First paragraph is the message/description
        paragraphs = re.split(r"\n\s*\n", body)
        message = clean_md_text(paragraphs[0]) if paragraphs else ""
        description = clean_md_text("\n\n".join(paragraphs[1:3])) if len(paragraphs) > 1 else ""
        if message:
            errors.append({
                "code": code,
                "message": message[:300],
                "description": description[:500],
            })
    return errors


# ── Product-specific parsers ──────────────────────────────────────────────────
# Each returns a list of error dicts:
#   { code, category, message, description, resolution, source_file }

def parse_entra(repo_dir: Path) -> list[dict]:
    """Parse Entra ID (Azure AD) error codes — AADSTS codes, sign-in errors."""
    errors = []

    # 1) AADSTS error codes from the reference page
    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        # Look for AADSTS error tables
        table_rows = parse_md_table(text)
        for row in table_rows:
            code = ""
            message = ""
            description = ""

            # Try different column name patterns
            for key in row:
                val = row[key]
                if not val:
                    continue
                if "error" in key and "code" in key or key in ("error", "code", "error code"):
                    code = clean_md_text(val)
                elif "message" in key or "description" in key.lower():
                    if not message:
                        message = clean_md_text(val)
                    else:
                        description = clean_md_text(val)

            # Match AADSTS pattern
            if code and re.match(r"AADSTS\d+", code):
                errors.append({
                    "code": code,
                    "category": "Authentication",
                    "message": message[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": rel_path,
                })

        # Also extract heading-based error blocks
        if "AADSTS" in text:
            blocks = extract_error_blocks(text, r"AADSTS\d+")
            for block in blocks:
                if not any(e["code"] == block["code"] for e in errors):
                    errors.append({
                        "code": block["code"],
                        "category": "Authentication",
                        "message": block["message"],
                        "description": block["description"],
                        "resolution": "",
                        "source_file": rel_path,
                    })

        # Conditional Access errors
        if "CA" in text and re.search(r"CA\d{5}", text):
            blocks = extract_error_blocks(text, r"CA\d{5}")
            for block in blocks:
                errors.append({
                    "code": block["code"],
                    "category": "Conditional Access",
                    "message": block["message"],
                    "description": block["description"],
                    "resolution": "",
                    "source_file": rel_path,
                })

    print(f"  Entra ID: {len(errors)} errors")
    return errors


def parse_graph(repo_dir: Path) -> list[dict]:
    """Parse Microsoft Graph API error codes and responses."""
    errors = []

    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        # Parse error tables
        table_rows = parse_md_table(text)
        for row in table_rows:
            code = ""
            message = ""
            description = ""

            for key in row:
                val = row[key]
                if not val:
                    continue
                k = key.lower()
                if k in ("error code", "code", "error", "status code", "http status code"):
                    code = clean_md_text(val)
                elif k in ("message", "description", "error message"):
                    if not message:
                        message = clean_md_text(val)
                    else:
                        description = clean_md_text(val)
                elif k in ("resolution", "solution", "fix", "action"):
                    description = clean_md_text(val)

            if code and (re.match(r"\d{3}", code) or re.match(r"[a-zA-Z]", code)):
                errors.append({
                    "code": code,
                    "category": categorize_graph_error(code, rel_path),
                    "message": message[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": rel_path,
                })

        # Extract error code patterns from headings
        error_patterns = [
            r"[A-Z][a-zA-Z]+Error",
            r"ErrorCode\.\w+",
            r"\d{3}\s",
        ]
        for pattern in error_patterns:
            blocks = extract_error_blocks(text, pattern)
            for block in blocks:
                if not any(e["code"] == block["code"] for e in errors):
                    errors.append({
                        "code": block["code"].strip(),
                        "category": categorize_graph_error(block["code"], rel_path),
                        "message": block["message"],
                        "description": block["description"],
                        "resolution": "",
                        "source_file": rel_path,
                    })

    print(f"  Microsoft Graph: {len(errors)} errors")
    return errors


def categorize_graph_error(code: str, path: str) -> str:
    """Categorize a Graph API error based on code or file path."""
    if re.match(r"4\d\d", code):
        return "Client Error"
    if re.match(r"5\d\d", code):
        return "Server Error"
    if "auth" in path.lower():
        return "Authentication"
    if "mail" in path.lower() or "message" in path.lower():
        return "Mail"
    if "user" in path.lower():
        return "User Management"
    if "group" in path.lower():
        return "Groups"
    return "API Error"


def parse_intune(repo_dir: Path) -> list[dict]:
    """Parse Intune error codes — enrollment, compliance, app deployment."""
    errors = []

    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        # Parse all tables in the file
        tables = parse_md_tables(text)
        for table_rows in tables:
            if not table_rows:
                continue
            headers = set(table_rows[0].keys())

            for row in table_rows:
                code = ""
                code_hex = ""
                code_dec = ""
                message = ""
                description = ""
                resolution = ""

                for key in row:
                    val = row[key]
                    if not val:
                        continue
                    k = key.lower()

                    # Dual-code columns: "Error code (Hex)" / "Error code (Dec)"
                    if "hex" in k and ("error" in k or "code" in k):
                        code_hex = clean_md_text(val)
                    elif "dec" in k and ("error" in k or "code" in k):
                        code_dec = clean_md_text(val)
                    # Code columns
                    elif any(x in k for x in ("hexadecimal error code", "hex error", "hex code")):
                        code_hex = clean_md_text(val) if not code_hex else code_hex
                    elif k in ("error code", "code", "error") and not code:
                        code = clean_md_text(val)
                    elif k in ("status code", "status") and not code:
                        code = clean_md_text(val)
                    # Symbolic name (secondary code info)
                    elif k in ("symbolic name",):
                        if not description:
                            description = clean_md_text(val)
                    # Message columns
                    elif any(x in k for x in ("error message", "message", "more information",
                                                "what to do", "what you should try")):
                        if not message:
                            message = clean_md_text(val)
                        elif not resolution:
                            resolution = clean_md_text(val)
                    elif any(x in k for x in ("description", "cause", "reason", "details")):
                        if not message:
                            message = clean_md_text(val)
                        elif not description:
                            description = clean_md_text(val)
                    elif any(x in k for x in ("resolution", "solution", "fix", "remediation",
                                                "troubleshoot", "action", "mitigation")):
                        resolution = clean_md_text(val)

                # If description is a symbolic name (e.g. OM_S_REBOOT_REQUIRED),
                # combine it with the actual message so it cannot shadow the
                # human-readable text in the UI (description || message).
                message, description = merge_symbolic_name(description, message)

                # If we found dual hex/dec columns, emit entries for both
                if code_hex or code_dec:
                    if not code:
                        code = code_hex or code_dec
                    category = categorize_intune_error(code_hex or code_dec or code, rel_path)
                    base = {
                        "category": category,
                        "message": message[:300],
                        "description": description[:500],
                        "resolution": resolution[:500],
                        "source_file": rel_path,
                    }
                    if code_hex and code_hex.lower() not in ("n/a", "none", "no status"):
                        errors.append({**base, "code": code_hex})
                    if code_dec and code_dec.lower() not in ("n/a", "none", "no status") and code_dec != code_hex:
                        errors.append({**base, "code": code_dec})
                    if code and code != code_hex and code != code_dec:
                        errors.append({**base, "code": code})
                elif code and len(code) > 1 and code.lower() not in ("n/a", "none", "no status"):
                    category = categorize_intune_error(code, rel_path)
                    errors.append({
                        "code": code,
                        "category": category,
                        "message": message[:300],
                        "description": description[:500],
                        "resolution": resolution[:500],
                        "source_file": rel_path,
                    })

        # Extract hex error codes from headings (0x8...)
        if "0x8" in text or "0x0" in text:
            blocks = extract_error_blocks(text, r"0x[0-9A-Fa-f]{6,10}")
            for block in blocks:
                if not any(e["code"] == block["code"] for e in errors):
                    errors.append({
                        "code": block["code"],
                        "category": categorize_intune_error(block["code"], rel_path),
                        "message": block["message"],
                        "description": block["description"],
                        "resolution": "",
                        "source_file": rel_path,
                    })

    print(f"  Intune: {len(errors)} errors")
    return errors


def categorize_intune_error(code: str, path: str) -> str:
    """Categorize Intune errors by file path context."""
    p = path.lower()
    if "enroll" in p:
        return "Enrollment"
    if "compliance" in p:
        return "Compliance"
    if "app" in p or "deploy" in p:
        return "App Deployment"
    if "config" in p or "profile" in p or "policy" in p:
        return "Configuration"
    if "certificate" in p or "scep" in p or "pkcs" in p:
        return "Certificates"
    if "vpn" in p or "wifi" in p or "network" in p:
        return "Network"
    return "General"


def classify_sccm_column(header: str) -> str:
    """Map an SCCM table header to the output field that should receive its value.

    Return values are output-field names, not header kinds:
    - "code": identifier columns (error, code, status, hex, decimal, Message ID)
    - "message": user-facing text, including headers that contain "description"
      (e.g. "Error description") as well as message/details/text/information
    - "symbolic": only the "Symbolic name" header (merged into message later)
    - "resolution": resolution/solution/action/fix
    - "": unrecognized

    Text columns are preferred over generic "error"/"code" matches so headers
    like "Error description" are not treated as code. "Message ID" /
    "message-id" style headers are code identifiers and must not match as message.
    """
    k = " ".join(header.lower().replace("-", " ").replace("_", " ").split())
    if "message id" in k:
        return "code"
    if k == "symbolic name":
        return "symbolic"
    if any(x in k for x in ("resolution", "solution", "action", "fix")):
        return "resolution"
    if any(x in k for x in ("description", "message", "details", "text", "information")):
        return "message"
    if any(x in k for x in ("error", "code", "status", "hex", "decimal")):
        return "code"
    return ""


def parse_sccm(repo_dir: Path) -> list[dict]:
    """Parse SCCM/ConfigMgr error codes and status messages."""
    errors = []

    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        # Parse error tables
        table_rows = parse_md_table(text)
        for row in table_rows:
            code = ""
            message = ""
            description = ""
            resolution = ""
            symbolic = ""

            for key in row:
                val = row[key]
                if not val:
                    continue
                kind = classify_sccm_column(key)
                if kind == "message":
                    if not message:
                        message = clean_md_text(val)
                    elif not description:
                        description = clean_md_text(val)
                elif kind == "symbolic":
                    if not symbolic:
                        symbolic = clean_md_text(val)
                elif kind == "resolution":
                    resolution = clean_md_text(val)
                elif kind == "code":
                    if not code:
                        code = clean_md_text(val)

            # Merge Symbolic name into message (same as Intune) so it cannot
            # hide human-readable text via the UI's description || message.
            if symbolic:
                message, leftover = merge_symbolic_name(symbolic, message)
                if leftover and not description:
                    description = leftover

            if code and len(code) > 1:
                errors.append({
                    "code": code,
                    "category": categorize_sccm_error(rel_path),
                    "message": message[:300],
                    "description": description[:500],
                    "resolution": resolution[:500],
                    "source_file": rel_path,
                })

        # Extract hex codes from headings
        if "0x8" in text:
            blocks = extract_error_blocks(text, r"0x[0-9A-Fa-f]{6,8}")
            for block in blocks:
                if not any(e["code"] == block["code"] for e in errors):
                    errors.append({
                        "code": block["code"],
                        "category": categorize_sccm_error(rel_path),
                        "message": block["message"],
                        "description": block["description"],
                        "resolution": "",
                        "source_file": rel_path,
                    })

    print(f"  SCCM: {len(errors)} errors")
    return errors


def categorize_sccm_error(path: str) -> str:
    p = path.lower()
    if "client" in p:
        return "Client"
    if "osd" in p or "task-sequence" in p or "deploy" in p:
        return "OS Deployment"
    if "software-update" in p or "wsus" in p:
        return "Software Updates"
    if "site" in p or "server" in p:
        return "Site Server"
    if "content" in p or "distribution" in p:
        return "Content Distribution"
    return "General"


def parse_exchange(repo_dir: Path) -> list[dict]:
    """Parse Exchange/mail-related error codes from SupportArticles and docs."""
    errors = []

    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue
        # Only process files that mention exchange, mail, or NDR
        text_lower = text.lower()
        if not any(kw in text_lower for kw in ("exchange", "mail", "ndr", "smtp",
                                                  "non-delivery", "transport")):
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        table_rows = parse_md_table(text)
        for row in table_rows:
            code = ""
            message = ""
            description = ""
            resolution = ""

            for key in row:
                val = row[key]
                if not val:
                    continue
                k = key.lower()
                if any(x in k for x in ("error", "code", "ndr", "status", "enhanced status")):
                    if not code:
                        code = val
                elif any(x in k for x in ("description", "message", "cause", "reason")):
                    if not message:
                        message = val
                    elif not description:
                        description = val
                elif any(x in k for x in ("resolution", "solution", "fix", "action")):
                    resolution = val

            if code and len(code) > 1:
                errors.append({
                    "code": code,
                    "category": categorize_exchange_error(code, rel_path),
                    "message": message[:300],
                    "description": description[:500],
                    "resolution": resolution[:500],
                    "source_file": rel_path,
                })

        # NDR codes like 4.x.x or 5.x.x
        if re.search(r"[45]\.\d+\.\d+", text):
            blocks = extract_error_blocks(text, r"[45]\.\d+\.\d+")
            for block in blocks:
                if not any(e["code"] == block["code"] for e in errors):
                    errors.append({
                        "code": block["code"],
                        "category": "NDR",
                        "message": block["message"],
                        "description": block["description"],
                        "resolution": "",
                        "source_file": rel_path,
                    })

    print(f"  Exchange: {len(errors)} errors")
    return errors


def categorize_exchange_error(code: str, path: str) -> str:
    p = path.lower()
    if "ndr" in p or "non-delivery" in p or re.match(r"[45]\.\d+\.\d+", code):
        return "NDR"
    if "mail-flow" in p or "transport" in p:
        return "Mail Flow"
    if "connect" in p:
        return "Connectivity"
    if "hybrid" in p:
        return "Hybrid"
    if "migration" in p or "move" in p:
        return "Migration"
    return "General"


def parse_windows_installer(repo_dir: Path) -> list[dict]:
    """Parse Windows Installer / MSI error codes from win32 docs.

    Creates two entries per error: one keyed by numeric value (e.g. '3010')
    and one keyed by symbolic name (e.g. 'ERROR_SUCCESS_REBOOT_REQUIRED').
    """
    errors = []

    for md_file in find_md_files(repo_dir):
        text = read_md(md_file)
        if not text:
            continue

        rel_path = str(md_file.relative_to(repo_dir))

        table_rows = parse_md_table(text)
        for row in table_rows:
            name = ""
            value = ""
            description = ""

            for key in row:
                val = row[key]
                if not val:
                    continue
                k = key.lower()
                if any(x in k for x in ("name", "error", "constant")):
                    name = clean_md_text(val)
                elif any(x in k for x in ("value", "code", "decimal", "number")):
                    value = clean_md_text(val)
                elif any(x in k for x in ("description", "message", "meaning")):
                    description = clean_md_text(val)

            if not name and not value:
                continue

            # Clean the numeric value — strip trailing L, spaces
            if value:
                value = re.sub(r"[Ll]$", "", value.strip())

            # Build a combined message for the numeric code entry
            if name and description:
                combined_msg = f"{name} \u2014 {description}"
            elif description:
                combined_msg = description
            elif name:
                combined_msg = name
            else:
                combined_msg = ""

            # Entry keyed by numeric value (e.g. "3010")
            if value and re.match(r"^\d+$", value):
                errors.append({
                    "code": value,
                    "category": "MSI Return Code",
                    "message": combined_msg[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": rel_path,
                })

            # Entry keyed by symbolic name (e.g. "ERROR_SUCCESS_REBOOT_REQUIRED")
            if name and re.match(r"^[A-Z][A-Z0-9_]+$", name):
                sym_msg = description if description else ""
                if value:
                    sym_msg = f"({value}) {sym_msg}".strip()
                errors.append({
                    "code": name,
                    "category": "MSI Return Code",
                    "message": sym_msg[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": rel_path,
                })

    print(f"  Windows Installer: {len(errors)} errors")
    return errors


# Learn returns 403 to a short scraper UA once a crawl is in flight. Keep this
# browser-style UA on every attempt, including 403/429/503 retries.
_FETCH_USER_AGENT = "Mozilla/5.0 (compatible; ErrorIndex/1.0)"


def _fetch_headers(_attempt: int) -> dict[str, str]:
    """Headers for a fetch attempt. The user agent does not change between retries."""
    return {
        "User-Agent": _FETCH_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    }


def fetch_url_text(url: str, timeout: int = 60, quiet: bool = False) -> str:
    """Fetch a URL and return the body as text. Returns empty string on failure."""
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers=_fetch_headers(attempt))
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code in (403, 429, 503) and attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            if not quiet and e.code != 404:
                print(f"  Warning: Fetch failed for {url}: {e}", file=sys.stderr)
            return ""
        except Exception as e:
            if attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            if not quiet:
                print(f"  Warning: Fetch failed for {url}: {e}", file=sys.stderr)
            return ""
    return ""


class _LearnContentParser(HTMLParser):
    """Turn Learn article bodies into markdown the table parsers already understand.

    Learn puts the title and the article in separate ``div.content`` nodes, so
    every such node is captured. Tables become GitHub-style markdown tables.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._div_depth = 0
        self._capturing = False
        self._skip = 0
        self._blocks: list[str] = []
        self._text: list[str] | None = None
        self._text_kind: str | None = None
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        classes = (dict(attrs).get("class") or "").split()
        if tag == "div":
            if self._capturing:
                self._div_depth += 1
            elif "content" in classes:
                self._capturing = True
                self._div_depth = 1
        if not self._capturing:
            return
        if tag in ("script", "style", "svg", "noscript"):
            self._skip += 1
            return
        if self._skip:
            return
        if tag == "table":
            self._finish_inline()
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._text = []
            self._text_kind = "cell"
        elif tag in ("h1", "h2", "h3", "h4") and self._table is None:
            self._finish_inline()
            self._text = []
            self._text_kind = tag
        elif tag == "p" and self._table is None and self._text_kind is None:
            self._text = []
            self._text_kind = "p"
        elif tag == "br" and self._text is not None:
            self._text.append(" ")

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in ("script", "style", "svg", "noscript") and self._skip:
            self._skip -= 1
            return
        if tag == "div" and self._capturing:
            self._div_depth -= 1
            if self._div_depth <= 0:
                self._finish_inline()
                self._flush_table()
                self._capturing = False
                self._div_depth = 0
            return
        if not self._capturing or self._skip:
            return
        if tag in ("td", "th") and self._text_kind == "cell":
            cell = _collapse_html_text(self._text or [])
            if self._row is not None:
                self._row.append(cell)
            self._text = None
            self._text_kind = None
        elif tag == "tr" and self._row is not None:
            if any(cell.strip() for cell in self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table":
            self._flush_table()
        elif tag in ("h1", "h2", "h3", "h4", "p") and self._text_kind == tag:
            self._finish_inline()

    def handle_data(self, data):
        if self._capturing and not self._skip and self._text is not None:
            self._text.append(data)

    def _finish_inline(self):
        if self._text_kind not in ("p", "h1", "h2", "h3", "h4"):
            return
        text = _collapse_html_text(self._text or [])
        kind = self._text_kind
        self._text = None
        self._text_kind = None
        if not text:
            return
        if kind == "p":
            self._blocks.append(text)
        else:
            self._blocks.append("#" * int(kind[1]) + " " + text)

    def _flush_table(self):
        rows = self._table or []
        self._table = None
        self._row = None
        if len(rows) < 2:
            return
        width = max(len(row) for row in rows)

        def fmt(row: list[str]) -> str:
            cells = list(row) + [""] * (width - len(row))
            return "| " + " | ".join(cells[:width]) + " |"

        header = fmt(rows[0])
        sep = "| " + " | ".join("---" for _ in range(width)) + " |"
        body = "\n".join(fmt(row) for row in rows[1:])
        self._blocks.append("\n".join((header, sep, body)))

    def markdown(self) -> str:
        self._finish_inline()
        self._flush_table()
        return "\n\n".join(block for block in self._blocks if block.strip())


def _collapse_html_text(parts: list[str]) -> str:
    text = re.sub(r"\s+", " ", "".join(parts)).strip()
    return text.replace("|", "/")


def html_content_to_markdown(html: str) -> str:
    """Convert Learn ``div.content`` regions to markdown tables and headings."""
    parser = _LearnContentParser()
    parser.feed(html)
    parser.close()
    return parser.markdown()


_LEARN_ORIGIN = "https://learn.microsoft.com/en-us"
_LEARN_MD_CACHE: dict[str, str] = {}
_CONFIGMGR_PAGES: list[str] | None = None
_CONFIGMGR_SECTION_FALLBACK = (
    "core", "tenant-attach", "develop", "comanage", "protect",
    "apps", "mdt", "sum", "osd", "compliance", "cloud-attach",
)


def _fetch_json(url: str):
    text = fetch_url_text(url, timeout=30, quiet=True)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _toc_paths(toc, base_dir: str) -> list[str]:
    """Page paths relative to the memdocs repo root, from a Learn toc.json."""
    pages = []

    def walk(node):
        if isinstance(node, dict):
            href = (node.get("href") or "").strip()
            if (
                href
                and href not in ("./", "../", "./", ".")
                and not href.startswith(("http://", "https://", "/", "mailto:", "#"))
                and "?" not in href
                and not href.endswith((".yml", ".yaml", ".json"))
            ):
                rel = href[2:] if href.startswith("./") else href
                rel = rel.strip("/")
                if rel and rel not in (".", ".."):
                    pages.append(f"{base_dir.strip('/')}/{rel}")
            for child in node.get("children") or []:
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    items = toc.get("items", toc) if isinstance(toc, dict) else toc
    walk(items)
    return pages


def _configmgr_section_names() -> list[str]:
    hub = fetch_url_text(f"{_LEARN_ORIGIN}/intune/configmgr/", timeout=30, quiet=True)
    sections = []
    if hub:
        for href in re.findall(r'href="([^"#?]+)"', hub):
            if href.startswith(("http", "/", "mailto", "#", "javascript")):
                continue
            seg = href.split("/", 1)[0].strip()
            if seg and seg not in (".", "..") and seg not in sections:
                sections.append(seg)
    for seg in _CONFIGMGR_SECTION_FALLBACK:
        if seg not in sections:
            sections.append(seg)
    return sections


def _pages_for_configmgr_sections(sections: list[str]) -> tuple[list[str], list[str]]:
    pages = []
    found = []
    for seg in sections:
        toc = None
        for attempt in range(3):
            toc = _fetch_json(f"{_LEARN_ORIGIN}/intune/configmgr/{seg}/toc.json")
            if toc:
                break
            time.sleep(2 * (attempt + 1))
        if not toc:
            continue
        found.append(seg)
        pages.extend(_toc_paths(toc, f"intune/configmgr/{seg}"))
    return pages, found


def _configmgr_pages() -> list[str]:
    """Pages under intune/configmgr, which Learn publishes as per-section TOCs."""
    global _CONFIGMGR_PAGES
    if _CONFIGMGR_PAGES is not None:
        return list(_CONFIGMGR_PAGES)
    sections = _configmgr_section_names()
    pages, found = _pages_for_configmgr_sections(sections)
    if not found:
        pages, found = _pages_for_configmgr_sections(list(_CONFIGMGR_SECTION_FALLBACK))
    seen = set()
    unique = []
    for page in pages:
        if page not in seen:
            seen.add(page)
            unique.append(page)
    _CONFIGMGR_PAGES = unique
    print(f"  Learn ConfigMgr: {len(unique)} pages in {', '.join(found) or 'no'} sections")
    return list(unique)


def learn_pages_for_sparse_path(sparse_path: str) -> list[str]:
    """Learn pages that correspond to a memdocs sparse-checkout path.

    A git sparse checkout of ``intune`` includes ``intune/configmgr``. Learn
    splits those into the Intune TOC plus one TOC per Configuration Manager
    section, and no longer publishes ``/intune/configmgr/toc.json``.
    """
    pages = []
    path = sparse_path.strip("/")
    if path == "intune":
        toc = _fetch_json(f"{_LEARN_ORIGIN}/intune/toc.json")
        if toc:
            pages.extend(_toc_paths(toc, "intune"))
        pages.extend(_configmgr_pages())
    elif path == "intune/configmgr" or path.startswith("intune/configmgr/"):
        pages.extend(_configmgr_pages())
    else:
        toc = _fetch_json(f"{_LEARN_ORIGIN}/{path}/toc.json")
        if toc:
            pages.extend(_toc_paths(toc, path))
    seen = set()
    unique = []
    for page in pages:
        if page not in seen:
            seen.add(page)
            unique.append(page)
    return unique


def materialize_learn_sparse(dest: Path, sparse_paths: list[str]) -> bool:
    """Write Learn articles as markdown so the git-repo parsers can read them.

    Used when MicrosoftDocs/memdocs cannot be cloned. Returns True if at least
    one article was written.
    """
    pages = []
    for sparse_path in sparse_paths:
        pages.extend(learn_pages_for_sparse_path(sparse_path))
    seen = set()
    unique = []
    for page in pages:
        if page not in seen:
            seen.add(page)
            unique.append(page)
    if not unique:
        print("  Learn fallback found no pages", file=sys.stderr)
        return False

    if dest.exists():
        subprocess.run(["rm", "-rf", str(dest)], check=True)
    dest.mkdir(parents=True)

    lock = threading.Lock()

    def one(rel: str) -> int:
        url = f"{_LEARN_ORIGIN}/{rel}"
        try:
            with lock:
                md = _LEARN_MD_CACHE.get(url)
            if md is None:
                html = fetch_url_text(url, timeout=25, quiet=True)
                if not html:
                    return 0
                md = html_content_to_markdown(html)
                with lock:
                    _LEARN_MD_CACHE[url] = md
            if not md.strip() or ("|" not in md and "0x" not in md.lower()):
                return 0
            path = dest / f"{rel}.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(md, encoding="utf-8")
            return 1
        except Exception as e:
            print(f"  Warning: Learn page {rel}: {e}", file=sys.stderr)
            return 0

    written = 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        for i, count in enumerate(pool.map(one, unique), 1):
            written += count
            if i % 200 == 0 or i == len(unique):
                print(f"  Learn fallback: {i}/{len(unique)} pages, {written} with tables")
    print(f"  Learn fallback: wrote {written} markdown files")
    return written > 0


def _parse_wu_from_html(html: str) -> list[dict]:
    """Parse Windows Update error codes from learn.microsoft.com HTML page.

    The page has h2 section headings and tables with columns:
    Error code | Message (symbolic name) | Description
    """
    errors = []
    source = "windows-update-error-reference"
    current_category = "General"

    sections = re.split(r"<h2[^>]*>", html)
    for section in sections:
        heading_match = re.match(r"([^<]+)</h2>", section)
        if heading_match:
            current_category = clean_md_text(heading_match.group(1).strip())

        table_blocks = re.findall(r"<table[^>]*>(.*?)</table>", section, re.DOTALL)
        for table_html in table_blocks:
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
            for row_html in rows:
                cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
                if len(cells) < 2:
                    continue

                code = clean_md_text(re.sub(r"<[^>]+>", "", cells[0]))
                sym_name = clean_md_text(re.sub(r"<[^>]+>", "", cells[1])) if len(cells) > 1 else ""
                description = clean_md_text(re.sub(r"<[^>]+>", "", cells[2])) if len(cells) > 2 else ""

                if not code or not re.match(r"^0x[0-9a-fA-F]+$", code):
                    continue

                if sym_name and description:
                    combined_msg = f"{sym_name} \u2014 {description}"
                elif sym_name:
                    combined_msg = sym_name
                elif description:
                    combined_msg = description
                else:
                    combined_msg = ""

                errors.append({
                    "code": code.upper().replace("0X", "0x"),
                    "category": current_category,
                    "message": combined_msg[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": source,
                })

                if sym_name and re.match(r"^[A-Z][A-Z0-9_]+$", sym_name):
                    sym_msg = description if description else ""
                    if code:
                        sym_msg = f"({code}) {sym_msg}".strip()
                    errors.append({
                        "code": sym_name,
                        "category": current_category,
                        "message": sym_msg[:300],
                        "description": description[:500],
                        "resolution": "",
                        "source_file": source,
                    })

    return errors


def _parse_wu_from_md(text: str, source_file: str) -> list[dict]:
    """Parse WU error codes from markdown text content."""
    errors = []
    tables = parse_md_tables(text)
    for table_rows in tables:
        if not table_rows:
            continue
        for row in table_rows:
            code = ""
            sym_name = ""
            message = ""
            description = ""
            for key in row:
                val = row[key]
                if not val:
                    continue
                k = key.lower()
                if any(x in k for x in ("error code", "hex", "code", "hresult")):
                    if not code:
                        code = clean_md_text(val)
                elif any(x in k for x in ("symbolic name", "name", "error name", "constant")):
                    sym_name = clean_md_text(val)
                elif any(x in k for x in ("description", "message", "meaning", "fix")):
                    if not message:
                        message = clean_md_text(val)
                    elif not description:
                        description = clean_md_text(val)

            if not code:
                continue
            if sym_name and message:
                combined_msg = f"{sym_name} \u2014 {message}"
            elif sym_name:
                combined_msg = sym_name
            elif message:
                combined_msg = message
            else:
                combined_msg = ""

            if re.match(r"^0x[0-9a-fA-F]+$", code):
                errors.append({
                    "code": code.upper().replace("0X", "0x"),
                    "category": "Windows Update",
                    "message": combined_msg[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": source_file,
                })
            if sym_name and re.match(r"^[A-Z][A-Z0-9_]+$", sym_name):
                sym_msg = description if description else ""
                if code:
                    sym_msg = f"({code}) {sym_msg}".strip()
                errors.append({
                    "code": sym_name,
                    "category": "Windows Update",
                    "message": sym_msg[:300],
                    "description": description[:500],
                    "resolution": "",
                    "source_file": source_file,
                })

    if "0x8" in text or "0x0" in text:
        blocks = extract_error_blocks(text, r"0x[0-9A-Fa-f]{6,10}")
        for block in blocks:
            if not any(e["code"] == block["code"] for e in errors):
                errors.append({
                    "code": block["code"],
                    "category": "Windows Update",
                    "message": block["message"],
                    "description": block["description"],
                    "resolution": "",
                    "source_file": source_file,
                })
    return errors


def parse_windows_update(source_path) -> list[dict]:
    """Parse Windows Update error codes from URL or local repo.

    Accepts either a Path (repo dir) or a str (URL) as source_path.
    The upstream repo (windows-itpro-docs) is private, so this falls back to
    fetching the learn.microsoft.com HTML page directly.
    """
    errors = []

    if isinstance(source_path, str) and source_path.startswith("http"):
        html = fetch_url_text(source_path)
        if html:
            errors = _parse_wu_from_html(html)
    elif isinstance(source_path, Path) and source_path.exists():
        for md_file in find_md_files(source_path):
            text = read_md(md_file)
            if not text:
                continue
            rel_path = str(md_file.relative_to(source_path))
            errors.extend(_parse_wu_from_md(text, rel_path))

    print(f"  Windows Update: {len(errors)} errors")
    return errors


# ── tplant.com.au Intune error code parser ────────────────────────────────────

def _categorize_from_heading(heading: str) -> str:
    """Map a blog section heading to an Intune error category."""
    h = heading.lower()
    if any(x in h for x in ("enroll", "registration", "join", "autopilot", "oobe", "hybrid")):
        return "Enrollment"
    if any(x in h for x in ("compliance", "noncompliant")):
        return "Compliance"
    if any(x in h for x in ("app", "deploy", "install", "package", "win32")):
        return "App Deployment"
    if any(x in h for x in ("config", "profile", "policy", "setting")):
        return "Configuration"
    if any(x in h for x in ("certificate", "scep", "pkcs", "cert")):
        return "Certificates"
    if any(x in h for x in ("vpn", "wifi", "wi-fi", "network", "proxy")):
        return "Network"
    return "General"


def _strip_html_tags(text: str) -> str:
    """Remove HTML tags and decode HTML entities."""
    text = re.sub(r"<[^>]+>", "", text)
    # Replace &nbsp; with a regular space before full entity decoding
    text = text.replace("&nbsp;", " ")
    text = _html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_tplant_html(html: str, source_url: str) -> list[dict]:
    """Parse error code tables and headings from a tplant.com.au HTML page.

    Handles:
    - HTML tables with th-based header detection (code / description / resolution)
    - h2/h3 section headings used for category context
    - Fallback positional column mapping when no headers are present
    """
    errors = []
    current_category = "General"

    # Split on heading tags to capture per-section category context
    parts = re.split(r"(<h[23][^>]*>.*?</h[23]>)", html, flags=re.DOTALL | re.IGNORECASE)

    for part in parts:
        # Update category from heading
        heading_match = re.match(r"<h[23][^>]*>(.*?)</h[23]>", part, re.DOTALL | re.IGNORECASE)
        if heading_match:
            heading_text = _strip_html_tags(heading_match.group(1))
            if heading_text:
                current_category = _categorize_from_heading(heading_text)
            continue

        # Parse all tables in this section
        table_blocks = re.findall(r"<table[^>]*>(.*?)</table>", part, re.DOTALL | re.IGNORECASE)
        for table_html in table_blocks:
            # Detect column semantics from <th> header cells
            headers_html = re.findall(r"<th[^>]*>(.*?)</th>", table_html, re.DOTALL | re.IGNORECASE)
            headers = [_strip_html_tags(h).lower() for h in headers_html]

            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE)
            for row_html in rows:
                cells_html = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL | re.IGNORECASE)
                if not cells_html:
                    continue
                cells = [clean_md_text(_strip_html_tags(c)) for c in cells_html]
                if not any(cells):
                    continue

                code = ""
                message = ""
                resolution = ""

                if headers:
                    for i, h in enumerate(headers):
                        if i >= len(cells):
                            break
                        v = cells[i]
                        if not v:
                            continue
                        if any(x in h for x in ("code", "error", "hex", "number", "#", "id")):
                            if not code:
                                code = v
                        elif any(x in h for x in ("resolution", "fix", "solution", "action",
                                                   "remediat", "what to do", "steps")):
                            resolution = v
                        elif any(x in h for x in ("description", "message", "detail",
                                                   "cause", "meaning", "reason", "text")):
                            if not message:
                                message = v
                        elif not message and i == 1:
                            # No recognised header: treat second column as message
                            message = v
                else:
                    # No headers — positional: code, description, [resolution]
                    code = cells[0]
                    message = cells[1] if len(cells) > 1 else ""
                    resolution = cells[2] if len(cells) > 2 else ""

                if not code:
                    continue

                errors.append({
                    "code": code,
                    "category": current_category,
                    "message": message[:300],   # same limit as the rest of the parsers
                    "description": "",
                    "resolution": resolution[:500],  # same limit as the rest of the parsers
                    "source_file": source_url,
                })

    return errors


_TPLANT_MAX_PAGES = 20  # safety cap on total pages visited during BFS


def _extract_tplant_links(html: str, base_url: str) -> list[str]:
    """Return unique same-domain Intune blog-post URLs found in the HTML page.

    Only collects URLs on the same hostname that contain both '/blog/' and
    'intune' to avoid following unrelated posts in navigation/footer links.
    """
    base_parsed = urlparse(base_url)
    # Match quoted and unquoted href values
    raw_links = re.findall(r'href=(?:["\']([^"\']+)["\']|([^\s>]+))', html, re.IGNORECASE)
    # Each match is a 2-tuple from the two capture groups; take whichever is non-empty
    result = []
    seen = {base_url}
    for quoted, unquoted in raw_links:
        link = quoted or unquoted
        if not link or link.startswith(("javascript:", "mailto:", "tel:")):
            continue
        full_url = urljoin(base_url, link).split("#")[0]
        parsed = urlparse(full_url)
        if (parsed.netloc == base_parsed.netloc
                and full_url not in seen
                and "/blog/" in full_url
                and "intune" in full_url.lower()):
            seen.add(full_url)
            result.append(full_url)
    return result


def parse_tplant_intune(source_path) -> list[dict]:
    """Parse Intune error codes from tplant.com.au blog post and linked pages.

    Accepts a URL string. Fetches the seed page, parses HTML error tables, then
    performs a BFS over intra-domain Intune /blog/ links found on every visited
    page up to _TPLANT_MAX_PAGES total pages.
    """
    errors = []

    if not (isinstance(source_path, str) and source_path.startswith("http")):
        print(f"  Warning: parse_tplant_intune expects an HTTP URL, got: {source_path!r}",
              file=sys.stderr)
        return errors

    visited: set[str] = set()
    queue: list[str] = [source_path]

    while queue and len(visited) < _TPLANT_MAX_PAGES:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        page_html = fetch_url_text(url)
        if not page_html:
            continue

        errors.extend(_parse_tplant_html(page_html, url))

        # Enqueue new links found on this page (BFS)
        for linked_url in _extract_tplant_links(page_html, url):
            if linked_url not in visited:
                queue.append(linked_url)

    print(f"  Intune (tplant): {len(errors)} errors ({len(visited)} page(s) fetched)")
    return errors


# ── Dispatcher ────────────────────────────────────────────────────────────────

PARSERS = {
    "parse_entra": parse_entra,
    "parse_graph": parse_graph,
    "parse_intune": parse_intune,
    "parse_sccm": parse_sccm,
    "parse_exchange": parse_exchange,
    "parse_windows_installer": parse_windows_installer,
    "parse_windows_update": parse_windows_update,
    "parse_tplant_intune": parse_tplant_intune,
}


# ── Validation & Deduplication ────────────────────────────────────────────────

# Pattern: looks like an actual error code, not a sentence
_CODE_PATTERNS = re.compile(
    r"^("
    r"AADSTS\d+|"                          # Entra: AADSTS50001
    r"0x[0-9a-fA-F]+|"                     # Hex: 0x80cf0001
    r"-?\d{5,}|"                           # Large numeric: -2147024891
    r"\d{1,5}|"                            # Short numeric: 0, 87, 3010, 1603
    r"[A-Z][A-Z0-9_]{2,}|"                # Symbolic: ERROR_SUCCESS, WU_E_*
    r"[45]\.\d+\.\d+|"                     # NDR: 5.1.1
    r"[A-Z]{2,}\d+|"                       # Mixed: MP_E_INVALID_CONTENT
    r"HR[A-Z_]+|"                          # HRESULT aliases
    r"\d+(\s*,\s*0x[0-9a-fA-F]+)?"        # SCCM: -2147418113, 0x8000ffff
    r")$"
)


def normalize_error_code(entry: dict) -> dict:
    """Clean up error code field: split 'AADSTS123: message' patterns, reject non-codes."""
    code = entry["code"].strip().strip('"').strip("'")

    # Split "AADSTS123456: Some message text" into code + message
    m = re.match(r"^(AADSTS\d+)\s*[:]\s*(.+)", code, re.DOTALL)
    if m:
        code = m.group(1)
        # Prepend the split-off text to message if message is empty
        if not entry.get("message"):
            entry["message"] = m.group(2).strip()[:300]
        elif m.group(2).strip() not in entry["message"]:
            entry["message"] = m.group(2).strip()[:150] + " " + entry["message"]

    entry["code"] = code
    return entry


def is_valid_error_code(code: str) -> bool:
    """Reject entries where 'code' is actually a sentence or message."""
    if not code or len(code) > 60:
        return False
    # Reject if it looks like a sentence (starts with quote, or has many spaces)
    if code.startswith('"') or code.startswith("'") or code.startswith(">"):
        return False
    if code.count(" ") > 4:
        return False
    # Must match at least one known code pattern
    # But also allow short alphanumeric codes we might not anticipate
    clean = code.split(",")[0].strip()  # handle "123, 0x..." format
    if _CODE_PATTERNS.match(clean):
        return True
    # Allow any code that's short and doesn't look like natural language
    if len(code) <= 25 and not any(c in code for c in ".!?") and code[0].isupper():
        return True
    return False


def is_valid_message(msg: str) -> bool:
    """Reject entries where the message is clearly garbage (markdown artifacts)."""
    if not msg:
        return True  # empty messages are OK, just no content
    # Reject markdown headings scraped as messages
    if msg.startswith("#") or msg.startswith("| "):
        return False
    # Reject markdown table separator lines
    if msg.startswith(":---") or msg.startswith("---"):
        return False
    # Reject numbered step instructions (not error messages)
    if re.match(r"^\d+\.\s+(In |Open |Choose |Click |Select |Go to )", msg):
        return False
    # Reject code snippets
    if msg.startswith("``") or msg.startswith("$") or msg.startswith("Example:"):
        return False
    # Reject very short non-informative messages
    if len(msg.strip()) < 3:
        return False
    return True


def _entry_text_len(entry: dict) -> int:
    return (
        len(entry.get("message") or "")
        + len(entry.get("description") or "")
        + len(entry.get("resolution") or "")
    )


def deduplicate(errors: list[dict]) -> list[dict]:
    """Remove duplicate error codes within the same product, keeping the richest entry."""
    seen = {}
    for e in errors:
        key = (e["code"], e.get("category", ""))
        if key in seen:
            existing = seen[key]
            # Keep whichever has more content
            if _entry_text_len(e) > _entry_text_len(existing):
                seen[key] = e
        else:
            seen[key] = e
    return list(seen.values())


def collapse_by_code(errors: list[dict]) -> list[dict]:
    """One record per code for the product JSON object.

    ``deduplicate`` keeps the same code in different categories, but the product
    file is a map keyed only by code. Later rows replace earlier ones, except
    an empty message must not hide a row that already has text.
    """
    chosen = {}
    order = []
    for entry in errors:
        code = entry["code"]
        previous = chosen.get(code)
        if previous is None:
            chosen[code] = entry
            order.append(code)
            continue
        # Later rows win, matching the historical writer, except an empty
        # message must not hide a row that already has one.
        new_msg = (entry.get("message") or "").strip()
        prev_msg = (previous.get("message") or "").strip()
        if new_msg or not prev_msg:
            chosen[code] = entry
    return [chosen[code] for code in order]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    workdir = Path(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "--workdir" else Path("docs_work")

    PRODUCTS_DIR.mkdir(parents=True, exist_ok=True)

    # Collect errors per product from all sources
    product_errors: dict[str, list[dict]] = defaultdict(list)
    cloned_repos: dict[str, Path] = {}  # repo_url+tag -> local path

    for source in SOURCES:
        product = source["product"]
        tag = source.get("tag", "")
        parser_fn = PARSERS[source["parser"]]

        # URL-based source (no git clone needed)
        if "url" in source:
            url = source["url"]
            print(f"\n{'─' * 60}")
            print(f"Processing: {product}")
            print(f"  URL: {url}")
            errors = parser_fn(url)
            for e in errors:
                e["product"] = product
            product_errors[product].extend(errors)
            continue

        repo_key = source["repo"] + "::" + tag + "::" + "|".join(sorted(source.get("sparse_paths", [])))
        print(f"\n{'─' * 60}")
        print(f"Processing: {product}" + (f" ({tag})" if tag else ""))
        print(f"  Repo: {source['repo']}")

        # Reuse clone if same repo+tag already cloned, else clone fresh
        if repo_key in cloned_repos:
            dest = cloned_repos[repo_key]
        else:
            slug = product.lower().replace(" ", "_") + ("_" + tag if tag else "")
            dest = workdir / slug
            ok = clone_sparse(source["repo"], dest, source["sparse_paths"])
            if not ok and source["repo"].rstrip("/").endswith("MicrosoftDocs/memdocs.git"):
                # The repo is no longer public (clone exits 128). The same
                # trees are still published on Learn, including the renamed
                # software-update agent page (ref-agent-error-codes.md).
                print("  memdocs clone failed; reading learn.microsoft.com instead")
                ok = materialize_learn_sparse(dest, source["sparse_paths"])
            if not ok:
                print(f"  Skipping {product} (clone failed)")
                continue
            cloned_repos[repo_key] = dest

        errors = parser_fn(dest)

        # Tag each error with product
        for e in errors:
            e["product"] = product

        product_errors[product].extend(errors)

    # Clean up all cloned repos
    if workdir.exists():
        subprocess.run(["rm", "-rf", str(workdir)], check=False)

    # Normalize, validate, deduplicate per product
    all_errors = []
    product_names = sorted(product_errors.keys())

    for product in product_names:
        raw = product_errors[product]
        # Normalize codes (split AADSTS123: message, strip quotes)
        normalized = [normalize_error_code(e) for e in raw]
        # Filter out invalid codes (sentences, empty, too long) and garbage messages
        valid = [e for e in normalized if is_valid_error_code(e["code"]) and is_valid_message(e.get("message", ""))]
        rejected = len(raw) - len(valid)
        if rejected:
            print(f"  {product}: rejected {rejected} invalid codes")
        errors = deduplicate(valid)
        # count stays the category-level total. The object below is one key
        # per code, so count can exceed the number of keys when one code is
        # filed under two categories.
        stored = collapse_by_code(errors)
        product_slug = product.lower().replace(" ", "-")
        product_data = {
            "product": product,
            "count": len(errors),
            "errors": {
                e["code"]: {
                    "message": e.get("message", ""),
                    "description": e.get("description", ""),
                    "resolution": e.get("resolution", ""),
                    "category": e.get("category", ""),
                    "source_file": e.get("source_file", ""),
                }
                for e in stored
            },
        }
        product_path = PRODUCTS_DIR / f"{product_slug}.json"
        product_path.write_text(
            json.dumps(product_data, separators=(",", ":")),
            encoding="utf-8",
        )
        print(f"  {product}: {len(errors)} errors → {product_path.name}")
        all_errors.extend(errors)

    # Sort all errors by code
    all_errors.sort(key=lambda e: (e["product"], e["code"]))

    # Build slim manifest
    manifest_entries = []
    descriptions = {}
    for e in all_errors:
        manifest_entries.append({
            "c": e["code"],
            "p": e["product"],
            "t": e.get("category", "General"),
            "m": e.get("message", "")[:120],
            "h": bool(e.get("resolution")),
        })
        desc = e.get("description", "")
        if desc:
            descriptions[f"{e['product']}::{e['code']}"] = desc

    manifest = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "products": product_names,
        "count": len(manifest_entries),
        "d": manifest_entries,
    }

    manifest_path = DATA_DIR / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, separators=(",", ":")),
        encoding="utf-8",
    )
    manifest_kb = manifest_path.stat().st_size / 1024
    print(f"\nWritten manifest: {manifest_path} ({len(manifest_entries)} entries, {manifest_kb:.0f}KB)")

    # Descriptions file (deferred loading)
    desc_path = DATA_DIR / "descriptions.json"
    desc_path.write_text(
        json.dumps(descriptions, separators=(",", ":")),
        encoding="utf-8",
    )
    desc_kb = desc_path.stat().st_size / 1024
    print(f"Written descriptions: {desc_path} ({len(descriptions)} entries, {desc_kb:.0f}KB)")

    # Stats
    print(f"\n{'─' * 60}")
    print("Stats:")
    product_counts = defaultdict(int)
    category_counts = defaultdict(int)
    for e in all_errors:
        product_counts[e["product"]] += 1
        category_counts[e.get("category", "General")] += 1

    for product, count in sorted(product_counts.items()):
        print(f"  {product}: {count} errors")
    print(f"  Total: {len(all_errors)} errors")
    print(f"  Categories: {', '.join(sorted(category_counts.keys()))}")
    with_resolution = sum(1 for e in all_errors if e.get("resolution"))
    print(f"  With resolution: {with_resolution} ({100 * with_resolution / max(len(all_errors), 1):.1f}%)")


if __name__ == "__main__":
    main()
