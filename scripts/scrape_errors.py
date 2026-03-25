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

import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

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
            "configmgr",
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
        ],
        "parser": "parse_exchange",
        "tag": "support-azure",
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
    text = re.sub(r"<br\s*/?>", " ", text)  # line breaks
    text = re.sub(r"<[^>]+>", "", text)  # HTML tags
    text = re.sub(r"\s+", " ", text)  # collapse whitespace
    return text.strip()


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
                message = ""
                description = ""
                resolution = ""

                for key in row:
                    val = row[key]
                    if not val:
                        continue
                    k = key.lower()

                    # Code columns
                    if any(x in k for x in ("hexadecimal error code", "hex error", "hex code")):
                        code = val
                    elif k in ("error code", "code", "error") and not code:
                        code = val
                    elif k in ("status code", "status") and not code:
                        code = val
                    # Symbolic name (secondary code info)
                    elif k in ("symbolic name",):
                        if not description:
                            description = val
                    # Message columns
                    elif any(x in k for x in ("error message", "message", "more information",
                                                "what to do", "what you should try")):
                        if not message:
                            message = val
                        elif not resolution:
                            resolution = val
                    elif any(x in k for x in ("description", "cause", "reason", "details")):
                        if not message:
                            message = val
                        elif not description:
                            description = val
                    elif any(x in k for x in ("resolution", "solution", "fix", "remediation",
                                                "troubleshoot", "action", "mitigation")):
                        resolution = val

                if code and len(code) > 1 and code.lower() not in ("n/a", "none", "no status"):
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

            for key in row:
                val = row[key]
                if not val:
                    continue
                k = key.lower()
                if any(x in k for x in ("error", "code", "message id", "status", "hex", "decimal")):
                    if not code:
                        code = clean_md_text(val)
                elif any(x in k for x in ("description", "message", "details", "text")):
                    if not message:
                        message = clean_md_text(val)
                    elif not description:
                        description = clean_md_text(val)
                elif any(x in k for x in ("resolution", "solution", "action", "fix")):
                    resolution = clean_md_text(val)

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


# ── Dispatcher ────────────────────────────────────────────────────────────────

PARSERS = {
    "parse_entra": parse_entra,
    "parse_graph": parse_graph,
    "parse_intune": parse_intune,
    "parse_sccm": parse_sccm,
    "parse_exchange": parse_exchange,
}


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(errors: list[dict]) -> list[dict]:
    """Remove duplicate error codes within the same product, keeping the richest entry."""
    seen = {}
    for e in errors:
        key = (e["code"], e.get("category", ""))
        if key in seen:
            existing = seen[key]
            # Keep whichever has more content
            existing_len = len(existing.get("message", "")) + len(existing.get("description", "")) + len(existing.get("resolution", ""))
            new_len = len(e.get("message", "")) + len(e.get("description", "")) + len(e.get("resolution", ""))
            if new_len > existing_len:
                seen[key] = e
        else:
            seen[key] = e
    return list(seen.values())


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
        repo_key = source["repo"] + "::" + tag
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
            if not ok:
                print(f"  Skipping {product} (clone failed)")
                continue
            cloned_repos[repo_key] = dest

        parser_fn = PARSERS[source["parser"]]
        errors = parser_fn(dest)

        # Tag each error with product
        for e in errors:
            e["product"] = product

        product_errors[product].extend(errors)

    # Clean up all cloned repos
    if workdir.exists():
        subprocess.run(["rm", "-rf", str(workdir)], check=False)

    # Deduplicate per product, write per-product files, collect all
    all_errors = []
    product_names = sorted(product_errors.keys())

    for product in product_names:
        errors = deduplicate(product_errors[product])
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
                for e in errors
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
