# Copilot Instructions — Microsoft Error Index

## Architecture

This is a single-page web app (`public/index.html`) that serves as a searchable reference for Microsoft product error codes. It is a self-contained HTML file with inline CSS and JavaScript — no build system, no framework, no dependencies. Web assets live in `public/`.

**Data flow:**
1. On page load, fetches `data/manifest.json` via the Fetch API
2. Error data is normalized into a flat array of `{code, product, category, message, hasResolution}` objects
3. Filtering/search operates client-side against this in-memory array
4. Detailed error info (description, resolution) lazy-loaded from `data/products/{product}.json`

**Data generation:** `scripts/scrape_errors.py` clones Microsoft documentation repos (sparse checkout), parses markdown files for error code tables and sections, and outputs JSON to `public/data/`.

## Products Covered

- **Entra ID** — AADSTS authentication errors, Conditional Access
- **Microsoft Graph** — API error codes and HTTP status codes
- **Intune** — Enrollment, compliance, app deployment errors
- **SCCM/ConfigMgr** — Client errors, OS deployment, software updates
- **Exchange** — NDRs, mail flow, connectivity errors
- **SharePoint** — API errors, sites, migration, sync

## Running Locally

```bash
python3 -m http.server 8000 -d public      # then open http://localhost:8000
```

## Key Conventions

- **Single-file frontend**: All HTML, CSS, and JS live in `public/index.html`. Do not extract into separate files unless explicitly asked.
- **No build step**: Changes are immediately testable — just reload the browser.
- **Two-tier data**: Manifest for search/filter, per-product JSON for details.
- **Python stdlib only**: The scraper has no pip dependencies.

## Data Schema

### manifest.json
```json
{
  "updated": "2024-01-01",
  "products": ["Entra ID", "Graph", ...],
  "count": 1234,
  "d": [
    { "c": "AADSTS50001", "p": "Entra ID", "t": "Authentication", "m": "Short message", "h": true }
  ]
}
```

### products/{product}.json
```json
{
  "product": "Entra ID",
  "count": 200,
  "errors": {
    "AADSTS50001": {
      "message": "...",
      "description": "...",
      "resolution": "...",
      "category": "Authentication",
      "source_file": "..."
    }
  }
}
```
