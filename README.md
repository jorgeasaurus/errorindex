# Microsoft Error Index

Searchable reference for error codes across Microsoft products — Entra ID, Graph, Intune, SCCM, Exchange, Windows Installer, Windows Update. Static site with no backend, no framework, no build step.

🔗 **Live site:** [jorgeasaurus.github.io/errorindex](https://jorgeasaurus.github.io/errorindex)

Deployed to GitHub Pages. Data updated daily via GitHub Actions.

## Quick Start

```bash
python3 -m http.server 8000 -d public
# http://localhost:8000
```

## Data Pipeline

A GitHub Actions workflow runs daily: clones Microsoft documentation repos (sparse checkout), parses markdown for error codes, commits changes, and deploys to Pages.

To regenerate locally:

```bash
python3 scripts/scrape_errors.py
```

### Data Files

| File | Purpose |
|------|---------|
| `public/data/manifest.json` | Compact error index. Loaded on init for search/filter |
| `public/data/products/*.json` | Per-product detail (descriptions, resolutions). Lazy-loaded on card expand |
| `public/data/descriptions.json` | Deferred descriptions |

The scraper uses Python stdlib only — no pip dependencies.

## Products Covered

| Product | Source Repo | Error Types |
|---------|-------------|-------------|
| Entra ID | MicrosoftDocs/entra-docs, SupportArticles-docs | AADSTS codes, Conditional Access |
| Microsoft Graph | microsoftgraph/microsoft-graph-docs-contrib | API errors, HTTP status codes |
| Intune | MicrosoftDocs/memdocs, SupportArticles-docs | Enrollment, compliance, app deployment |
| SCCM | MicrosoftDocs/memdocs, SupportArticles-docs | Client, OSD, software updates |
| Exchange | MicrosoftDocs/SupportArticles-docs | NDRs, mail flow, connectivity |
| Windows Installer | MicrosoftDocs/win32 | MSI return codes (0, 1603, 3010, etc.) |
| Windows Update | MicrosoftDocs/windows-itpro-docs | WU_E_* hex error codes |

## Key Features

- **Fuzzy search** with weighted scoring across code, message, category
- **Filters** — product, category, has-resolution
- **Two-tier loading** — manifest on init, product detail on demand
- **Keyboard navigation** — `/` search, `j`/`k` navigate, `Enter` expand
- **URL hash state** — shareable filtered views
- **Export** — JSON or CSV of filtered results
- **Dark/light mode** with system preference detection

## Project Structure

```
public/
  index.html                  Single-file frontend
  data/
    manifest.json             Error index (~compact)
    descriptions.json         Deferred descriptions
    products/                 Per-product detail JSON
scripts/
  scrape_errors.py            Python scraper (stdlib only)
.github/workflows/
  update-error-data.yml       Daily automation + GitHub Pages deploy
```
