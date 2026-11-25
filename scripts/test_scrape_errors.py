#!/usr/bin/env python3
"""Offline checks for Learn HTML fallback parsing."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import scrape_errors


# Learn renders the title and the article as two separate div.content nodes.
# 0x00cf0005 lives in the second one.
_LEARN_FIXTURE = """
<div class="content"><h1>Software update agent error codes</h1></div>
<div class="content">
<table>
<thead>
<tr><th>Error code</th><th>Symbolic name</th><th>More information</th></tr>
</thead>
<tbody>
<tr>
<td><strong><code>0x00cf0005</code></strong></td>
<td><code>OM_S_REBOOT_REQUIRED</code></td>
<td>The system must be restarted to complete the update installation.</td>
</tr>
</tbody>
</table>
</div>
"""


class LearnHtmlFallbackTest(unittest.TestCase):
    def test_renamed_agent_page_keeps_symbolic_message(self):
        markdown = scrape_errors.html_content_to_markdown(_LEARN_FIXTURE)
        self.assertIn("0x00cf0005", markdown)
        self.assertIn("OM_S_REBOOT_REQUIRED", markdown)

        with tempfile.TemporaryDirectory() as tmp:
            page = Path(tmp) / "intune/device-updates/windows/ref-agent-error-codes.md"
            page.parent.mkdir(parents=True)
            page.write_text(markdown, encoding="utf-8")
            errors = scrape_errors.parse_intune(Path(tmp))

        match = [e for e in errors if e["code"].lower() == "0x00cf0005"]
        self.assertEqual(len(match), 1)
        self.assertEqual(
            match[0]["message"],
            "OM_S_REBOOT_REQUIRED \u2014 The system must be restarted to complete the update installation.",
        )
        self.assertEqual(match[0]["description"], "")

    def test_scrape_keeps_previous_on_empty_or_sharp_drop(self):
        keep = scrape_errors.scrape_should_keep_previous
        self.assertTrue(keep(0, 516))
        self.assertTrue(keep(257, 516))  # more than 50% below
        self.assertFalse(keep(258, 516))  # exactly half remains
        self.assertFalse(keep(400, 516))
        self.assertFalse(keep(0, None))
        self.assertFalse(keep(0, 0))
        self.assertFalse(keep(10, 10))
        self.assertEqual(
            scrape_errors.previous_committed_count({"count": 516, "errors": {"a": {}, "b": {}}}),
            516,
        )

    def test_patient_backoff_outlasts_the_short_retry(self):
        self.assertEqual(scrape_errors.fetch_backoff_seconds(0, patient=False), 2)
        self.assertEqual(scrape_errors.fetch_backoff_seconds(2, patient=False), 6)
        self.assertGreaterEqual(scrape_errors.fetch_backoff_seconds(0, patient=True), 8)
        self.assertGreater(
            scrape_errors.fetch_backoff_seconds(2, patient=True),
            scrape_errors.fetch_backoff_seconds(2, patient=False),
        )

    def test_kept_product_reuses_committed_manifest_rows(self):
        manifest = {
            "d": [
                {"c": "0x1", "p": "Windows Update", "t": "General", "m": "one", "h": False},
                {"c": "0x1", "p": "Windows Update", "t": "Other", "m": "two", "h": False},
                {"c": "AADSTS1", "p": "Entra ID", "t": "Authentication", "m": "aad", "h": False},
            ]
        }
        rows = scrape_errors.manifest_rows_for_product(manifest, "Windows Update")
        self.assertEqual(len(rows), 2)
        self.assertTrue(scrape_errors.scrape_should_keep_previous(0, len(rows)))

    def test_non_json_body_is_not_pinned_in_the_fetch_cache(self):
        url = "https://learn.microsoft.com/en-us/intune/configmgr/core/toc.json"
        scrape_errors._FETCH_BODY_CACHE[url] = "<html>not json</html>"
        try:
            self.assertIsNone(scrape_errors._fetch_json(url))
            self.assertNotIn(url, scrape_errors._FETCH_BODY_CACHE)
        finally:
            scrape_errors._FETCH_BODY_CACHE.pop(url, None)

    def test_fetch_retries_keep_the_accepted_user_agent(self):
        agents = [scrape_errors._fetch_headers(attempt)["User-Agent"] for attempt in range(4)]
        self.assertEqual(len(set(agents)), 1)
        self.assertNotIn("ErrorIndex-Scraper", agents[0])

    def test_collapse_keeps_richer_text_for_shared_code(self):
        collapsed = scrape_errors.collapse_by_code([
            {
                "code": "0x80cf0440",
                "category": "General",
                "message": "OM_E_PT_ENDPOINT_DISCONNECTED \u2014 The connection to the service endpoint terminated.",
                "description": "",
                "resolution": "",
            },
            {
                "code": "0x80cf0440",
                "category": "Enrollment",
                "message": "",
                "description": "",
                "resolution": "Trial or paid account is suspended.",
            },
        ])
        self.assertEqual(len(collapsed), 1)
        self.assertIn("OM_E_PT_ENDPOINT_DISCONNECTED", collapsed[0]["message"])


if __name__ == "__main__":
    unittest.main()
