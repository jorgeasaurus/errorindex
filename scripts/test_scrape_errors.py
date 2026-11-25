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
