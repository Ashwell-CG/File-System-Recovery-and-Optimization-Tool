"""
export_pdf.py – Convert the project explanation markdown to a styled HTML file
with Mermaid diagram rendering, ready for browser-based PDF export.
"""

import re
import os
import markdown

INPUT_MD = r"C:\Users\ashwe\.gemini\antigravity\brain\7ce4ac8e-fbed-44be-bb2e-a28ec3c95afe\complete_project_explanation.md"
OUTPUT_HTML = r"c:\Ashwell\Project\OS Project\Complete_Project_Explanation.html"

# Read markdown
with open(INPUT_MD, "r", encoding="utf-8") as f:
    md_text = f.read()

# Extract mermaid blocks before markdown conversion (so they don't get mangled)
mermaid_blocks = []
def replace_mermaid(match):
    idx = len(mermaid_blocks)
    mermaid_blocks.append(match.group(1))
    return f'<div class="mermaid" id="mermaid-{idx}">\n{match.group(1)}\n</div>'

md_text = re.sub(r'```mermaid\s*\n(.*?)```', replace_mermaid, md_text, flags=re.DOTALL)

# Convert markdown to HTML
html_body = markdown.markdown(
    md_text,
    extensions=['tables', 'fenced_code', 'codehilite', 'toc', 'nl2br'],
)

# Fix any leftover mermaid placeholders
# Build full HTML document
html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>File System Simulator – Complete Project Explanation</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>
  mermaid.initialize({{ startOnLoad: true, theme: 'default', securityLevel: 'loose' }});
</script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'Inter', -apple-system, sans-serif;
    line-height: 1.7;
    color: #1a1a2e;
    background: #ffffff;
    padding: 40px 60px;
    max-width: 1000px;
    margin: 0 auto;
    font-size: 14px;
  }}

  h1 {{
    font-size: 28px;
    font-weight: 700;
    color: #0f3460;
    border-bottom: 3px solid #e94560;
    padding-bottom: 12px;
    margin: 40px 0 20px 0;
  }}

  h2 {{
    font-size: 22px;
    font-weight: 700;
    color: #16213e;
    margin: 35px 0 15px 0;
    border-left: 4px solid #e94560;
    padding-left: 12px;
  }}

  h3 {{
    font-size: 17px;
    font-weight: 600;
    color: #0f3460;
    margin: 25px 0 10px 0;
  }}

  h4 {{
    font-size: 15px;
    font-weight: 600;
    color: #533483;
    margin: 20px 0 8px 0;
  }}

  p {{
    margin: 10px 0;
  }}

  blockquote {{
    background: #f0f4ff;
    border-left: 4px solid #0f3460;
    padding: 15px 20px;
    margin: 15px 0;
    border-radius: 0 8px 8px 0;
    font-style: italic;
    color: #333;
  }}

  code {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 12.5px;
    background: #f4f4f8;
    padding: 2px 6px;
    border-radius: 4px;
    color: #e94560;
  }}

  pre {{
    background: #1a1a2e;
    color: #e0e0e0;
    padding: 18px 22px;
    border-radius: 10px;
    overflow-x: auto;
    margin: 15px 0;
    font-size: 12.5px;
    line-height: 1.6;
  }}

  pre code {{
    background: none;
    color: #e0e0e0;
    padding: 0;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 15px 0;
    font-size: 13px;
  }}

  th {{
    background: #0f3460;
    color: white;
    padding: 10px 14px;
    text-align: left;
    font-weight: 600;
  }}

  td {{
    padding: 9px 14px;
    border-bottom: 1px solid #e0e0e0;
  }}

  tr:nth-child(even) {{
    background: #f8f9ff;
  }}

  hr {{
    border: none;
    height: 2px;
    background: linear-gradient(to right, #e94560, #0f3460);
    margin: 30px 0;
  }}

  ul, ol {{
    margin: 10px 0 10px 25px;
  }}

  li {{
    margin: 4px 0;
  }}

  strong {{
    color: #0f3460;
  }}

  .mermaid {{
    background: #fafbff;
    border: 1px solid #e0e4ef;
    border-radius: 10px;
    padding: 20px;
    margin: 20px 0;
    text-align: center;
  }}

  @media print {{
    body {{
      padding: 20px 30px;
      font-size: 12px;
    }}
    pre {{
      font-size: 11px;
      padding: 12px;
    }}
    .mermaid {{
      page-break-inside: avoid;
    }}
    h1, h2, h3 {{
      page-break-after: avoid;
    }}
  }}
</style>
</head>
<body>
{html_body}
</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html_doc)

print(f"HTML exported to: {OUTPUT_HTML}")
print("Open this file in your browser and press Ctrl+P to save as PDF.")
