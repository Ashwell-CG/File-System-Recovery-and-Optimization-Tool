import asyncio
from playwright.async_api import async_playwright
import os

HTML_PATH = os.path.abspath(r"C:\Ashwell\Project\OS Project\c_fs\C_Complete_Explanation.html")
PDF_PATH = os.path.abspath(r"C:\Ashwell\Project\OS Project\c_fs\C_Complete_Explanation.pdf")

async def generate_pdf():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        # Load the HTML file
        await page.goto(f"file://{HTML_PATH}")
        
        # Wait for mermaid rendering to finish: wait for any svg to appear inside the mermaid div and wait a bit more for stability
        print("Waiting for diagrams to render...")
        await page.wait_for_selector('div.mermaid svg')
        await page.wait_for_timeout(2000)
        
        # Save to PDF
        print(f"Saving PDF to {PDF_PATH}...")
        await page.pdf(
            path=PDF_PATH,
            format="A4",
            print_background=True,
            margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"}
        )
        
        await browser.close()
        print("Done!")

if __name__ == "__main__":
    asyncio.run(generate_pdf())
