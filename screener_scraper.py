import os
import time
from pathlib import Path

from bs4 import BeautifulSoup
from bs4.element import NavigableString
from PyPDF2 import PdfMerger
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from weasyprint import CSS, HTML
from webdriver_manager.chrome import ChromeDriverManager


def expand_all_buttons(driver, parent_id: str, btn_clsname: str):
    wait = WebDriverWait(driver, 10)
    try:
        parent_section = wait.until(EC.presence_of_element_located((By.ID, parent_id)))
        buttons = parent_section.find_elements(By.CLASS_NAME, btn_clsname)

        for button in buttons:
            try:
                driver.execute_script("arguments[0].click();", button)
                time.sleep(0.3)
            except Exception as click_err:
                print(f"Failed to click button: {click_err}")
    except Exception as e:
        print(f"Error loading section {parent_id}: {e}")


def save_table(driver, parent_id: str, filepath: Path):
    if filepath.exists():
        print(f"[✓] Skipping {filepath.name}, already exists.")
        return

    try:
        parent_element = driver.find_element(By.ID, parent_id)
        table_html = parent_element.find_element(By.TAG_NAME, 'table').get_attribute('outerHTML')

        soup = BeautifulSoup(table_html, "html.parser")
        for button in soup.find_all("button"):
            text = button.get_text(strip=True)
            button.replace_with(NavigableString(text))

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(str(soup))
        print(f"[+] Saved {filepath.name}")
    except Exception as e:
        print(f"[-] Failed to extract/save table for {parent_id}: {e}")


def export_html_to_pdf(input_html_path: Path, output_path: Path):
    HTML(input_html_path.as_posix()).write_pdf(
        output_path.as_posix(),
        stylesheets=[
            CSS(string='''
                @page {
                    size: A4 landscape;
                    margin: 1cm;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 10pt;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 4px;
                    text-align: left;
                }
            ''')
        ]
    )
    print(f"[+] PDF generated: {output_path.name}")

def merge_pdfs_by_company(root_dir : Path):
    """
    For each subdirectory in `scraped_tables`, merge all PDFs into a single <company>.pdf
    """
    for company_dir in Path(root_dir).iterdir():
        if company_dir.is_dir():
            merger = PdfMerger()
            pdf_files = sorted(company_dir.glob("*.pdf"))

            if not pdf_files:
                print(f"No PDFs to merge for {company_dir.name}")
                continue

            for pdf_file in pdf_files:
                merger.append(str(pdf_file))

            output_path = company_dir / f"{company_dir.name}.pdf"
            merger.write(str(output_path))
            merger.close()

            print(f"Merged {len(pdf_files)} PDFs into {output_path}")


if __name__ == "__main__":
    elements = ['quarters', 'peers', 'profit-loss', 'balance-sheet', 'ratios', 'cash-flow', 'shareholding']
    company_slugs = ["ULTRACEMCO", "AMBUJACEM", "ACC", "SHREECEM"]  # Add more slugs here

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        root_dir = Path("scraped_tables")
        for slug in company_slugs:
            print(f"\n=== Processing {slug} ===")
            company_url = f"https://www.screener.in/company/{slug}/"
            driver.get(company_url)

            company_dir = root_dir / slug
            company_dir.mkdir(parents=True, exist_ok=True)

            for section_id in elements:
                html_path = company_dir / f"{slug}_{section_id}.html"
                pdf_path = company_dir / f"{slug}_{section_id}.pdf"

                if not html_path.exists():
                    expand_all_buttons(driver, section_id, 'button-plain')

                save_table(driver, section_id, html_path)
                export_html_to_pdf(html_path, pdf_path)

        merge_pdfs_by_company(root_dir=root_dir)
    finally:
        driver.quit() 