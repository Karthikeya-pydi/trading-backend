
"""
Updated Financial Data Scraper that properly integrates with Django models
Handles both row expansion (+) buttons and hidden columns properly
UPDATED: Fixed database integration and model compatibility
"""

import time
import re
import os
import csv
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from decimal import Decimal, InvalidOperation


class FinancialDataScraper:
    """Enhanced scraper with proper Django model integration"""
    
    def __init__(self, headless: bool = True, save_csv: bool = True):
        self.driver = self.setup_chrome_driver(headless)
        self.save_csv = save_csv
        self.csv_base_dir = "scraped_data_csv"
        
        if self.save_csv:
            os.makedirs(self.csv_base_dir, exist_ok=True)

    def setup_chrome_driver(self, headless: bool = True) -> webdriver.Chrome:
        """Setup Chrome WebDriver with proper configuration"""
        options = webdriver.ChromeOptions()
        
        if headless:
            options.add_argument("--headless=new")  # Use new headless mode
        
        # Essential options for proper rendering
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        options.page_load_strategy = 'normal'
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
        
        driver.execute_script("window.scrollTo(0, 0);")
        
        return driver
    
    # def setup_chrome_driver(self, headless: bool = True) -> webdriver.Chrome:
    #     """Setup Chrome WebDriver with proper configuration for Ubuntu/Linux"""
    #     options = webdriver.ChromeOptions()
        
    #     # Linux/Ubuntu specific options
    #     if headless:
    #         options.add_argument("--headless=new")
        
    #     # Essential options for proper rendering on Linux
    #     options.add_argument("--no-sandbox")
    #     options.add_argument("--disable-dev-shm-usage")
    #     options.add_argument("--disable-gpu")
    #     options.add_argument("--disable-software-rasterizer")
    #     options.add_argument("--disable-background-timer-throttling")
    #     options.add_argument("--disable-backgrounding-occluded-windows")
    #     options.add_argument("--disable-renderer-backgrounding")
    #     options.add_argument("--disable-features=TranslateUI")
    #     options.add_argument("--disable-ipc-flooding-protection")
    #     options.add_argument("--window-size=1920,1080")
    #     options.add_argument("--start-maximized")
    #     options.add_argument("--disable-blink-features=AutomationControlled")
    #     options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #     options.add_experimental_option('useAutomationExtension', False)
        
    #     # Try to find Chrome binary on different paths
    #     chrome_paths = [
    #         "/usr/bin/google-chrome",
    #         "/usr/bin/google-chrome-stable",
    #         "/usr/bin/chromium-browser",
    #         "/usr/bin/chromium",
    #         "/snap/bin/chromium",
    #         "/opt/google/chrome/chrome"
    #     ]
        
    #     chrome_binary = None
    #     for path in chrome_paths:
    #         if os.path.exists(path):
    #             chrome_binary = path
    #             break
        
    #     if chrome_binary:
    #         options.binary_location = chrome_binary
    #         print(f"[+] Using Chrome binary: {chrome_binary}")
    #     else:
    #         print("[-] Chrome binary not found. Attempting automatic installation...")
    #         # Try to install Chrome automatically
    #         try:
    #             import subprocess
    #             print("[+] Installing Google Chrome...")
    #             subprocess.run([
    #                 "wget", "-q", "-O", "-", "https://dl.google.com/linux/linux_signing_key.pub"
    #             ], check=True, capture_output=True)
    #             subprocess.run([
    #                 "sudo", "apt-get", "update"
    #             ], check=True, capture_output=True)
    #             subprocess.run([
    #                 "sudo", "apt-get", "install", "-y", "google-chrome-stable"
    #             ], check=True, capture_output=True)
    #             options.binary_location = "/usr/bin/google-chrome-stable"
    #             print("[+] Chrome installed successfully")
    #         except Exception as install_error:
    #             print(f"[-] Could not install Chrome automatically: {install_error}")
    #             print("[!] Please install Chrome manually:")
    #             print("    sudo apt update")
    #             print("    sudo apt install -y google-chrome-stable")
    #             print("    # OR")
    #             print("    sudo apt install -y chromium-browser")
    #             raise Exception("Chrome browser not found and could not be installed automatically")
        
    #     options.page_load_strategy = 'normal'
        
    #     try:
    #         # Try to create driver with ChromeDriverManager
    #         driver = webdriver.Chrome(
    #             service=Service(ChromeDriverManager().install()), 
    #             options=options
    #         )
    #     except Exception as e:
    #         print(f"[-] Error with ChromeDriverManager: {e}")
    #         print("[+] Trying with system chromedriver...")
            
    #         # Try with system chromedriver
    #         try:
    #             driver = webdriver.Chrome(options=options)
    #         except Exception as e2:
    #             print(f"[-] Error with system chromedriver: {e2}")
    #             print("[!] Please ensure ChromeDriver is installed:")
    #             print("    sudo apt install -y chromium-chromedriver")
    #             print("    # OR download from: https://chromedriver.chromium.org/")
    #             raise Exception(f"Could not initialize Chrome WebDriver: {e2}")
        
    #     driver.execute_script("window.scrollTo(0, 0);")
    #     print(f"[+] Chrome WebDriver initialized successfully")
    #     return driver
    
    def parse_number_from_text(self, text: str) -> Optional[Decimal]:
        """Parse numeric value from text and return as Decimal for database compatibility"""
        if not text or text.strip() == '-' or text.strip() == '':
            return None
        
        cleaned = re.sub(r'[₹,\s]', '', text.strip())
        
        if '%' in cleaned:
            cleaned = cleaned.replace('%', '')
        
        multiplier = Decimal('1')
        if 'Cr.' in text or 'Cr' in text:
            multiplier = Decimal('1')
            cleaned = re.sub(r'Cr\.?', '', cleaned)
        elif 'Lakh' in text:
            multiplier = Decimal('0.01')
            cleaned = re.sub(r'Lakh', '', cleaned)
        
        if '(' in text and ')' in text:
            cleaned = '-' + cleaned.replace('(', '').replace(')', '')
        
        numeric_match = re.search(r'-?\d+(?:,\d+)*(?:\.\d+)?', cleaned)
        if numeric_match:
            number_str = numeric_match.group().replace(',', '')
            try:
                return Decimal(number_str) * multiplier
            except (ValueError, InvalidOperation):
                return None
        
        return None
    
    def expand_all_row_buttons(self, section_element):
        """Click all '+' buttons to expand row details"""
        try:
            print("[+] Expanding row detail buttons ('+' buttons)...")
            
            buttons_expanded = 0
            plus_buttons = section_element.find_elements(By.CSS_SELECTOR, "button.button-plain")
            
            for button in plus_buttons:
                try:
                    button_text = button.text
                    if '+' in button_text or 'blue-icon' in button.get_attribute('innerHTML'):
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(0.2)
                        
                        try:
                            button.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", button)
                        
                        buttons_expanded += 1
                        time.sleep(0.3)
                        
                except Exception:
                    continue
            
            # Also try XPath for blue-icon spans
            try:
                blue_icons = section_element.find_elements(By.XPATH, ".//span[@class='blue-icon' and text()='+']")
                for icon in blue_icons:
                    try:
                        parent_button = icon.find_element(By.XPATH, "./..")
                        if parent_button.tag_name == 'button':
                            self.driver.execute_script("arguments[0].click();", parent_button)
                            buttons_expanded += 1
                            time.sleep(0.3)
                    except:
                        continue
            except:
                pass
            
            if buttons_expanded > 0:
                print(f"[+] Expanded {buttons_expanded} row detail buttons")
                time.sleep(1)
                
        except Exception as e:
            print(f"[-] Error expanding row buttons: {e}")
    
    def ensure_all_columns_visible(self, section_element):
        """Ensure ALL table columns are visible including hidden ones"""
        try:
            print("[+] Ensuring all columns are visible...")
            
            tables = section_element.find_elements(By.TAG_NAME, "table")
            
            for table in tables:
                self.driver.execute_script("""
                    // Make table show all columns
                    arguments[0].style.width = 'auto';
                    arguments[0].style.maxWidth = 'none';
                    arguments[0].style.overflow = 'visible';
                    
                    // Find parent containers and make them wide
                    var parent = arguments[0].parentElement;
                    while(parent) {
                        if(parent.classList.contains('responsive-holder') || 
                           parent.classList.contains('card') ||
                           parent.style.overflow === 'hidden' ||
                           parent.style.overflow === 'auto') {
                            parent.style.overflow = 'visible';
                            parent.style.width = 'auto';
                            parent.style.maxWidth = 'none';
                        }
                        parent = parent.parentElement;
                        if(parent && parent.tagName === 'BODY') break;
                    }
                    
                    // Show all table cells
                    var cells = arguments[0].getElementsByTagName('td');
                    for(var i = 0; i < cells.length; i++) {
                        cells[i].style.display = '';
                        cells[i].style.visibility = 'visible';
                    }
                    
                    var headers = arguments[0].getElementsByTagName('th');
                    for(var i = 0; i < headers.length; i++) {
                        headers[i].style.display = '';
                        headers[i].style.visibility = 'visible';
                    }
                    
                    // Remove any responsive classes that might hide columns
                    arguments[0].classList.remove('responsive-text-nowrap');
                    arguments[0].parentElement.classList.remove('responsive-holder');
                """, table)
                
                time.sleep(0.5)
                
            # Check for "show more" buttons
            more_buttons = section_element.find_elements(By.XPATH, ".//button[contains(text(), 'more') or contains(text(), 'More') or contains(text(), '>>')]")
            for button in more_buttons:
                try:
                    if button.is_displayed():
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.5)
                except:
                    continue
                    
        except Exception as e:
            print(f"[-] Error ensuring columns visible: {e}")
    
    def parse_table_from_section(self, section_element, section_id: str = "") -> Tuple[List[str], List[List[str]], List[Dict]]:
        """
        Extract ALL headers and rows from table with proper handling for each section type
        Returns: (headers, rows, hierarchy_map)
        hierarchy_map contains parent-child relationships
        """
        headers = []
        rows = []
        hierarchy_map = []  # Track parent-child relationships
        
        try:
            self.ensure_all_columns_visible(section_element)
            
            # For non-peers sections, we need to track hierarchy before expansion
            parent_child_mapping = {}
            if section_id != 'peers':
                # First, identify potential parent rows (those with +/- buttons)
                parent_child_mapping = self.identify_parent_child_structure(section_element)
                
                # Then expand all row details
                self.expand_all_row_buttons(section_element)
            
            time.sleep(2)  # Wait for all expansions to complete
            
            # Get the HTML after all expansions
            section_html = section_element.get_attribute('outerHTML')
            soup = BeautifulSoup(section_html, "html.parser")
            
            # Find the main data table
            table = soup.find('table', class_='data-table')
            if not table:
                table = soup.find('table')
            
            if not table:
                print("[-] No table found in section")
                return headers, rows, hierarchy_map
            
            # Extract headers
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
            else:
                header_row = table.find('tr')
            
            if header_row:
                all_headers = []
                for th in header_row.find_all(['th', 'td']):
                    header_text = th.get_text(strip=True)
                    
                    # Check for additional text in spans
                    span = th.find('span')
                    if span:
                        span_text = span.get_text(strip=True)
                        if span_text and span_text not in header_text:
                            header_text = f"{header_text} {span_text}"
                    
                    all_headers.append(header_text)
                
                # Special handling for peers section - exclude S.No. column
                if section_id == 'peers':
                    if all_headers and ('S.No' in all_headers[0] or 'S No' in all_headers[0] or all_headers[0].strip() == 'S.No.'):
                        print(f"[+] Peers section detected - excluding S.No. column")
                        headers = all_headers[1:]  # Skip first column
                    else:
                        headers = all_headers
                else:
                    headers = all_headers
            
            print(f"[+] Found {len(headers)} columns:")
            print(f"    Headers: {headers}")
            
            # Extract data rows with hierarchy tracking
            tbody = table.find('tbody')
            if tbody:
                all_rows = tbody.find_all('tr')
            else:
                all_rows = table.find_all('tr')[1:]  # Skip header row
            
            current_parent = None
            parent_row_index = None
            processing_children = False
            
            for row_idx, row in enumerate(all_rows):
                cells = row.find_all(['td', 'th'])
                row_data = []
                is_parent_row = False
                is_child_row = False
                
                for cell_idx, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    
                    # Check if this cell contains a button with +/- (parent row indicator)
                    button = cell.find('button')
                    if button and cell_idx == 0:  # Only check first column
                        button_text = button.get_text(strip=True)
                        if '+' in button_text or '-' in button_text:
                            is_parent_row = True
                            cell_text = button_text.replace('+', '').replace('-', '').strip()
                            current_parent = cell_text
                            parent_row_index = len(rows)
                            processing_children = True
                            print(f"[DEBUG] Found parent: {current_parent}")
                    
                    # If no button but we're processing children, check for indentation
                    elif cell_idx == 0 and processing_children and not is_parent_row:
                        # Check various indentation indicators
                        cell_classes = cell.get('class', [])
                        cell_style = cell.get('style', '')
                        
                        # Look for common child row indicators
                        is_likely_child = (
                            # CSS class indicators
                            any('child' in str(cls).lower() or 'indent' in str(cls).lower() 
                                for cls in cell_classes) or
                            # Style indicators
                            'padding-left' in cell_style or
                            'margin-left' in cell_style or
                            # Text indentation
                            cell_text.startswith('  ') or
                            cell_text.startswith('\t') or
                            # Compare with parent name to see if it's a sub-category
                            (current_parent and current_parent.lower() in cell_text.lower() and 
                             cell_text.lower() != current_parent.lower())
                        )
                        
                        # Also check the actual HTML structure for indentation
                        raw_html = str(cell)
                        if ('style=' in raw_html and 'padding-left' in raw_html) or \
                           ('style=' in raw_html and 'margin-left' in raw_html):
                            is_likely_child = True
                        
                        # If this row doesn't look like a child and has substantial content,
                        # it might be the end of children for current parent
                        if not is_likely_child and cell_text and len(cell_text) > 3:
                            # Check if this could be a new parent or a standalone metric
                            next_button = cell.find('button')
                            if not next_button:
                                # This is likely end of children, check if it's a new section
                                processing_children = False
                                current_parent = None
                                parent_row_index = None
                        else:
                            is_child_row = is_likely_child
                    
                    row_data.append(cell_text)
                
                # Special handling for peers section - exclude S.No. column data
                if section_id == 'peers' and row_data and len(headers) == len(row_data) - 1:
                    row_data = row_data[1:]  # Skip first column
                
                # Only add rows with correct column count and meaningful data
                if row_data and len(row_data) == len(headers) and row_data[0].strip():
                    # Track hierarchy
                    if is_parent_row:
                        hierarchy_map.append({
                            'row_index': len(rows),
                            'metric_name': current_parent,
                            'is_parent': True,
                            'is_child': False,
                            'parent_metric': None,
                            'parent_row_index': None,
                            'level': 0
                        })
                        print(f"[DEBUG] Added parent: {current_parent} at row {len(rows)}")
                    elif is_child_row and current_parent:
                        hierarchy_map.append({
                            'row_index': len(rows),
                            'metric_name': row_data[0],
                            'is_parent': False,
                            'is_child': True,
                            'parent_metric': current_parent,
                            'parent_row_index': parent_row_index,
                            'level': 1
                        })
                        print(f"[DEBUG] Added child: {row_data[0]} -> {current_parent}")
                    else:
                        # Regular row (neither parent nor child)
                        hierarchy_map.append({
                            'row_index': len(rows),
                            'metric_name': row_data[0],
                            'is_parent': False,
                            'is_child': False,
                            'parent_metric': None,
                            'parent_row_index': None,
                            'level': 0
                        })
                    
                    rows.append(row_data)
                elif row_data and len(row_data) > 0 and row_data[0].strip():
                    # Pad or truncate row to match headers
                    while len(row_data) < len(headers):
                        row_data.append('')
                    if len(row_data) > len(headers):
                        row_data = row_data[:len(headers)]
                    
                    # Add hierarchy info for padded rows too
                    if is_child_row and current_parent:
                        hierarchy_map.append({
                            'row_index': len(rows),
                            'metric_name': row_data[0],
                            'is_parent': False,
                            'is_child': True,
                            'parent_metric': current_parent,
                            'parent_row_index': parent_row_index,
                            'level': 1
                        })
                    else:
                        hierarchy_map.append({
                            'row_index': len(rows),
                            'metric_name': row_data[0],
                            'is_parent': False,
                            'is_child': False,
                            'parent_metric': None,
                            'parent_row_index': None,
                            'level': 0
                        })
                    
                    rows.append(row_data)
            
            print(f"[+] Extracted {len(rows)} data rows")
            parent_count = len([h for h in hierarchy_map if h['is_parent']])
            child_count = len([h for h in hierarchy_map if h['is_child']])
            print(f"[+] Identified {parent_count} parent rows")
            print(f"[+] Identified {child_count} child rows")
            
            # Debug: Print parent-child relationships
            for h in hierarchy_map:
                if h['is_parent']:
                    print(f"[DEBUG] Parent: {h['metric_name']}")
                elif h['is_child']:
                    print(f"[DEBUG] Child: {h['metric_name']} -> {h['parent_metric']}")
            
            # Verify date columns for non-peers sections
            if section_id != 'peers':
                date_columns = [h for h in headers if any(month in h for month in 
                              ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) or 
                              'TTM' in h or re.search(r'20\d{2}', h)]
                
                if date_columns:
                    print(f"[✓] Date columns captured: {', '.join(date_columns)}")
            
        except Exception as e:
            print(f"[-] Error parsing table: {e}")
            import traceback
            traceback.print_exc()
        
        return headers, rows, hierarchy_map
    
    def identify_parent_child_structure(self, section_element):
        """Identify parent-child structure before expanding rows"""
        try:
            print("[+] Identifying parent-child structure...")
            
            # Find all buttons with '+' - these are parent rows
            plus_buttons = section_element.find_elements(By.CSS_SELECTOR, "button.button-plain")
            parent_info = {}
            
            for button in plus_buttons:
                try:
                    if '+' in button.text:
                        # Find the row this button belongs to
                        parent_row = button.find_element(By.XPATH, "./ancestor::tr[1]")
                        first_cell = parent_row.find_element(By.TAG_NAME, "td")
                        parent_metric = first_cell.text.strip().replace('+', '').strip()
                        
                        # Get row index
                        all_rows = section_element.find_elements(By.CSS_SELECTOR, "tbody tr")
                        for idx, row in enumerate(all_rows):
                            if row == parent_row:
                                parent_info[idx] = parent_metric
                                break
                except:
                    continue
            
            print(f"[+] Found {len(parent_info)} potential parent rows")
            return parent_info
            
        except Exception as e:
            print(f"[-] Error identifying structure: {e}")
            return {}
    
    def scrape_section_data(self, section_id: str) -> Dict:
        """Scrape a section with proper data type handling and hierarchy tracking"""
        section_data = {
            'section_id': section_id,
            'data_types': [],
            'error': None,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            print(f"\n{'='*50}")
            print(f"[+] Processing section: {section_id}")
            print(f"{'='*50}")
            
            wait = WebDriverWait(self.driver, 15)
            section_element = wait.until(
                EC.presence_of_element_located((By.ID, section_id))
            )
            
            # Scroll to section
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", section_element)
            time.sleep(1)
            
            # Special handling for peers section
            if section_id == 'peers':
                print(f"[+] Peers section - no toggle available, scraping single view")
                
                headers, rows, hierarchy_map = self.parse_table_from_section(section_element, section_id)
                
                if headers and rows:
                    peers_data = {
                        'data_type': 'default',
                        'column_headers': headers,
                        'table_data': rows,
                        'hierarchy_map': hierarchy_map,
                        'total_columns': len(headers),
                        'total_rows': len(rows)
                    }
                    section_data['data_types'].append(peers_data)
                    print(f"[✓] Captured peers: {len(rows)} rows × {len(headers)} columns")
                    
                    if self.save_csv:
                        company_slug = self.driver.current_url.split('/company/')[-1].split('/')[0]
                        self.save_to_csv(company_slug, section_id, 'default', headers, rows, hierarchy_map)
                
                return section_data
            
            # Handle other sections with standalone/consolidated toggle
            toggle_link = None
            current_type = "consolidated"  # Default
            
            try:
                links = section_element.find_elements(By.TAG_NAME, "a")
                for link in links:
                    link_text = link.text.strip()
                    if "View Standalone" in link_text:
                        current_type = "consolidated"
                        toggle_link = link
                        break
                    elif "View Consolidated" in link_text:
                        current_type = "standalone"
                        toggle_link = link
                        break
            except:
                pass
            
            print(f"[+] Currently viewing: {current_type}")
            
            # Scrape current view
            headers, rows, hierarchy_map = self.parse_table_from_section(section_element, section_id)
            
            if headers and rows:
                first_data = {
                    'data_type': current_type,
                    'column_headers': headers,
                    'table_data': rows,
                    'hierarchy_map': hierarchy_map,
                    'total_columns': len(headers),
                    'total_rows': len(rows)
                }
                section_data['data_types'].append(first_data)
                print(f"[✓] Captured {current_type}: {len(rows)} rows × {len(headers)} columns")
                print(f"[✓] Hierarchy: {len([h for h in hierarchy_map if h['is_parent']])} parents, {len([h for h in hierarchy_map if h['is_child']])} children")
                
                if self.save_csv:
                    company_slug = self.driver.current_url.split('/company/')[-1].split('/')[0]
                    self.save_to_csv(company_slug, section_id, current_type, headers, rows, hierarchy_map)
            
            # Toggle to get the other view
            if toggle_link:
                try:
                    print(f"[+] Clicking to switch view...")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", toggle_link)
                    time.sleep(0.5)
                    
                    try:
                        toggle_link.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", toggle_link)
                    
                    time.sleep(3)
                    
                    # Get the section again after reload
                    section_element = wait.until(
                        EC.presence_of_element_located((By.ID, section_id))
                    )
                    
                    other_type = "standalone" if current_type == "consolidated" else "consolidated"
                    print(f"[+] Switched to: {other_type}")
                    
                    # Scrape the other view
                    headers2, rows2, hierarchy_map2 = self.parse_table_from_section(section_element, section_id)
                    
                    if headers2 and rows2:
                        second_data = {
                            'data_type': other_type,
                            'column_headers': headers2,
                            'table_data': rows2,
                            'hierarchy_map': hierarchy_map2,
                            'total_columns': len(headers2),
                            'total_rows': len(rows2)
                        }
                        section_data['data_types'].append(second_data)
                        print(f"[✓] Captured {other_type}: {len(rows2)} rows × {len(headers2)} columns")
                        print(f"[✓] Hierarchy: {len([h for h in hierarchy_map2 if h['is_parent']])} parents, {len([h for h in hierarchy_map2 if h['is_child']])} children")
                        
                        if self.save_csv:
                            company_slug = self.driver.current_url.split('/company/')[-1].split('/')[0]
                            self.save_to_csv(company_slug, section_id, other_type, headers2, rows2, hierarchy_map2)
                    
                    print(f"[✓✓] Successfully captured BOTH views for {section_id}")
                    
                except Exception as e:
                    print(f"[-] Error toggling view: {e}")
                    section_data['error'] = f"Toggle error: {str(e)}"
            
        except Exception as e:
            print(f"[-] Error scraping section {section_id}: {e}")
            section_data['error'] = str(e)
        
        return section_data
    
    def save_to_csv(self, company_slug: str, section_id: str, data_type: str, 
                    headers: List[str], rows: List[List[str]], hierarchy_map: List[Dict] = None):
        """Save data to CSV for verification with hierarchy information"""
        if not self.save_csv:
            return
        
        try:
            company_dir = os.path.join(self.csv_base_dir, company_slug)
            os.makedirs(company_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{section_id}_{data_type}_{timestamp}.csv"
            filepath = os.path.join(company_dir, filename)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write metadata
                writer.writerow([f'# Section: {section_id} - {data_type}'])
                writer.writerow([f'# Columns: {len(headers)}'])
                writer.writerow([f'# Rows: {len(rows)}'])
                if section_id == 'peers':
                    writer.writerow([f'# Note: S.No. column excluded, Name is now index column'])
                
                # Write hierarchy information if available
                if hierarchy_map:
                    parent_count = len([h for h in hierarchy_map if h.get('is_parent')])
                    child_count = len([h for h in hierarchy_map if h.get('is_child')])
                    writer.writerow([f'# Hierarchy: {parent_count} parents, {child_count} children'])
                
                writer.writerow([])
                
                # Write headers with hierarchy indicator
                if hierarchy_map:
                    hierarchy_headers = ['Level', 'Type', 'Parent'] + headers
                    writer.writerow(hierarchy_headers)
                    
                    # Write data with hierarchy information
                    for idx, row in enumerate(rows):
                        hierarchy_info = hierarchy_map[idx] if idx < len(hierarchy_map) else {}
                        
                        level = hierarchy_info.get('level', 0)
                        row_type = 'Parent' if hierarchy_info.get('is_parent') else 'Child' if hierarchy_info.get('is_child') else 'Regular'
                        parent_metric = hierarchy_info.get('parent_metric', '')
                        
                        hierarchy_row = [level, row_type, parent_metric] + row
                        writer.writerow(hierarchy_row)
                else:
                    # Write standard headers and data
                    writer.writerow(headers)
                    for row in rows:
                        writer.writerow(row)
            
            print(f"[+] CSV saved: {filepath}")
            
            # Also save hierarchy map as JSON if available
            if hierarchy_map:
                json_filename = f"{section_id}_{data_type}_hierarchy_{timestamp}.json"
                json_filepath = os.path.join(company_dir, json_filename)
                
                with open(json_filepath, 'w', encoding='utf-8') as jsonfile:
                    json.dump(hierarchy_map, jsonfile, indent=2, ensure_ascii=False)
                
                print(f"[+] Hierarchy JSON saved: {json_filepath}")
            
        except Exception as e:
            print(f"[-] Error saving CSV: {e}")
    
    def scrape_company_header_info(self) -> Dict:
        """Scrape company header information from the page"""
        company_info = {
            'name': None,
            'symbol': None,
            'bse_code': None,
            'nse_code': None,
            'website': None,
            'parent_website': None,
            'current_price': None,
            'high_price': None,
            'low_price': None,
            'market_cap': None,
            'book_value': None,
            'dividend_yield': None,
            'face_value': None,
            'stock_pe': None,
            'roe': None,
            'sector': None,
            'industry': None,
            'about_company': None,
            'key_points': None,
            'error': None
        }
        
        try:
            # Get company name
            try:
                name_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
                company_info['name'] = name_element.text.strip()
                print(f"[+] Company name: {company_info['name']}")
            except:
                print("[-] Could not find company name")
            
            # Extract BSE/NSE codes from links
            try:
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href') or ''
                    text = link.text.strip()
                    
                    if 'BSE:' in text:
                        company_info['bse_code'] = text.replace('BSE:', '').strip()
                    elif 'NSE:' in text:
                        company_info['nse_code'] = text.replace('NSE:', '').strip()
                    elif 'www.' in href and not company_info['website']:
                        company_info['website'] = href
                    elif 'http' in href and '.com' in href and not company_info['parent_website']:
                        if company_info['website'] != href:
                            company_info['parent_website'] = href
            except:
                pass
            
            # Extract financial metrics from the ratios section
            try:
                # Look for the ratios/metrics section
                metrics_container = self.driver.find_element(By.CSS_SELECTOR, "[data-v-*], .top-ratios, #top-ratios")
                metrics_items = metrics_container.find_elements(By.TAG_NAME, "li")
                
                for item in metrics_items:
                    try:
                        text = item.text.strip()
                        if not text:
                            continue
                            
                        # Parse different metrics
                        if 'Market Cap' in text:
                            value = re.search(r'₹\s*([\d,\.]+)', text)
                            if value:
                                company_info['market_cap'] = self.parse_number_from_text(value.group())
                        elif 'Current Price' in text:
                            value = re.search(r'₹\s*([\d,\.]+)', text)
                            if value:
                                company_info['current_price'] = self.parse_number_from_text(value.group())
                        elif 'High / Low' in text:
                            values = re.findall(r'₹\s*([\d,\.]+)', text)
                            if len(values) >= 2:
                                company_info['high_price'] = self.parse_number_from_text(values[0])
                                company_info['low_price'] = self.parse_number_from_text(values[1])
                        elif 'Stock P/E' in text:
                            value = re.search(r'([\d\.]+)', text)
                            if value:
                                company_info['stock_pe'] = self.parse_number_from_text(value.group())
                        elif 'Book Value' in text:
                            value = re.search(r'₹\s*([\d,\.]+)', text)
                            if value:
                                company_info['book_value'] = self.parse_number_from_text(value.group())
                        elif 'Dividend Yield' in text:
                            value = re.search(r'([\d\.]+)\s*%', text)
                            if value:
                                company_info['dividend_yield'] = self.parse_number_from_text(value.group(1))
                        elif 'Face Value' in text:
                            value = re.search(r'₹\s*([\d\.]+)', text)
                            if value:
                                company_info['face_value'] = self.parse_number_from_text(value.group(1))
                        
                        elif 'ROE' in text:
                            value = re.search(r'([\d\.]+)\s*%', text)
                            if value:
                                company_info['roe'] = self.parse_number_from_text(value.group(1))
                    except:
                        continue
            except:
                print("[-] Could not extract metrics from page")
            
            # Extract about company and key points from sidebar
            try:
                about_section = self.driver.find_element(By.CSS_SELECTOR, ".about, #about")
                company_info['about_company'] = about_section.text.strip()
            except:
                pass
            
            try:
                key_points_section = self.driver.find_element(By.CSS_SELECTOR, ".key-points, #key-points")
                company_info['key_points'] = key_points_section.text.strip()
            except:
                pass
            
            print(f"[+] Extracted company header info")
            
        except Exception as e:
            company_info['error'] = str(e)
            print(f"[-] Error extracting company info: {e}")
        
        return company_info
    
    def scrape_key_ratios(self) -> Dict:
        """Scrape key financial ratios from the top section"""
        ratios_data = {
            'section_id': 'top-ratios',
            'ratios': [],
            'error': None,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            wait = WebDriverWait(self.driver, 10)
            
            # Try multiple selectors for ratios section
            ratios_section = None
            selectors = ["#top-ratios", ".top-ratios", "[class*='ratio']", "ul li"]
            
            for selector in selectors:
                try:
                    ratios_section = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
            
            if not ratios_section:
                print("[-] Could not find ratios section")
                return ratios_data
            
            # Find all ratio items
            ratio_items = ratios_section.find_elements(By.TAG_NAME, "li")
            
            for item in ratio_items:
                try:
                    # Try different structures for ratio items
                    name_elem = None
                    value_elem = None
                    
                    # Method 1: Look for .name and .value classes
                    try:
                        name_elem = item.find_element(By.CLASS_NAME, "name")
                        value_elem = item.find_element(By.CLASS_NAME, "value")
                    except:
                        pass
                    
                    # Method 2: Parse from text if no specific structure
                    if not name_elem or not value_elem:
                        item_text = item.text.strip()
                        if item_text and ':' in item_text:
                            parts = item_text.split(':', 1)
                            if len(parts) == 2:
                                ratio_name = parts[0].strip()
                                ratio_value = parts[1].strip()
                            else:
                                continue
                        else:
                            continue
                    else:
                        ratio_name = name_elem.text.strip()
                        ratio_value = value_elem.text.strip()
                    
                    if ratio_name and ratio_value:
                        numeric_value = self.parse_number_from_text(ratio_value)
                        unit = None
                        
                        if '%' in ratio_value:
                            unit = '%'
                        elif '₹' in ratio_value:
                            unit = '₹'
                        elif 'times' in ratio_value.lower():
                            unit = 'times'
                        elif 'days' in ratio_value.lower():
                            unit = 'days'
                        elif 'Cr.' in ratio_value or 'Cr' in ratio_value:
                            unit = 'Cr.'
                        
                        ratio_info = {
                            'name': ratio_name,
                            'raw_value': ratio_value,
                            'numeric_value': float(numeric_value) if numeric_value else None,
                            'unit': unit
                        }
                        ratios_data['ratios'].append(ratio_info)
                
                except Exception:
                    continue
            
            print(f"[+] Scraped {len(ratios_data['ratios'])} ratios")
            
        except Exception as e:
            print(f"[-] Error extracting ratios: {e}")
            ratios_data['error'] = str(e)
        
        return ratios_data
    
    def scrape_company_data(self, company_slug: str, sections: List[str] = None) -> Dict:
        """Main function to scrape all data for a company"""
        # if sections is None:
        sections = ['quarters', 'profit-loss', 'balance-sheet', 'ratios', 'cash-flow', 'shareholding', 'peers']
        
        result = {
            'company_slug': company_slug,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'started',
            'company_info': {},
            'key_ratios': {},
            'financial_sections': [],
            'errors': [],
            'summary': {
                'total_sections_attempted': len(sections),
                'total_sections_completed': 0,
                'total_tables_scraped': 0,
                'total_ratios_scraped': 0
            }
        }
        
        try:
            # Navigate to company page
            company_url = f"https://www.screener.in/company/{company_slug}/consolidated"
            print(f"[+] Loading {company_url}")
            
            self.driver.get(company_url)
            
            # Wait for page to fully load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            result['status'] = 'scraping'
            
            # Scrape company header info
            print(f"[+] Scraping company info for {company_slug}")
            result['company_info'] = self.scrape_company_header_info()
            
            # Scrape key ratios
            print(f"[+] Scraping key ratios")
            ratios_data = self.scrape_key_ratios()
            result['key_ratios'] = ratios_data
            
            if ratios_data.get('ratios'):
                result['summary']['total_ratios_scraped'] = len(ratios_data['ratios'])
            
            # Scrape each section
            for section_id in sections:
                try:
                    section_data = self.scrape_section_data(section_id)
                    result['financial_sections'].append(section_data)
                    
                    if section_data.get('data_types'):
                        result['summary']['total_sections_completed'] += 1
                        result['summary']['total_tables_scraped'] += len(section_data['data_types'])
                        print(f"[✓] Section {section_id} completed")
                    
                except Exception as e:
                    error_msg = f"Error in section {section_id}: {str(e)}"
                    print(f"[-] {error_msg}")
                    result['errors'].append(error_msg)
                
                time.sleep(1)
            
            # Set final status
            if result['summary']['total_sections_completed'] > 0:
                result['status'] = 'completed'
            else:
                result['status'] = 'failed'
            
            print(f"\n[✓] Completed scraping {company_slug}")
            print(f"    Sections: {result['summary']['total_sections_completed']}/{result['summary']['total_sections_attempted']}")
            print(f"    Tables: {result['summary']['total_tables_scraped']}")
            
        except Exception as e:
            error_msg = f"Critical error: {str(e)}"
            print(f"[-] {error_msg}")
            result['status'] = 'failed'
            result['errors'].append(error_msg)
        
        return result
    
    def scrape_multiple_companies(self, company_slugs: List[str], sections: List[str] = None) -> Dict:
        """Scrape multiple companies"""
        results = {
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'companies_requested': company_slugs,
            'total_companies': len(company_slugs),
            'companies_data': [],
            'summary': {
                'companies_completed': 0,
                'companies_failed': 0,
                'total_tables_scraped': 0,
                'total_ratios_scraped': 0,
                'total_errors': 0
            }
        }
        
        try:
            for slug in company_slugs:
                print(f"\n{'='*60}")
                print(f"Processing: {slug}")
                print(f"{'='*60}")
                
                company_data = self.scrape_company_data(slug, sections)
                results['companies_data'].append(company_data)
                
                if company_data['status'] == 'completed':
                    results['summary']['companies_completed'] += 1
                else:
                    results['summary']['companies_failed'] += 1
                
                results['summary']['total_tables_scraped'] += company_data['summary']['total_tables_scraped']
                results['summary']['total_ratios_scraped'] += company_data['summary']['total_ratios_scraped']
                results['summary']['total_errors'] += len(company_data['errors'])
                
                if slug != company_slugs[-1]:
                    time.sleep(3)
            
            print(f"\n{'='*60}")
            print(f"SUMMARY")
            print(f"{'='*60}")
            print(f"✅ Completed: {results['summary']['companies_completed']}")
            print(f"❌ Failed: {results['summary']['companies_failed']}")
            print(f"📊 Tables: {results['summary']['total_tables_scraped']}")
            
        except Exception as e:
            print(f"[-] Critical error: {e}")
            results['critical_error'] = str(e)
        finally:
            self.cleanup()
        
        return results
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if self.driver:
                self.driver.quit()
                print("[+] WebDriver closed")
        except Exception as e:
            print(f"[-] Error closing WebDriver: {e}")


# Main function for API
def scrape_companies_to_json(company_slugs: List[str], sections: List[str] = None, save_csv: bool = True) -> Dict:
    """
    Main function to be used by API views
    
    Args:
        company_slugs: List of company identifiers
        sections: List of sections to scrape
        save_csv: Whether to save CSV files
    
    Returns:
        Dict containing all scraped data
    """
    scraper = FinancialDataScraper(headless=True, save_csv=save_csv)
    
    try:
        if len(company_slugs) == 1:
            return scraper.scrape_company_data(company_slugs[0], sections)
        else:
            return scraper.scrape_multiple_companies(company_slugs, sections)
    finally:
        scraper.cleanup()


def save_company_data_to_db(scraped_data: Dict, company_slug: str = None):
    """
    Enhanced function to save scraped company data to Django models
    Properly handles all model relationships and data types
    
    Args:
        scraped_data: The JSON data returned by scraper
        company_slug: Company slug for identification (optional, extracted from data if not provided)
    
    Returns:
        Dict with save results including success status, tables created, metrics created
    """
    try:
        from .models import Company, FinancialDataTable, KeyFinancialMetric
        from django.utils import timezone
        from decimal import Decimal, InvalidOperation
        import json
        
        # Extract company slug from data if not provided
        if not company_slug:
            company_slug = scraped_data.get('company_slug')
        
        print(f"[+] Saving data for {company_slug} to database...")
        
        # Get or create company
        company_info = scraped_data.get('company_info', {})
        
        # Helper function to safely convert to Decimal
        def safe_decimal(value):
            if value is None:
                return None
            try:
                if isinstance(value, (int, float)):
                    return Decimal(str(value))
                elif isinstance(value, str):
                    # Clean the string and convert
                    cleaned = re.sub(r'[₹,\s]', '', value.strip())
                    if cleaned and cleaned != '-':
                        return Decimal(cleaned)
                return None
            except (ValueError, InvalidOperation, TypeError):
                return None
        
        company_defaults = {
            'name': company_info.get('name', ''),
            
            'sector': company_info.get('sector'),
            'industry': company_info.get('industry'),
            
            'about_company': company_info.get('about_company'),
            'key_points': company_info.get('key_points'),
            
            'last_scraped': timezone.now(),
            'is_active': True,
        }
        
        company, created = Company.objects.get_or_create(
            slug=company_slug,
            defaults=company_defaults
        )
        
        # Update existing company with new data
        if not created:
            for field, value in company_defaults.items():
                if hasattr(company, field) and value is not None:
                    setattr(company, field, value)
            company.save()
        
        print(f"[+] {'Created' if created else 'Updated'} company: {company.name or company.slug}")
        
        # Process financial sections
        table_type_mapping = {
            'quarters': 'quarters',
            'peers': 'peers', 
            'profit-loss': 'profit-loss',
            'balance-sheet': 'balance-sheet',
            'ratios': 'ratios',
            'cash-flow': 'cash-flow',
            'shareholding': 'shareholding',
        }
        
        tables_created = 0
        metrics_created = 0
        errors = []
        
        for section in scraped_data.get('financial_sections', []):
            section_id = section.get('section_id')
            table_type = table_type_mapping.get(section_id, section_id)
            
            for data_type_info in section.get('data_types', []):
                data_type = data_type_info.get('data_type', 'default')
                headers = data_type_info.get('column_headers', [])
                table_data = data_type_info.get('table_data', [])
                
                if not headers or not table_data:
                    continue
                
                try:
                    # Delete existing table if it exists (we'll replace it)
                    FinancialDataTable.objects.filter(
                        company=company,
                        table_type=table_type,
                        data_type=data_type
                    ).delete()
                    
                    # Create new table
                    new_table = FinancialDataTable.objects.create(
                        company=company,
                        table_type=table_type,
                        data_type=data_type,
                        column_headers=headers,
                        table_data=table_data,
                        total_columns=len(headers),
                        total_rows=len(table_data),
                        table_title=f"{table_type.replace('-', ' ').title()} - {data_type.title()}",
                        is_active=True
                    )
                    
                    tables_created += 1
                    print(f"[+] Created table: {table_type} ({data_type}) - {len(table_data)} rows")
                    
                    # Get hierarchy map if available
                    hierarchy_map = data_type_info.get('hierarchy_map', [])
                    
                    # Create metrics with parent-child relationships
                    created_metrics = {}  # Map row_index to metric object for parent linking
                    
                    # Extract key financial metrics from this table
                    for row_idx, row_data in enumerate(table_data):
                        if not row_data or len(row_data) == 0:
                            continue
                        
                        metric_name = str(row_data[0]).strip()
                        if not metric_name:
                            continue
                        
                        # Get hierarchy info for this row
                        hierarchy_info = None
                        if row_idx < len(hierarchy_map):
                            hierarchy_info = hierarchy_map[row_idx]
                        
                        # Process each column (time period)
                        for col_idx, cell_value in enumerate(row_data[1:], 1):
                            if col_idx >= len(headers):
                                break
                            
                            period = headers[col_idx] if col_idx < len(headers) else f"Period_{col_idx}"
                            raw_value = str(cell_value).strip()
                            
                            if not raw_value or raw_value == '-':
                                continue
                            
                            # Parse numeric value
                            numeric_value = safe_decimal(raw_value)
                            
                            # Determine period type
                            period_type = 'annual'
                            if 'Q' in period or 'quarter' in period.lower():
                                period_type = 'quarterly'
                            elif 'TTM' in period:
                                period_type = 'ttm'
                            
                            # Determine unit
                            unit = None
                            if '%' in raw_value:
                                unit = '%'
                            elif '₹' in raw_value:
                                unit = '₹'
                            elif 'Cr' in raw_value:
                                unit = 'Cr.'
                            elif 'times' in raw_value.lower():
                                unit = 'times'
                            elif 'days' in raw_value.lower():
                                unit = 'days'
                            
                            # Determine parent relationship
                            parent_metric = None
                            if hierarchy_info and hierarchy_info.get('is_child'):
                                parent_row_index = hierarchy_info.get('parent_row_index')
                                if parent_row_index is not None:
                                    # Find the parent metric for this period
                                    parent_key = f"{parent_row_index}_{col_idx}"
                                    parent_metric = created_metrics.get(parent_key)
                            
                            # Create metric
                            try:
                                metric = KeyFinancialMetric.objects.create(
                                    company=company,
                                    source_table=new_table,
                                    metric_name=metric_name,
                                    period=period,
                                    metric_category=table_type,
                                    period_type=period_type,
                                    raw_value=raw_value,
                                    numeric_value=numeric_value,
                                    unit=unit,
                                    row_index=row_idx,
                                    column_index=col_idx,
                                    parent_relation=parent_metric,  # Set parent relationship
                                )
                                
                                # Store this metric for potential child linking
                                metric_key = f"{row_idx}_{col_idx}"
                                created_metrics[metric_key] = metric
                                
                                metrics_created += 1
                                
                                # Log parent-child relationship
                                if hierarchy_info:
                                    if hierarchy_info.get('is_parent'):
                                        print(f"[+] Created parent metric: {metric_name} ({period})")
                                    elif hierarchy_info.get('is_child'):
                                        parent_name = hierarchy_info.get('parent_metric', 'Unknown')
                                        print(f"[+] Created child metric: {metric_name} -> {parent_name} ({period})")
                                
                            except Exception as metric_error:
                                # Handle duplicate key or other metric creation errors
                                print(f"[!] Metric creation error: {metric_error}")
                                
                except Exception as e:
                    error_msg = f"Error saving table {table_type}-{data_type}: {str(e)}"
                    errors.append(error_msg)
                    print(f"[-] {error_msg}")
        
        # Process key ratios as a special table (no hierarchy needed)
        key_ratios = scraped_data.get('key_ratios', {}).get('ratios', [])
        
        if key_ratios:
            try:
                # Delete existing key ratios table
                # FinancialDataTable.objects.filter(
                #     company=company,
                #     table_type='ratios',
                #     data_type='consolidated'
                # ).delete()
                
                # Create a special table for key ratios
                ratios_headers = ['Metric', 'Value']
                ratios_data = []
                
                for ratio in key_ratios:
                    ratios_data.append([
                        ratio.get('name', ''),
                        ratio.get('raw_value', '')
                    ])
                
                # ratios_table = FinancialDataTable.objects.create(
                #     company=company,
                #     table_type='ratios',
                #     data_type='consolidated',
                #     column_headers=ratios_headers,
                #     table_data=ratios_data,
                #     total_columns=2,
                #     total_rows=len(ratios_data),
                #     table_title='Key Financial Ratios',
                #     is_active=True
                # )

                ratios_table = FinancialDataTable.objects.get(
                company=company,source_table='ratios',data_type='consolidated'
                )
                # Create metrics from key ratios (no parent-child relationships)
                for idx, ratio in enumerate(key_ratios):
                    try:
                        KeyFinancialMetric.objects.create(
                            company=company,
                            source_table=ratios_table,
                            metric_name=ratio.get('name', ''),
                            period='Mar 2025',
                            metric_category='Key Ratios',
                            period_type='annual',
                            raw_value=ratio.get('raw_value', ''),
                            numeric_value=safe_decimal(ratio.get('numeric_value')),
                            unit=ratio.get('unit'),
                            row_index=idx,
                            column_index=1,
                            parent_relation=None,  # Key ratios don't have parent relationships
                        )
                        metrics_created += 1
                    except Exception as e:
                        print(f"[!] Key ratio metric error: {e}")
                        
                print(f"[+] Created key ratios table with {len(key_ratios)} ratios")
                        
            except Exception as e:
                error_msg = f"Error saving key ratios: {str(e)}"
                errors.append(error_msg)
                print(f"[-] {error_msg}")
        
        # Generate hierarchy summary
        total_parent_metrics = 0
        total_child_metrics = 0
        
        try:
            # Count parent and child metrics created
            parent_metrics = KeyFinancialMetric.objects.filter(
                company=company,
                parent_relation__isnull=True
            ).exclude(metric_category='Company Info').count()
            
            child_metrics = KeyFinancialMetric.objects.filter(
                company=company,
                parent_relation__isnull=False
            ).count()
            
            total_parent_metrics = parent_metrics
            total_child_metrics = child_metrics
            
        except Exception as e:
            print(f"[!] Error counting parent/child metrics: {e}")
        
        print(f"[✓] Database save completed:")
        print(f"    - Company: {company.name or company.slug}")
        print(f"    - Tables created: {tables_created}")
        print(f"    - Metrics created: {metrics_created}")
        print(f"    - Parent metrics: {total_parent_metrics}")
        print(f"    - Child metrics: {total_child_metrics}")
        print(f"    - Key ratios processed: {len(key_ratios)}")
        if errors:
            print(f"    - Errors: {len(errors)}")
        
        return {
            'success': True,
            'company_slug': company_slug,
            'company_id': company.id,
            'company_name': company.name or company.slug,
            'tables_created': tables_created,
            'metrics_created': metrics_created,
            'parent_metrics_created': total_parent_metrics,
            'child_metrics_created': total_child_metrics,
            'ratios_processed': len(key_ratios),
            'errors': errors
        }
        
    except Exception as e:
        print(f"[-] Critical error saving to database: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'company_slug': company_slug,
            'company_id': None,
            'company_name': None,
            'tables_created': 0,
            'metrics_created': 0,
            'ratios_processed': 0,
            'errors': [str(e)]
        }


# Helper functions for querying parent-child relationships
def get_metric_hierarchy(company_slug: str, table_type: str = None, data_type: str = None):
    """
    Get parent-child hierarchy for metrics of a company
    
    Args:
        company_slug: Company identifier
        table_type: Filter by table type (optional)
        data_type: Filter by data type (optional)
    
    Returns:
        Dict containing hierarchy information
    """
    try:
        from .models import Company, KeyFinancialMetric
        
        company = Company.objects.get(slug=company_slug)
        
        # Build query filters
        filters = {'company': company}
        if table_type:
            filters['metric_category'] = table_type
        if data_type:
            filters['source_table__data_type'] = data_type
        
        # Get all metrics
        metrics = KeyFinancialMetric.objects.filter(**filters).select_related(
            'parent_relation', 'source_table'
        ).order_by('source_table__table_type', 'row_index', 'column_index')
        
        # Organize into hierarchy
        hierarchy = {
            'company': company.name or company.slug,
            'total_metrics': metrics.count(),
            'parent_metrics': [],
            'child_metrics': [],
            'orphan_metrics': []
        }
        
        for metric in metrics:
            metric_info = {
                'id': metric.id,
                'name': metric.metric_name,
                'period': metric.period,
                'value': metric.raw_value,
                'numeric_value': float(metric.numeric_value) if metric.numeric_value else None,
                'unit': metric.unit,
                'table_type': metric.metric_category,
                'data_type': metric.source_table.data_type,
                'row_index': metric.row_index,
                'column_index': metric.column_index,
            }
            
            if metric.parent_relation:
                # This is a child metric
                metric_info['parent'] = {
                    'id': metric.parent_relation.id,
                    'name': metric.parent_relation.metric_name,
                    'period': metric.parent_relation.period,
                }
                hierarchy['child_metrics'].append(metric_info)
            else:
                # Check if this metric has children
                children = KeyFinancialMetric.objects.filter(parent_relation=metric)
                if children.exists():
                    # This is a parent metric
                    metric_info['children'] = [
                        {
                            'id': child.id,
                            'name': child.metric_name,
                            'period': child.period,
                            'value': child.raw_value,
                        }
                        for child in children
                    ]
                    hierarchy['parent_metrics'].append(metric_info)
                else:
                    # This is an orphan metric (no parent, no children)
                    hierarchy['orphan_metrics'].append(metric_info)
        
        return hierarchy
        
    except Exception as e:
        return {'error': str(e)}


def get_metric_children(metric_id: int):
    """Get all child metrics for a given parent metric"""
    try:
        from .models import KeyFinancialMetric
        
        parent_metric = KeyFinancialMetric.objects.get(id=metric_id)
        children = KeyFinancialMetric.objects.filter(parent_relation=parent_metric)
        
        return {
            'parent': {
                'id': parent_metric.id,
                'name': parent_metric.metric_name,
                'period': parent_metric.period,
                'value': parent_metric.raw_value,
            },
            'children': [
                {
                    'id': child.id,
                    'name': child.metric_name,
                    'period': child.period,
                    'value': child.raw_value,
                    'numeric_value': float(child.numeric_value) if child.numeric_value else None,
                    'unit': child.unit,
                }
                for child in children
            ]
        }
        
    except Exception as e:
        return {'error': str(e)}


def get_metric_parents(company_slug: str, table_type: str = None):
    """Get all parent metrics (metrics that have children) for a company"""
    try:
        from .models import Company, KeyFinancialMetric
        
        company = Company.objects.get(slug=company_slug)
        
        # Get metrics that have children
        filters = {'company': company, 'children__isnull': False}
        if table_type:
            filters['metric_category'] = table_type
        
        parent_metrics = KeyFinancialMetric.objects.filter(**filters).distinct()
        
        result = []
        for parent in parent_metrics:
            children_count = KeyFinancialMetric.objects.filter(parent_relation=parent).count()
            result.append({
                'id': parent.id,
                'name': parent.metric_name,
                'period': parent.period,
                'value': parent.raw_value,
                'table_type': parent.metric_category,
                'children_count': children_count,
            })
        
        return {
            'company': company.name or company.slug,
            'total_parents': len(result),
            'parent_metrics': result
        }
        
    except Exception as e:
        return {'error': str(e)}


if __name__ == "__main__":
    
    try:
        test_companies = ["AXISBANK"]  # Add more company slugs as needed
        result = scrape_companies_to_json(test_companies, save_csv=True)
        
        print("\n[✓] Scraping completed!")
        print(f"Status: {result.get('status')}")
        print(f"Check 'scraped_data_csv/{test_companies[0]}/' for CSV files")
        
        # Test database save if Django is available
        try:
            save_result = save_company_data_to_db(result, test_companies[0])
            print(f"\n[✓] Database save test: {'Success' if save_result['success'] else 'Failed'}")
            if save_result['success']:
                print(f"    Company: {save_result['company_name']}")
                print(f"    Tables: {save_result['tables_created']}")
                print(f"    Metrics: {save_result['metrics_created']}")
        except Exception as e:
            print(f"\n[!] Database test skipped (Django not available): {e}")
            
    except Exception as e:
        print(f"\n[-] Scraper test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure Chrome is properly installed")
        print("2. Check internet connection")
        print("3. Try running with sudo if permission issues")
        print("4. Install additional dependencies: sudo apt install -y xvfb")


# def install_chrome_ubuntu():
#     """Helper function to install Chrome on Ubuntu"""
#     import subprocess
    
#     try:
#         print("[+] Installing Google Chrome on Ubuntu...")
        
#         # Add Google's signing key
#         subprocess.run([
#             "wget", "-q", "-O", "-", 
#             "https://dl.google.com/linux/linux_signing_key.pub"
#         ], check=True)
        
#         # Add Chrome repository
#         subprocess.run([
#             "sudo", "sh", "-c", 
#             "echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' >> /etc/apt/sources.list.d/google-chrome.list"
#         ], check=True)
        
#         # Update package list
#         subprocess.run(["sudo", "apt", "update"], check=True)
        
#         # Install Chrome
#         subprocess.run(["sudo", "apt", "install", "-y", "google-chrome-stable"], check=True)
        
#         print("[✓] Google Chrome installed successfully!")
#         return True
        
#     except subprocess.CalledProcessError as e:
#         print(f"[-] Installation failed: {e}")
#         return False
#     except Exception as e:
#         print(f"[-] Unexpected error during installation: {e}")
#         return False