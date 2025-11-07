"""
Screener.in Query Scraper - Focused on Query Results Table Only

This script:
1. Logs in to screener.in
2. Navigates to Tools > Create a stock screen
3. Executes a financial query
4. Scrapes ONLY the Query Results table
5. Handles pagination correctly
6. Exports data to Excel

Usage:
    python screener_query_scraper.py
"""

import time
import os
import pandas as pd
from datetime import datetime
from typing import Optional, List
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import logging
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('screener_query_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def clear_webdriver_cache():
    """Clear webdriver-manager cache to fix architecture mismatches"""
    import shutil
    cache_dir = os.path.join(os.path.expanduser("~"), ".wdm")
    if os.path.exists(cache_dir):
        try:
            shutil.rmtree(cache_dir)
            logger.info(f"Cleared webdriver-manager cache at: {cache_dir}")
            return True
        except Exception as e:
            logger.warning(f"Could not clear cache: {e}")
            return False
    else:
        logger.info("No cache directory found")
        return True


class ScreenerQueryScraper:
    """Scrape ONLY the Query Results table from screener.in"""
    
    def __init__(self, headless: bool = False, email: Optional[str] = None, password: Optional[str] = None):
        self.driver = self.setup_chrome_driver(headless)
        self.data = None
        self.email = email
        self.password = password
        self.logged_in = False
        self.results_page_url = None
        
    def setup_chrome_driver(self, headless: bool = False) -> webdriver.Chrome:
        """Setup Chrome WebDriver"""
        import platform
        import os
        
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.page_load_strategy = 'normal'
        
        try:
            driver_path = ChromeDriverManager().install()
            if os.path.exists(driver_path):
                if platform.system() == "Windows":
                    if os.path.isdir(driver_path):
                        possible_paths = [
                            os.path.join(driver_path, "chromedriver.exe"),
                            os.path.join(driver_path, "chromedriver-win64", "chromedriver.exe"),
                        ]
                        for path in possible_paths:
                            if os.path.exists(path):
                                driver_path = path
                                break
                    elif not driver_path.endswith('.exe'):
                        driver_dir = os.path.dirname(driver_path)
                        exe_path = os.path.join(driver_dir, "chromedriver.exe")
                        if os.path.exists(exe_path):
                            driver_path = exe_path
                
                service = Service(driver_path)
            else:
                service = Service(ChromeDriverManager().install())
        except:
            service = Service()
        
        try:
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.warning(f"First attempt failed: {e}, clearing cache and retrying...")
            try:
                import shutil
                cache_dir = os.path.join(os.path.expanduser("~"), ".wdm")
                if os.path.exists(cache_dir):
                    shutil.rmtree(cache_dir)
                driver_path = ChromeDriverManager().install()
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=options)
            except Exception as e2:
                raise Exception(f"Could not initialize Chrome WebDriver: {e2}")
        
        return driver
    
    def login(self, email: Optional[str] = None, password: Optional[str] = None):
        """Login to screener.in"""
        try:
            email = email or self.email
            password = password or self.password
            
            if not email or not password:
                raise ValueError("Email and password are required")
            
            logger.info("Navigating to login page...")
            self.driver.get("https://www.screener.in/login/")
            
            # Wait for page to fully load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Check if already logged in
            page_text = self.driver.page_source.lower()
            current_url = self.driver.current_url
            
            if "logout" in page_text or "/logout/" in current_url or "karthikeya" in page_text:
                logger.info("Already logged in!")
                self.logged_in = True
                return True
            
            # Try to find email input with multiple strategies
            email_element = None
            email_selectors = [
                "input[type='email']",
                "input[name='email']",
                "input[name='username']",
                "input[id*='email']",
                "input[id*='username']",
                "input[placeholder*='email' i]",
                "input[placeholder*='Email']",
                "input[autocomplete='email']",
                "input[autocomplete='username']",
                "input[type='text'][name*='email']",
                "input[type='text'][name*='user']"
            ]
            
            logger.info("Looking for email input field...")
            for selector in email_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        if elem.is_displayed():
                            email_element = elem
                            logger.info(f"Found email input with selector: {selector}")
                            break
                    if email_element:
                        break
                except:
                    continue
            
            # If still not found, try XPath
            if not email_element:
                try:
                    email_element = self.driver.find_element(By.XPATH, "//input[contains(@type, 'email') or contains(@name, 'email') or contains(@placeholder, 'email')]")
                    if email_element.is_displayed():
                        logger.info("Found email input using XPath")
                except:
                    pass
            
            if not email_element:
                # Check if there's a Google login button (OAuth)
                try:
                    google_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Google')] | //a[contains(text(), 'Google')] | //button[contains(@class, 'google')]")
                    if google_button.is_displayed():
                        logger.warning("Found Google login button. Email/password login may not be available.")
                        logger.info("Please login manually in the browser...")
                        input("Press Enter after you have logged in manually...")
                        self.logged_in = True
                        return True
                except:
                    pass
                
                # Log page source snippet for debugging
                logger.error("Could not find email input field")
                logger.debug(f"Current URL: {current_url}")
                logger.debug(f"Page title: {self.driver.title}")
                raise Exception("Could not find email input - page structure may have changed")
            
            # Find password input
            logger.info("Looking for password input field...")
            password_element = None
            password_selectors = [
                "input[type='password']",
                "input[name='password']",
                "input[id*='password']",
                "input[autocomplete='password']"
            ]
            
            for selector in password_selectors:
                try:
                    password_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if password_element.is_displayed():
                        logger.info(f"Found password input with selector: {selector}")
                        break
                except:
                    continue
            
            if not password_element:
                raise Exception("Could not find password input")
            
            # Enter credentials
            logger.info("Entering credentials...")
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", email_element)
            time.sleep(0.5)
            email_element.clear()
            email_element.click()
            time.sleep(0.3)
            email_element.send_keys(email)
            time.sleep(0.5)
            
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", password_element)
            time.sleep(0.5)
            password_element.clear()
            password_element.click()
            time.sleep(0.3)
            password_element.send_keys(password)
            time.sleep(1)
            
            # Find and click submit button
            logger.info("Submitting login form...")
            submit_button = None
            submit_selectors = [
                "button[type='submit']",
                "input[type='submit']",
                "button:contains('Login')",
                "button:contains('Sign in')",
                "button.btn-primary",
                "button[class*='submit']"
            ]
            
            for selector in submit_selectors:
                try:
                    if ":contains" in selector:
                        text = selector.split(":contains('")[1].split("')")[0]
                        xpath = f"//button[contains(text(), '{text}')] | //input[contains(@value, '{text}')]"
                        submit_button = self.driver.find_element(By.XPATH, xpath)
                    else:
                        buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for btn in buttons:
                            if btn.is_displayed():
                                btn_text = (btn.text + " " + (btn.get_attribute('value') or '')).lower()
                                if any(kw in btn_text for kw in ['login', 'sign in', 'submit']):
                                    submit_button = btn
                                    break
                    if submit_button:
                        break
                except:
                    continue
            
            if submit_button:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit_button)
                time.sleep(0.5)
                try:
                    submit_button.click()
                except:
                    self.driver.execute_script("arguments[0].click();", submit_button)
            else:
                # Try pressing Enter
                logger.info("No submit button found, pressing Enter...")
                password_element.send_keys(Keys.RETURN)
            
            # Wait for login to complete
            logger.info("Waiting for login to complete...")
            time.sleep(5)
            
            # Check login success
            page_text_after = self.driver.page_source.lower()
            url_after = self.driver.current_url
            
            if ("logout" in page_text_after or 
                "/logout/" in url_after or 
                "karthikeya" in page_text_after or
                url_after != "https://www.screener.in/login/"):
                logger.info("Login successful!")
                self.logged_in = True
                return True
            else:
                # Check for error messages
                try:
                    error_elements = self.driver.find_elements(By.CSS_SELECTOR, ".error, .alert, [class*='error'], [class*='alert']")
                    for err in error_elements:
                        if err.is_displayed():
                            error_text = err.text.strip()
                            if error_text:
                                raise Exception(f"Login failed: {error_text}")
                except Exception as check_err:
                    if "Login failed" in str(check_err):
                        raise check_err
                
                raise Exception("Login failed - unable to verify success. Check credentials.")
                
        except Exception as e:
            logger.error(f"Login failed: {str(e)}")
            # Log more debugging info
            logger.debug(f"Current URL: {self.driver.current_url}")
            logger.debug(f"Page title: {self.driver.title}")
            raise
    
    def navigate_to_query_page(self):
        """Navigate to query builder"""
        if not self.logged_in and self.email and self.password:
            self.login()
        
        logger.info("Navigating to screen builder...")
        self.driver.get("https://www.screener.in/screen/new/")
        time.sleep(3)
    
    def enter_and_execute_query(self, query: str):
        """Enter query and execute it"""
        try:
            logger.info(f"Entering query: {query}")
            
            # Find query textarea
            query_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "textarea"))
            )
            
            query_element.clear()
            query_element.send_keys(query)
            time.sleep(1)
            
            # Execute query
            logger.info("Executing query...")
            try:
                # Try to find Run/Execute button
                run_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Run') or contains(text(), 'Execute')]")
                run_button.click()
            except:
                # Press Enter
                query_element.send_keys(Keys.RETURN)
            
            # Wait for results page
            logger.info("Waiting for query results...")
            time.sleep(5)
            
            # Wait for results table
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table, tbody"))
            )
            
            # Store results page URL
            self.results_page_url = self.driver.current_url
            logger.info(f"Results page URL: {self.results_page_url}")
            
            # Verify we're on query results page
            if "Query Results" not in self.driver.page_source and "results found" not in self.driver.page_source.lower():
                raise Exception("Not on query results page!")
            
            logger.info("Query executed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise
    
    def is_results_page(self) -> bool:
        """Verify we're still on the Query Results page"""
        try:
            current_url = self.driver.current_url
            page_text = self.driver.page_source.lower()
            
            # Check URL
            if '/screen/' in current_url or '/query/' in current_url:
                # Check page content
                if "query results" in page_text or "results found" in page_text:
                    # Check for results table
                    if self.driver.find_elements(By.CSS_SELECTOR, "table"):
                        return True
            return False
        except:
            return False
    
    def scrape_results_table(self):
        """Scrape ONLY the Query Results table - do not navigate away"""
        try:
            # Verify we're on results page
            if not self.is_results_page():
                logger.error("Not on Query Results page! Cannot scrape.")
                return []
            
            logger.info("Scraping Query Results table...")
            time.sleep(2)
            
            # Find the main results table
            # Look for table with "Query Results" context
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table")
            
            results_table = None
            for table in tables:
                if table.is_displayed():
                    table_html = table.get_attribute('outerHTML')
                    # Look for table with company data (has Name column, CMP, etc.)
                    if 'name' in table_html.lower() or 'cmp' in table_html.lower():
                        # Count rows to ensure it's the data table
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        if len(rows) > 2:  # Has header + data rows
                            results_table = table
                            break
            
            if not results_table:
                logger.error("Could not find Query Results table")
                return []
            
            # Extract headers - try multiple strategies
            headers = []
            
            # Strategy 1: Try thead with th elements (most common)
            try:
                thead = results_table.find_element(By.TAG_NAME, "thead")
                header_rows = thead.find_elements(By.TAG_NAME, "tr")
                for header_row in header_rows:
                    header_cells = header_row.find_elements(By.TAG_NAME, "th")
                    if header_cells:
                        for cell in header_cells:
                            header_text = cell.text.strip()
                            # Remove any sorting arrows or icons
                            header_text = header_text.replace('▲', '').replace('▼', '').replace('↑', '').replace('↓', '').strip()
                            if header_text:
                                headers.append(header_text)
                        if headers:
                            logger.info(f"Extracted headers from thead: {headers}")
                            break
            except Exception as e1:
                logger.debug(f"Could not extract from thead: {e1}")
            
            # Strategy 2: If no headers from thead, try first row with th or td
            if not headers:
                try:
                    all_rows = results_table.find_elements(By.TAG_NAME, "tr")
                    if all_rows:
                        first_row = all_rows[0]
                        # Try th elements first
                        header_cells = first_row.find_elements(By.TAG_NAME, "th")
                        if not header_cells:
                            # Fallback to td elements in first row
                            header_cells = first_row.find_elements(By.TAG_NAME, "td")
                        
                        for cell in header_cells:
                            header_text = cell.text.strip()
                            header_text = header_text.replace('▲', '').replace('▼', '').replace('↑', '').replace('↓', '').strip()
                            if header_text:
                                headers.append(header_text)
                        
                        if headers:
                            logger.info(f"Extracted headers from first row: {headers}")
                except Exception as e2:
                    logger.debug(f"Could not extract from first row: {e2}")
            
            # Strategy 3: Try to get headers from column headers in table structure
            if not headers:
                try:
                    # Look for any th elements in the table
                    all_headers = results_table.find_elements(By.TAG_NAME, "th")
                    if all_headers:
                        for header_cell in all_headers:
                            header_text = header_cell.text.strip()
                            header_text = header_text.replace('▲', '').replace('▼', '').replace('↑', '').replace('↓', '').strip()
                            if header_text and header_text not in headers:
                                headers.append(header_text)
                        if headers:
                            logger.info(f"Extracted headers from th elements: {headers}")
                except Exception as e3:
                    logger.debug(f"Could not extract from th elements: {e3}")
            
            # Strategy 4: If still no headers, try to infer from data row structure
            # But first check if we have a tbody to distinguish headers from data
            if not headers:
                logger.warning("Could not extract headers from table structure. Will use column indices.")
            
            # Extract data rows
            rows = []
            try:
                tbody = results_table.find_element(By.TAG_NAME, "tbody")
                data_rows = tbody.find_elements(By.TAG_NAME, "tr")
            except:
                all_rows = results_table.find_elements(By.TAG_NAME, "tr")
                data_rows = all_rows[1:] if len(all_rows) > 1 else []
            
            for row in data_rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    row_data = [cell.text.strip() for cell in cells]
                    
                    # Only add rows with meaningful data (at least 2 non-empty cells)
                    non_empty = [c for c in row_data if c.strip()]
                    if len(non_empty) >= 2:
                        rows.append(row_data)
                except:
                    continue
            
            if not headers and rows:
                max_cols = max(len(row) for row in rows) if rows else 0
                headers = [f"Column_{i+1}" for i in range(max_cols)]
            
            logger.info(f"Scraped {len(rows)} rows with {len(headers)} columns from current page")
            return rows, headers
            
        except Exception as e:
            logger.error(f"Error scraping table: {str(e)}")
            return [], []
    
    def find_next_button(self):
        """Find the Next pagination button - only look at pagination controls"""
        try:
            # Look for pagination section first
            pagination_selectors = [
                "nav[aria-label*='pagination']",
                ".pagination",
                "[class*='pagination']",
                "[class*='pager']"
            ]
            
            pagination_container = None
            for selector in pagination_selectors:
                try:
                    containers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for container in containers:
                        if container.is_displayed():
                            pagination_container = container
                            break
                    if pagination_container:
                        break
                except:
                    continue
            
            # Search for Next button
            next_selectors = [
                "a[aria-label*='Next' i]",
                "button[aria-label*='Next' i]",
                "a:contains('Next')",
                "button:contains('Next')"
            ]
            
            search_area = pagination_container if pagination_container else self.driver
            
            for selector in next_selectors:
                try:
                    if ":contains" in selector:
                        text = selector.split(":contains('")[1].split("')")[0]
                        xpath = f".//button[contains(text(), '{text}')] | .//a[contains(text(), '{text}')]"
                        elements = search_area.find_elements(By.XPATH, xpath)
                    else:
                        elements = search_area.find_elements(By.CSS_SELECTOR, selector)
                    
                    for elem in elements:
                        if elem.is_displayed():
                            elem_text = elem.text.lower()
                            aria_label = (elem.get_attribute('aria-label') or '').lower()
                            if 'next' in elem_text or 'next' in aria_label:
                                if 'prev' not in elem_text and 'prev' not in aria_label:
                                    # Check if disabled
                                    classes = (elem.get_attribute('class') or '').lower()
                                    if 'disabled' not in classes and 'inactive' not in classes:
                                        if elem.is_enabled():
                                            return elem
                except:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error finding next button: {e}")
            return None
    
    def scrape_all_pages(self):
        """Scrape all pages of Query Results - stay on results page only"""
        all_data = []
        headers = []
        page_num = 1
        max_pages = 1000
        consecutive_empty = 0
        
        logger.info("Starting to scrape all pages...")
        
        while page_num <= max_pages:
            # Verify we're still on results page
            if not self.is_results_page():
                logger.warning(f"Not on Query Results page at page {page_num}. Stopping.")
                break
            
            # Scrape current page
            logger.info(f"Scraping page {page_num}...")
            page_data, page_headers = self.scrape_results_table()
            
            # Store headers from first page (important - capture column names!)
            if page_headers and len(page_headers) > 0 and not headers:
                headers = page_headers
                logger.info(f"✓ Captured column headers from page {page_num}: {headers}")
            elif page_headers and len(page_headers) > 0 and headers != page_headers:
                # Headers might have changed - log warning but keep original
                logger.warning(f"Headers on page {page_num} differ from original. Keeping original headers.")
                logger.debug(f"Original: {headers}")
                logger.debug(f"Page {page_num}: {page_headers}")
            
            if page_data and len(page_data) > 0:
                all_data.extend(page_data)
                consecutive_empty = 0
                logger.info(f"Page {page_num}: {len(page_data)} rows (Total: {len(all_data)} rows)")
            else:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.warning("Got empty pages - stopping")
                    break
                logger.warning(f"Page {page_num} had no data")
            
            # Try to find Next button
            next_button = self.find_next_button()
            
            if not next_button:
                logger.info("No Next button found - reached last page")
                break
            
            # Click Next button
            try:
                logger.info(f"Clicking Next to go to page {page_num + 1}...")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(0.5)
                
                before_url = self.driver.current_url
                next_button.click()
                
                # Wait for page to change
                wait_count = 0
                while wait_count < 15:
                    if self.driver.current_url != before_url:
                        break
                    time.sleep(0.5)
                    wait_count += 1
                
                # Wait for table to reload
                time.sleep(3)
                
                # Verify we're still on results page
                if not self.is_results_page():
                    logger.warning("Clicked Next but not on results page anymore. Stopping.")
                    break
                
                page_num += 1
                
            except Exception as e:
                logger.error(f"Error clicking Next button: {e}")
                break
        
        logger.info(f"Finished scraping {page_num} pages. Total rows: {len(all_data)}")
        if headers:
            logger.info(f"✓ Final column headers: {headers}")
        else:
            logger.warning("⚠ No headers were captured - will use generic column names")
        return all_data, headers
    
    def scrape_query_results(self, query: str) -> pd.DataFrame:
        """Complete flow: login, execute query, scrape all results"""
        try:
            # Navigate and login
            self.navigate_to_query_page()
            
            # Enter and execute query
            self.enter_and_execute_query(query)
            
            # Scrape all pages
            all_rows, headers = self.scrape_all_pages()
            
            if not all_rows:
                logger.warning("No data scraped")
                return pd.DataFrame()
            
            # Fallback: use column count from data if headers not found
            if not headers or len(headers) == 0:
                max_cols = max(len(row) for row in all_rows if row) if all_rows else 0
                headers = [f"Column_{i+1}" for i in range(max_cols)]
                logger.warning(f"⚠ No headers found - using generic column names: {headers}")
            else:
                logger.info(f"✓ Using captured headers ({len(headers)} columns): {headers[:5]}{'...' if len(headers) > 5 else ''}")
            
            # Ensure all rows have same length as headers
            max_cols = len(headers) if headers else (max(len(row) for row in all_rows if row) if all_rows else 0)
            
            # Normalize row lengths
            normalized_rows = []
            for row in all_rows:
                while len(row) < max_cols:
                    row.append('')
                normalized_rows.append(row[:max_cols])
            
            # Create DataFrame with proper column names
            final_headers = headers[:max_cols] if len(headers) >= max_cols else headers + [f"Column_{i+1}" for i in range(len(headers), max_cols)]
            self.data = pd.DataFrame(normalized_rows, columns=final_headers)
            logger.info(f"✓ Created DataFrame: {self.data.shape[0]} rows × {self.data.shape[1]} columns")
            logger.info(f"✓ Column names: {list(self.data.columns)}")
            
            return self.data
            
        except Exception as e:
            logger.error(f"Error in scrape_query_results: {str(e)}")
            raise
    
    def save_to_excel(self, filename: Optional[str] = None) -> str:
        """Save scraped data to Excel"""
        if self.data is None or self.data.empty:
            raise ValueError("No data to save")
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"screener_query_results_{timestamp}.xlsx"
        
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            script_dir = os.getcwd()
        
        output_dir = os.path.join(script_dir, "screener_output")
        os.makedirs(output_dir, exist_ok=True)
        
        filepath = os.path.join(output_dir, filename)
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                self.data.to_excel(writer, sheet_name='Query Results', index=False)
                worksheet = writer.sheets['Query Results']
                for idx, col in enumerate(self.data.columns):
                    max_length = max(
                        self.data[col].astype(str).str.len().max() if len(self.data) > 0 else 0,
                        len(str(col))
                    )
                    col_letter = chr(65 + (idx % 26))
                    worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)
            
            abs_filepath = os.path.abspath(filepath)
            logger.info(f"Data saved to: {abs_filepath}")
            return abs_filepath
            
        except ImportError:
            csv_filepath = filepath.replace('.xlsx', '.csv')
            self.data.to_csv(csv_filepath, index=False)
            abs_csv_filepath = os.path.abspath(csv_filepath)
            logger.info(f"Data saved to: {abs_csv_filepath}")
            return abs_csv_filepath
        except Exception as e:
            logger.error(f"Error saving to Excel: {str(e)}")
            raise
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")
        except Exception as e:
            logger.warning(f"Error closing WebDriver: {e}")


def main():
    """Main function"""
    scraper = None
    try:
        email = "pydikarthikeya77@gmail.com"
        password = "Chinnu@123"
        query = "Market Capitalization > 0"
        
        import sys
        if len(sys.argv) > 1:
            query = " ".join(sys.argv[1:])
        
        print(f"\n{'='*60}")
        print("SCREENER.IN QUERY SCRAPER")
        print(f"{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}\n")
        
        try:
            scraper = ScreenerQueryScraper(headless=False, email=email, password=password)
        except Exception as init_error:
            error_msg = str(init_error).lower()
            if "win32" in error_msg or "not a valid" in error_msg or "193" in error_msg:
                logger.warning("Clearing cache...")
                clear_webdriver_cache()
                scraper = ScreenerQueryScraper(headless=False, email=email, password=password)
            else:
                raise
        
        df = scraper.scrape_query_results(query)
        
        if df is not None and not df.empty:
            print(f"\n✓ Scraped {len(df)} rows with {len(df.columns)} columns")
            print(f"\n📊 Column Names:")
            for i, col in enumerate(df.columns, 1):
                print(f"   {i}. {col}")
            
            filepath = scraper.save_to_excel()
            print(f"\n✓ Data saved to: {filepath}")
            print(f"\n✓ Excel file includes all column headers: {list(df.columns)}")
        else:
            print("\n✗ No data scraped")
        
    except Exception as e:
        logger.error(f"Scraper failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.cleanup()


if __name__ == "__main__":
    main()

