import os
import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from bs4.element import NavigableString
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from app.models.stock_screening import StockScreening
from app.core.database import SessionLocal

class StockScreeningService:
    def __init__(self):
        self.base_url = "https://www.screener.in/company"
        self.data_sections = [
            'quarters', 'peers', 'profit-loss', 'balance-sheet', 
            'ratios', 'cash-flow', 'shareholding', 'overview',
            'technical', 'valuation', 'growth', 'industry'
        ]
        
    def _setup_driver(self):
        """Setup Chrome driver with essential options"""
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            
            driver = webdriver.Chrome(options=options)
            return driver
                
        except Exception as e:
            print(f"Failed to setup Chrome driver: {e}")
            return None
    
    def _expand_all_buttons(self, driver, parent_id: str, btn_clsname: str):
        """Expand all collapsible sections"""
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
    
    def _extract_table_data(self, driver, section_id: str) -> Dict[str, Any]:
        """Extract table data and convert to structured format"""
        try:
            parent_element = driver.find_element(By.ID, section_id)
            table_html = parent_element.find_element(By.TAG_NAME, 'table').get_attribute('outerHTML')
            
            soup = BeautifulSoup(table_html, "html.parser")
            
            # Remove buttons and clean up
            for button in soup.find_all("button"):
                text = button.get_text(strip=True)
                button.replace_with(NavigableString(text))
            
            # Extract table headers
            headers = []
            header_row = soup.find('thead').find('tr') if soup.find('thead') else soup.find('tr')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            
            # Extract table rows
            rows = []
            tbody = soup.find('tbody') if soup.find('tbody') else soup
            for row in tbody.find_all('tr'):
                if row.find('th'):  # Skip header rows
                    continue
                row_data = [td.get_text(strip=True) for td in row.find_all('td')]
                if row_data:
                    rows.append(row_data)
            
            return {
                "headers": headers,
                "rows": rows,
                "raw_html": str(soup)
            }
            
        except Exception as e:
            print(f"[-] Failed to extract table for {section_id}: {e}")
            return {"error": str(e)}
    
    def _extract_overview_data(self, driver, stock_symbol: str) -> Dict[str, Any]:
        """Extract company overview and key metrics"""
        try:
            overview_data = {}
            
            # Try multiple selectors for company name
            company_name_selectors = [
                "h1.company-name",
                ".company-name", 
                "h1",
                ".company-header h1",
                ".stock-name",
                ".company-title"
            ]
            
            company_name = None
            for selector in company_name_selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, selector)
                    company_name = elem.get_text(strip=True)
                    if company_name and company_name != "":
                        break
                except:
                    continue
            
            # If no company name found, use stock symbol as fallback
            if not company_name or company_name == "":
                company_name = stock_symbol
            
            overview_data["company_name"] = company_name
            
            # Try to extract page title as backup
            try:
                page_title = driver.title
                if page_title and "screener.in" in page_title:
                    title_parts = page_title.split(" - ")
                    if len(title_parts) > 0:
                        title_company = title_parts[0].strip()
                        if title_company and title_company != "":
                            overview_data["company_name"] = title_company
            except:
                pass
            
            # Key metrics
            try:
                metrics = driver.find_elements(By.CSS_SELECTOR, ".key-metrics .metric, .company-info .metric, .metric")
                for metric in metrics:
                    try:
                        label_elem = metric.find_element(By.CSS_SELECTOR, ".label, .name, .metric-label")
                        value_elem = metric.find_element(By.CSS_SELECTOR, ".value, .data, .metric-value")
                        label = label_elem.get_text(strip=True)
                        value = value_elem.get_text(strip=True)
                        if label and value:
                            overview_data[label] = value
                    except:
                        continue
            except:
                pass
            
            return overview_data
            
        except Exception as e:
            print(f"[-] Failed to extract overview data: {e}")
            return {"error": str(e)}
    
    def scrape_stock_data(self, stock_symbol: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        """Main method to scrape all available data for a stock"""
        driver = None
        scraped_data = {}
        
        try:
            driver = self._setup_driver()
            
            if driver is None:
                return {"error": "Failed to initialize Chrome WebDriver. Please check Chrome installation."}
            
            company_url = f"{self.base_url}/{stock_symbol}/"
            driver.get(company_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Check if page loaded successfully
            page_title = driver.title
            if "404" in page_title or "not found" in page_title.lower():
                return {"error": f"Stock {stock_symbol} not found on screener.in"}
            
            # Extract overview data first
            scraped_data["overview"] = self._extract_overview_data(driver, stock_symbol)
            
            # Extract data from all sections
            for section_id in self.data_sections:
                if section_id == "overview":
                    continue  # Already handled
                    
                # Expand buttons for this section
                self._expand_all_buttons(driver, section_id, 'button-plain')
                time.sleep(1)
                
                # Extract table data
                section_data = self._extract_table_data(driver, section_id)
                scraped_data[section_id.replace('-', '_')] = section_data
                
                time.sleep(0.5)  # Small delay between sections
            
            return scraped_data
            
        except Exception as e:
            print(f"[-] Error scraping {stock_symbol}: {e}")
            return {"error": str(e)}
            
        finally:
            if driver:
                driver.quit()
    
    def save_to_database(self, stock_symbol: str, scraped_data: Dict[str, Any], 
                        stock_name: Optional[str] = None) -> StockScreening:
        """Save scraped data to database"""
        db = SessionLocal()
        try:
            # Check if stock already exists
            existing_stock = db.query(StockScreening).filter(
                StockScreening.stock_symbol == stock_symbol
            ).first()
            
            if existing_stock:
                # Update existing record
                existing_stock.quarters_data = scraped_data.get('quarters')
                existing_stock.peers_data = scraped_data.get('peers')
                existing_stock.profit_loss_data = scraped_data.get('profit_loss')
                existing_stock.balance_sheet_data = scraped_data.get('balance_sheet')
                existing_stock.ratios_data = scraped_data.get('ratios')
                existing_stock.cash_flow_data = scraped_data.get('cash_flow')
                existing_stock.shareholding_data = scraped_data.get('shareholding')
                existing_stock.overview_data = scraped_data.get('overview')
                existing_stock.technical_data = scraped_data.get('technical')
                existing_stock.valuation_data = scraped_data.get('valuation')
                existing_stock.growth_data = scraped_data.get('growth')
                existing_stock.industry_data = scraped_data.get('industry')
                existing_stock.last_scraped_at = datetime.now(timezone.utc)
                existing_stock.scraping_status = "success"
                existing_stock.error_message = None
                
                # Update stock name if provided or if we have better data from overview
                if stock_name:
                    existing_stock.stock_name = stock_name
                elif scraped_data.get('overview', {}).get('company_name'):
                    existing_stock.stock_name = scraped_data['overview']['company_name']
                
                db.commit()
                return existing_stock
            else:
                # Create new record
                # Get stock name from overview data if not provided
                final_stock_name = stock_name
                if not final_stock_name and scraped_data.get('overview', {}).get('company_name'):
                    final_stock_name = scraped_data['overview']['company_name']
                elif not final_stock_name:
                    final_stock_name = stock_symbol  # Fallback to stock symbol
                
                new_stock = StockScreening(
                    stock_symbol=stock_symbol,
                    stock_name=final_stock_name,
                    company_url=f"{self.base_url}/{stock_symbol}/",
                    quarters_data=scraped_data.get('quarters'),
                    peers_data=scraped_data.get('peers'),
                    profit_loss_data=scraped_data.get('profit_loss'),
                    balance_sheet_data=scraped_data.get('balance_sheet'),
                    ratios_data=scraped_data.get('ratios'),
                    cash_flow_data=scraped_data.get('cash_flow'),
                    shareholding_data=scraped_data.get('shareholding'),
                    overview_data=scraped_data.get('overview'),
                    technical_data=scraped_data.get('technical'),
                    valuation_data=scraped_data.get('valuation'),
                    growth_data=scraped_data.get('growth'),
                    industry_data=scraped_data.get('industry'),
                    last_scraped_at=datetime.now(timezone.utc),
                    scraping_status="success"
                )
                
                db.add(new_stock)
                db.commit()
                db.refresh(new_stock)
                return new_stock
                
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()
    
    def get_stock_data(self, stock_symbol: str) -> Optional[StockScreening]:
        """Get stock data from database"""
        db = SessionLocal()
        try:
            return db.query(StockScreening).filter(
                StockScreening.stock_symbol == stock_symbol
            ).first()
        finally:
            db.close()
    
    def get_all_stocks(self, skip: int = 0, limit: int = 100) -> List[StockScreening]:
        """Get all stocks with pagination"""
        db = SessionLocal()
        try:
            return db.query(StockScreening).offset(skip).limit(limit).all()
        finally:
            db.close()
    
    def search_stocks(self, query: str) -> List[StockScreening]:
        """Search stocks by symbol or name"""
        db = SessionLocal()
        try:
            return db.query(StockScreening).filter(
                (StockScreening.stock_symbol.ilike(f"%{query}%")) |
                (StockScreening.stock_name.ilike(f"%{query}%"))
            ).all()
        finally:
            db.close()