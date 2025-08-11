# auto_pipeline.py
import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from config import *
from manual_pipeline import LinkedInManualExportPipeline
import time

class LinkedInAutomationPipeline:
    def __init__(self):
        self.pipeline = LinkedInManualExportPipeline()
        
    async def login_linkedin(self, page):
        """Enhanced LinkedIn login with better error handling"""
        try:
            print("🔐 Logging into LinkedIn...")
            await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
            
            # Wait for login form to load
            await page.wait_for_selector("#username", timeout=10000)
            
            await page.fill("#username", LINKEDIN_EMAIL)
            await page.fill("#password", LINKEDIN_PASSWORD)
            
            # Click login button
            await page.click('button[type="submit"]')
            
            # Wait for either successful login or error
            try:
                # Wait for navigation away from login page
                await page.wait_for_url(lambda url: "login" not in url, timeout=15000)
                print("✅ Successfully logged into LinkedIn")
                
                # Handle potential verification or security check
                await asyncio.sleep(3)
                
                # Check if we're on a verification page
                if "challenge" in page.url or "checkpoint" in page.url:
                    print("⚠️ Security verification required. Please complete manually...")
                    await asyncio.sleep(30)  # Give time for manual intervention
                    
            except PlaywrightTimeoutError:
                print("❌ Login may have failed or requires verification")
                # Check if there are error messages
                error_elements = await page.query_selector_all(".form__label--error, .alert")
                if error_elements:
                    for error in error_elements:
                        error_text = await error.text_content()
                        print(f"Login error: {error_text}")
                raise Exception("Login failed")
                
        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            raise

    async def wait_for_analytics_page_load(self, page):
        """Wait for analytics page to fully load"""
        try:
            # Wait for common analytics page elements
            selectors_to_wait = [
                '[data-test-id="analytics-chart"]',
                '.analytics-chart',
                '[class*="analytics"]',
                '[class*="chart"]',
                'canvas',
                '.artdeco-card'
            ]
            
            for selector in selectors_to_wait:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    print(f"✅ Found analytics element: {selector}")
                    break
                except:
                    continue
            
            # Additional wait for dynamic content
            await asyncio.sleep(3)
            
        except Exception as e:
            print(f"⚠️ Analytics page load warning: {str(e)}")

    async def find_date_filter_button(self, page):
        """Find date filter button with multiple selectors"""
        date_filter_selectors = [
            'button[aria-label="Date range filter"]',
            'button[aria-label*="Date"]',
            'button[aria-label*="date"]',
            'button:has-text("Date")',
            'button:has-text("date")',
            '.date-range-filter button',
            '[data-test-id*="date"] button',
            'button[class*="date"]',
            '.analytics-date-filter button',
            'button:has([class*="calendar"])',
            'button:has([class*="date"])'
        ]
        
        for selector in date_filter_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    # Check if element is visible and enabled
                    is_visible = await element.is_visible()
                    is_enabled = await element.is_enabled()
                    if is_visible and is_enabled:
                        print(f"✅ Found date filter button: {selector}")
                        return element
            except:
                continue
        
        return None

    async def find_export_button(self, page):
        """Find export button with multiple selectors"""
        export_selectors = [
            'button:has-text("Export")',
            'button:has-text("export")',
            'button:has-text("Download")',
            'button:has-text("download")',
            '[data-test-id*="export"] button',
            'button[aria-label*="Export"]',
            'button[aria-label*="export"]',
            '.export-button',
            'button[class*="export"]',
            'a:has-text("Export")',
            'a:has-text("Download")'
        ]
        
        for selector in export_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    is_visible = await element.is_visible()
                    is_enabled = await element.is_enabled()
                    if is_visible and is_enabled:
                        print(f"✅ Found export button: {selector}")
                        return element
            except:
                continue
        
        return None

    async def set_date_range(self, page):
        """Set date range with robust error handling"""
        try:
            # Try to find and click date filter
            date_button = await self.find_date_filter_button(page)
            if not date_button:
                print("⚠️ Date filter button not found, proceeding with default date range")
                return True
            
            await date_button.click()
            await asyncio.sleep(2)
            
            # Look for custom range option
            custom_range_selectors = [
                'text=Custom range',
                'text=custom range',
                'text=Custom',
                '[data-value="custom"]',
                'button:has-text("Custom")',
                '.date-range-option:has-text("Custom")'
            ]
            
            custom_clicked = False
            for selector in custom_range_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    custom_clicked = True
                    print("✅ Selected custom date range")
                    break
                except:
                    continue
            
            if not custom_clicked:
                print("⚠️ Custom date range not found, using default")
                return True
            
            await asyncio.sleep(1)
            
            # Try to fill date inputs
            start_date_selectors = [
                'input[placeholder="Start date"]',
                'input[placeholder*="start"]',
                'input[placeholder*="Start"]',
                'input[name*="start"]',
                'input[data-test-id*="start"]',
                '.date-input:first-of-type input'
            ]
            
            end_date_selectors = [
                'input[placeholder="End date"]',
                'input[placeholder*="end"]',
                'input[placeholder*="End"]',
                'input[name*="end"]',
                'input[data-test-id*="end"]',
                '.date-input:last-of-type input'
            ]
            
            # Fill start date
            for selector in start_date_selectors:
                try:
                    await page.fill(selector, START_DATE, timeout=3000)
                    print(f"✅ Set start date: {START_DATE}")
                    break
                except:
                    continue
            
            # Fill end date
            for selector in end_date_selectors:
                try:
                    await page.fill(selector, END_DATE, timeout=3000)
                    print(f"✅ Set end date: {END_DATE}")
                    break
                except:
                    continue
            
            # Apply date range
            apply_selectors = [
                'button:has-text("Apply")',
                'button:has-text("apply")',
                'button:has-text("OK")',
                'button:has-text("Save")',
                '[data-test-id*="apply"] button'
            ]
            
            for selector in apply_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    print("✅ Applied date range")
                    await asyncio.sleep(3)
                    return True
                except:
                    continue
            
            return True
            
        except Exception as e:
            print(f"⚠️ Date range setting failed: {str(e)}")
            return True  # Continue anyway

    async def download_data(self, page, file_label):
        """Download data with robust export handling"""
        try:
            export_button = await self.find_export_button(page)
            if not export_button:
                print(f"❌ Export button not found for {file_label}")
                return None
            
            # Set up download handler
            download_promise = page.wait_for_event("download", timeout=30000)
            
            # Click export button
            await export_button.click()
            
            try:
                download = await download_promise
                file_path = os.path.join(DOWNLOAD_DIR, f"{file_label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                await download.save_as(file_path)
                print(f"✅ Saved {file_label} to {file_path}")
                return file_path
                
            except PlaywrightTimeoutError:
                print(f"⚠️ Download timeout for {file_label}")
                return None
            
        except Exception as e:
            print(f"❌ Download error for {file_label}: {str(e)}")
            return None

    async def scrape_analytics_page(self, page, url, file_label):
        """Main function to scrape analytics page"""
        try:
            print(f"📄 Navigating to {file_label} page...")
            await page.goto(url, wait_until="domcontentloaded")
            
            # Wait for page to load
            await self.wait_for_analytics_page_load(page)
            
            # Check if we have access to this page
            if "access" in page.url.lower() or "permission" in page.url.lower():
                print(f"❌ Access denied to {file_label} page")
                return None
            
            # Set date range
            await self.set_date_range(page)
            
            # Download data
            file_path = await self.download_data(page, file_label)
            
            return file_path
            
        except Exception as e:
            print(f"❌ Error scraping {file_label}: {str(e)}")
            return None

    async def run_automation(self):
        """Main automation runner"""
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        
        async with async_playwright() as p:
            # Launch browser with better settings
            browser = await p.chromium.launch(
                headless=False,  # Keep visible for debugging
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--allow-running-insecure-content'
                ]
            )
            
            context = await browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = await context.new_page()
            
            try:
                # Login
                await self.login_linkedin(page)
                
                # Build URLs
                followers_url = f"https://www.linkedin.com/company/{COMPANY_NUMERIC_ID}/admin/analytics/followers/"
                posts_url = f"https://www.linkedin.com/company/{COMPANY_NUMERIC_ID}/admin/analytics/updates/"
                
                downloaded_files = []
                
                # Try to scrape followers analytics
                try:
                    followers_csv = await self.scrape_analytics_page(page, followers_url, "followers_analytics")
                    if followers_csv:
                        downloaded_files.append(("followers", followers_csv))
                except Exception as e:
                    print(f"❌ Followers analytics failed: {str(e)}")
                
                # Try to scrape posts analytics
                try:
                    posts_csv = await self.scrape_analytics_page(page, posts_url, "posts_analytics")
                    if posts_csv:
                        downloaded_files.append(("posts", posts_csv))
                except Exception as e:
                    print(f"❌ Posts analytics failed: {str(e)}")
                
                # Process downloaded files
                if downloaded_files:
                    print(f"\n🚀 Processing {len(downloaded_files)} downloaded files...")
                    
                    for file_type, file_path in downloaded_files:
                        try:
                            if file_type == "posts" and os.path.exists(file_path):
                                result = self.pipeline.process_linkedin_posts_export(file_path)
                                print(f"✅ Processed posts: {result}")
                            elif file_type == "followers" and os.path.exists(file_path):
                                result = self.pipeline.process_follower_analytics_export(file_path)
                                print(f"✅ Processed followers: {result}")
                        except Exception as e:
                            print(f"❌ Error processing {file_type}: {str(e)}")
                    
                    print("🎯 Automation completed!")
                else:
                    print("⚠️ No files were downloaded successfully")
                    
            except Exception as e:
                print(f"❌ Automation error: {str(e)}")
                # Take screenshot for debugging
                try:
                    await page.screenshot(path=f"error_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                    print("📷 Error screenshot saved")
                except:
                    pass
            
            finally:
                await browser.close()

# Alternative: Manual fallback with better instructions
class ManualFallbackPipeline:
    def __init__(self):
        self.pipeline = LinkedInManualExportPipeline()
    
    def run_manual_process(self):
        """Guide user through manual process"""
        print("""
🔧 MANUAL PROCESS FALLBACK
========================

Since automation failed, please follow these steps:

1. Go to https://www.linkedin.com/company/{}/admin/analytics/
2. Navigate to 'Updates' tab
3. Set your desired date range
4. Click 'Export' and save as CSV
5. Place the file in: {}
6. Run manual processing

Files should be named with these patterns:
- posts_analytics_*.csv
- followers_analytics_*.csv
        """.format(COMPANY_NUMERIC_ID, DOWNLOAD_DIR))
        
        # Check for existing files
        if os.path.exists(DOWNLOAD_DIR):
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.csv')]
            if files:
                print(f"\n📁 Found {len(files)} CSV files:")
                for file in files:
                    print(f"  - {file}")
                
                response = input("\nProcess these files? (y/n): ")
                if response.lower() == 'y':
                    for file in files:
                        file_path = os.path.join(DOWNLOAD_DIR, file)
                        try:
                            if 'post' in file.lower():
                                result = self.pipeline.process_linkedin_posts_export(file_path)
                                print(f"✅ Processed posts from {file}: {result}")
                            elif 'follow' in file.lower():
                                result = self.pipeline.process_follower_analytics_export(file_path)
                                print(f"✅ Processed followers from {file}: {result}")
                        except Exception as e:
                            print(f"❌ Error processing {file}: {str(e)}")

async def main():
    """Main execution with fallback"""
    try:
        # Try automation first
        automation = LinkedInAutomationPipeline()
        await automation.run_automation()
        
    except Exception as e:
        print(f"❌ Automation failed: {str(e)}")
        print("\n🔄 Switching to manual fallback...")
        
        # Fallback to manual process
        manual = ManualFallbackPipeline()
        manual.run_manual_process()

if __name__ == "__main__":
    asyncio.run(main())