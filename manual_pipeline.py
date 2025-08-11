import os
import re
import time
import json
import hashlib
import logging
import schedule
import openpyxl
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional
from supabase import create_client, Client


# Load environment variables
load_dotenv()


# Configuration
class Config:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    LINKEDIN_DATA_PATH = "./linkedin_exports/"
    COMPANY_ID = "wentors"
    COMPANY_NAME = "Wentors"

    @classmethod
    def validate_config(cls):
        """Validate that all required environment variables are set"""
        if not cls.SUPABASE_URL:
            raise ValueError("SUPABASE_URL not found in environment variables")
        if not cls.SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY not found in environment variables")


class LinkedInSupabaseAutomation:
    def __init__(self):
        Config.validate_config()
        self.supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_ROLE_KEY)
        self.setup_logging()
        self.verify_tables()

    def setup_logging(self):
        """Configure logging with UTF-8 encoding to handle emojis"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('linkedin_automation.log', encoding='utf-8'),
                logging.StreamHandler()
            ],
            force=True
        )
        self.logger = logging.getLogger(__name__)

        # Set console handler encoding for Windows
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream.name == '<stderr>':
                handler.stream.reconfigure(encoding='utf-8')

    def verify_tables(self):
        """Verify that required tables exist in Supabase"""
        tables_to_check = ['post_analytics', 'company_analytics', 'follower_analytics']
        for table in tables_to_check:
            try:
                result = self.supabase.table(table).select('*').limit(1).execute()
                self.logger.info(f"✅ Table '{table}' is accessible")
            except Exception as e:
                self.logger.error(f"❌ Table '{table}' is not accessible: {str(e)}")

    def generate_unique_id(self, *args) -> str:
        """Generate a unique ID based on multiple fields"""
        combined_string = '|'.join([str(arg) for arg in args if arg is not None])
        return hashlib.md5(combined_string.encode()).hexdigest()[:16]

    def check_duplicate_exists(self, table: str, unique_fields: dict) -> bool:
        """Check if a record with the same unique fields already exists"""
        try:
            query = self.supabase.table(table).select('*')
            for field, value in unique_fields.items():
                query = query.eq(field, value)
            result = query.limit(1).execute()
            return len(result.data) > 0
        except Exception as e:
            self.logger.warning(f"Error checking duplicates in {table}: {str(e)}")
            return False

    def load_linkedin_file_data(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """Load LinkedIn export files with better header detection"""
        try:
            self.logger.info(f"Loading LinkedIn file: {file_path}, sheet: {sheet_name}")

            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return pd.DataFrame()

            df = None

            if file_path.endswith(('.xls', '.xlsx')):
                try:
                    # For LinkedIn exports, try different header positions (0-5)
                    for header_row in range(6):
                        try:
                            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, engine='xlrd')
                            # Check if we got meaningful column names (not mostly "Unnamed")
                            unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed')]
                            meaningful_cols = len(df.columns) - len(unnamed_cols)
                            # If we have more meaningful columns than unnamed ones, this is likely correct
                            if meaningful_cols > len(unnamed_cols) and not df.empty:
                                self.logger.info(f"Found good header at row {header_row}")
                                break
                            # If we found some meaningful columns and data, keep it
                            if meaningful_cols >= 2 and len(df) > 0:
                                self.logger.info(f"Using header at row {header_row} (partial match)")
                                break
                        except Exception as e:
                            continue

                    # If still no good data, try reading without header and manually find it
                    if df is None or df.empty or len([col for col in df.columns if not str(col).startswith('Unnamed')]) < 2:
                        self.logger.info("Trying to find header row manually...")
                        df = self._find_header_manually(file_path, sheet_name)

                except Exception as e:
                    self.logger.error(f"Excel reading failed: {str(e)}")
                    return pd.DataFrame()
            else:
                # CSV file - try different encodings and header positions
                df = self._try_csv_read_with_headers(file_path)

            if df is not None and not df.empty:
                # Clean the dataframe
                df = self._clean_linkedin_dataframe(df)
                self.logger.info(f"Successfully loaded {len(df)} rows with {len(df.columns)} columns")
                self.logger.info(f"Final columns: {list(df.columns)}")
                return df
            else:
                self.logger.error(f"Failed to load any data from {file_path}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error loading LinkedIn file {file_path}: {str(e)}")
            return pd.DataFrame()

    def _find_header_manually(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """Manually find the header row in LinkedIn exports"""
        try:
            # Read file without header to examine raw structure
            raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='xlrd')
            self.logger.info(f"Raw file has {len(raw_df)} rows")

            # Look for rows that might be headers
            for row_idx in range(min(10, len(raw_df))):  # Check first 10 rows
                row = raw_df.iloc[row_idx]
                # Convert to strings and check for header-like content
                row_str = [str(cell).strip().lower() for cell in row if pd.notna(cell)]
                # Common LinkedIn column indicators
                header_indicators = [
                    'date', 'impressions', 'clicks', 'shares', 'likes', 'comments',
                    'engagement', 'reach', 'post', 'content', 'message', 'update',
                    'location', 'function', 'seniority', 'industry', 'company',
                    'followers', 'sponsored', 'organic'
                ]
                # Count how many header indicators we find
                matches = sum(1 for indicator in header_indicators
                             for cell in row_str
                             if indicator in cell)
                if matches >= 2:  # Found likely header row
                    self.logger.info(f"Found likely header at row {row_idx} with {matches} indicators")
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=row_idx, engine='xlrd')
                    return df

            # If no header found, use row 0 as fallback
            self.logger.warning("Could not find clear header row, using row 0")
            return pd.read_excel(file_path, sheet_name=sheet_name, header=0, engine='xlrd')

        except Exception as e:
            self.logger.error(f"Error finding header manually: {str(e)}")
            return pd.DataFrame()

    def _clean_linkedin_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean LinkedIn-specific DataFrame issues"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Clean column names - LinkedIn exports often have messy headers
        new_columns = []
        for col in df.columns:
            col_str = str(col).strip()
            # Handle unnamed columns by trying to find meaningful names from first few rows
            if col_str.startswith('Unnamed:'):
                # Try to use the first non-null value as column name
                first_vals = df[col].dropna().head(3).astype(str).str.strip()
                if not first_vals.empty and len(first_vals.iloc[0]) < 50:  # Reasonable column name length
                    col_str = first_vals.iloc[0]
                else:
                    col_str = f"Column_{len(new_columns)}"
            new_columns.append(col_str)

        df.columns = new_columns
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]

        # Remove rows that are clearly not data (like description rows)
        def is_data_row(row):
            # Check if row has numeric data or reasonable text length
            non_null_vals = row.dropna()
            if len(non_null_vals) == 0:
                return False
            # If we have numbers, it's likely data
            numeric_count = sum(1 for val in non_null_vals if pd.api.types.is_numeric_dtype(type(val)) or str(val).replace('.', '').replace(',', '').isdigit())
            if numeric_count > 0:
                return True
            # Check if text values are reasonable length (not description paragraphs)
            text_vals = [str(val) for val in non_null_vals]
            avg_length = sum(len(val) for val in text_vals) / len(text_vals)
            return avg_length < 100  # Not a long description

        # Filter out non-data rows
        df = df[df.apply(is_data_row, axis=1)]
        # Reset index
        df = df.reset_index(drop=True)
        return df

    def load_file_data(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """Load data from Excel or CSV file with proper header handling"""
        try:
            self.logger.info(f"Loading file: {file_path}, sheet: {sheet_name}")

            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return pd.DataFrame()

            # Get file size for logging
            file_size = os.path.getsize(file_path)
            self.logger.info(f"File size: {file_size} bytes")

            df = None

            if file_path.endswith(('.xls', '.xlsx')):
                # Try Excel reading with different approaches
                engines_to_try = ['openpyxl']
                if file_path.endswith('.xls'):
                    engines_to_try = ['xlrd', 'openpyxl']

                for engine in engines_to_try:
                    try:
                        # Try different header positions
                        for header_row in [0, 1, 2]:
                            try:
                                df = pd.read_excel(file_path, engine=engine, header=header_row, sheet_name=sheet_name)
                                # Validate that we got meaningful columns
                                meaningful_cols = [col for col in df.columns if not str(col).startswith('Unnamed')]
                                if len(meaningful_cols) >= 2 and not df.empty:
                                    self.logger.info(f"Successfully read Excel with engine {engine}, header row {header_row}")
                                    break
                            except Exception as header_error:
                                continue
                        if df is not None and not df.empty:
                            break
                    except Exception as e:
                        self.logger.warning(f"Excel engine {engine} failed: {str(e)}")
                        continue

                # If Excel fails completely, try as CSV
                if df is None or df.empty:
                    self.logger.info("Excel reading failed, trying as CSV...")
                    df = self._try_csv_read(file_path)
            else:
                # CSV file
                df = self._try_csv_read(file_path)

            if df is not None and not df.empty:
                # Clean the dataframe
                df = self._clean_dataframe(df)
                self.logger.info(f"Successfully loaded {len(df)} rows with {len(df.columns)} columns")
                self.logger.info(f"Columns: {list(df.columns)}")

                # Log sample data for debugging
                if len(df) > 0:
                    self.logger.info("Sample data (first row):")
                    for col, val in df.iloc[0].items():
                        self.logger.info(f"  {col}: {val}")

                return df
            else:
                self.logger.error(f"Failed to load any data from {file_path}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error loading file {file_path}: {str(e)}")
            return pd.DataFrame()

    def _try_csv_read(self, file_path: str) -> pd.DataFrame:
        """Try reading file as CSV with different encodings"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                self.logger.info(f"Successfully read CSV with encoding {encoding}")
                return df
            except Exception as e:
                if encoding == encodings[-1]:
                    self.logger.error(f"All CSV encoding attempts failed. Last error: {str(e)}")
                continue
        return pd.DataFrame()

    def _try_csv_read_with_headers(self, file_path: str) -> pd.DataFrame:
        """Try reading CSV file with different header positions"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
        for encoding in encodings:
            for header_row in range(3):  # Try first few rows as headers
                try:
                    df = pd.read_csv(file_path, encoding=encoding, header=header_row)
                    meaningful_cols = [col for col in df.columns if not str(col).startswith('Unnamed')]
                    if len(meaningful_cols) >= 2 and not df.empty:
                        self.logger.info(f"Successfully read CSV with encoding {encoding}, header row {header_row}")
                        return df
                except Exception as e:
                    continue
        self.logger.error(f"All CSV reading attempts failed for {file_path}")
        return pd.DataFrame()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize DataFrame"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        # Reset index
        df = df.reset_index(drop=True)
        return df

    def find_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """Find column by trying different possible names (case-insensitive)"""
        df_columns_lower = [col.lower().strip() for col in df.columns]
        for name in possible_names:
            name_lower = name.lower()
            # Exact match first
            if name_lower in df_columns_lower:
                return df.columns[df_columns_lower.index(name_lower)]
            # Partial match
            for i, col in enumerate(df_columns_lower):
                if name_lower in col or any(keyword in col for keyword in name_lower.split()):
                    return df.columns[i]
        return None

    def get_excel_sheets(self, file_path: str) -> List[str]:
        """Get list of sheet names in Excel file"""
        try:
            if file_path.endswith(('.xls', '.xlsx')):
                xl_file = pd.ExcelFile(file_path)
                return xl_file.sheet_names
            else:
                return []
        except Exception as e:
            self.logger.error(f"Error getting Excel sheets: {str(e)}")
            return []

    def process_company_overview_posts(self, file_path: str) -> bool:
        """Process LinkedIn company overview file containing post data"""
        try:
            # Check if it's an Excel file with multiple sheets
            sheet_names = self.get_excel_sheets(file_path)
            if sheet_names:
                self.logger.info(f"Found Excel sheets: {sheet_names}")
                # Look for post-related sheets
                post_sheets = [sheet for sheet in sheet_names if any(keyword in sheet.lower()
                              for keyword in ['post', 'content', 'update', 'organic', 'sponsored'])]
                if not post_sheets:
                    # Use the first sheet if no specific post sheet found
                    post_sheets = [sheet_names[0]]
                df = self.load_file_data(file_path, post_sheets[0])
            else:
                df = self.load_file_data(file_path)

            if df.empty:
                self.logger.warning(f"No data loaded from {file_path}")
                return False

            self.logger.info(f"Processing company overview posts with {len(df)} rows")

            # Find relevant columns for posts
            post_content_col = self.find_column(df, ['post', 'content', 'message', 'text', 'description', 'update'])
            date_col = self.find_column(df, ['date', 'created', 'published', 'time', 'posted'])
            impressions_col = self.find_column(df, ['impressions', 'views', 'reach', 'seen'])
            clicks_col = self.find_column(df, ['clicks', 'click', 'ctr'])
            likes_col = self.find_column(df, ['likes', 'reactions', 'love', 'thumbs'])
            comments_col = self.find_column(df, ['comments', 'comment'])
            shares_col = self.find_column(df, ['shares', 'share', 'reposts', 'repost'])
            engagement_col = self.find_column(df, ['engagement', 'rate', 'er'])
            post_type_col = self.find_column(df, ['type', 'format', 'media', 'kind'])
            post_url_col = self.find_column(df, ['url', 'link', 'permalink', 'href'])

            self.logger.info(f"Found columns - Content: {post_content_col}, Date: {date_col}, Impressions: {impressions_col}")

            posts_data = []
            skipped_rows = 0

            for idx, row in df.iterrows():
                try:
                    # Skip rows without essential data
                    post_content = self.safe_string(row.get(post_content_col) if post_content_col else None)
                    post_date = row.get(date_col) if date_col else None
                    if not post_content and pd.isna(post_date):
                        skipped_rows += 1
                        continue

                    # Extract metrics
                    impressions = self.safe_int(row.get(impressions_col) if impressions_col else 0)
                    clicks = self.safe_int(row.get(clicks_col) if clicks_col else 0)
                    likes = self.safe_int(row.get(likes_col) if likes_col else 0)
                    comments = self.safe_int(row.get(comments_col) if comments_col else 0)
                    shares = self.safe_int(row.get(shares_col) if shares_col else 0)
                    engagement_rate = self.safe_float(row.get(engagement_col) if engagement_col else 0)
                    post_type = self.safe_string(row.get(post_type_col) if post_type_col else 'text')
                    post_url = self.safe_string(row.get(post_url_col) if post_url_col else '')

                    # Parse date
                    parsed_date = self.parse_date(post_date)

                    # Generate unique post ID
                    post_id = self.generate_unique_id(
                        Config.COMPANY_ID,
                        post_content[:100] if post_content else '',
                        parsed_date[:10]  # Use date part only
                    )

                    # Check for duplicates
                    if self.check_duplicate_exists('post_analytics', {'post_id': post_id}):
                        self.logger.debug(f"Skipping duplicate post: {post_id}")
                        continue

                    # Prepare post data
                    post_data = {
                        'post_id': post_id,
                        'company_id': Config.COMPANY_ID,
                        'post_content': post_content[:2000] if post_content else '',  # Limit content length
                        'post_date': parsed_date,
                        'impressions': impressions,
                        'clicks': clicks,
                        'likes': likes,
                        'comments': comments,
                        'shares': shares,
                        'engagement_rate': min(engagement_rate, 100.0),  # Cap at 100%
                        'reach': impressions,  # Using impressions as reach approximation
                        'post_type': post_type,
                        'post_url': post_url,
                        'date_collected': datetime.now().isoformat()
                    }
                    posts_data.append(post_data)

                except Exception as row_error:
                    self.logger.error(f"Error processing row {idx}: {str(row_error)}")
                    skipped_rows += 1
                    continue

            self.logger.info(f"Processed {len(posts_data)} posts, skipped {skipped_rows} rows")

            # Insert posts in batches
            if posts_data:
                batch_size = 100
                for i in range(0, len(posts_data), batch_size):
                    batch = posts_data[i:i + batch_size]
                    try:
                        result = self.supabase.table('post_analytics').insert(batch).execute()
                        self.logger.info(f"Successfully inserted batch {i//batch_size + 1}: {len(batch)} posts")
                    except Exception as e:
                        self.logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
                        # Try upsert as fallback
                        try:
                            result = self.supabase.table('post_analytics').upsert(batch, on_conflict='post_id').execute()
                            self.logger.info(f"Successfully upserted batch {i//batch_size + 1}: {len(batch)} posts")
                        except Exception as e2:
                            self.logger.error(f"Error upserting batch {i//batch_size + 1}: {str(e2)}")
                            continue

                # Update company analytics summary
                self._update_company_analytics(posts_data)
                return True
            else:
                self.logger.warning("No valid post data found to insert")
                return False

        except Exception as e:
            self.logger.error(f"Error processing company overview posts: {str(e)}")
            return False

    def process_demographics_data(self, file_path: str) -> bool:
        """Process LinkedIn demographics file with multiple sheets"""
        try:
            sheet_names = self.get_excel_sheets(file_path)
            if not sheet_names:
                # Not an Excel file or no sheets
                return self._process_single_demographic_sheet(file_path, None)

            self.logger.info(f"Processing demographics file with sheets: {sheet_names}")
            total_processed = 0
            success = False

            # Process each sheet as a different demographic category
            for sheet_name in sheet_names:
                try:
                    self.logger.info(f"Processing demographic sheet: {sheet_name}")
                    # Load sheet data
                    df = self.load_file_data(file_path, sheet_name)
                    if df.empty:
                        self.logger.warning(f"No data in sheet {sheet_name}")
                        continue

                    # Process the sheet
                    processed = self._process_demographic_sheet(df, sheet_name)
                    if processed > 0:
                        total_processed += processed
                        success = True
                        self.logger.info(f"Successfully processed {processed} records from sheet {sheet_name}")

                except Exception as sheet_error:
                    self.logger.error(f"Error processing sheet {sheet_name}: {str(sheet_error)}")
                    continue

            self.logger.info(f"Total demographic records processed: {total_processed}")
            return success

        except Exception as e:
            self.logger.error(f"Error processing demographics data: {str(e)}")
            return False

    def _process_demographic_sheet(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Process a single demographic sheet"""
        try:
            demographics_data = []
            # Determine demographic type from sheet name
            sheet_lower = sheet_name.lower() if sheet_name else 'general'
            if any(keyword in sheet_lower for keyword in ['location', 'country', 'city', 'region', 'geography']):
                demo_type = 'location'
            elif any(keyword in sheet_lower for keyword in ['function', 'industry', 'job', 'role', 'occupation']):
                demo_type = 'job_function'
            elif any(keyword in sheet_lower for keyword in ['seniority', 'level', 'experience', 'senior', 'junior']):
                demo_type = 'seniority'
            elif any(keyword in sheet_lower for keyword in ['company', 'organization', 'employer', 'size']):
                demo_type = 'company_size'
            else:
                demo_type = sheet_name.lower().replace(' ', '_') if sheet_name else 'general'

            self.logger.info(f"Processing demographic type: {demo_type}")

            # Try to find value and count columns
            value_col = None
            count_col = None
            percentage_col = None

            # Look for common column patterns
            for col in df.columns:
                col_lower = col.lower().strip()
                if not value_col and any(keyword in col_lower for keyword in ['name', 'value', 'location', 'function', 'title', 'category']):
                    value_col = col
                elif not count_col and any(keyword in col_lower for keyword in ['count', 'number', 'followers', 'audience', 'members', 'total']):
                    count_col = col
                elif not percentage_col and any(keyword in col_lower for keyword in ['percentage', 'percent', '%', 'share', 'pct']):
                    percentage_col = col

            # If we can't find specific columns, use the first two columns
            if not value_col and len(df.columns) > 0:
                value_col = df.columns[0]
            if not count_col and len(df.columns) > 1:
                count_col = df.columns[1]

            self.logger.info(f"Using columns - Value: {value_col}, Count: {count_col}, Percentage: {percentage_col}")

            if not value_col or not count_col:
                self.logger.warning(f"Could not identify value and count columns in sheet {sheet_name}")
                return 0

            # Process rows
            for idx, row in df.iterrows():
                try:
                    value = self.safe_string(row.get(value_col))
                    count = self.safe_int(row.get(count_col))
                    percentage = self.safe_float(row.get(percentage_col) if percentage_col else 0)
                    if not value or count <= 0:
                        continue

                    # Create demographic record
                    demo_record = {
                        'company_id': Config.COMPANY_ID,
                        'date_collected': datetime.now().isoformat(),
                        'total_followers': self._get_current_followers(),
                        'new_followers': 0,  # Not available in demographic breakdown
                        'demographic_type': demo_type,
                        'demographic_value': value[:255],  # Limit length
                        'count': count,
                        'percentage': min(percentage, 100.0) if percentage > 0 else 0
                    }

                    # Check for duplicates
                    if not self._is_duplicate_demographic(demo_record):
                        demographics_data.append(demo_record)

                except Exception as row_error:
                    self.logger.error(f"Error processing row {idx} in sheet {sheet_name}: {str(row_error)}")
                    continue

            # Insert data
            if demographics_data:
                try:
                    result = self.supabase.table('follower_analytics').insert(demographics_data).execute()
                    self.logger.info(f"Successfully inserted {len(demographics_data)} records for {demo_type}")
                    return len(demographics_data)
                except Exception as e:
                    self.logger.error(f"Error inserting demographics for {demo_type}: {str(e)}")
                    # Try upsert
                    try:
                        result = self.supabase.table('follower_analytics').upsert(demographics_data).execute()
                        self.logger.info(f"Successfully upserted {len(demographics_data)} records for {demo_type}")
                        return len(demographics_data)
                    except Exception as e2:
                        self.logger.error(f"Error upserting demographics for {demo_type}: {str(e2)}")
                        return 0
            return 0

        except Exception as e:
            self.logger.error(f"Error processing demographic sheet {sheet_name}: {str(e)}")
            return 0

    def _process_single_demographic_sheet(self, file_path: str, sheet_name: str) -> bool:
        """Process a single demographics file (non-Excel or single sheet)"""
        try:
            df = self.load_file_data(file_path, sheet_name)
            if df.empty:
                return False
            processed = self._process_demographic_sheet(df, sheet_name or "demographics")
            return processed > 0
        except Exception as e:
            self.logger.error(f"Error processing single demographic sheet: {str(e)}")
            return False

    def _is_duplicate_demographic(self, record: dict) -> bool:
        """Check if demographic record already exists"""
        return self.check_duplicate_exists('follower_analytics', {
            'company_id': record['company_id'],
            'demographic_type': record['demographic_type'],
            'demographic_value': record['demographic_value'],
            'date_collected': record['date_collected'][:10]  # Compare by date only
        })

    def _update_company_analytics(self, posts_data: List[dict]):
        """Update company analytics summary based on posts data"""
        try:
            if not posts_data:
                return
            total_impressions = sum(post['impressions'] for post in posts_data)
            total_clicks = sum(post['clicks'] for post in posts_data)
            total_engagement = sum(post['likes'] + post['comments'] + post['shares'] for post in posts_data)
            avg_engagement_rate = sum(post['engagement_rate'] for post in posts_data) / len(posts_data)
            company_summary = {
                'company_id': Config.COMPANY_ID,
                'company_name': Config.COMPANY_NAME,
                'followers_count': self._get_current_followers(),
                'impressions': total_impressions,
                'unique_impressions': total_impressions,  # Approximation
                'clicks': total_clicks,
                'engagement_rate': round(avg_engagement_rate, 2),
                'reach': total_impressions,
                'date_collected': datetime.now().isoformat(),
                'total_posts': len(posts_data),
                'avg_post_engagement': round(avg_engagement_rate, 2)
            }
            self.supabase.table('company_analytics').upsert(company_summary, on_conflict='company_id').execute()
            self.logger.info("Successfully updated company analytics summary")
        except Exception as e:
            self.logger.error(f"Error updating company analytics: {str(e)}")

    def _get_current_followers(self) -> int:
        """Get current follower count"""
        try:
            # Try follower_analytics first
            recent_data = self.supabase.table('follower_analytics').select('total_followers').eq('company_id', Config.COMPANY_ID).order('date_collected', desc=True).limit(1).execute()
            if recent_data.data and recent_data.data[0]['total_followers']:
                return recent_data.data[0]['total_followers']
            # Fallback to company_analytics
            company_data = self.supabase.table('company_analytics').select('followers_count').eq('company_id', Config.COMPANY_ID).order('date_collected', desc=True).limit(1).execute()
            if company_data.data and company_data.data[0]['followers_count']:
                return company_data.data[0]['followers_count']
            return 1000  # Default fallback
        except Exception as e:
            self.logger.error(f"Error getting current followers: {str(e)}")
            return 1000

    def safe_string(self, value) -> str:
        """Safely convert value to string"""
        if pd.isna(value) or value is None:
            return ''
        return str(value).strip()

    def safe_int(self, value) -> int:
        """Safely convert value to integer"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0
            # Handle percentage and formatted numbers
            clean_value = str(value).replace('%', '').replace(',', '').strip()
            # Extract numeric part if there's text
            numeric_match = re.search(r'[\d,]+\.?\d*', clean_value)
            if numeric_match:
                clean_value = numeric_match.group().replace(',', '')
            return int(float(clean_value))
        except Exception:
            return 0

    def safe_float(self, value) -> float:
        """Safely convert value to float"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0.0
            # Handle percentage and formatted numbers
            clean_value = str(value).replace('%', '').replace(',', '').strip()
            # Extract numeric part if there's text
            numeric_match = re.search(r'[\d,]+\.?\d*', clean_value)
            if numeric_match:
                clean_value = numeric_match.group().replace(',', '')
            return float(clean_value)
        except Exception:
            return 0.0

    def parse_date(self, date_value) -> str:
        """Parse various date formats"""
        try:
            if pd.isna(date_value) or date_value == '' or date_value is None:
                return datetime.now().isoformat()
            if isinstance(date_value, datetime):
                return date_value.isoformat()
            # Handle LinkedIn date formats
            date_str = str(date_value).strip()
            formats_to_try = [
                '%m/%d/%Y',     # 08/07/2024
                '%d/%m/%Y',     # 07/08/2024
                '%Y-%m-%d',     # 2024-08-07
                '%m-%d-%Y',     # 08-07-2024
                '%d-%m-%Y',     # 07-08-2024
                '%Y/%m/%d',     # 2024/08/07
                '%m/%d/%y',     # 08/07/24
                '%d/%m/%y',     # 07/08/24
                '%B %d, %Y',    # August 7, 2024
                '%b %d, %Y',    # Aug 7, 2024
                '%d %B %Y',     # 7 August 2024
                '%d %b %Y',     # 7 Aug 2024
            ]
            for fmt in formats_to_try:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.isoformat()
                except ValueError:
                    continue
            # Try pandas as last resort
            try:
                parsed_date = pd.to_datetime(date_value)
                return parsed_date.isoformat()
            except:
                pass
            self.logger.warning(f"Could not parse date '{date_value}', using current time")
            return datetime.now().isoformat()
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_value}': {str(e)}")
            return datetime.now().isoformat()

    def scan_and_process_files(self):
        """Scan for new LinkedIn files and process them"""
        try:
            if not os.path.exists(Config.LINKEDIN_DATA_PATH):
                os.makedirs(Config.LINKEDIN_DATA_PATH)
                self.logger.info(f"Created directory: {Config.LINKEDIN_DATA_PATH}")
                return
            files_found = [f for f in os.listdir(Config.LINKEDIN_DATA_PATH)
                          if f.endswith(('.csv', '.xls', '.xlsx')) and not f.startswith('~')]
            if not files_found:
                self.logger.info("No files found to process")
                return
            self.logger.info(f"Found {len(files_found)} files to process: {files_found}")
            processed_files = []
            failed_files = []
            for filename in files_found:
                file_path = os.path.join(Config.LINKEDIN_DATA_PATH, filename)
                self.logger.info(f"Processing file: {filename}")
                try:
                    # Determine file type and process
                    filename_lower = filename.lower()
                    success = False
                    if any(keyword in filename_lower for keyword in ['company', 'overview', 'content', 'post']):
                        success = self.process_company_overview_posts(file_path)
                        file_type = "posts"
                    elif any(keyword in filename_lower for keyword in ['demographic', 'audience', 'follower']):
                        success = self.process_demographics_data(file_path)
                        file_type = "demographics"
                    else:
                        self.logger.info(f"Unknown file type for {filename}, trying as posts file...")
                        success = self.process_company_overview_posts(file_path)
                        file_type = "posts (assumed)"
                    if success:
                        processed_files.append((filename, file_type))
                        self._archive_file(file_path, filename)
                    else:
                        failed_files.append(filename)
                except Exception as file_error:
                    self.logger.error(f"Error processing {filename}: {str(file_error)}")
                    failed_files.append(filename)

            # Log results
            if processed_files:
                self.logger.info(f"[SUCCESS] Successfully processed {len(processed_files)} files:")
                for filename, file_type in processed_files:
                    self.logger.info(f"  - {filename} ({file_type})")
            if failed_files:
                self.logger.warning(f"[WARNING] Failed to process {len(failed_files)} files: {failed_files}")
        except Exception as e:
            self.logger.error(f"Error scanning files: {str(e)}")

    def _archive_file(self, file_path: str, filename: str):
        """Archive processed file with timestamp"""
        try:
            archive_path = os.path.join(Config.LINKEDIN_DATA_PATH, 'processed')
            if not os.path.exists(archive_path):
                os.makedirs(archive_path)
            # Add timestamp to prevent filename conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = filename.rsplit('.', 1)
            archived_filename = f"{timestamp}_{name_parts[0]}.{name_parts[1]}"
            archived_path = os.path.join(archive_path, archived_filename)
            os.rename(file_path, archived_path)
            self.logger.info(f"Archived {filename} as {archived_filename}")
        except Exception as e:
            self.logger.error(f"Error archiving file {filename}: {str(e)}")

    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data to prevent database bloat"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
            # Clean up old post analytics
            try:
                result = self.supabase.table('post_analytics').delete().lt('date_collected', cutoff_date).execute()
                self.logger.info(f"Cleaned up old post analytics data")
            except Exception as e:
                self.logger.error(f"Error cleaning post analytics: {str(e)}")
            # Clean up old follower analytics
            try:
                result = self.supabase.table('follower_analytics').delete().lt('date_collected', cutoff_date).execute()
                self.logger.info(f"Cleaned up old follower analytics data")
            except Exception as e:
                self.logger.error(f"Error cleaning follower analytics: {str(e)}")
            # Clean up old archived files
            try:
                archive_path = os.path.join(Config.LINKEDIN_DATA_PATH, 'processed')
                if os.path.exists(archive_path):
                    cutoff_timestamp = datetime.now() - timedelta(days=days_to_keep)
                    for filename in os.listdir(archive_path):
                        file_path = os.path.join(archive_path, filename)
                        file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_modified < cutoff_timestamp:
                            os.remove(file_path)
                            self.logger.info(f"Deleted old archived file: {filename}")
            except Exception as e:
                self.logger.error(f"Error cleaning archived files: {str(e)}")
            self.logger.info(f"Cleanup completed for data older than {days_to_keep} days")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    def generate_summary_report(self) -> Optional[dict]:
        """Generate a comprehensive summary report"""
        try:
            self.logger.info("Generating summary report...")
            # Get company analytics
            try:
                company_data = self.supabase.table('company_analytics').select('*').eq('company_id', Config.COMPANY_ID).order('date_collected', desc=True).limit(1).execute()
                current_followers = company_data.data[0]['followers_count'] if company_data.data else 0
                total_impressions = company_data.data[0]['impressions'] if company_data.data else 0
                avg_engagement = company_data.data[0]['avg_post_engagement'] if company_data.data else 0
            except Exception as e:
                self.logger.error(f"Error getting company data: {str(e)}")
                current_followers = total_impressions = avg_engagement = 0
            # Get recent posts count
            try:
                thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
                recent_posts = self.supabase.table('post_analytics').select('post_id').gte('post_date', thirty_days_ago).execute()
                recent_posts_count = len(recent_posts.data)
            except Exception as e:
                self.logger.error(f"Error getting recent posts: {str(e)}")
                recent_posts_count = 0
            # Get total posts count
            try:
                all_posts = self.supabase.table('post_analytics').select('post_id').execute()
                total_posts_count = len(all_posts.data)
            except Exception as e:
                self.logger.error(f"Error getting total posts: {str(e)}")
                total_posts_count = 0
            # Get demographics count
            try:
                demographics = self.supabase.table('follower_analytics').select('demographic_type').eq('company_id', Config.COMPANY_ID).execute()
                unique_demographics = len(set(item['demographic_type'] for item in demographics.data))
            except Exception as e:
                self.logger.error(f"Error getting demographics: {str(e)}")
                unique_demographics = 0
            # Get top performing post
            try:
                top_post = self.supabase.table('post_analytics').select('post_content, impressions, engagement_rate').order('impressions', desc=True).limit(1).execute()
                top_post_info = {
                    'content': top_post.data[0]['post_content'][:100] + '...' if top_post.data else 'None',
                    'impressions': top_post.data[0]['impressions'] if top_post.data else 0,
                    'engagement_rate': top_post.data[0]['engagement_rate'] if top_post.data else 0
                }
            except Exception as e:
                self.logger.error(f"Error getting top post: {str(e)}")
                top_post_info = {'content': 'None', 'impressions': 0, 'engagement_rate': 0}

            report = {
                'company_name': Config.COMPANY_NAME,
                'company_id': Config.COMPANY_ID,
                'report_generated': datetime.now().isoformat(),
                'followers': {
                    'current_count': current_followers,
                    'demographic_segments': unique_demographics
                },
                'content': {
                    'total_posts': total_posts_count,
                    'posts_last_30_days': recent_posts_count,
                    'total_impressions': total_impressions,
                    'avg_engagement_rate': avg_engagement
                },
                'top_performing_post': top_post_info,
                'data_collection_status': 'Active',
                'last_file_processed': self._get_last_processed_file_time()
            }

            self.logger.info("[REPORT] Summary Report Generated:")
            self.logger.info(f"  Company: {report['company_name']}")
            self.logger.info(f"  Followers: {report['followers']['current_count']:,}")
            self.logger.info(f"  Total Posts: {report['content']['total_posts']}")
            self.logger.info(f"  Posts (Last 30 days): {report['content']['posts_last_30_days']}")
            self.logger.info(f"  Total Impressions: {report['content']['total_impressions']:,}")
            self.logger.info(f"  Avg Engagement Rate: {report['content']['avg_engagement_rate']:.2f}%")
            self.logger.info(f"  Demographic Segments: {report['followers']['demographic_segments']}")
            return report

        except Exception as e:
            self.logger.error(f"Error generating summary report: {str(e)}")
            return None

    def _get_last_processed_file_time(self) -> str:
        """Get timestamp of last processed file"""
        try:
            archive_path = os.path.join(Config.LINKEDIN_DATA_PATH, 'processed')
            if not os.path.exists(archive_path):
                return "No files processed yet"
            files = [f for f in os.listdir(archive_path) if f.endswith(('.csv', '.xls', '.xlsx'))]
            if not files:
                return "No files processed yet"
            # Get the most recently modified file
            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(archive_path, f)))
            latest_time = datetime.fromtimestamp(os.path.getmtime(os.path.join(archive_path, latest_file)))
            return latest_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.logger.error(f"Error getting last processed file time: {str(e)}")
            return "Unknown"

    def health_check(self) -> bool:
        """Perform system health check"""
        try:
            self.logger.info("[CHECK] Performing health check...")
            # Check Supabase connection
            try:
                self.supabase.table('company_analytics').select('*').limit(1).execute()
                self.logger.info("[OK] Supabase connection: OK")
                supabase_ok = True
            except Exception as e:
                self.logger.error(f"[ERROR] Supabase connection failed: {str(e)}")
                supabase_ok = False
            # Check data directory
            try:
                if os.path.exists(Config.LINKEDIN_DATA_PATH):
                    files_count = len([f for f in os.listdir(Config.LINKEDIN_DATA_PATH)
                                     if f.endswith(('.csv', '.xls', '.xlsx'))])
                    self.logger.info(f"[OK] Data directory: OK ({files_count} files pending)")
                    directory_ok = True
                else:
                    self.logger.warning("[WARNING] Data directory does not exist")
                    directory_ok = False
            except Exception as e:
                self.logger.error(f"[ERROR] Data directory check failed: {str(e)}")
                directory_ok = False
            # Check recent data
            try:
                recent_data = self.supabase.table('post_analytics').select('*').order('date_collected', desc=True).limit(1).execute()
                if recent_data.data:
                    last_update = recent_data.data[0]['date_collected']
                    self.logger.info(f"[OK] Recent data: Last update {last_update}")
                    data_ok = True
                else:
                    self.logger.warning("[WARNING] No recent data found")
                    data_ok = False
            except Exception as e:
                self.logger.error(f"[ERROR] Recent data check failed: {str(e)}")
                data_ok = False

            overall_health = supabase_ok and directory_ok
            status = "HEALTHY" if overall_health else "ISSUES DETECTED"
            self.logger.info(f"[STATUS] Overall Health Status: {status}")
            return overall_health

        except Exception as e:
            self.logger.error(f"Error during health check: {str(e)}")
            return False

    def debug_file_processing(self):
        """Debug method to check file processing issues"""
        try:
            self.logger.info("=== DEBUG: File Processing Analysis ===")
            # Check directory exists
            if not os.path.exists(Config.LINKEDIN_DATA_PATH):
                self.logger.error(f"Directory does not exist: {Config.LINKEDIN_DATA_PATH}")
                return
            # List all files
            all_files = os.listdir(Config.LINKEDIN_DATA_PATH)
            self.logger.info(f"All files in directory: {all_files}")
            # Check valid file extensions
            valid_files = [f for f in all_files if f.endswith(('.csv', '.xls', '.xlsx')) and not f.startswith('~')]
            self.logger.info(f"Valid files found: {valid_files}")
            # Test each file
            for filename in valid_files:
                file_path = os.path.join(Config.LINKEDIN_DATA_PATH, filename)
                self.logger.info(f"\n--- Testing file: {filename} ---")
                self.logger.info(f"File size: {os.path.getsize(file_path)} bytes")
                # Test file reading
                try:
                    if filename.endswith(('.xls', '.xlsx')):
                        # Check Excel sheets
                        sheets = self.get_excel_sheets(file_path)
                        self.logger.info(f"Excel sheets: {sheets}")
                        # Try reading first sheet
                        df = self.load_file_data(file_path, sheets[0] if sheets else None)
                        self.logger.info(f"Rows loaded: {len(df)}, Columns: {len(df.columns)}")
                        if not df.empty:
                            self.logger.info(f"Column names: {list(df.columns)}")
                            self.logger.info(f"Sample data (first 3 rows):")
                            for i, row in df.head(3).iterrows():
                                self.logger.info(f"  Row {i}: {dict(row)}")
                    else:
                        # CSV file
                        df = self.load_file_data(file_path)
                        self.logger.info(f"CSV - Rows: {len(df)}, Columns: {len(df.columns)}")
                        if not df.empty:
                            self.logger.info(f"Column names: {list(df.columns)}")
                except Exception as e:
                    self.logger.error(f"Error reading file {filename}: {str(e)}")
            self.logger.info("=== DEBUG: End Analysis ===")
        except Exception as e:
            self.logger.error(f"Debug analysis failed: {str(e)}")

    def debug_database_content(self):
        """Debug method to check what's in the database"""
        try:
            self.logger.info("=== DEBUG: Database Content ===")
            # Check post_analytics
            posts = self.supabase.table('post_analytics').select('*').execute()
            self.logger.info(f"Posts in database: {len(posts.data)}")
            if posts.data:
                self.logger.info(f"Sample post: {posts.data[0]}")
            # Check follower_analytics
            followers = self.supabase.table('follower_analytics').select('*').execute()
            self.logger.info(f"Follower records in database: {len(followers.data)}")
            if followers.data:
                self.logger.info(f"Sample follower record: {followers.data[0]}")
            # Check company_analytics
            company = self.supabase.table('company_analytics').select('*').execute()
            self.logger.info(f"Company records in database: {len(company.data)}")
            if company.data:
                self.logger.info(f"Sample company record: {company.data[0]}")
            self.logger.info("=== DEBUG: End Database Check ===")
        except Exception as e:
            self.logger.error(f"Database debug failed: {str(e)}")


def main():
    """Main automation function"""
    try:
        automation = LinkedInSupabaseAutomation()
        # Perform initial health check
        if not automation.health_check():
            automation.logger.warning("[WARNING] System health check failed. Continuing anyway...")
        # Schedule regular tasks
        schedule.every(1).hours.do(automation.scan_and_process_files)
        schedule.every().day.at("06:00").do(automation.cleanup_old_data)
        schedule.every().day.at("09:00").do(automation.generate_summary_report)
        schedule.every().day.at("12:00").do(automation.health_check)
        automation.logger.info("[START] LinkedIn to Supabase automation started successfully!")
        automation.logger.info("[SCHEDULE] Scheduled tasks:")
        automation.logger.info("  - File processing: Every hour")
        automation.logger.info("  - Data cleanup: Daily at 6:00 AM")
        automation.logger.info("  - Summary report: Daily at 9:00 AM")
        automation.logger.info("  - Health check: Daily at 12:00 PM")
        # Run initial scan
        automation.logger.info("[SCAN] Running initial file scan...")
        automation.scan_and_process_files()
        # Generate initial report
        automation.logger.info("[REPORT] Generating initial summary report...")
        automation.generate_summary_report()
        # Keep the script running
        automation.logger.info("[RUNNING] Automation is now running. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n[STOP] Automation stopped by user")
    except Exception as e:
        print(f"[ERROR] Failed to start automation: {str(e)}")
        print("\n[TROUBLESHOOT] Troubleshooting:")
        print("1. Check your .env file contains:")
        print("   SUPABASE_URL=your_supabase_url")
        print("   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key")
        print("2. Ensure Supabase tables exist: post_analytics, company_analytics, follower_analytics")
        print("3. Check file permissions for the linkedin_exports directory")
        print("4. Verify your network connection to Supabase")


if __name__ == "__main__":
    main()