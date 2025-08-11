import os
import pandas as pd

def test_files_after_upload():
    """Test your LinkedIn files after placing them in the folder"""
    
    print("=== TESTING LINKEDIN FILES ===")
    
    linkedin_path = "linkedin_exports"
    
    if not os.path.exists(linkedin_path):
        print("❌ linkedin_exports folder not found!")
        return
    
    files = os.listdir(linkedin_path)
    linkedin_files = [f for f in files if f.endswith(('.csv', '.xls', '.xlsx')) and not f.startswith('~')]
    
    print(f"📁 Total items in folder: {len(files)}")
    print(f"📊 Valid LinkedIn files found: {len(linkedin_files)}")
    
    if not linkedin_files:
        print("❌ No LinkedIn files found!")
        print("📝 Please add your .csv, .xls, or .xlsx files to the linkedin_exports folder")
        print(f"📍 Folder location: {os.path.abspath(linkedin_path)}")
        return
    
    print(f"\n📊 Files to process:")
    for file in linkedin_files:
        file_path = os.path.join(linkedin_path, file)
        file_size = os.path.getsize(file_path)
        print(f"  ✅ {file} ({file_size:,} bytes)")
        
        # Determine file type
        if any(keyword in file.lower() for keyword in ['company', 'overview', 'content', 'post']):
            file_type = "🏢 Company/Posts data"
        elif any(keyword in file.lower() for keyword in ['demographic', 'audience', 'follower']):
            file_type = "👥 Demographics data"
        else:
            file_type = "❓ Unknown type (will try as posts)"
        
        print(f"     Type: {file_type}")
        
        # Try to read file structure
        try:
            if file.endswith(('.xls', '.xlsx')):
                xl_file = pd.ExcelFile(file_path)
                print(f"     📋 Excel sheets: {xl_file.sheet_names}")
                
                # Read first sheet to check columns
                df = pd.read_excel(file_path, sheet_name=xl_file.sheet_names[0], nrows=3)
                print(f"     📊 Columns in first sheet: {list(df.columns)}")
                print(f"     📊 Sample rows: {len(df)}")
                
            elif file.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=3)
                print(f"     📊 Columns: {list(df.columns)}")
                print(f"     📊 Sample rows: {len(df)}")
                
        except Exception as e:
            print(f"     ❌ Error reading file: {e}")
        
        print()  # Empty line for readability
    
    print("🚀 Ready to process! Your automation script should now detect and process these files.")
    print("💡 Run your main automation script to start processing!")

if __name__ == "__main__":
    test_files_after_upload()