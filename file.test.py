from dotenv import load_dotenv
from pathlib import Path
from export_ingester import load_posts_df_robust

# Load .env variables
load_dotenv()

# Find LinkedIn export Excel files
files = list(Path("./linkedin_exports").glob("*.xls"))
print("Found:", [f.name for f in files])

if files:
    df = load_posts_df_robust(files[0])
    print("Rows:", len(df), "Cols:", len(df.columns))
    print("Columns:", list(df.columns)[:20])
    print(df.head(2).to_string())
