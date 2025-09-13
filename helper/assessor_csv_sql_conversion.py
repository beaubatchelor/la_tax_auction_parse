import sqlite3
import pandas as pd

# csv_path = r"C:\Users\Roshi\Downloads\Assessor_Parcels_Data_-_2006_thru_2021_output.csv"
csv_path = r"C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\2025B-Auction-Book.csv"
db_path  = r"C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\assessor.sqlite"

con = sqlite3.connect(db_path)
cur = con.cursor()

# Speed-up PRAGMAs for one-time load (less safe if power loss)
cur.execute("PRAGMA journal_mode=OFF;")
cur.execute("PRAGMA synchronous=OFF;")
cur.execute("PRAGMA temp_store=MEMORY;")
cur.execute("PRAGMA cache_size=-200000;")  # about 200 MB

# Stream in chunks; keep as TEXT first to avoid type pain
first = True
for chunk in pd.read_csv(csv_path):
    chunk.columns = [c.strip().replace(" ", "_") for c in chunk.columns]  # tidy names
    chunk.to_sql("auction", con, if_exists="append" if not first else "replace", index=False)
    first = False

con.commit()

# Add indexes on the columns you filter by most
# cur.execute("CREATE INDEX IF NOT EXISTS ix_auction_apn ON auction(APN);")
# con.commit()

# Query
print(pd.read_sql_query("SELECT COUNT(*) AS n FROM auction;", con))
print(pd.read_sql_query("SELECT * FROM auction LIMIT 5;", con))

con.close()
