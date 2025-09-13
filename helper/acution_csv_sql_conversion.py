import sqlite3
import pandas as pd

csv_path = r"C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\2025B-Auction-Book.csv"
db_path  = r"C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\assessor.sqlite"

df = pd.read_csv(csv_path)  # if TSV, use: pd.read_csv(csv_path, sep="\t")

con = sqlite3.connect(db_path)
df.to_sql("auction", con, if_exists="replace", index=False)

print(pd.read_sql_query("SELECT COUNT(*) AS n FROM auction;", con))
con.close()
