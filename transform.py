import sqlite3, pandas as pd

db_path = r"C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\assessor.sqlite"
con = sqlite3.connect(db_path)
cur = con.cursor()

# 1) Normalize join keys (trim; remove dashes if needed) — do this once.
cur.executescript("""
UPDATE auction  SET AIN        = TRIM(AIN);
UPDATE assessor SET AssessorID = TRIM(AssessorID);
""")
con.commit()

# If formats differ (e.g., assessor has dashes), normalize both sides the same way:
# cur.execute("UPDATE assessor SET AssessorID = REPLACE(AssessorID, '-', '')")
# cur.execute("UPDATE auction  SET AIN        = REPLACE(AIN,        '-', '')")
# con.commit()

# 2) Create indexes (critical for speed)
cur.execute("CREATE INDEX IF NOT EXISTS ix_assessor_AssessorID ON assessor(AssessorID);")
cur.execute("CREATE INDEX IF NOT EXISTS ix_auction_AIN        ON auction(AIN);")
con.commit()

print('next')
# (Optional) See if SQLite will use the index:
# print(cur.execute("""
# EXPLAIN QUERY PLAN
# SELECT au.*, a.*
# FROM auction au
# LEFT JOIN assessor a ON a.AssessorID = au.AIN;
# """).fetchall())

# 3) Now run the join (SQLite does the heavy work; pandas just fetches)
df_merged = pd.read_sql_query("""
SELECT au.*, a.*
FROM auction au
LEFT JOIN assessor a ON a.AssessorID = au.AIN
""", con)

df_merged.to_csv(r'C:\Users\Roshi\Documents\Git_Repos\la_tax_auction_parse\data\auction_enhanced.csv', index=False)

print(df_merged.shape)
con.close()
