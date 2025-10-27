
from transform.stg_table_prozessor import StgProzessor
import pandas as pd
from utils.paths import DATA_DIR

stg_prozessor = StgProzessor()
df = pd.read_csv(DATA_DIR / "DAX_2025-10-13.csv")

df = stg_prozessor.normalize_df_for_stg_prices(df, "DAX")

print(df)


