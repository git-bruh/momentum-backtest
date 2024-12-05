import pandas as pd
import matplotlib.pyplot as plt
import util
import backtest

YEARS = [2014, 2015, 2016, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
# Though we have the index data for each month, we follow the regular
# rebalancing schedule
MONTHS = ["Mar", "Sep"]

INDEX = "NIFTY_200"
ROLLING_RETURNS_PERIOD = 20 * 6

periods = []
for year in YEARS:
    for month in MONTHS:
        periods.append((month, year))

stonks_map = util.get_index_constituents(INDEX, periods)

try:
    backtest = backtest.Backtest()
except FileNotFoundError:
    util.download_historical_data(stonks_map)
    backtest = backtest.Backtest()

pf_nav = backtest.run(YEARS, stonks_map)
index_nav = util.read_index_data(
    "data/NIFTY200 MOMENTUM 30_Historical_PR_01122009to03122024.csv"
)

df = pd.DataFrame(
    {"N200BRUH30": pf_nav, "N200MOM30": index_nav.loc[pf_nav.index]["Close"]}
)

plt.figure(figsize=(10, 6))
(df.pct_change(periods=ROLLING_RETURNS_PERIOD, fill_method=None) * 100).plot()
plt.title(f"Rolling Returns ({ROLLING_RETURNS_PERIOD // 20} Months)")
plt.xlabel("Date")
plt.ylabel("Rolling Returns")
plt.legend()
plt.grid(True)
plt.savefig("returns.png")
