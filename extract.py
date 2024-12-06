import pandas as pd
import matplotlib.pyplot as plt
import util
import backtest

YEARS = [2014, 2015, 2016, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
# Though we have the index data for each month, we follow the regular
# rebalancing schedule
MONTHS = ["Mar", "Sep"]

YEARS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]

INDEX = "NIFTY_200"
ROLLING_RETURNS_PERIOD = 20 * 12 * 1

periods = []
for year in YEARS:
    for month in MONTHS:
        periods.append((month, year))

stonks_map = util.get_index_constituents(INDEX, periods)

nifty200 = util.read_index_data(
    "data/NIFTY200 MOMENTUM 30_Historical_PR_01122009to03122024.csv"
)
nifty50 = util.read_index_data(
    "data/NIFTY 50_Historical_PR_01122012to06122024.csv"
)

try:
    backtest = backtest.Backtest(
        rebalance_frequency=1,
        periods_to_consider=20 * 1,
        index_dates=nifty50.index,
    )
except FileNotFoundError:
    util.download_historical_data(stonks_map)
    backtest = backtest.Backtest()

pf_nav = backtest.run(YEARS, stonks_map)

df = pd.DataFrame(
    {
        "N200BRUH30": pf_nav,
        "NIFTY50 PR": nifty50.loc[pf_nav.index]["Close"],
        "N200MOM30 PR": nifty200.loc[pf_nav.index]["Close"],
    }
)

(df.pct_change(periods=ROLLING_RETURNS_PERIOD, fill_method=None) * 100).plot()
plt.title(f"Rolling Returns ({ROLLING_RETURNS_PERIOD // 20} Months)")
plt.xlabel("Date")
plt.ylabel("Rolling Returns")
plt.legend()
plt.grid(True)
plt.savefig("returns.png")
