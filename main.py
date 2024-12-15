import pandas as pd
import matplotlib.pyplot as plt
import util
import backtest
import sys
import pickle
import argparse

YEARS = [2014, 2015, 2016, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
# Though we have the index data for each month, we follow the regular
# rebalancing schedule
MONTHS = ["Mar", "Sep"]

YEARS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]

parser = argparse.ArgumentParser(description="backtester")
parser.add_argument("--index-universe", default="NIFTY 500", type=str)
parser.add_argument("--comparison-period-months", default=12, type=int)
parser.add_argument("--num-stocks", default=30, type=int)
parser.add_argument("--rebalance-frequency-months", default=6, type=int)
parser.add_argument("--consider-returns-months", default=6, type=int)
args = parser.parse_args()

periods = []
for year in YEARS:
    for month in MONTHS:
        periods.append((month, year))

stonks_map = util.get_index_constituents(args.index_universe, periods)

nifty200 = util.read_index_data(
    "data/NIFTY200 MOMENTUM 30_Historical_PR_01122009to03122024.csv"
)
nifty500 = util.read_index_data(
    "data/NIFTY500 MOMENTUM 50_Historical_PR_01122006to14122024.csv"
)
niftymidcap150 = util.read_index_data(
    "data/NIFTY MIDCAP150 MOMENTUM 50_Historical_PR_01122006to14122024.csv"
)
nifty50 = util.read_index_data(
    "data/NIFTY 50_Historical_PR_01122012to06122024.csv"
)

try:
    backtest = backtest.Backtest(
        rebalance_frequency=args.rebalance_frequency_months,
        periods_to_consider=util.PERIODS_IN_MONTH
        * args.consider_returns_months,
        num_stocks=args.num_stocks,
        index_dates=nifty50.index,
    )
except FileNotFoundError:
    util.download_historical_data(stonks_map)
    print("Historical data downloaded")
    sys.exit()

pf_nav = backtest.run(YEARS, stonks_map)

df = pd.DataFrame(
    {
        "BRUHMOMENTUM": pf_nav,
        # "NIFTY50 PR": nifty50.loc[pf_nav.index]["Close"],
        "N200MOM30 PR": nifty200.loc[pf_nav.index]["Close"],
        "N500MOM50 PR": nifty500.loc[pf_nav.index]["Close"],
        # "NM150MOM50 PR": niftymidcap150.loc[pf_nav.index]["Close"],
    }
)

(
    df.pct_change(periods=20 * args.comparison_period_months, fill_method=None)
    * 100
).plot()
plt.title(
    f"{args.consider_returns_months} Mo. Return, {args.rebalance_frequency_months} Mo. Rebalance - {args.num_stocks} Stocks ({args.index_universe})"
)
plt.xlabel("Date")
plt.ylabel(f"Rolling Returns ({args.comparison_period_months}) Months)")
plt.legend()
plt.grid(True)
plt.savefig("returns.png")
