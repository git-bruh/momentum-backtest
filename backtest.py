import pandas as pd
import util


class Backtest:
    def __init__(
        self,
        amount=10000,
        rebalance_frequency=6,
        periods_to_consider=util.PERIODS_IN_MONTH * 6,
        num_stocks=30,
        index_dates=[],
    ):
        self.amount = amount
        self.rebalance_frequency = rebalance_frequency
        self.historical_df = util.read_historical_data(
            periods_to_consider, index_dates
        )
        self.num_stocks = num_stocks

    def run(self, years, stonks_map):
        last_pf = set()
        last_index = None
        last_qty = {}

        count = 0
        amount = self.amount

        pf_nav = pd.Series()

        for year in years:
            for month in util.ALL_MONTHS:
                date = (year, month)

                stonks = util.find_closest_index_stonks(stonks_map, date)
                if stonks is None:
                    print(f"Skipping {date}")
                    continue

                count += 1
                skip = ((count - 1) % self.rebalance_frequency) != 0

                index = self.historical_df.index[
                    self.historical_df.index >= util.date_to_str(date)
                ][0]

                if last_index is not None:
                    monthly_df = (
                        self.historical_df["Adj Close"][list(last_pf)].loc[
                            last_index:index
                        ]
                        * last_qty
                    )

                    for key, val in monthly_df.sum(axis=1).items():
                        pf_nav[key] = val

                    # new amount is the latest df entry
                    amount = monthly_df.iloc[-1].sum()

                if skip:
                    continue

                filter = []
                for col in (
                    "Adj Close",
                    "DMA 50",
                    "DMA 200",
                    "Percentage Change",
                    "Positive Closing",
                    "Momentum Ratio 6 Months",
                    "Momentum Ratio 12 Months",
                ):
                    for stonk in stonks:
                        filter.append((col, stonk))
                on_date = self.historical_df.loc[index][filter]
                n = self.num_stocks

                if False:
                    momentum_ratio = on_date["Momentum Ratio 12 Months"]
                    zscore_12mo = (
                        momentum_ratio - momentum_ratio.mean()
                    ) / momentum_ratio.std()

                    momentum_ratio = on_date["Momentum Ratio 6 Months"]
                    zscore_6mo = (
                        momentum_ratio - momentum_ratio.mean()
                    ) / momentum_ratio.std()

                    normalized_zscore = (
                        (0.5 * zscore_12mo) + (0.5 * zscore_6mo)
                    ).map(
                        lambda score: (
                            (1 + score) if score >= 0 else (1 - score) ** -1
                        )
                    )

                    new_pf = set(
                        normalized_zscore.sort_values(ascending=False)
                        .dropna()
                        .head(n)
                        .index
                    )
                else:
                    # TODO we can't buy fractional stonks
                    # filtered = on_date['Adj Close'] <= (amount / 30)
                    filtered = on_date["Positive Closing"] & (
                        on_date["Adj Close"] > on_date["DMA 200"]
                    )

                    # TODO historical data for few stonks is missing due to mergers/delisting
                    # drop nan values here
                    periodic_returns_df = (
                        on_date["Percentage Change"][filtered]
                        .sort_values(ascending=False)
                        .dropna()
                        * 100
                    )

                    new_pf = set(periodic_returns_df.head(n).index)

                last_index = index
                last_qty = {}

                # Allocate the amount equally across stonks
                for stonk in new_pf:
                    price = on_date["Adj Close"][stonk]
                    last_qty[stonk] = (amount / n) / price

                if len(new_pf) < n:
                    print(
                        f"too less eligible stocks at {index}, want {n} got {len(new_pf)}: {new_pf}"
                    )
                    # add the remaining amount to LIQUIDBEES
                    last_qty["LIQUIDBEES.NS"] = (amount / n) * (
                        n - len(new_pf)
                    )
                    new_pf.add("LIQUIDBEES.NS")
                elif len(new_pf) > n:
                    raise Exception(
                        "{len(new_pf)} stocks exceed limit {n} at {index}: {new_pf}"
                    )

                exits = last_pf - new_pf
                entries = new_pf - last_pf

                last_pf = new_pf

                print(f"{date}\nEntries: {entries}\nExits: {exits}\n")

        return pf_nav
