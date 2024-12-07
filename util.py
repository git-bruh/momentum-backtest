import pdfplumber
import pandas as pd
import multiprocessing
import yfinance

BASE_DIR = "./data"

ALL_MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

MONTHS_MAP = {}
for idx, month in enumerate(ALL_MONTHS):
    MONTHS_MAP[month] = idx


def extract_index_constituents(index, period):
    index_map = {
        "NIFTY_200": ["cnx200", "nifty200", "NIFTY_200"],
        "NIFTY_500": ["cnx500", "nifty500", "NIFTY_500"],
    }

    month, year = period

    for alt in index_map.get(index, [index]):
        file = BASE_DIR + f"/indices/{alt}_{month}{year}.pdf"
        try:
            with pdfplumber.open(file) as pdf:
                stonks = pd.DataFrame()
                for page in pdf.pages:
                    table = page.extract_tables()[0]
                    stonks = pd.concat(
                        [stonks, pd.DataFrame(table[1:], columns=table[0])],
                        ignore_index=True,
                    )
                return stonks
        except FileNotFoundError:
            continue

    raise Exception(f"no data found for {index} in period {period}")


def get_index_constituents(index, periods):
    stonks_map = {}
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        for period, df in zip(
            periods,
            pool.starmap(
                extract_index_constituents,
                ((index, period) for period in periods),
            ),
        ):
            month, year = period
            if year not in stonks_map:
                stonks_map[year] = [None] * 12
            stonks_map[year][MONTHS_MAP[month]] = list(df["Symbol"])
    return stonks_map


def find_closest_index_stonks(stonks_map, date):
    year, month = date
    month = MONTHS_MAP[month]
    while year in stonks_map:
        if month == 0:
            year -= 1
            month = 11
            continue
        month -= 1
        if (stonks := stonks_map[year][month]) is not None:
            return [stonk + ".NS" for stonk in stonks]
    return None


def download_historical_data(stonks_map):
    stonks = set()
    for year in stonks_map:
        for stonks_list in stonks_map[year]:
            if stonks_list is not None:
                stonks |= set(stonks_list)

    print(f"Downloading data for {len(stonks)} stonks: {stonks}")

    df = yfinance.download(
        [stonk + ".NS" for stonk in stonks], period="max", interval="1d"
    )
    df.to_pickle(BASE_DIR + "/historical_data.p")


def read_historical_data(rolling_return_periods, index_dates):
    df = pd.read_pickle(BASE_DIR + "/historical_data.p")
    # Get only the common dates to avoid NaN values from trading holidays
    df = df.loc[sorted(list(set(index_dates) & set(df.index)))]
    # Muhurat trades
    df = df.drop(labels=["2014-10-23", "2015-11-11"])

    # Dummy entry for liquidbees
    df.loc[:, ("Adj Close", "LIQUIDBEES.NS")] = [1] * len(df.index)

    percentage_change = df["Adj Close"].pct_change(
        periods=rolling_return_periods, fill_method=None
    )
    df[
        [("Percentage Change", stonk) for stonk in percentage_change.columns]
    ] = percentage_change

    dma200 = df["Adj Close"].ewm(span=200, adjust=False).mean()
    df[[("DMA 200", stonk) for stonk in dma200.columns]] = dma200

    positive_closing = (df["Adj Close"].diff() > 0).rolling(
        window=rolling_return_periods
    ).sum() > (rolling_return_periods / 2)
    df[[("Positive Closing", stonk) for stonk in positive_closing.columns]] = (
        positive_closing
    )

    return df.copy()


def read_index_data(file):
    df = pd.read_csv(file, index_col="Date")[::-1]  # Reverse order
    df.index = pd.to_datetime(df.index)
    return df


def date_to_str(date):
    year, month = date
    return f"{year}-{MONTHS_MAP[month] + 1:02}-01"
