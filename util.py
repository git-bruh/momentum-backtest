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
    month, year = period
    file = BASE_DIR + f"/indices/{index}_{month}{year}.pdf"
    stonks = pd.DataFrame()
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            table = page.extract_tables()[0]
            stonks = pd.concat(
                [stonks, pd.DataFrame(table[1:], columns=table[0])],
                ignore_index=True,
            )
    return stonks


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
            stonks_map[year][MONTHS_MAP[month]] = set(df["Symbol"])
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
        for stonks_set in stonks_map[year]:
            if stonks_set is not None:
                stonks |= stonks_set

    print(f"Downloading data for {len(stonks)} stonks: {stonks}")

    df = yfinance.download(
        [stonk + ".NS" for stonk in stonks], period="max", interval="1d"
    )
    df.to_pickle(BASE_DIR + "/historical_data.p")


def read_historical_data(rolling_return_periods):
    df = pd.read_pickle(BASE_DIR + "/historical_data.p")
    percentage_change = df["Adj Close"].pct_change(
        periods=rolling_return_periods, fill_method=None
    )
    df[
        [("Percentage Change", stonk) for stonk in percentage_change.columns]
    ] = percentage_change
    return df.copy()


def read_index_data(file):
    df = pd.read_csv(file, index_col="Date")[::-1]  # Reverse order
    df.index = pd.to_datetime(df.index)
    return df


def date_to_str(date):
    year, month = date
    return f"{year}-{MONTHS_MAP[month] + 1:02}-01"
