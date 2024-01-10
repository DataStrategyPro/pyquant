import time
from io import BytesIO
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
from click import progressbar
from logbook import Logger
from six import iteritems
from six.moves.urllib.parse import urlencode

log = Logger(__name__)

DATA_START_DATE = "2000-01-01"
ONE_MEGABYTE = 1024 * 1024
DATALINK_DATA_URL = "https://data.nasdaq.com/api/v3/datatables/QUOTEMEDIA/PRICES"
MAX_DOWNLOAD_TRIES = 5


def format_metadata_url(api_key):
    """Build the query URL for Quandl WIKI Prices metadata."""
    columns = ",".join(
        [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
            "split",
        ]
    )

    query_params = [
        ("date.gte", DATA_START_DATE),
        ("api_key", api_key),
        ("qopts.export", "true"),
        ("qopts.columns", columns),
    ]
    return f"{DATALINK_DATA_URL}?{urlencode(query_params)}"


def fetch_download_link(table_url, max_download_tries=MAX_DOWNLOAD_TRIES):
    log.info(f"Attempting to fetch download link with ...")

    status = None
    cnt = 0

    while status != "fresh" and cnt < max_download_tries:
        log.info(f"Fetching download link...")
        try:
            resp = requests.get(table_url)
            resp.raise_for_status()
        except:
            log.info("Failed to get download link from Quandl")

        payload = resp.json()

        status = payload["datatable_bulk_download"]["file"]["status"]

        if status == "fresh":
            link = payload["datatable_bulk_download"]["file"]["link"]
            log.info(f"Status is {status}. Returning download link: {link}")
            return link

        log.info(f"Status is {status}. Retrying in 10 seconds...")

        time.sleep(10)


def load_data_table(file, index_col=None):
    """Load data table from zip file provided by Quandl."""
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        eod_prices = file_names.pop()
        with zip_file.open(eod_prices) as table_file:
            log.info("Parsing raw data.")
            data_table = pd.read_csv(
                table_file,
                header=0,
                names=[
                    "ticker",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dividend",
                    "split",
                ],
                parse_dates=["date"],
                index_col=index_col,
                usecols=[
                    "ticker",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dividend",
                    "split",
                ],
            ).rename(
                columns={
                    "ticker": "symbol",
                    "dividend": "ex_dividend",
                    "split": "split_ratio",
                }
            )

    return data_table


def fetch_data_table(api_key):
    """Fetch WIKI Prices data table from Quandl"""
    log.info(f"Fetching data table...")

    table_url = format_metadata_url(api_key)
    download_link = fetch_download_link(table_url)
    raw_file = download_with_progress(download_link, chunk_size=ONE_MEGABYTE)

    return load_data_table(file=raw_file)


def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": ["min", "max"]})
    data.reset_index(inplace=True)
    data["start_date"] = data.date.min(axis=1)
    data["end_date"] = data.date.max(axis=1)
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "QUOTEMEDIA"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data


def parse_splits(data, show_progress):
    if show_progress:
        log.info("Parsing split data.")

    data["split_ratio"] = 1.0 / data.split_ratio
    data.rename(
        columns={"split_ratio": "ratio", "date": "effective_date"},
        inplace=True,
        copy=False,
    )
    return data


def parse_dividends(data, show_progress):
    if show_progress:
        log.info("Parsing dividend data.")

    data["record_date"] = data["declared_date"] = data["pay_date"] = pd.NaT
    data.rename(
        columns={"ex_dividend": "amount", "date": "ex_date"}, inplace=True, copy=False
    )
    return data


def parse_pricing_and_vol(data, sessions, symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = (
            data.xs(symbol, level=1).reindex(sessions.tz_localize(None)).fillna(0.0)
        )
        yield asset_id, asset_data


def daily_us_equities_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    """
    daily_us_equities_bundle builds a daily dataset using Quotemedia
    end of day equities data. For more information on the Quotemedia
    data see here: https://data.nasdaq.com/databases/EOD
    """
    api_key = environ.get("DATALINK_API_KEY")
    if api_key is None:
        raise ValueError(
            "Please set your DATALINK_API_KEY environment variable and retry."
        )

    raw_data = fetch_data_table(api_key)

    start_session, end_session = raw_data.date.min(), raw_data.date.max()
    asset_metadata = gen_asset_metadata(raw_data[["symbol", "date"]], show_progress)

    exchanges = pd.DataFrame(
        data=[["QUOTEMEDIA", "QUOTEMEDIA", "US"]],
        columns=["exchange", "canonical_name", "country_code"],
    )
    asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)

    raw_data.set_index(["date", "symbol"], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(raw_data, sessions, symbol_map),
        show_progress=show_progress,
    )

    raw_data.reset_index(inplace=True)
    raw_data["symbol"] = raw_data["symbol"].astype("category")
    raw_data["sid"] = raw_data.symbol.cat.codes
    adjustment_writer.write(
        splits=parse_splits(
            raw_data[["sid", "date", "split_ratio"]].loc[raw_data.split_ratio != 1],
            show_progress=show_progress,
        ),
        dividends=parse_dividends(
            raw_data[["sid", "date", "ex_dividend"]].loc[raw_data.ex_dividend != 0],
            show_progress=show_progress,
        ),
    )


def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.
    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.
    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers["content-length"])
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data