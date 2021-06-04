from datetime import datetime, timedelta
import typing as t

import cbpro
import pyarrow as pa
import pyarrow.parquet as pq

from batch_ingest.candlesticks import CoinbaseCandleCollector


class MarketDownloader:
    def __init__(self, root_path: str,
                 fs: pa.fs.FileSystem = pa.fs.LocalFileSystem()):
        self.root_path = root_path
        self.fs = fs

    def download(self, start: datetime, end: datetime,
                 markets: t.Optional[t.List[str]] = None) -> None:
        frequencies = {'1m': timedelta(minutes=1)}
        if markets is None:
            products = cbpro.PublicClient().get_products()
            markets = [description['id'] for description in products]
        collector = CoinbaseCandleCollector()
        for freq_name, frequency in frequencies.items():
            df = collector.get_markets(markets, start, end, frequency)
            df = df.reset_index()
            df['freq'] = freq_name
            df['date'] = df['time'].dt.strftime('%F')
            df = df.set_index(['freq', 'market', 'time'])
            tbl = pa.Table.from_pandas(df)
            pq.write_to_dataset(tbl, self.root_path, filesystem=self.fs,
                                partition_cols=['date', 'freq'])


if __name__ == '__main__':
    downloader = MarketDownloader('../.market_data')
    downloader.download(datetime(2021, 5, 1), datetime(2021, 5, 30, 20, 46))
