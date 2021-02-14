from datetime import datetime, timedelta
from enum import Enum
from typing import Collection
from cbpro import PublicClient
from time import sleep
from pandas.core.frame import DataFrame
from pymongo import MongoClient
from pymongo.database import Database
from tqdm import tqdm
import argparse
import cbpro
import pandas
from config import Config


class Granularity(Enum):
    ONE_MINUTE = 1,
    FIVE_MINUTES = 2,
    FIFTEEN_MINUTES = 3,
    ONE_HOUR = 4,
    SIX_HOUR = 5,
    ONE_DAY = 6


# Time periods to query granularity
granularity_time_increment_map = {
    Granularity.ONE_MINUTE: timedelta(hours=4),
    Granularity.FIFTEEN_MINUTES: timedelta(hours=24),
    Granularity.FIFTEEN_MINUTES: timedelta(hours=72),
    Granularity.ONE_HOUR: timedelta(hours=288),
    Granularity.SIX_HOUR: timedelta(hours=1728),
    Granularity.ONE_DAY: timedelta(hours=7200)
}

granularity_default_time_period = {
    Granularity.ONE_MINUTE: timedelta(days=1),
    Granularity.FIFTEEN_MINUTES: timedelta(days=5),
    Granularity.FIFTEEN_MINUTES: timedelta(days=15),
    Granularity.ONE_HOUR: timedelta(days=30),
    Granularity.SIX_HOUR: timedelta(days=180),
    Granularity.ONE_DAY: timedelta(days=365)
}

# Granularity description map
granularity_option_map = {
    '1m': Granularity.ONE_MINUTE,
    '5m': Granularity.ONE_MINUTE,
    '15m': Granularity.FIFTEEN_MINUTES,
    '1hr': Granularity.ONE_HOUR,
    '6h': Granularity.SIX_HOUR,
    '1d': Granularity.ONE_DAY
}


def find_time_span(**kwargs):
    start_time = kwargs.get('start_time', None)
    end_time = kwargs.get('end_time', None)
    # TODO: Add timespan eventually
    #time_span = kwargs.get('end_time', None)
    m_granularity = kwargs.get('granularity', Granularity.ONE_MINUTE)

    if (start_time is not None) and (end_time and not None):
        if start_time > end_time:
            raise ValueError("start time cannot be after end time")

        return start_time, end_time

    # TODO: We need to figure out a time span format and then parse that for this function
    # if time_span:
    #     now = datetime.now()
    #     now.second = 0
    #
    #     start_time = now
    #     end_time = now - time_span
    #
    #     return start_time, end_time

    now = datetime.now()

    time_delta = granularity_default_time_period[m_granularity]

    return now - time_delta, now


def seconds_to_hours(time_span):
    return time_span.total_seconds() // 3600


def process(m_granularity: Granularity = Granularity.ONE_MINUTE, **kwargs) -> None:
    start_time, end_time = find_time_span(
        time_span=kwargs.get('time_span'),
        start_time=kwargs.get('start_time'),
        end_time=kwargs.get('end_time'),
        granularity=m_granularity
    )

    host = Config['mongo_db']['host']
    port = Config['mongo_db']['port']
    db_name = Config['mongo_db']['database_name']
    historical_candle_stick_suffix = Config['mongo_db']['historical_candle_stick_suffix']

    mongo_client = MongoClient(host, port)

    db: Database = mongo_client[db_name]

    cb_public_client: PublicClient = cbpro.PublicClient()

    product_collection = cb_public_client.get_products()

    for product in product_collection:
        get_historic_candles_for_product(
            cb_public_client,
            db,
            historical_candle_stick_suffix,
            product,
            start_time,
            end_time,
            m_granularity=m_granularity)


def get_new_records(df: DataFrame, collection: Collection):
    scraped_timestamps = df['timestamp'].to_list()

    cursor = collection.find({'timestamp': {'$in': scraped_timestamps}}, {'_id': 0, 'timestamp': 1})

    existing_timestamps = [result['timestamp'] for result in cursor]

    records = df.to_dict('records')

    return [record for record in records if record['timestamp'] not in existing_timestamps]


def get_historic_candles_for_product(
        cb_public_client: PublicClient,
        db: Database,
        historical_data_suffix: str,
        product: dict,
        start_time: datetime,
        end_time: datetime,
        m_granularity=Granularity.ONE_MINUTE) -> None:
    granularity_desc = next(key for key, val in granularity_option_map.items() if val == m_granularity)

    collection_name = f'{product["id"].lower()}-{granularity_desc}-{historical_data_suffix}'

    collection = db[collection_name]

    current_time = start_time
    time_diff = end_time - start_time
    time_increment = granularity_time_increment_map[m_granularity]

    request_count = 0

    bar_title = f'Processing {product["id"]}'
    bar_length = seconds_to_hours(time_diff)

    total_record_count = 0

    with tqdm(total=bar_length, desc=bar_title) as progress_bar:
        while current_time < end_time:
            # coinbase pro public api has a rate limit of 3 requests per second.
            # So for every three requests we will wait 1 second.
            if request_count == 3:
                sleep(1)
                request_count = 0

            previous_time = current_time

            current_time = previous_time + time_increment

            candle_stick_data = cb_public_client.get_product_historic_rates(
                product["id"],
                start=previous_time.isoformat(),
                end=current_time.isoformat(),
                granularity=300)

            # put our data into a pandas dataframe for easy processing
            df = pandas.DataFrame(
                candle_stick_data,
                columns=['timestamp', 'low', 'high', 'open', 'close', 'volume'])

            records = get_new_records(df, collection)

            if len(records):
                total_record_count = total_record_count + len(records)

                collection.insert_many(records)

            request_count = request_count + 1

            progress_bar.update(seconds_to_hours(time_increment))
            progress_bar.desc = f'Processing {product["id"]} : Record count {total_record_count}'


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Gets the historical candlestick data from coinbase')

        parser.add_argument('-g', '--granularity', choices=list(granularity_option_map.keys()),
                            required=True, help="the granularity of the candle sticks to collect")
        parser.add_argument('-s', '--start', required=False, type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                            help="start time in the format Y-m-d")
        parser.add_argument('-e', '--end', required=False, type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                            help="snd time in the format Y-m-d")
        # parser.add_argument('-t', '--time-span', required=False, help="gets a number of days in the past", type=int)

        args = parser.parse_args()
        granularity: Granularity = granularity_option_map[args.granularity]

        if (args.start and not args.end) or (args.end and not args.start):
            process(granularity, start_time=args.start, end_time=args.end)
        # elif args.time_span:
        #     process(granularity, time_span=args.timespan)
        else:
            process(granularity)
    except KeyboardInterrupt as kex:
        exit(0)
    # TODO: We need to handle errors properly
    # except ValueError as vex:
    #     print(vex)
    # except Exception as ex:
    #     print(ex)
    #     print(ex)