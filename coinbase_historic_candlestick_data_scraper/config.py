import configparser
from pathlib import Path


def get_config() -> dict:
    file = Path.home().joinpath('.cb-candlesticks.ini')
    config = configparser.ConfigParser()
    config.read(file)

    host = 'localhost'
    port = 27017
    database = 'coinbase-pro'
    collection_suffix = 'historical-candlestick-data'

    if 'mongo_db' in config:
        host = config['mongo_db'].get('host', host)
        port = config['mongo_db'].getint('port', port)
        database = config['mongo_db'].get('database', database)
        collection_suffix = config['mongo_db'].get('collection_suffix', collection_suffix)

    return {
        'mongo_db': {
            'host': host,
            'port': port,
            'database': database,
            'collection_suffix': collection_suffix
        }
    }


Config = get_config()
