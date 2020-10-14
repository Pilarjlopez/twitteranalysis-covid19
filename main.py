from datetime import datetime

from twitter_fetcher_to_csv import TwitterFetcherToCsv


def main():
    fetcher = TwitterFetcherToCsv() \
        .with_accounts(['el_pais', 'elmundoes', 'ElMundoData', 'abc_es', 'LaVanguardia', 'elconfidencial', 'ECLaboratorio', 'eldiarioes']) \
        .with_key_words(['coronavirus', 'covid19', 'SARS-CoV-2', 'pandemia']) \
        .with_since(datetime(2020, 2, 1)) \
        .with_until(datetime(2020, 7, 7))
    fetcher.start_process()


if __name__ == "__main__":
    main()
