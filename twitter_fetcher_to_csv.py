import os
import emoji
import csv
from bluebird import BlueBird
from datetime import datetime
from datetime import timedelta
from csv import DictWriter


class TwitterFetcherToCsv:
    _accounts = []
    _key_words = []
    _since = datetime(1970, 1, 1)
    _until = datetime(1970, 1, 1)
    _regenerate_csv = True
    _fetched_tweets_id = []
    _csv_field_names = ['id', 'id_str', 'created_at', 'tweet_url', 'full_text', 'entities', 'retweeted',
                        'supplemental_language', 'possibly_sensitive_editable', 'lang', 'in_reply_to_user_id', 'geo',
                        'user_id', 'truncated', 'in_reply_to_status_id', 'source', 'conversation_id_str', 'user_id_str',
                        'possibly_sensitive', 'in_reply_to_screen_name', 'in_reply_to_status_id_str', 'retweet_count',
                        'favorite_count', 'coordinates', 'in_reply_to_user_id_str', 'place', 'is_quote_status',
                        'display_text_range', 'contributors', 'favorited', 'conversation_id', 'extended_entities',
                        'self_thread', 'quoted_status_permalink', 'quoted_status_id_str', 'quoted_status_id',
                        'withheld_copyright', 'withheld_in_countries', 'withheld_scope']

    def with_accounts(self, accounts):
        self._accounts = accounts
        return self

    def with_key_words(self, key_words):
        self._key_words = key_words
        return self

    def with_since(self, since):
        self._since = since
        return self

    def with_until(self, until):
        self._until = until
        return self

    def with_regenerate_csv(self, regenerate_csv):
        self._regenerate_csv = regenerate_csv
        return self

    def start_process(self):
        print(emoji.emojize(":snake::snake::snake: Process started :snake::snake::snake:"))
        self._initialize_files()
        self._fetch_all_data_to_csv()
        print(emoji.emojize(":snake::snake::snake: Process finished :snake::snake::snake:"))

    def _initialize_files(self):
        if self._regenerate_csv:
            self._delete_all_csv_files()
        if not os.path.exists("data"):
            os.makedirs("data")
        if self._regenerate_csv:
            self._create_all_csv_files()

    def _delete_all_csv_files(self):
        for account in self._accounts:
            file_path = f"data/{account}.csv"
            if os.path.exists(file_path):
                os.remove(file_path)

    def _create_all_csv_files(self):
        for account in self._accounts:
            file_path = f"data/{account}.csv"
            with open(file_path, 'w', newline='') as file:
                wr = csv.writer(file, quoting=csv.QUOTE_ALL)
                wr.writerow(self._csv_field_names)

    def _fetch_all_data_to_csv(self):
        for account in self._accounts:
            self._fetch_tweets_for_account(account)

    def _fetch_tweets_for_account(self, account):
        print(emoji.emojize(f":newspaper: Fetching tweets for {account} :newspaper:"))
        date = self._since
        while date <= self._until:
            self._fetch_tweets_for_account_and_date(account, date)
            date = date + timedelta(days=1)

    def _fetch_tweets_for_account_and_date(self, account, date):
        print(emoji.emojize(f":calendar: Fetching tweets for {date}"))
        query = self._get_query(account, date)
        counter = self._process_tweets(account, BlueBird().search(query))
        print(emoji.emojize(f":bird: Fetched {counter} tweets"))
        print()

    def _get_query(self, account, date):
        return {
            'fields': [
                {'items': self._key_words},
                {'items': [account], 'target': 'from', 'match': 'any'},
            ],
            'since': date.strftime("%Y-%m-%d"),
            'until': (date + timedelta(days=1)).strftime("%Y-%m-%d")
        }

    def _process_tweets(self, account, tweets):
        counter = 0
        for tweet in tweets:
            if tweet['id'] in self._fetched_tweets_id:
                break
            tweet['tweet_url'] = f"https://twitter.com/{account}/status/{tweet['id']}"
            self._save_tweet_to_file(f"{account}.csv", tweet)
            self._fetched_tweets_id.append(tweet['id'])
            counter += 1
        return counter

    def _save_tweet_to_file(self, file_name, dict_of_elem):
        with open(f"data/{file_name}", 'a+', newline='') as write_obj:
            dict_writer = DictWriter(write_obj, fieldnames=self._csv_field_names)
            dict_writer.writerow(dict_of_elem)
