import asyncio
import logging.config
import math
import platform
import random
import time
from pathlib import Path
from typing import Optional, Union
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep, before_log

from datetime import datetime
import pytz

import aiohttp
import orjson
from httpx import Client

from .constants import *
from .login import login
from .util import set_qs, get_headers

reset = '\u001b[0m'
colors = [f'\u001b[{i}m' for i in range(30, 38)]
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)

if platform.system() != 'Windows':
	import uvloop

class Search:
	def __init__(self, login_session: login, search_config: dict):
		self.api = 'https://api.twitter.com/2/search/adaptive.json?'
		self.login_session = login_session
		self.search_config = search_config

	def run_batch(self, queries: Union[str, list[str]], limit: Optional[int] = None) -> list:
		queries = [queries] if isinstance(queries, str) else queries

		with Client(headers=get_headers(self.login_session)) as login_session:
			return [[tweet for tweet in self.paginate(query, login_session, limit)] for query in queries]

	def run(self, query: str, limit: Optional[int] = None) -> list:
		with Client(headers=get_headers(self.login_session)) as login_session:
			for tweet in self.paginate(query, login_session, limit):
				yield tweet

	def paginate(self, query: str, session: Client, limit: Optional[int] = None) -> list[dict]:
		next_cursor = True
		ids = set()
		all_data = []

		search_config = self.search_config.copy()
		search_config['q'] = query

		while next_cursor:
			if isinstance(next_cursor, str):
				search_config['cursor'] = next_cursor
			
			r, data, next_cursor = self.get(session, search_config)

			parsed_tweets = self.parse_page(data)
			for tweet in parsed_tweets:
				yield tweet

			ids |= set(data['globalObjects']['tweets'])
			if limit and len(ids) >= limit:
				return all_data

			if len(data['globalObjects']['tweets']) < 20:
				next_cursor = None

		return all_data

	@staticmethod
	def parse_page(data: dict) -> list[dict]:

		tweeters = {}
		for tweeter_id_str in data['globalObjects']['users']:
			tweeter = data['globalObjects']['users'][tweeter_id_str]

			tweeters[tweeter_id_str] = {
					'user_screen_name': tweeter['screen_name'],
					'user_name': tweeter['name'],
					'user_followers': tweeter['followers_count'],
					'user_profile_image_url': tweeter['profile_image_url_https'].replace('_normal', '400x400'),
				}

		tweets = []
		for tweet_id_str in data['globalObjects']['tweets']:
			tweet = data['globalObjects']['tweets'][tweet_id_str]
			
			item = {
					'id': tweet['id_str'],
					'collection_time': datetime.now().isoformat(),
					'impression_count': tweet['ext_views']['count'],
					'like_count': tweet['favorite_count'],
					'reply_count': tweet['reply_count'],
					'reply_count': tweet['reply_count'],
					'local_time': Search.parse_date(tweet['created_at']),
					'text': tweet['full_text'],
					'media': [{
						'type': media['type'],
						'url': media['media_url_https'],
					} for media in tweet['entities']['media']] if 'media' in tweet['entities'] else None,
					'to_tweetid': tweet.get('in_reply_to_status_id_str', None),
					'quoted_id': tweet.get('quoted_status_id_str', None),
				}
			item.update(tweeters[tweet['user_id_str']])
			tweets.append(item)
		
		return tweets

	@staticmethod
	def parse_date(utc_datetime_str: str) -> datetime:
		utc_dt = datetime.strptime(utc_datetime_str, "%a %b %d %H:%M:%S +0000 %Y")
		return utc_dt.strftime("%Y-%m-%d %H:%M:%S")

	@retry(stop=stop_after_attempt(12), wait=wait_exponential(multiplier=1, min=1, max=60), before_sleep=before_log(logger, logging.DEBUG))
	def get(self, session: Client, params: dict) -> tuple:
		url = set_qs(self.api, params, update=True, safe='()')

		r = session.get(url)
		data = r.json()
		next_cursor = self.get_cursor(data)

		if not data.get('globalObjects', {}).get('tweets'):
			raise Exception

		return r, data, next_cursor

	def get_cursor(self, res: dict):
		for instr in res['timeline']['instructions']:
			if replaceEntry := instr.get('replaceEntry'):
				cursor = replaceEntry['entry']['content']['operation']['cursor']
				if cursor['cursorType'] == 'Bottom':
					return cursor['value']
				continue
			for entry in instr['addEntries']['entries']:
				if entry['entryId'] == 'sq-cursor-bottom':
					return entry['content']['operation']['cursor']['value']