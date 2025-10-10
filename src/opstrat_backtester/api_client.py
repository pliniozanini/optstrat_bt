
import os
import requests
import pandas as pd
from typing import List

OPLAB_API_BASE_URL = "https://api.oplab.com.br"
ACCESS_TOKEN = os.getenv("OPLAB_ACCESS_TOKEN")

class APIError(Exception):
	"""Custom exception for API-related errors."""
	pass

class OplabClient:
	"""
	A dedicated, reusable HTTP client for all Oplab API interactions.
	"""
	def __init__(self, access_token: str = ACCESS_TOKEN, test_mode: bool = False):
		if not access_token and not test_mode:
			raise ValueError("Oplab access token not found. Set the OPLAB_ACCESS_TOKEN environment variable.")
		self.base_url = OPLAB_API_BASE_URL
		self._session = requests.Session()
		self._session.headers.update({
			"Authorization": f"Bearer {access_token or 'test_token'}",
			"Content-Type": "application/json"
		})

	def _get_json(self, path: str, params: dict = None) -> dict:
		full_url = f"{self.base_url}{path}"
		try:
			response = self._session.get(full_url, params=params)
			response.raise_for_status()
			return response.json()
		except requests.exceptions.HTTPError as e:
			raise APIError(f"HTTP Error for {full_url}: {e.response.status_code} - {e.response.text}") from e
		except requests.exceptions.RequestException as e:
			raise APIError(f"Request failed for {full_url}: {e}") from e

	def historical_options(self, spot: str, target_date: str) -> pd.DataFrame:
		path = f"/market/historical/options/{spot}/{target_date}/{target_date}"
		data = self._get_json(path)
		df = pd.DataFrame(data.get('data', []))
		return df

	def historical_instruments_details(self, tickers: List[str], target_date: str) -> pd.DataFrame:
		if not tickers:
			return pd.DataFrame()
		path = "/market/historical/instruments"
		params = {"tickers": ",".join(tickers), "date": target_date}
		data = self._get_json(path, params=params)
		df = pd.DataFrame(data.get('data', []))
		return df

	def historical_stock(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
		path = f"/market/historical/stock/{symbol}"
		params = {"start_date": start_date, "end_date": end_date}
		data = self._get_json(path, params=params)
		df = pd.DataFrame(data.get('data', []))
		return df
