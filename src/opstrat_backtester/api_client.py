# src/opstrat_backtester/api_client.py
import traceback
import os
import requests
import pandas as pd
from typing import List, Dict, Any
import json

# 1. Corrigir a URL base da API para incluir /v3
OPLAB_API_BASE_URL = "https://api.oplab.com.br/v3"
ACCESS_TOKEN = os.getenv("OPLAB_ACCESS_TOKEN")

class APIError(Exception):
    """Custom exception for API-related errors."""
    pass

class OplabClient:
    """
    A dedicated, reusable HTTP client for all Oplab API interactions,
    updated for the v3 API.
    """
    def __init__(self, access_token: str = ACCESS_TOKEN, test_mode: bool = False):
        if not access_token and not test_mode:
            raise ValueError("Oplab access token not found. Set the OPLAB_ACCESS_TOKEN environment variable.")
        
        self.base_url = OPLAB_API_BASE_URL
        self._session = requests.Session()
        
        # 2. Corrigir o cabeçalho de autenticação para "Access-Token"
        self._session.headers.update({
            "Access-Token": access_token or 'test_token',
            "Content-Type": "application/json"
        })

    def _get_json(self, path: str, params: dict = None) -> Dict | List:
        full_url = f"{self.base_url}{path}"
        

        try:
            response = self._session.get(full_url, params=params)
            response.raise_for_status()

            # Gracefully handle an empty response
            # if not response.text:
            #     return [] # Return an empty list instead of crashing

            return response.json()

        except requests.exceptions.HTTPError as e:
            raise APIError(f"HTTP Error for {full_url}: {e.response.status_code} - {e.response.text}") from e
        except json.JSONDecodeError as e:
            # This will catch the "Expecting value" error and provide more context
            print(f"FATAL: Failed to decode JSON from response. Text was: '{response.text}'")
            # Full url and params for debugging in browser
            print(f"FATAL: Full URL was: {full_url+'?' + requests.compat.urlencode(params) if params else full_url}")
            raise APIError("Failed to decode JSON response from API.") from e
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed for {full_url}: {e}") from e

    def historical_options(self, spot: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Endpoint: /market/historical/options/{spot}/{from}/{to}
        """
        path = f"/market/historical/options/{spot}/{start_date}/{end_date}"
        # A resposta deste endpoint é uma lista, não um objeto com a chave 'data'
        data = self._get_json(path)
        df = pd.DataFrame(data)
        return df

    def historical_instruments_details(self, tickers: List[str], target_date: str) -> pd.DataFrame:
        """
        Busca detalhes de múltiplos instrumentos em uma data específica.
        Endpoint: /market/historical/instruments
        """
        if not tickers:
            return pd.DataFrame()

        all_details = []
        batch_size = 200  # Processar em lotes de 100 para segurança

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            path = "/market/historical/instruments"
            params = {"tickers": ",".join(batch), "date": target_date}
            
            try:
                data = self._get_json(path, params=params)
                if data:
                    all_details.extend(data)
            except APIError as e:
                print(f"Warning: API error fetching details for batch on {target_date}: {e}")
                continue

        return pd.DataFrame(all_details)

    def historical_stock(self, symbol: str, start_date: str, end_date: str, resolution: str = "1d") -> pd.DataFrame:
        """
        # 4. Atualizar o método para o novo endpoint de dados históricos de instrumentos.
        Busca a série histórica de um único instrumento (ação).
        Endpoint: /market/historical/{symbol}/{resolution}
        """
        path = f"/market/historical/{symbol}/{resolution}"
        params = {"from": start_date, "to": end_date}
        # A resposta deste endpoint é um objeto com a chave 'data'
        json_response = self._get_json(path, params=params)
        df = pd.DataFrame(json_response.get('data', []))
        df['date'] = pd.to_datetime(df['time'], unit='ms')
        return df