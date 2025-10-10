# Mock data for the OpLab API responses used in tests

# --- Mock data for 'historical_options' (Discovery) ---
MOCK_OPTIONS_LIST = {
    "count": 2,
    "data": [
        {"ticker": "PETRA110", "type": "CALL", "strike": 11.00, "expiration_date": "2024-01-19"},
        {"ticker": "PETRM110", "type": "PUT", "strike": 11.00, "expiration_date": "2024-01-19"},
    ]
}

# --- Mock data for 'historical_instruments_details' (Enrichment) ---
MOCK_INSTRUMENTS_DETAILS = {
    "count": 2,
    "data": [
        {
            "ticker": "PETRA110", "spot": "PETR4", "date": "2023-11-10",
            "open": 0.50, "high": 0.55, "low": 0.48, "close": 0.52,
            "volume": 1500, "dte": 45, "delta": 0.55, "gamma": 0.05,
            "theta": -0.01, "vega": 0.02, "iv": 0.35
        },
        {
            "ticker": "PETRM110", "spot": "PETR4", "date": "2023-11-10",
            "open": 0.20, "high": 0.25, "low": 0.18, "close": 0.21,
            "volume": 1200, "dte": 45, "delta": -0.45, "gamma": 0.05,
            "theta": -0.01, "vega": 0.02, "iv": 0.38
        }
    ]
}

# --- Mock data for 'historical_stock' ---
MOCK_STOCK_HISTORY = {
    "count": 1,
    "data": [
        {
            "ticker": "PETR4", "date": "2023-11-10", "open": 35.10, 
            "high": 35.50, "low": 35.00, "close": 35.45, "volume": 50000000
        }
    ]
}