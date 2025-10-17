# Mock data for the OpLab API responses used in tests

# --- Mock data for 'historical_options' (Discovery) ---
MOCK_OPTIONS_LIST = {
    "count": 2,
    "data": [
        {
            "symbol": "PETRA110", "type": "CALL", "strike": 11.00,
            "expiry_date": "2024-01-19", "time": "2023-11-10",
            "high": 0.55, "low": 0.48, "close": 0.52
        },
        {
            "symbol": "PETRM110", "type": "PUT", "strike": 11.00,
            "expiry_date": "2024-01-19", "time": "2023-11-10",
            "high": 0.25, "low": 0.18, "close": 0.21
        }
    ]
}

# --- Mock data for 'historical_instruments_details' (Enrichment) ---
MOCK_INSTRUMENTS_DETAILS = {
    "count": 2,
    "data": [
        {
            "symbol": "PETRA110", "spot": "PETR4", "time": "2023-11-10",
            "delta": 0.55, "gamma": 0.05, "theta": -0.01,
            "vega": 0.02, "iv": 0.35
        },
        {
            "symbol": "PETRM110", "spot": "PETR4", "time": "2023-11-10",
            "delta": -0.45, "gamma": 0.05, "theta": -0.01,
            "vega": 0.02, "iv": 0.38
        }
    ]
}

# --- Mock data for 'historical_stock' ---
MOCK_STOCK_HISTORY = {
    "count": 1,
    "data": [
        {
            "date": "2023-11-10", "close": 35.45,
            "high": 35.50, "low": 35.00, "volume": 50000000
        }
    ]
}