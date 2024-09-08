# import os
import json
from classes import Settings
from dotenv import load_dotenv

load_dotenv()


# Constant class
class Const:
    MONGODB = 'xtb'
    PGDB = 'tinyco'
    SYMBOL_DEFAULT = (
        ('GOLD', 5), ('GOLD', 15), ('GOLD', 30), ('GOLD', 60),
        ('GOLD.FUT', 15), ('GOLD.FUT', 30), ('GOLD.FUT', 60),
        ('OIL.WTI', 15), ('OIL.WTI', 30), ('OIL.WTI', 60),
        ('USDJPY', 15), ('USDJPY', 30), ('USDJPY', 60),
        ('EURUSD', 15), ('EURUSD', 30), ('EURUSD', 60),
    )
    SYMBOL_SUBSCRIBE = (
        ('GOLD', 240),
    )
    SYMBOL_ID = {'GOLD': 1,
                 'GOLD.FUT': 2,
                 'BITCOIN': 3,
                 'EURUSD': 4,
                 'OIL.WTI': 5,
                 'USDJPY': 6}
    PERIOD_ID = {1: 0, 5: 1, 15: 2, 30: 3, 60: 4, 240: 5, 1440: 6, 10080: 7, 43200: 8}


# Indicator Presets
ind_presets = {
    "TA_RSI_L14_XA70_XB30": [
        {
            "kind": "rsi", "length": 14, "signal_indicators": True,
            "xa": 70, "xb": 30
        }
    ],
    "TA_RSI_L14_XA65_XB35": [
        {
            "kind": "rsi", "length": 14, "signal_indicators": True,
            "xa": 65, "xb": 35
        }
    ],
    "TA_STOCH_K14_XA80_XB20": [
        {
            "kind": "stoch", "k": 14, "d": 3, "smooth_k": 3,
            "xa": 80, "xb": 20
        }
    ],
    "TA_EMAX_F10_S25": [
        {"kind": "ema", "length": 10},
        {"kind": "ema", "length": 25},
    ],
    "TA_EMAX_F10_S50": [
        {"kind": "ema", "length": 10},
        {"kind": "ema", "length": 50},
    ],
    "TA_EMAX_F25_S50": [
        {"kind": "ema", "length": 25},
        {"kind": "ema", "length": 50},
    ],
}

# Account configuration loading
accounts: dict = json.load(open('account.json'))
# Setting configuration loading
settings = Settings(**json.load(open('settings.json')))

if __name__ == '__main__':
    print(settings)
