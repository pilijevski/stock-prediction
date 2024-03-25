from .helpers import get_stock_data, get_stock_symbols
import pandas as pd
from tqdm import tqdm

DOC_TYPES = ["balance-sheet", "cash-flow-statement", "income_statement", "ratios"]

def build_stocks(stocks_path):
    stock_names = list(get_stock_symbols())
    stock_list = []
    with tqdm(total=len(stock_names)) as pbar:
        for ticker in stock_names:
            pbar.set_description(f"{ticker}")
            pbar.update(1)
            stock_data = get_stock_data(ticker)
            stock_list.append(stock_data)
            
    return pd.concat(stock_list, axis=1).to_csv(stocks_path)

