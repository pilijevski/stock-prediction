from .helpers import get_stock_data, get_stock_symbols
import pandas as pd
import yfinance as yf
import numpy as np
from pandas.tseries.offsets import DateOffset
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

def get_meta_data(stock_symbols, stocks):
    meta_data = []
    for ticker in tqdm(stock_symbols):
        try:
            stock = yf.Ticker(ticker)
            stock_info = stock.info
        except:
            continue
        stock_periods = stocks[stocks.ticker == ticker].index

        if len(stock_periods) < 8:
            print(f'The stock with ticker:{ticker} has less than 8 periods reported')
            continue
        for i in range(len(stock_periods) - 1, -1, -1):
            stock_features = []
            try:
                dividends = stock.dividends[stock_periods[i]- DateOffset(months=3):stock_periods[i]][-1]
            except:
                dividends = np.nan
            try:
                fte = stock_info['fullTimeEmployees']
            except:
                fte = np.nan

            try:
                auditrisk = stock_info['auditRisk']
            except:
                auditrisk = np.nan
            try:
                compensationrisk = stock_info['compensationRisk']
            except:
                compensationrisk = np.nan

            try:
                boardrisk = stock_info['boardRisk']
            except:
                boardrisk = np.nan

            try:
                shareholderrightsrisk = stock_info['shareHolderRightsRisk']
            except:
                shareholderrightsrisk = np.nan
            try:
                maxage = stock_info['maxAge']
            except:
                maxage = np.nan
            try:
                industry = stock_info['industry']
            except:
                industry =  np.nan

            try:
                sector = stock_info['sectorKey']
            except:
                sector = np.nan
            stock_features += [stock_periods[i], ticker, industry, sector, fte, auditrisk, compensationrisk,
                            boardrisk, shareholderrightsrisk, maxage, dividends]
            hist = stock.history(start=stock_periods[i] - DateOffset(months=3), end=stock_periods[i])
            try:
                hist['week_number'] = hist.index.week
            except:
                continue
            weekly_mean_close = hist.groupby('week_number')['Close'].mean()
            stock_features += list(weekly_mean_close.values[:10])
            stock_feature_columns = ['end_of_quarter','ticker', 'industry', 'sector', 'fte', 'auditrisk',
                                    'compensationrisk', 'boardrisk', 'shareholderrightsrisk',
                                    'maxage', 'dividends'] + [f'weekly_price_{w}' for w in range(len(list(weekly_mean_close.values[:10])))] + ['increase']

            try:
                hist = stock.history(start=stock_periods[i], end=stock_periods[i] + DateOffset(months=3))
                next_quarter_increase = (hist['Close'][-1] - hist['Close'][0]) / hist['Close'][0]
                stock_features += [next_quarter_increase]
                meta_data.append(stock_features)
            except:
                continue
    meta_df = pd.DataFrame(meta_data, columns=stock_feature_columns)
    return meta_df

