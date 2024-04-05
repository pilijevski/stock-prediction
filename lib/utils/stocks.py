import os
import requests
from .helpers import get_stock_data, get_stock_symbols, get_earnings_release_dates
import pandas as pd
import yfinance as yf
import numpy as np
from pandas.tseries.offsets import DateOffset
from tqdm import tqdm
from datetime import date

DOC_TYPES = ["balance-sheet", "cash-flow-statement", "income_statement", "ratios"]

def build_stocks(stocks_path):
    today = date.today()

    today_str = today.strftime("%Y-%m-%d")

    stock_names = list(get_stock_symbols())
    stock_list = []
    if not os.path.exists("lib/stocks/{today_str}"):
        os.makedirs(f"lib/stocks/{today_str}", exist_ok=True)

    with tqdm(total=len(stock_names)) as pbar:
        for ticker in stock_names:
            pbar.set_description(f"{ticker}")
            pbar.update(1)
            if not os.path.exists(f"lib/stocks/{today_str}/{ticker}.csv"):
                try:
                    stock_data = get_stock_data(ticker)
                    stock_data['end_of_quarter'] = stock_data.index
                    stock_data.to_csv(f"lib/stocks/{today_str}/{ticker}.csv", index=False)
                except Exception as e:
                    print(f"Error with ticker: {ticker}")
                    continue
            else:
                stock_data = pd.read_csv(f"lib/stocks/{today_str}/{ticker}.csv")

            stock_list.append(stock_data)
            
    return pd.concat(stock_list,axis=0).to_csv(stocks_path, index=False)


def get_meta_data(stock_symbols, stocks):
    meta_data = []
    N = 0
    headers = {'User-Agent': "email@address.com"}
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
        )
    company_tickers = pd.DataFrame(companyTickers.json()).T.set_index("cik_str")
    for ticker in tqdm(stock_symbols):
        try:
            stock = yf.Ticker(ticker)
            stock_info = stock.info
        except:
            continue
        stock_periods = stocks[stocks.ticker == ticker].index
        stock_periods
        
        try:
            d = get_earnings_release_dates(ticker, company_tickers, headers)
        except Exception as e:
            print("Error in getting earnings dates", e, ticker)
            continue
        if len(stock_periods) < 8:
            print(f'The stock with ticker:{ticker} has less than 8 periods reported')
            continue
        for i in range(len(stock_periods) - 1, -1, -1):
            stock_features = []
            try:
                industry = stock_info['industry']
            except:
                industry =  np.nan
            try:
                earnings_date = pd.Timestamp(d.loc[stock_periods[i]:].index.unique().values[0]) + DateOffset(days=7)
            except Exception as e:
                print(e, ticker, "earnings_date=pd.Timestamp")
                continue
            # earnings_date = pd.Timestamp(d[stock_periods[i]:].index.values[0]) + DateOffset(days=1)
            stock_features += [stock_periods[i], earnings_date, ticker, industry]
            

            stock_feature_columns = ['end_of_quarter', 'end_of_quarter_release', 'ticker', 'industry'] + ['increase']

            try:
                hist = stock.history(start=earnings_date, end=earnings_date + DateOffset(months=6))
                next_quarter_increase = (hist['Close'][-1] - hist['Close'][0]) / hist['Close'][0]
                stock_features += [next_quarter_increase]
                meta_data.append(stock_features)
            except Exception as e:
                print(e, ticker)
                continue
        meta_df = pd.DataFrame(meta_data, columns=stock_feature_columns)
        if N % 25==0:
            meta_df.to_csv(f'meta_df_{N}_.csv', index=False)
        N+=1
    return meta_df

