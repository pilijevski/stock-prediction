import os
from itertools import product
from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm
from tenacity import retry
from concurrent.futures import ThreadPoolExecutor







def process_stock_document(stock, documents):
    data_list = list()
    for document in documents:
        data = get_data(stock, document, freq="quarterly")
        data_list.append(data)
    df = pd.concat(data_list, axis=0).set_index("Quarter Ended").T
    df.to_csv(f"stocks/{stock}.csv")


if __name__ == "__main__":
    types = ["balance-sheet", "cash-flow-statement", "income_statement", "ratios"]

    for stock in tqdm(stock_symbols):
        data_list = list()
        if f"{stock}.csv" in os.listdir("stocks/quarterly"):
            continue
        else:
            for document in types:
                data = get_data(stock,document, freq="quarterly")
                data_list.append(data)
            try:
                df = pd.concat(data_list,axis=0).set_index("Quarter Ended").T
                df.to_csv(f"stocks/quarterly/{stock}.csv")
            except KeyError:
                try:
                    df = pd.concat(data_list, axis=0).set_index("Year").T
                    df.to_csv(f"stocks/yearly/{stock}.csv")
                except KeyError:
                    print(stock)
