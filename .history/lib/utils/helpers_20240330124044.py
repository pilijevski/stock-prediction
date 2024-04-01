import requests
from bs4 import BeautifulSoup
from tenacity import retry
import pandas as pd
from tenacity import retry

DOC_TYPES = ["balance-sheet", "cash-flow-statement", "income_statement", "ratios"]



def get_snp_companies(subset='500'):
    WIKI_URL = f"http://en.wikipedia.org/wiki/List_of_S%26P_{subset}_companies"
    resp = requests.get(WIKI_URL)
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text[:-1]
        tickers.append(ticker)
    return tickers


def get_stock_symbols():
    stock_symbols = get_snp_companies('500') + get_snp_companies('400') + get_snp_companies('600')
    stock_symbols = set(stock_symbols)
    return stock_symbols

@retry(retry=lambda e: isinstance(e, requests.exceptions.RequestException))
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for errors in the HTTP response
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching HTML content: {e}")
        raise requests.exceptions.RequestException(e)


def extract_numerical_columns(df):
    numerical_columns = []
    for col in df.columns:
        try:
            pd.to_numeric(df[col], errors='raise')
            numerical_columns.append(col)
        except ValueError:
            pass
    return numerical_columns

def extract_table_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    if table:
        # Extract table rows
        rows = table.find_all('tr')

        # Extract column headers
        headers = [header.get_text(strip=True) for header in rows[0].find_all('th')]

        # Extract table data
        data = []
        for row in rows[1:]:
            row_data = [td.get_text(strip=True) for td in row.find_all('td')]
            data.append(row_data)

        return headers, data
    else:
        print("No table found.")
        return None, None
    

def get_stockanalysis_data(ticker: str, document_type: str, freq: str = "quarterly") -> pd.DataFrame:
    url = f"https://stockanalysis.com/stocks/{ticker.lower()}/financials"
    if document_type != "income_statement":
        url = f"{url}/{document_type}"
    url = f"{url}/?p={freq}"
    # Fetch HTML content from the URL
    html_content = fetch_html_content(url)

    try:
        # Extract table data
        headers, table_data = extract_table_data(html_content)
        if headers and table_data:
            df = pd.DataFrame(table_data, columns=headers)
            return df
    except Exception:
        return pd.DataFrame()
    

def get_stock_data(ticker):
    data_list = list()
    for document in DOC_TYPES:
        data = get_stockanalysis_data(ticker,document, freq="quarterly")
        data_list.append(data)
        
    df = pd.concat(data_list,axis=0).set_index("Quarter Ended").T
    df['ticker'] = ticker
    df=process_df(df)
    return df



def process_df(df):
    r = "^\+\s*\d+\s*Quarters$"
    df = df[~df.index.str.contains(r, regex=True)]
    df = df[~df.index.str.contains("Current", regex=True)]
    df["year"] = pd.to_datetime(df.index).year
    df = df.T.drop_duplicates().T  # drop duplicate columns

    return df

def get_cols_to_be_divided_by_total_assets():
    return ['Cash & Equivalents', 'Short-Term Investments', 'Cash & Cash Equivalents', 'Receivables',
            'Inventory','Other Current Assets','Total Current Assets','Property, Plant & Equipment',
            'Long-Term Investments','Goodwill and Intangibles','Other Long-Term Assets','Total Long-Term Assets',
            'Total Assets','Accounts Payable','Deferred Revenue','Current Debt','Other Current Liabilities',
            'Total Current Liabilities','Long-Term Debt','Other Long-Term Liabilities','Total Long-Term Liabilities','Total Liabilities',
            'Total Debt','Common Stock','Retained Earnings','Comprehensive Income',"Shareholders' Equity",'Working Capital','Net Income',
            'Depreciation & Amortization','Share-Based Compensation','Other Operating Activities','Operating Cash Flow',
            'Capital Expenditures','Acquisitions','Change in Investments','Other Investing Activities','Investing Cash Flow',
            'Dividends Paid','Other Financing Activities','Financing Cash Flow','Exchange Rate Effect','Net Cash Flow','Free Cash Flow',
            'Revenue','Revenue Growth (YoY)','Cost of Revenue','Gross Profit','Selling, General & Admin','Research & Development',
            'Other Operating Expenses','Operating Expenses','Operating Income','Pretax Income','Income Tax','Net Income.1',
            'Shares Outstanding (Basic)','Shares Outstanding (Diluted)','Free Cash Flow.1','Gross Margin',
            'Operating Margin','Profit Margin','Free Cash Flow Margin.1','Effective Tax Rate','EBITDA','EBITDA Margin',
            'Depreciation & Amortization.1','EBIT','EBIT Margin','Market Capitalization','Enterprise Value',
            'Net Income Common','Goodwill','Intangible Assets','Total Liabilities and Equity','Common Stock Issued',
            'Share Repurchases','Interest Income','Interest Expense','Asset Turnover']