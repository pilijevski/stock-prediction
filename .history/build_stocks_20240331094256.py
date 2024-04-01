from lib.utils.stocks import build_stocks
from datetime import date

today = date.today()

today_str = today.strftime("%Y-%m-%d")

if __name__ == "__main__":
    build_stocks(f"lib/stocks/quarterly_statements_{today_str}.csv")