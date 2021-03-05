from .dividend import Dividend
from .historical_price import HistoricalPrice
from .stock_split import StockSplit
from .symbol_info import SymbolInfo
from .trend_table import TrendTable

class Keeper:

    def __init__(self):
        self.dividend = Dividend()
        self.historical_price = HistoricalPrice()
        self.stock_split = StockSplit()
        self.symbol_info = SymbolInfo()
        self.trend_table = TrendTable()