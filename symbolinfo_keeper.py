from .lightdf import Dataframe
from . import mysqlite

class SymbolInfoKeeper:

    def __init__(self, db_folder_path: str):
        self.db_path = db_folder_path + "/symbolinfo.db"
        self.db = mysqlite.DB(self.db_path)
        self.__setup_table()

    def query(self) -> Dataframe:
        query = self.master.query()
        df = self.__get_dataframe_temple()
        df.from_dict(query)
        return df
    
    def update(self, data: dict):
        self.master.update(data)
        self.db.commit()
    
    def __setup_table(self):
        if not "master" in self.db.list_tb():
            self.master = self.db.create_tb("master", "symbol", "CHAR(100)")
            self.master.addCol("short_name", "CHAR(300)")
            self.master.addCol("long_name", "CHAR(500)")
            self.master.addCol("type", "CHAR(20)")
            self.master.addCol("market", "CHAR(2)")
            self.master.addCol("sector", "CHAR(100)")
            self.master.addCol("industry", "CHAR(100)")
            self.master.addCol("shares_outstanding", "BIGINT")
            self.master.addCol("market_cap", "BIGINT")
            self.master.addCol("fin_currency", "CHAR(10)")
            self.master.addCol("enable", "BOOLEAN")
        else:
            self.master = self.db.TB("master")
        self.mastertb = self.master.query()
    
    def __get_dataframe_temple(self) -> Dataframe:
        df = Dataframe("symbol", str)
        df.add_col("short_name", str)
        df.add_col("long_name", str)
        df.add_col("type", str)
        df.add_col("market", str)
        df.add_col("sector", str)
        df.add_col("industry", str)
        df.add_col("shares_outstanding", int)
        df.add_col("market_cap", int)
        df.add_col("fin_currency", str)
        df.add_col("enable", bool)
        return df