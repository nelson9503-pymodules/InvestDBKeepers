from .lightdf import Dataframe
from . import mysqlite
from datetime import datetime, timedelta


class DividendKeeper:

    def __init__(self, db_folder_path: str):
        self.db_path = db_folder_path + "/dividend.db"
        self.db = mysqlite.DB(self.db_path)
        self.__setup_master_table()

    def query(self, symbol: str):
        exist = self.__check_table_exists(symbol)
        if exist == False:
            return exist
        tb = self.db.TB(symbol)
        query = tb.query()
        df = self.__get_dataframe_temple()
        df.from_dict(query)
        return df

    def query_master(self) -> Dataframe:
        tb = self.master.query()
        df = self.__get_master_dataframe_temple()
        df.from_dict(tb)
        return df

    def update(self, symbol: str, data: dict):
        # check table exists
        exist = self.__check_table_exists(symbol)
        if exist == False:
            self.__setup_table(symbol)
        # get table object
        tb = self.db.TB(symbol)
        # update data
        update_df = self.__get_dataframe_temple()
        last_date = self.mastertb[symbol]["last_date"]
        for date in data:
            if date < last_date:  # skip for row has been updated
                continue
            try:
                process_date = self.__process_timestamp(date)
                update_df.from_dict({process_date: data[date]})
            except ValueError:  # skip for none values
                pass
        # update database
        updates = update_df.to_dict()
        tb.update(updates)
        self.__update_last_update(symbol)
        self.__update_first_last_date_and_data_points(symbol)
        self.db.commit()

    def __update_last_update(self, symbol: str):
        today = self.__get_today_timestamp()
        if not symbol in self.mastertb:
            self.mastertb[symbol] = {}
        self.mastertb[symbol]["last_update"] = today
        self.master.update(self.mastertb)

    def __update_first_last_date_and_data_points(self, symbol: str):
        tb = self.db.TB(symbol)
        data = tb.query("date")
        dates = list(data.keys())
        if len(dates) > 0:
            self.mastertb[symbol]["first_date"] = min(dates)
            self.mastertb[symbol]["last_date"] = max(dates)
            self.mastertb[symbol]["data_points"] = len(dates)
        self.master.update(self.mastertb)

    def __get_today_timestamp(self) -> int:
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        return int(now.timestamp())

    def __process_timestamp(self, timestamp: int) -> int:
        d0 = datetime.fromtimestamp(0)
        # analysis timestamp
        delta = timedelta(seconds=timestamp)
        d = d0 + delta
        d = datetime(d.year, d.month, d.day)
        # generate timestamp
        delta = d - d0
        timestamp = delta.days * 86400 + delta.seconds
        return int(timestamp)

    def __get_dataframe_temple(self) -> Dataframe:
        df = Dataframe("date", int)
        df.add_col("dividend", float, none_value=False)
        return df

    def __get_master_dataframe_temple(self) -> Dataframe:
        df = Dataframe("table_name", str)
        df.add_col("last_update", int)
        df.add_col("first_date", int)
        df.add_col("last_date", int)
        df.add_col("data_points", int)
        return df

    def __setup_table(self, symbol: str):
        tb = self.db.create_tb(symbol, "date", "INT")
        tb.add_col("dividend", "FLOAT")
        self.master.update({
            symbol: {
                "last_update": 0,
                "first_date": 0,
                "last_date": 0,
                "data_points": 0
            }})
        self.mastertb = self.master.query()
        self.db.commit()

    def __setup_master_table(self):
        if not "master" in self.db.list_tb():
            self.master = self.db.create_tb(
                "master", "table_name", "CHAR(100)")
            self.master.add_col("last_update", "INT")
            self.master.add_col("first_date", "INT")
            self.master.add_col("last_date", "INT")
            self.master.add_col("data_points", "INT")
        else:
            self.master = self.db.TB("master")
        self.mastertb = self.master.query()

    def __check_table_exists(self, symbol: str) -> bool:
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                return False
        return True
