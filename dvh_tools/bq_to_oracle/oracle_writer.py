from oracledb import connect


class OracleWriter:
    def __init__(self, config, target_table=None):
        self.__config = config
        self.con = connect(
            user=self.__config["DB_USER"],
            password=self.__config["DB_PASSWORD"],
            dsn=self.__config["DB_DSN"],
        )
        self.target_table = target_table or self.__config["target-table"]
        if self._table_is_not_empty():
            raise ValueError(f"Target table {self.target_table} must be empty before ETL")
        self.insert_string = None
        self.total_rows_inserted = 0

    def _table_is_not_empty(self) -> bool:
        with self.con.cursor() as cursor:
            cursor.execute(f"select 1 from {self.target_table} where rownum=1")
            row = cursor.fetchone()
        return row != None

    def write_batch(self, batch, dry_run=True, datatypes={}):
        if dry_run:
            return
        if self.total_rows_inserted == 0:
            # self.prepare_table()
            pass
        if not self.insert_string:
            self.create_insert_string(batch)
        with self.con.cursor() as cursor:
            try:
                if datatypes:
                    cursor.setinputsizes(**datatypes)
                cursor.executemany(self.insert_string, batch)
                self.total_rows_inserted += cursor.rowcount
            except Exception as e:
                self.cleanup(is_healthy=False)
                print(e)
                raise RuntimeError(e)
        self.con.commit()

    def cleanup(self, is_healthy=True):
        if is_healthy:
            self.con.commit()
        else:
            self.con.rollback()
        self.con.close()

    def prepare_table(self):
        with self.con.cursor() as cursor:
            cursor.execute(f"truncate table {self.target_table}")
        return True

    def get_oracle_sysdate(self):
        with self.con.cursor() as cursor:
            cursor.execute(f"select sysdate from dual")
            row = cursor.fetchone()
        return row[0]

    def create_insert_string(self, batch):
        column_names = batch[0].keys()
        # lastet_tid = self.get_oracle_sysdate()
        self.insert_string = f"""
        insert into {self.target_table}
        ({', '.join(column_names)}, LASTET_TID) 
        values({', '.join([f':{col}' for col in column_names])}, SYSDATE)
        """
        return self.insert_string