import json
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
        self.insert_string = None
        self.total_rows_inserted = 0
        self.execution_time = self.get_oracle_sysdate()

    def write_batch(self, batch, convert_lists=False, datatypes={}):
        if self.total_rows_inserted == 0:
            # self.prepare_table()
            pass
        if convert_lists:
            self.convert_lists_and_dicts_in_batch_to_json(batch)

        # Legger til lastet tid
        self.add_execution_time_to_batch(self.execution_time, batch)

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
        self.insert_string = f"""
        insert into {self.target_table}
        ({', '.join(column_names)}) 
        values({', '.join([f':{col}' for col in column_names])})
        """
        return self.insert_string

    @staticmethod
    def convert_lists_and_dicts_in_batch_to_json(batch: list):
        """Looper gjennom alle dicts i batch og konverterer nesta lister og dicts til json"""
        for i, ele in enumerate(batch):
            for key in ele:
                if isinstance(ele[key], list):
                    ele[key] = json.dumps(ele[key])
                if isinstance(ele[key], dict):
                    ele[key] = json.dumps(ele[key])
            batch[i] = ele

    @staticmethod
    def add_execution_time_to_batch(time, batch: list):
        """legger til lastet_tid i batch"""
        for i, ele in enumerate(batch):
            ele["lastet_tid"] = time
            batch[i] = ele
