import json

import pymysql
from sqlalchemy import create_engine
import pandas as pd
from time import time
from tqdm import tqdm


def create_measure_code_table(engine):
    measure_code_json = json.load(open("./json/measure_code.json", "r", encoding="UTF-8"))
    mCode = [code for code in measure_code_json.keys()]
    dongCode = [measure_code_json[code]["dongCode"] for code in measure_code_json.keys()]
    address = [measure_code_json[code]["address"] for code in measure_code_json.keys()]
    df = pd.DataFrame.from_dict({"mCode": mCode, "dongCode": dongCode, "address": address})

    conn = engine.connect()
    df.to_sql(name=f"measure_code", con=conn, if_exists='replace', index=False)
    conn.close()

    pyconn = pymysql.connect(host='localhost', user='ssafy', password='ssafy', db='happyhouse', charset='utf8',
                             autocommit=True)
    cur = pyconn.cursor()

    alterSql = f"ALTER TABLE `happyhouse`.`measure_code`" \
               "CHANGE COLUMN `mCode` `mCode` VARCHAR(10) NOT NULL ," \
               "CHANGE COLUMN `dongCode` `dongCode` VARCHAR(10) NULL DEFAULT NULL ," \
               "ADD PRIMARY KEY (`mCode`);" \
               ";"

    cur.execute(alterSql)
    pyconn.close()


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# sqlalchemy로 데이터 적재
def insert_with_tqdm(engine, df, table_name):
    conn = engine.connect()
    chunksize = int(len(df) / 1000)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(name=f"{table_name}", con=conn, if_exists=replace, index=False)
            pbar.update(chunksize)
            tqdm._instances.clear()

    conn.close()
    alter_table(table_name)


# alter Table
def alter_table(table_name):
    pyconn = pymysql.connect(host='localhost', user='ssafy', password='ssafy', db='happyhouse', charset='utf8',
                             autocommit=True)
    cur = pyconn.cursor()

    alterSql = f"ALTER Table `{table_name}`" \
               "CHANGE COLUMN `mCode` `mCode` VARCHAR(10) NOT NULL ," \
               "CHANGE COLUMN `mDateTime` `mDateTime` VARCHAR(20) NOT NULL ," \
               "CHANGE COLUMN `SO2` `SO2` VARCHAR(10) NULL DEFAULT NULL ," \
               "CHANGE COLUMN `CO` `CO` VARCHAR(10) NULL DEFAULT NULL ," \
               "CHANGE COLUMN `O3` `O3` VARCHAR(10) NULL DEFAULT NULL ," \
               "CHANGE COLUMN `NO2` `NO2` VARCHAR(10) NULL DEFAULT NULL ," \
               "CHANGE COLUMN `PM10` `PM10` VARCHAR(10) NULL DEFAULT NULL ," \
               "CHANGE COLUMN `PM25` `PM25` VARCHAR(10) NULL DEFAULT NULL ," \
               "ADD PRIMARY KEY (`mCode`, `mDateTime`);" \
               ";"

    cur.execute(alterSql)
    pyconn.close()


if __name__ == "__main__":
    engine = create_engine("mysql+pymysql://ssafy:ssafy@localhost:3306/happyhouse")
    create_measure_code_table(engine)
    for year in range(2017, 2022):
        # csv파일 읽기
        st = time()
        table_name = f"air_{year}"
        df = pd.read_csv(f"./processed/csv_by_year/{table_name}.csv", encoding="euc-kr")
        df = df.round({'SO2': 5, 'CO': 5, 'O3': 5, "NO2": 5})
        print(f"reading csv file finished - {(time() - st):.4f}s")
        insert_with_tqdm(engine, df, table_name)

    engine.dispose()
