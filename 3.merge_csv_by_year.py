from glob import glob
import pandas as pd
import multiprocessing as mp
from time import time
import os

'''
월별로 존재하는 파일들을 연도별로 정리
'''


folder = "./processed/xlsx_to_csv"


def work(number: int, year: int):
    st = time()
    print(f"worker {number} started - year : {year}")

    paths_by_year: list = glob(f"{folder}/{year}*.csv")
    new_df: pd.DataFrame = pd.read_csv(paths_by_year[0], encoding="euc-kr")
    for path in paths_by_year[1:]:
        df = pd.read_csv(path, encoding="euc-kr")
        new_df = pd.concat((new_df, df))

    new_df = new_df.drop(['address'], axis=1)
    new_df = new_df.drop_duplicates(["mCode", "mDateTime"])
    new_df = new_df.sort_values(by=["mCode", "mDateTime"], ascending=True)
    if not os.path.exists(f"./processed/csv_by_year"):
        os.mkdir(f"./processed/csv_by_year")
    new_df.to_csv(f"./processed/csv_by_year/air_{year}.csv", index=False, encoding='euc-kr')

    print(f"worker {number} finished - {(time() - st):.4f}")


if __name__ == "__main__":
    paths = glob("./Files/*.xlsx")

    num_cores = len(range(2021, 2021)) + 1
    pool = mp.Pool(num_cores)
    pool.starmap(work, zip([(i+1) for i in range(num_cores)], [i for i in range(2021, 2022)]))
    pool.close()
    pool.join()
