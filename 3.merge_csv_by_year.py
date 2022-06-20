from glob import glob
import pandas as pd
import multiprocessing as mp
from time import time

folder = "./processed/drop_address"


def work(number: int, year: int):
    st = time()
    print(f"worker {number} started - year : {year}")

    paths: list = glob(f"{folder}/{year}*.csv")
    new_df: pd.DataFrame = pd.read_csv(paths[0], encoding="euc-kr")
    for path in paths[1:]:
        df = pd.read_csv(path, encoding="euc-kr")
        new_df = pd.concat((new_df, df))

    new_df = new_df.drop_duplicates(["mCode", "mDateTime"])
    new_df = new_df.sort_values(by=["mCode", "mDateTime"], ascending=True)
    new_df.to_csv(f"./processed/csv_by_year/air_{year}.csv", index=False, encoding='euc-kr')

    print(f"worker {number} finished - {(time() - st):.4f}")


if __name__ == "__main__":
    paths = glob("./Files/*.xlsx")

    num_cores = len(range(2017, 2021)) + 1
    pool = mp.Pool(num_cores)
    pool.starmap(work, zip([(i+1) for i in range(num_cores)], [i for i in range(2017, 2022)]))
    pool.close()
    pool.join()
