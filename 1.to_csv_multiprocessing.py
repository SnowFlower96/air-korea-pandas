from glob import glob
from tqdm import tqdm
import pandas as pd
import multiprocessing as mp


def worker(number: int, paths: list):
    print(f"worker {number} started")
    for path in tqdm(paths):
        df = pd.read_excel(path)
        dist = path.split("\\")[1][:-5] + ".csv"

        df_new = df.drop(["지역", "망", "측정소명"], axis=1)
        print(df_new.columns)
        df_new.columns = ["mCode", "mDate", "SO2", "CO", "O3", "NO2", "PM10", "PM25", "address"]
        print(df_new.columns)
        exit()
        df_new.to_csv(dist, index=False, encoding='euc-kr')
    print(f"worker {number} finished")


if __name__ == "__main__":
    num_cores = mp.cpu_count()
    pool = mp.Pool(num_cores)
    paths = glob("./Files/*.xlsx")
    workers = [mp.Process(target=worker, args=(path,)) for path in paths]
    pool.close()
    pool.join()

