from glob import glob
from time import time
import pandas as pd
import multiprocessing as mp
import os

'''
엑셀형태의 파일을 csv 형태로 변환하며 필요없는 열 제거
Multiprocess.Pool 사용
'''


def work(number: int, path: str):
    st = time()
    print(f"worker {number} started")

    file_name = path.split("\\")[1][:-5]
    dist = "./processed/xlsx_to_csv/" + file_name + ".csv"

    df = pd.read_excel(path)

    if "망" in df.keys():
        df_new = df.drop(["지역", "망", "측정소명"], axis=1)
    else:
        df_new = df.drop(["지역", "측정소명"], axis=1)

    df_new.columns = ["mCode", "mDateTime", "SO2", "CO", "O3", "NO2", "PM10", "PM25", "address"]

    if not os.path.exists("./processed/xlsx_to_csv"):
        os.mkdir("./processed/xlsx_to_csv")
    df_new.to_csv(dist, index=False, encoding='euc-kr')

    print(f"worker {number} finished - {(time() - st):.4f}")


if __name__ == "__main__":
    paths = glob("./Files/*.xlsx")

    num_cores = int(mp.cpu_count())
    pool = mp.Pool(num_cores)
    pool.starmap(work, zip([(i + 1) for i in range(len(paths))], paths))
    pool.close()
    pool.join()
