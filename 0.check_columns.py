from glob import glob
import pandas as pd
import multiprocessing as mp
from time import time

'''
엑셀형태의 파일을 읽어와 각 파일들이 의도하는 열의 형태를 가지고 있는지 확인하는 파일  
'''


def work(number: int, path: str):
    st = time()
    print(f"worker {number} started")

    df = pd.read_excel(path)
    cols = list(df.columns.values)
    if "지역" in cols:
        cols.remove("지역")
    if "망" in cols:
        cols.remove("망")
    if "측정소명" in cols:
        cols.remove("측정소명")

    print(f"worker {number} finished - {(time() - st):.4f}")

    return cols


if __name__ == "__main__":
    paths = glob("./Files/*.xlsx")
    num_cores = int(mp.cpu_count())
    pool = mp.Pool(num_cores)
    columns = pool.starmap(work, zip([(i + 1) for i in range(len(paths))], paths))
    print(len(columns))
    print(columns)
    pool.close()
    pool.join()

    standard = columns[0]
    flag = True
    for col in columns[1:]:
        if len(standard) != len(col):
            flag = False
            break

        for i in range(len(col)):
            try:
                if standard[i] != col[i]:
                    flag = False
                    break
            except Exception as e:
                print(e)

    print(flag)
