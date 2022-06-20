import pandas as pd
from glob import glob
from tqdm import tqdm
import json
import requests


json_path = "./json/measure_code.json"
api_key = "7947471b870615b5a25ced31c5c27c24"
sidoDict = {"서울": "서울특별시", "대전": "대전광역시", "대구": "대구광역시", "부산": "부산광역시", "광주": "광주광역시", "울산": "울산광역시", "인천": "인천광역시",
            "경기": "경기도", "강원": "강원도", "충북": "충청북도", "충남": "충청남도", "전북": "전라북도", "전남": "전라남도", "경북": "경상북도",
            "경남": "경상남도",
            "제주": "제주특별시",
            "세종특별자치시": "세종특별자치시"}


def search_address(addr):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
    headers = {"Authorization": "KakaoAK " + api_key}
    result = json.loads(str(requests.get(url, headers=headers).text))
    return result


def to_measure(csv_path: str, dong_code: list):
    mCode: dict = json.load(open(json_path, 'r', encoding="UTF-8"))

    # drop address column
    file_name = path.split("\\")[1][:-4]
    dist = "./processed/drop_address/" + file_name + ".csv"
    df = pd.read_csv(csv_path, encoding="euc-kr")
    df_new = df.drop(["address"], axis=1)
    df_new.to_csv(dist, index=False, encoding='euc-kr')

    un = df.groupby(["mCode", "address"]).size().reset_index(name="freq")
    for idx, row in tqdm(un.iterrows()):
        code = row["mCode"]
        address = row["address"]
        sido = ""
        gugun = ""
        dong = ""

        if str(code) in mCode.keys():
            if mCode[str(code)]["sidoName"] != "" \
                    and mCode[str(code)]["gugunName"] != "" \
                    and mCode[str(code)]["dongName"] != "":
                continue

        doc = search_address(address)["documents"]
        if len(doc) != 1:
            pass
        if len(doc) > 0 and doc[0]["road_address"] is not None:
            sido = doc[0]["road_address"]["region_1depth_name"]
            if sido in sidoDict.keys():
                sido = sidoDict[sido]
            gugun = doc[0]["road_address"]["region_2depth_name"]
            dong = doc[0]["road_address"]["region_3depth_name"]
        elif len(doc) > 0 and doc[0]["address"] is not None:
            sido = doc[0]["address"]["region_1depth_name"]
            if sido in sidoDict.keys():
                sido = sidoDict[sido]
            gugun = doc[0]["address"]["region_2depth_name"]
            dong = doc[0]["address"]["region_3depth_h_name"]
            if dong == "":
                dong = doc[0]["address"]["region_3depth_name"]

        if sido == "세종특별자치시" and gugun == "":
            gugun = sido

        # *n동 => *동
        if any(map(str.isdigit, dong)):
            idx = dong.find("동")
            if idx > 0 and dong[idx - 1].isdigit():
                dong = dong.replace(f"{dong[idx - 1]}", "")

        # append dong code
        ret = next((item for item in dong_code if
                    item["sidoName"] == sido
                    and item["gugunName"] == gugun
                    and item["dongName"] == dong), None)
        if ret is None:
            continue

        mCode[str(code)] = {'address': address, 'dongCode': ret["dongCode"], 'sidoName': sido, 'gugunName': gugun,
                            'dongName': dong}

    with open(json_path, "w", encoding="UTF-8") as codeFile:
        json.dump(mCode, codeFile, ensure_ascii=False)


if __name__ == "__main__":
    paths = glob("./processed/xlsx_to_csv/*.csv")

    # open(json_path, "w+", encoding="UTF-8")

    dongCodeList: list = json.load(open("./json/dongCode.json", 'r'))

    for i, path in enumerate(paths):
        print(f"{i + 1} / {len(paths)} - started")
        to_measure(path, dongCodeList)

    with open(json_path, "r", encoding="UTF-8") as codeFile:
        result = json.load(codeFile)
        print(len(result.keys()))
