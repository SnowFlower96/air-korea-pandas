import pandas as pd
from glob import glob
from tqdm import tqdm
import json
import requests


'''
csv로 변환된 모든 파일들에 대해 측정소코드와 주소를 구하고 주소에 따른 법정동코드를 검색 
'''


json_path = "./json/measure_code.json"
api_key = "key"
sidoDict = {"서울": "서울특별시", "대전": "대전광역시", "대구": "대구광역시", "부산": "부산광역시", "광주": "광주광역시", "울산": "울산광역시", "인천": "인천광역시",
            "경기": "경기도", "강원": "강원도", "충북": "충청북도", "충남": "충청남도", "전북": "전라북도", "전남": "전라남도", "경북": "경상북도",
            "경남": "경상남도",
            "제주": "제주특별시",
            "세종특별자치시": "세종특별자치시"}


# 카카오 주소 검색 API 활용
def search_address(addr):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
    headers = {"Authorization": "KakaoAK " + api_key}
    result = json.loads(str(requests.get(url, headers=headers).text))
    return result


def get_measure_code(files: list, dong_code: list) -> dict:
    m_codes = {}
    for i, file in enumerate(files):
        print(f'{i + 1} / {len(files)} started')
        df = pd.read_csv(file, encoding="euc-kr")
        un = df.groupby(["mCode", "address"]).size().reset_index(name="freq")
        for row in tqdm(un.iterrows()):
            mCode = row[1]['mCode']
            address = row[1]['address']
            if mCode not in m_codes:
                doc = search_address(address)['documents']
                dongCode = None
                # 검색 결과가 없으면
                if len(doc) < 1:
                    continue
                doc_address = doc[0]['address']
                # 법정동 결과가 있으면
                if doc_address is not None:
                    dongCode = doc[0]['address']['b_code']
                    
                # 법정동 결과가 없으면 도로명 검색 결과 확인
                else:
                    doc_road_address = doc[0]['road_address']
                    if doc_road_address is not None:  # 도로명 검색 결과가 있으면
                        sido = doc[0]["road_address"]["region_1depth_name"]
                        if sido in sidoDict.keys():
                            sido = sidoDict[sido]
                        gugun = doc[0]["road_address"]["region_2depth_name"]
                        dong = doc[0]["road_address"]["region_3depth_name"]
                        ret = next((item for item in dong_code if
                                    item["sidoName"] == sido
                                    and item["gugunName"] == gugun
                                    and item["dongName"] == dong), None)

                        dongCode = ret['dongCode']

                m_codes[mCode] = {'dongCode': dongCode, 'address': address}

    return m_codes


if __name__ == "__main__":
    paths = glob("./processed/xlsx_to_csv/*.csv")
    dongCodeList: list = json.load(open("./json/dongCode.json", 'r'))

    measure_codes = get_measure_code(paths, dongCodeList)

    with open(json_path, "w", encoding="UTF-8") as codeFile:
        json.dump(measure_codes, codeFile, ensure_ascii=False)
