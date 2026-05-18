import requests
import json

def test():
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": "KakaoAK 545d14c4a406db675503b6d170297d2c"}
    
    addresses = [
        "서울특별시 종로구 평창동 175-1",
        "서울특별시 종로구 사직동 48-2",
        "서울특별시 종로구 누하동 7-24",
        "서울특별시 종로구 동숭동 215-52",
        "서울특별시 종로구 창신동 436-48"
    ]
    
    for addr in addresses:
        res = requests.get(url, headers=headers, params={"query": addr})
        docs = res.json().get('documents', [])
        if docs:
            addr_info = docs[0].get('address', {})
            b_code = addr_info.get('b_code', '')
            print(f"Address: {addr}")
            print(f"  b_code: {b_code}")
            print(f"  lat/lng: {docs[0].get('y')}, {docs[0].get('x')}")
        else:
            print(f"Address: {addr} -> FAILED")

if __name__ == "__main__":
    test()
