"""
파일명: test_coords_and_bjdong.py
설명: 좌표 데이터 및 법정동 매핑 정합성 테스트
단계 및 처리과정:
1. 타겟 좌표군 추출: 테스트를 위한 무작위 샘플 혹은 경계 좌표를 추출합니다.
2. 리버스 지오코딩 검증: 추출된 좌표를 법정동 코드로 변환하여 원본과 대조합니다.
3. 오차율 분석: 지리 정보와 법정동 단위 간의 불일치(Mismatch) 발생 빈도를 계산합니다.
4. 매핑 딕셔너리 보정: 검증 과정에서 나타난 예외 케이스를 반영하여 매핑 사전을 업데이트합니다.
5. 테스트 결과 기록: 정합성 평가 로그와 수정 내역을 산출하여 문서화합니다.
"""

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
