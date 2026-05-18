import pandas as pd
import numpy as np
import sys
import os

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.features.address_restorer import AddressRestorer

def main():
    csv_path = "data/processed/avm_precision_set.csv"
    if not os.path.exists(csv_path):
        print(f"오류: {csv_path} 파일이 존재하지 않습니다.")
        return
        
    print("=" * 70)
    print(" [새로운 다차원 보강 API 파이프라인 스모크 테스트]")
    print("=" * 70)
    
    df = pd.read_csv(csv_path, dtype={"sggCd": str, "jibun": str})
    
    # 누락된 행 중 유효한 행 선별
    missing_mask = df['buildYear'].isna() | (df['buildYear'].astype(str).str.strip() == '') | (df['buildYear'] == 0)
    valid_missing_df = df[missing_mask].dropna(subset=['sggCd', 'sggNm', 'umdNm', 'jibun'])
    valid_missing_df = valid_missing_df[valid_missing_df['jibun'].astype(str).str.strip() != '']
    
    if len(valid_missing_df) == 0:
        print("테스트할 결측치 데이터가 없습니다.")
        return
        
    restorer = AddressRestorer()
    
    # 상위 5건 추출
    test_records = valid_missing_df.head(5).to_dict(orient='records')
    
    for idx, row in enumerate(test_records):
        print(f"\n[스모크 테스트 #{idx + 1}]")
        print(f"  * 주소: {row.get('sggNm')} {row.get('umdNm')} {row.get('jibun')}")
        print(f"  * 조건: 건물유형={row.get('buildingType')} | 전용면적={row.get('buildingAr')}㎡ | 층={row.get('floor')}층")
        print("  " + "-" * 55)
        print("  * 보강 전 값:")
        print(f"    - 준공년도(buildYear): {row.get('buildYear')}")
        print(f"    - 대지면적(plottageAr): {row.get('plottageAr')}")
        print(f"    - [신규] 총 주차대수: (컬럼 없음)")
        print(f"    - [신규] 공용면적: (컬럼 없음)")
        print("  " + "-" * 55)
        
        # 보강용 테스트 아이템 빌드
        test_item = {
            "sggCd": str(row.get('sggCd')),
            "sggNm": row.get('sggNm'),
            "umdNm": row.get('umdNm'),
            "jibun": str(row.get('jibun')),
            "buildingAr": str(row.get('buildingAr')),
            "buildingType": str(row.get('buildingType')),
            "floor": str(row.get('floor')),
            "buildYear": ""
        }
        
        enriched = restorer.restore_and_enrich(test_item)
        
        print("  * \033[92m보강 후 값:\033[0m")
        print(f"    - 건물명: {enriched.get('buildingName')}")
        print(f"    - 도로명 주소: {enriched.get('road_address')}")
        print(f"    - 준공년도(buildYear): \033[94m{enriched.get('buildYear')}\033[0m 년도")
        print(f"    - 대지면적(plottageAr): \033[94m{enriched.get('plottageAr')}\033[0m ㎡")
        print(f"    - 총 주차대수(parkingCount): \033[94m{enriched.get('parkingCount')}\033[0m 대")
        print(f"    - 공용면적(pubuseAr): \033[94m{enriched.get('pubuseAr')}\033[0m ㎡")
        print("=" * 70)
        
    print("\n[성공] 5개 누락 레코드 다차원 스모크 테스트 완료!")
    print("=" * 70)

if __name__ == "__main__":
    main()
