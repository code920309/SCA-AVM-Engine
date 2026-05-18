import pandas as pd
import numpy as np
import sys
import os
import time

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
    print(" [전체 데이터셋 대상 다차원 결측치 보강 프로세스 시작]")
    print("=" * 70)
    
    # 데이터 로드
    df = pd.read_csv(csv_path, dtype={"sggCd": str, "jibun": str})
    total_rows = len(df)
    
    # 결측 대상 식별
    missing_mask = df['buildYear'].isna() | (df['buildYear'].astype(str).str.strip() == '') | (df['buildYear'] == 0)
    missing_indices = df[missing_mask].index.tolist()
    missing_count = len(missing_indices)
    
    print(f"  * 전체 데이터 수: {total_rows:,} 건")
    print(f"  * 건축물 준공년도 결측 대상: {missing_count:,} 건")
    print(f"  * 일일 공공데이터 트래픽 제한(10,000건) 이내로 안전하게 전수 보강 가능합니다.")
    print("-" * 70)
    
    # 필요한 신규 컬럼들 초기화 (기존 컬럼이 없으면 생성)
    if 'parkingCount' not in df.columns:
        df['parkingCount'] = np.nan
    if 'pubuseAr' not in df.columns:
        df['pubuseAr'] = np.nan
        
    restorer = AddressRestorer()
    
    success_count = 0
    start_time = time.time()
    
    print("\n보강 작업을 실행하는 중입니다 (트래픽 안정성을 위해 주기적으로 저장합니다)...")
    
    for idx, df_idx in enumerate(missing_indices):
        row = df.loc[df_idx]
        
        # 보강용 딕셔너리 데이터 구성
        item_data = {
            "sggCd": str(row.get('sggCd')),
            "sggNm": row.get('sggNm'),
            "umdNm": row.get('umdNm'),
            "jibun": str(row.get('jibun')),
            "buildingAr": str(row.get('buildingAr')),
            "buildingType": str(row.get('buildingType')),
            "floor": str(row.get('floor')),
            "buildYear": ""
        }
        
        try:
            # API 보강 호출
            enriched = restorer.restore_and_enrich(item_data)
            
            # DataFrame에 반영
            if enriched.get('buildYear'):
                df.at[df_idx, 'buildYear'] = enriched['buildYear']
            if enriched.get('plottageAr'):
                df.at[df_idx, 'plottageAr'] = enriched['plottageAr']
            if enriched.get('parkingCount') is not None:
                df.at[df_idx, 'parkingCount'] = enriched['parkingCount']
            if enriched.get('pubuseAr') is not None:
                df.at[df_idx, 'pubuseAr'] = enriched['pubuseAr']
                
            success_count += 1
            
        except Exception as e:
            print(f"\n  [오류 발생 - 인덱스 {df_idx}]: {e}")
            
        # API 서버 부하 조절을 위한 미세 딜레이 (0.15초)
        time.sleep(0.15)
        
        # 진행 상황 출력 (100건마다)
        if (idx + 1) % 100 == 0 or (idx + 1) == missing_count:
            elapsed = time.time() - start_time
            speed = elapsed / (idx + 1)
            eta = speed * (missing_count - (idx + 1))
            print(f"  >> 진행도: {idx + 1}/{missing_count} 건 ({(idx + 1)/missing_count*100:.1f}%) | ETA: {eta:.0f}초")
            
        # 안정성을 위해 500건마다 임시 중간 저장
        if (idx + 1) % 500 == 0:
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"  [안전 저장] {idx + 1}건 완료 시점 중간 저장 성공!")
            
    # 최종 결과물 저장
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print("\n" + "=" * 70)
    print(" [보강 완료]")
    print(f"  * 성공적으로 보강된 결측치: {success_count:,} 건 / 시도: {missing_count:,} 건")
    print(f"  * 전체 소요시간: {time.time() - start_time:.1f} 초")
    print(f"  * 데이터 파일 업데이트 완료: {csv_path}")
    print("=" * 70)

if __name__ == "__main__":
    main()
