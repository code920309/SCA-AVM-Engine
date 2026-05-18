import os
import sys
import time
import numpy as np
import pandas as pd

# UTF-8 출력 강제 설정 (Windows 콘솔 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main():
    input_path = "data/processed/avm_precision_set.csv"
    output_path = "data/processed/avm_precision_set_clean.csv"
    
    print("=" * 80)
    print(" [SCA AVM Engine - 부동산 집합건물 데이터셋 초정밀 전처리 & 클렌징 시작]")
    print("=" * 80)
    
    if not os.path.exists(input_path):
        print(f"오류: {input_path} 파일이 존재하지 않습니다.")
        return
        
    start_time = time.time()
    
    # 0. 데이터셋 로드 및 초기 정보 획득
    df = pd.read_csv(input_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
    initial_shape = df.shape
    initial_nulls = df.isna().sum()
    
    print(f"  * 로드된 초기 데이터 셰이프: {initial_shape[0]:,}행, {initial_shape[1]:,}열")
    
    # ----------------------------------------------------
    # 1. 세종시 행정명 복원
    # ----------------------------------------------------
    print("\n[Step 1] 세종시 행정명 복원 진행 중...")
    sejong_mask = (df['sggCd'] == '36110') & (df['sggNm'].isna() | (df['sggNm'].astype(str).str.strip() == ''))
    sejong_count = sejong_mask.sum()
    df.loc[sejong_mask, 'sggNm'] = '세종특별자치시'
    print(f"  -> 복원 완료: 세종특별자치시 sggNm 결측치 {sejong_count:,}건 복원 완료.")

    # ----------------------------------------------------
    # 2. 층수(floor) 공백 문자 처리
    # ----------------------------------------------------
    print("\n[Step 2] 층수(floor) 공백 문자 및 결측치 처리 진행 중...")
    # 양 끝 공백 제거 및 결측치/빈문자열 색출
    df['floor'] = df['floor'].astype(str).str.strip()
    floor_missing_mask = (df['floor'] == '') | (df['floor'].isna()) | (df['floor'].str.lower() == 'nan') | (df['floor'] == 'None')
    floor_missing_count = floor_missing_mask.sum()
    df.loc[floor_missing_mask, 'floor'] = 'Unknown'
    print(f"  -> 처리 완료: 결측/공백 층수 {floor_missing_count:,}건을 'Unknown' 범주형으로 보전 완료.")

    # ----------------------------------------------------
    # 3. 가짜 0값(False Zeros) 마스킹
    # ----------------------------------------------------
    print("\n[Step 3] 가짜 0값(False Zeros) 식별 및 마스킹 진행 중...")
    # 가짜 0값 카운팅용 변수
    initial_zero_pubuse = (df['pubuseAr'] == 0.0).sum()
    initial_zero_parking = (df['parkingCount'] == 0.0).sum()
    
    # 공용면적(pubuseAr)이 0.0인 데이터는 전부 결측치(np.nan)로 변환
    df.loc[df['pubuseAr'] == 0.0, 'pubuseAr'] = np.nan
    
    # 주차대수(parkingCount)가 0.0인 데이터 마스킹
    # 단, buildYear가 1980년 이전이면서 전용면적 buildingAr이 30㎡ 이하인 경우는 0.0 유지
    df['buildYear_numeric'] = pd.to_numeric(df['buildYear'], errors='coerce')
    parking_mask = (df['parkingCount'] == 0.0) & ~(
        (df['buildYear_numeric'] < 1980) & (df['buildingAr'] <= 30.0)
    )
    masked_parking_count = parking_mask.sum()
    df.loc[parking_mask, 'parkingCount'] = np.nan
    
    print(f"  -> 마스킹 완료 (공용면적): {initial_zero_pubuse:,}건의 0.0 값을 np.nan으로 처리.")
    print(f"  -> 마스킹 완료 (주차대수): {initial_zero_parking:,}건 중 {masked_parking_count:,}건을 np.nan으로 처리 (조건부 유지: {initial_zero_parking - masked_parking_count:,}건).")

    # ----------------------------------------------------
    # 4. 빌딩 레벨 결측치 추적 복원 (Advanced Group Imputation)
    # ----------------------------------------------------
    print("\n[Step 4] 빌딩 레벨 결측치 추적 복원 진행 중...")
    
    # 복원 전 결측치 상태 확인
    nan_pubuse_before = df['pubuseAr'].isna().sum()
    nan_parking_before = df['parkingCount'].isna().sum()
    
    # ■ 1순위 복원: 동일 'buildingName'과 'road_address'를 공유하는 다른 호실들의 평균값 적용
    print("  * [1순위] 동일 빌딩/도로명 내 타 호실 평균값으로 채움 중...")
    
    # 결측치 채우기용 빌딩 레벨 그룹 평균 계산
    pubuse_bld_mean = df.groupby(['buildingName', 'road_address'])['pubuseAr'].transform('mean')
    parking_bld_mean = df.groupby(['buildingName', 'road_address'])['parkingCount'].transform('mean')
    
    df['pubuseAr'] = df['pubuseAr'].fillna(pubuse_bld_mean)
    df['parkingCount'] = df['parkingCount'].fillna(parking_bld_mean)
    
    nan_pubuse_mid = df['pubuseAr'].isna().sum()
    nan_parking_mid = df['parkingCount'].isna().sum()
    print(f"    -> 1순위 완료 (공용면적): {nan_pubuse_before - nan_pubuse_mid:,}건 복원 완료 (잔여 결측: {nan_pubuse_mid:,}건)")
    print(f"    -> 1순위 완료 (주차대수): {nan_parking_before - nan_parking_mid:,}건 복원 완료 (잔여 결측: {nan_parking_mid:,}건)")
    
    # ■ 2순위 복원: 동일 'umdNm'(읍면동)과 'buildingUse'(건물용도)를 공유하는 그룹의 평균 '전용률' 및 '면적당 주차대수'로 역산
    print("  * [2순위] 읍면동 & 건물용도 그룹 평균 비율 기준 역산 복원 중...")
    
    # 임시 계산용 전용률 및 주차밀도 산출 (NaN이 아닌 데이터 기준)
    df['temp_ex_rate'] = df['buildingAr'] / (df['buildingAr'] + df['pubuseAr'])
    df['temp_pkg_ratio'] = df['parkingCount'] / df['buildingAr']
    
    # 그룹 평균 계산 (초고속 벡터라이즈 연산!)
    df['group_ex_rate'] = df.groupby(['umdNm', 'buildingUse'])['temp_ex_rate'].transform('mean')
    df['group_pkg_ratio'] = df.groupby(['umdNm', 'buildingUse'])['temp_pkg_ratio'].transform('mean')
    
    # 만약 해당 그룹 전체가 NaN이어서 그룹 평균이 NaN인 경우를 대비한 글로벌 디폴트 폴백
    global_ex_rate_mean = df['temp_ex_rate'].mean()
    global_pkg_ratio_mean = df['temp_pkg_ratio'].mean()
    
    df['group_ex_rate'] = df['group_ex_rate'].fillna(global_ex_rate_mean)
    df['group_pkg_ratio'] = df['group_pkg_ratio'].fillna(global_pkg_ratio_mean)
    
    # 전용률(ex_rate)이 1.0(공용면적 0)인 비정상적인 경우를 방지하기 위해 0.99로 상한선 클램핑
    df['group_ex_rate'] = df['group_ex_rate'].clip(upper=0.99)
    
    # 역산 공식 적용
    # pubuseAr = buildingAr * (1/ex_rate - 1)
    pubuse_imputed = df['buildingAr'] * (1.0 / df['group_ex_rate'] - 1.0)
    # parkingCount = buildingAr * pkg_ratio
    parking_imputed = df['buildingAr'] * df['group_pkg_ratio']
    
    # 2순위 결측치 최종 대입
    df['pubuseAr'] = df['pubuseAr'].fillna(pubuse_imputed).round(2)
    df['parkingCount'] = df['parkingCount'].fillna(parking_imputed).round(2)
    
    nan_pubuse_after = df['pubuseAr'].isna().sum()
    nan_parking_after = df['parkingCount'].isna().sum()
    
    print(f"    -> 2순위 완료 (공용면적): {nan_pubuse_mid - nan_pubuse_after:,}건 복원 완료 (잔여 결측: {nan_pubuse_after:,}건)")
    print(f"    -> 2순위 완료 (주차대수): {nan_parking_mid - nan_parking_after:,}건 복원 완료 (잔여 결측: {nan_parking_after:,}건)")

    # ----------------------------------------------------
    # 5. 파생변수 최종 생성
    # ----------------------------------------------------
    print("\n[Step 5] 핵심 도메인 파생 변수 생성 중...")
    df['exclusive_rate'] = (df['buildingAr'] / (df['buildingAr'] + df['pubuseAr'])).round(4)
    df['parking_density'] = (df['parkingCount'] / df['buildingAr']).round(4)
    print("  -> 생성 완료: 'exclusive_rate' (전용률), 'parking_density' (주차 밀도) 추가 완료.")

    # ----------------------------------------------------
    # 6. 시장 왜곡 거래(이상치) 제거
    # ----------------------------------------------------
    print("\n[Step 6] 시장 왜곡 거래(이상치) 탐색 및 제거 진행 중...")
    lower_limit = df['adjusted_price_per_m2'].quantile(0.02)
    upper_limit = df['adjusted_price_per_m2'].quantile(0.98)
    
    outliers_mask = (df['adjusted_price_per_m2'] < lower_limit) | (df['adjusted_price_per_m2'] > upper_limit)
    outlier_count = outliers_mask.sum()
    
    # 아웃라이어 필터링
    df_clean = df[~outliers_mask].copy()
    
    # 임시 사용했던 가공 변수 정리
    df_clean = df_clean.drop(columns=[
        'buildYear_numeric', 'temp_ex_rate', 'temp_pkg_ratio', 
        'group_ex_rate', 'group_pkg_ratio'
    ])
    
    print(f"  -> 이상치 경계값: 하위 2% = {lower_limit:,.1f}원/㎡ | 상위 2% = {upper_limit:,.1f}원/㎡")
    print(f"  -> 제거 완료: 시장 왜곡 거래 아웃라이어 총 {outlier_count:,}건 제거 완료.")

    # ----------------------------------------------------
    # 7. 파일 저장 및 최종 결과 리포트 출력
    # ----------------------------------------------------
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    elapsed_time = time.time() - start_time
    
    final_shape = df_clean.shape
    final_nulls = df_clean.isna().sum()
    
    print("\n" + "=" * 80)
    print(" 📊 SCA AVM Engine - 데이터 전처리/클렌징 완료 최종 보고서")
    print("=" * 80)
    print(f"  * 소요 시간: {elapsed_time:.2f} 초")
    print(f"  * 데이터 셰이프 변화: {initial_shape[0]:,}행 x {initial_shape[1]:,}열 -> {final_shape[0]:,}행 x {final_shape[1]:,}열")
    print("-" * 80)
    print("  [전처리 전후 주요 컬럼 결측치 변화량]")
    
    cols_to_report = ['sggNm', 'floor', 'pubuseAr', 'parkingCount', 'exclusive_rate', 'parking_density']
    for col in cols_to_report:
        before_val = initial_nulls.get(col, 0)
        # 만약 기존 데이터셋에 컬럼이 아예 없었던 경우, 결측치 건수는 전체 행 개수와 동일
        if col in ['exclusive_rate', 'parking_density']:
            before_val = initial_shape[0]
            
        after_val = final_nulls.get(col, 0)
        print(f"  - {col:18s} | 전처리 전 결측: {before_val:6,}건 -> 전처리 후 결측: {after_val:5,}건")
        
    print("-" * 80)
    print(f"  * 저장된 파일 경로: {output_path}")
    print("=" * 80)

if __name__ == "__main__":
    main()
