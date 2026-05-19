"""
파일명: add_spatiotemporal_embeddings.py
설명: 시공간 임베딩(Spatiotemporal Embeddings) 피처 추가
단계 및 처리과정:
1. 데이터 로드: 전처리된 시계열 및 공간 데이터를 로드합니다.
2. 시간 피처 추출: 거래 연월 등 시간적 특성을 수치 및 카테고리 임베딩으로 변환합니다.
3. 공간 피처 처리: 공간 정보(좌표, 행정동)를 임베딩 공간에 매핑합니다.
4. 시공간 결합: 시간과 공간의 상호작용 피처를 생성하고 결합합니다.
5. 데이터 병합 및 저장: 시공간 임베딩을 원본 데이터에 병합하고 저장합니다.
"""

import pandas as pd
import numpy as np
import os
import time
from sklearn.model_selection import KFold
from sklearn.neighbors import KNeighborsRegressor
import sys

# UTF-8 출력 강제 설정 (Windows 콘솔 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees) using numpy vectorization.
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

def main():
    input_path = "data/processed/avm_precision_set_enriched.csv"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} does not exist.")
        return
        
    print("=" * 80)
    print(" [SCA AVM Engine - 시공간 융합 임베딩 (Spatial-Temporal) 피처 생성]")
    print("=" * 80)
    
    start_time = time.time()
    
    # 1. 데이터셋 로드
    df = pd.read_csv(input_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
    print(f"  * 데이터 로드 완료: {len(df):,}행 x {df.shape[1]:,}열")
    
    # 2. [공간] dist_to_sgg_center 생성 (시군구 행정·상권 중심지 거리)
    print("\n[Step 1] 'dist_to_sgg_center' (시군구 중심지 거리) 피처 생성 중...")
    
    # 시군구별 lat/lng의 중간값(Median)을 중심 좌표로 설정 (이상치 방지)
    sgg_centers = df.groupby('sggCd')[['lat', 'lng']].median().reset_index()
    sgg_centers.columns = ['sggCd', 'sgg_center_lat', 'sgg_center_lng']
    
    # 만약 시군구 전체가 NaN인 경우를 대비한 글로벌 중간값 설정
    global_lat_median = df['lat'].median()
    global_lng_median = df['lng'].median()
    sgg_centers['sgg_center_lat'] = sgg_centers['sgg_center_lat'].fillna(global_lat_median)
    sgg_centers['sgg_center_lng'] = sgg_centers['sgg_center_lng'].fillna(global_lng_median)
    
    # 중심 좌표 머지
    df = df.merge(sgg_centers, on='sggCd', how='left')
    
    # 결측치 보완 및 거리 계산
    lat_prop = df['lat'].fillna(df['sgg_center_lat'])
    lng_prop = df['lng'].fillna(df['sgg_center_lng'])
    
    df['dist_to_sgg_center'] = haversine_np(lng_prop, lat_prop, df['sgg_center_lng'], df['sgg_center_lat']).round(4)
    df = df.drop(columns=['sgg_center_lat', 'sgg_center_lng'])
    
    print(f"  -> 완료: 'dist_to_sgg_center' 평균 거리 = {df['dist_to_sgg_center'].mean():.2f} km")
    
    # 3. [공간] spatial_knn_price 생성 (Data Leakage 없는 5-Fold OOF KNN 공간 보정 표면)
    print("\n[Step 2] 'spatial_knn_price' (KNeighborsRegressor 5-Fold OOF 공간 보정) 생성 중...")
    
    valid_mask = df['lat'].notna() & df['lng'].notna()
    valid_indices = df[valid_mask].index.values
    
    df['spatial_knn_price'] = np.nan
    
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    
    fold_times = []
    for fold, (train_sub_idx, val_sub_idx) in enumerate(kf.split(valid_indices)):
        fold_start = time.time()
        train_idx = valid_indices[train_sub_idx]
        val_idx = valid_indices[val_sub_idx]
        
        knn = KNeighborsRegressor(n_neighbors=15, weights='distance', n_jobs=-1)
        knn.fit(df.loc[train_idx, ['lat', 'lng']], df.loc[train_idx, 'adjusted_price_per_m2'])
        
        df.loc[val_idx, 'spatial_knn_price'] = knn.predict(df.loc[val_idx, ['lat', 'lng']])
        fold_times.append(time.time() - fold_start)
        print(f"    - Fold {fold + 1}/5 완료 ({fold_times[-1]:.2f}초)")
        
    # lat/lng 결측 등의 행은 글로벌 타겟 평균으로 대체
    df['spatial_knn_price'] = df['spatial_knn_price'].fillna(df['adjusted_price_per_m2'].mean()).round(2)
    print(f"  -> 완료: 'spatial_knn_price' 생성 완료 (OOF 공간 가격 모델)")
    
    # 4. [시간] price_momentum 생성 (거시경제/모멘텀 반영 연속 시간축 피처)
    print("\n[Step 3] 'price_momentum' (연속 시간축 가격 모멘텀) 생성 중...")
    df['price_momentum'] = (df['dealYear'] - 2021) * 4 + (df['dealMonth'] // 3)
    print(f"  -> 완료: 'price_momentum' 생성 완료 (최소값: {df['price_momentum'].min()}, 최대값: {df['price_momentum'].max()})")
    
    # 5. 최종 데이터셋 덮어쓰기 저장
    df.to_csv(input_path, index=False, encoding='utf-8-sig')
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(" 📊 시공간 융합 임베딩 피처 반영 완료")
    print("=" * 80)
    print(f"  * 소요 시간: {elapsed:.2f} 초")
    print(f"  * 최종 데이터 셰이프: {df.shape[0]:,}행 x {df.shape[1]:,}열")
    print(f"  * 업데이트 파일 경로: {input_path}")
    print("=" * 80)

if __name__ == "__main__":
    main()
