import pandas as pd
import numpy as np
import os
import sys
import pickle
import time
from sklearn.ensemble import HistGradientBoostingRegressor

# UTF-8 출력 강제 설정 (Windows 콘솔 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main():
    data_path = "data/processed/avm_precision_set_v3_20percent.csv"
    export_dir = "data/model"
    
    if not os.path.exists(data_path):
        print(f"Error: {data_path} does not exist. Please run add_multi_centroid_and_oof.py first.")
        return
        
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
        
    print("=" * 85)
    print(" 🚀 [SCA AVM Engine - 상용 프로덕션 배포용 최종 모델 추출 파이프라인]")
    print("=" * 85)
    
    start_time = time.time()
    
    # 1. 고도화 데이터셋 로드
    df = pd.read_csv(data_path, low_memory=False)
    print(f"  * 마스터 데이터 로드 완료: {len(df):,}행 x {df.shape[1]:,}열")
    
    # 2. 실시간 추론을 위한 메타데이터 맵 구축
    print("\n[Step 1] 실시간 추론(Inference) 연동용 메타데이터 맵 추출...")
    
    # ① 읍면동 OOF 타겟 인코딩 매핑 테이블 추출 (새로운 매물 입력 시 읍면동 명칭만으로 단가 자동 매핑)
    # 전체 단가 로그 변환 기준 평균 연산
    df_log = np.log1p(df['adjusted_price_per_m2'])
    umd_to_price_log = df_log.groupby(df['umdNm']).mean().to_dict()
    global_median_log = df_log.median()
    
    umd_mapping_package = {
        'umd_to_price_log': umd_to_price_log,
        'global_median_log': global_median_log
    }
    
    # ② 시군구별 핵심 상권 거점(Hotspot) 좌표 사전 구축
    sgg_to_hotspots_map = {}
    for sgg, group in df.groupby('sggNm'):
        valid_grp = group[group['lat'].notna() & group['lng'].notna()]
        if len(valid_grp) == 0:
            continue
            
        q_val = valid_grp['adjusted_price_per_m2'].quantile(0.95)
        hotspots = valid_grp[valid_grp['adjusted_price_per_m2'] >= q_val]
        
        if len(hotspots) == 0:
            hotspots = valid_grp.nlargest(1, 'adjusted_price_per_m2')
            
        sgg_to_hotspots_map[sgg] = {
            'lats': hotspots['lat'].values.tolist(),
            'lngs': hotspots['lng'].values.tolist()
        }
    
    print(f"    - 읍면동 인코딩 딕셔너리 구축 완료 (총 {len(umd_to_price_log)}개 읍면동)")
    print(f"    - 시군구 핫스팟 좌표 사전 구축 완료 (총 {len(sgg_to_hotspots_map)}개 시군구)")
    
    # 3. 모델 학습용 변수 정리 (실제 프로덕션 배포 시에는 데이터 100%를 학습에 투입하여 성능 극대화)
    print("\n[Step 2] 프로덕션용 전체 데이터 대상 최종 모델 학습 개시...")
    y = df['adjusted_price_per_m2']
    y_log = np.log1p(y)
    
    base_drops = ['adjusted_price_per_m2', 'dealAmount', 'jibun', 'road_address', 'sggNm', 'umdNm', 'strctCdNm', 'cache_key', 'umd_encoded']
    X = df.drop(columns=[c for c in base_drops if c in df.columns])
    
    # 범주형 인덱스 수집 및 캐스팅
    categorical_cols = ['buildingUse', 'landUse', 'floor', 'buyerGbn', 'dealingGbn', 'slerGbn']
    cat_indices = []
    for col in categorical_cols:
        if col in X.columns:
            X[col] = X[col].astype('category')
            cat_indices.append(X.columns.get_loc(col))
            
    # 최종 튜닝 모델 인스턴스 생성
    final_model = HistGradientBoostingRegressor(
        loss='squared_error',
        max_iter=700,
        learning_rate=0.025,
        max_depth=12,
        l2_regularization=5.0,
        categorical_features=cat_indices,
        random_state=42
    )
    
    # 전체 5만여개 데이터 학습 진행 (프로덕션 버전)
    final_model.fit(X, y_log)
    print(f"    - 학습 완료: {X.shape[0]:,}행 x {X.shape[1]:,}열 대상 전체 피팅 성공")
    
    # 4. 모델 패키징 및 직렬화 저장
    print("\n[Step 3] 모델 및 인공지능 자산 바이너리 패키징 저장...")
    
    # 추론 시 피처 순서 보장을 위한 컬럼 명세 보관
    feature_names = X.columns.tolist()
    
    model_package = {
        'model': final_model,
        'feature_names': feature_names,
        'categorical_cols': categorical_cols,
        'cat_indices': cat_indices,
        'umd_mapping': umd_mapping_package,
        'sgg_hotspots': sgg_to_hotspots_map
    }
    
    export_path = os.path.join(export_dir, "final_avm_model_package.pkl")
    with open(export_path, 'wb') as f:
        pickle.dump(model_package, f, protocol=pickle.HIGHEST_PROTOCOL)
        
    print(f"  -> [추출 성공] 모델 패키지가 성공적으로 추출 및 저장되었습니다:")
    print(f"     * 파일 경로: {export_path}")
    print(f"     * 파일 용량: {os.path.getsize(export_path) / (1024 * 1024):.2f} MB")
    
    print("\n[Step 4] 실시간 Inference API 서빙 연동 코드 가이드 (로드 샘플)")
    print("=" * 85)
    print("""
    # Python API 서버 내 로드 및 예측 예제:
    import pickle
    import numpy as np
    
    # 1. 모델 패키지 불러오기
    with open("data/model/final_avm_model_package.pkl", "rb") as f:
        package = pickle.load(f)
        
    model = package['model']
    umd_map = package['umd_mapping']['umd_to_price_log']
    global_median_log = package['umd_mapping']['global_median_log']
    sgg_hotspots = package['sgg_hotspots']
    
    # 2. 신규 매물 데이터 인입 시 실시간 변환 예시:
    # - umdNm 명칭 -> umd_encoded_oof 값 매핑
    # - lat/lng -> 최인접 sgg_hotspots 최단 거리 계산
    # - features DataFrame 구성 후 model.predict(X_inference) 기동 -> np.expm1 복원!
    """)
    print("=" * 85)
    
    total_time = time.time() - start_time
    print(f"  * 전체 모델 추출 완료 소요 시간: {total_time:.2f}초")

if __name__ == "__main__":
    main()
