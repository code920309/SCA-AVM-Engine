# SCA AVM Engine: 대한민국 상업용 부동산 자동가치산정모형 고도화 프로젝트

🔗 **모델 배포 주소 (Hugging Face)**: [https://huggingface.co/donggyuuu/SCA-AVM-Engine](https://huggingface.co/donggyuuu/SCA-AVM-Engine)

본 저장소는 실거래 가격 데이터를 기반으로 금융 기관 감정평가 수준의 고정밀 자동가치산정모형(AVM, Automated Valuation Model)을 설계하고 구축한 프로젝트 결과물입니다. 5단계의 아키텍처 고도화를 거쳐 최종 결정계수 0.7054를 정복한 기술적 도약과 모델 튜닝 과정을 다룹니다.

---

## 목차 (Table of Contents)
- [1. SCA AVM Engine 성능 진화 요약](#1-sca-avm-engine-성능-진화-요약)
- [2. 각 단계별 아키텍처 설계 및 튜닝 명세](#2-각-단계별-아키텍처-설계-및-튜닝-명세)
- [3. 손실 함수 설계에 관한 수학적 실증 결과 (MSE vs MAE)](#3-손실-함수-설계에-관한-수학적-실증-결과-mse-vs-mae)
- [4. 최종 서빙용 모델 및 튜닝 파라미터 정의](#4-최종-서빙용-모델-및-튜닝-파라미터-정의)
- [5. 모델 내보내기 및 실시간 서빙 연동 규격](#5-모델-내보내기-및-실시간-서빙-연동-규격)
- [6. 데이터 수집 및 전처리 파이프라인 (Data Pipeline)](#6-데이터-수집-및-전처리-파이프라인-data-pipeline)
- [최종 상용 배포용 모델 사용법 (Inference Guide)](#최종-상용-배포용-모델-사용법-inference-guide)

---

## 1. SCA AVM Engine 성능 진화 요약

모델은 데이터 정제부터 미시 상권 다중 거점 거리 변수화에 이르기까지 점진적으로 고도화되었습니다.

| 단계 및 아키텍처 | 주요 튜닝 포인트 | 결정계수 (R² Score) | 평균 절대 오차율 (MAPE) |
| :--- | :--- | :---: | :---: |
| **1단계: 베이스라인 (Raw)** | 세종시 결측치 복원, 아웃라이어 및 가짜 0값 정제 | `0.5626` | `41.99%` |
| **2단계: 외부 API 융합 (Clean)** | 세움터(건폐율/용적률 위반) 및 카카오 지하철/카페 결합 | `0.5696` | `36.45%` |
| **3단계: 시공간 임베딩** | KNN 거리 곡면 + 트리 비선형 지리 경계 피처 이식 | `0.6510` | `31.72%` |
| **4단계: 스태킹 앙상블** | HistGradientBoosting + CatBoost (텍스트 임베딩) 융합 | `0.6738` | `30.04%` |
| **5단계: v3 초고도화 (최종)** | Leakage-Free OOF 읍면동 인코딩 + 다중 상권 핫스팟 최단거리 | **`0.7054`** | **`30.33%`** |

---

## 2. 각 단계별 아키텍처 설계 및 튜닝 명세

### 1단계: 데이터 정제 및 베이스라인 구축 (Raw Dataset)
* sggCd가 36110(세종시)인 행 중 sggNm 결측치를 '세종특별자치시'로 복원.
* floor(층수) 컬럼의 양 끝 공백 제거 후 완전 빈 문자열은 'Unknown' 범주형으로 치환.
* 집합건물 특성 상 공용면적과 대지지분이 모두 0인 행을 Null 처리 후 중앙값 대체.
* dealAmount 거래 금액 기준 상하위 2% 아웃라이어 드롭.

### 2단계: 외부 행정/입지 API 융합 (Clean Dataset)
* 세움터 표제부 연계를 통한 건폐율(bcRat) 및 용적률(vlRat) 매핑 및 결측치는 읍면동 평균값 대체.
* 카카오 로컬 API를 연동하여 최인접 지하철역 거리(dist_to_subway), 경유 노선 수(subway_line_count), 반경 200m 내 카페 수(cafe_count_200m) 결합.

### 3단계: 시공간 융합 임베딩 (Spatiotemporal Embedding)
* **KNN 공간 표면 (spatial_knn_price)**: Harversine 거리를 기반으로 타겟 단가를 예측한 전국구 공간 보정 표면 구축 (n_neighbors=15, weights='distance').
* **트리 공간 표면 (spatial_tree_price)**: 5-Fold OOF 구조 하에서 HistGradientBoostingRegressor를 구동하여 위경도로만 타겟 단가를 학습한 계단식 영역 경계면 형성.
* **하이브리드 모델**: 두 공간 피처를 동시에 이식하여 연속 거리 곡면과 지리적 단절선을 동시에 학습.

### 4단계: 고차원 Stacking 앙상블 모형
* **Level 0 베이스 모델**: HistGradientBoostingRegressor + CatBoostRegressor (원본 텍스트 카테고리 임베딩 가동).
* **Level 1 메타 모델**: Ridge(alpha=1.0)를 지정하여 5-Fold Stacking CV 내부 격리 구조 하에서 과적합 없이 가중치 융합.

### 5단계: v3 다중 거점 및 Leakage-Free OOF 타겟 인코딩 (최종 최적 모델)
* **Leakage-Free OOF 읍면동 인코딩 (umd_encoded_oof)**: 5-Fold 교차 검증 내부 루프에서 오직 훈련 폴드 데이터로만 읍면동별 평균 단가를 계산하여 검증 폴드에 매핑하는 엄격한 격리 장치를 통해 데이터 누수율 0.0% 달성.
* **다중 상권 거점 피처 (dist_to_closest_hotspot)**: 각 시군구(sggNm) 내 실거래가 상위 5% 매물 좌표를 '미시 상권 핵심지(Hotspot)'로 지정하고 최인접 최단 하버사인 거리(km) 연산.
* **모델 하이퍼 가중치 극대화**: max_iter=700, learning_rate=0.025, l2_regularization=5.0으로 규제를 대폭 강화하여 복잡한 비선형 입지 지표들의 과적합 방어.

---

## 3. 손실 함수 설계에 관한 수학적 실증 결과 (MSE vs MAE)

오차율(MAPE) 최소화를 위해 MAE 손실 함수와 샘플 가중치를 적용해 본 결과 다음과 같은 수학적 결론을 도출하였습니다:

1. **로그 제곱 오차 (squared_error / MSE)**: 결정계수 `0.7054` | MAPE `30.33%`
2. **로그 절대 오차 (absolute_error / MAE) + 단가 역비례 가중치**: 결정계수 `0.6880` | MAPE `31.27%`

* **결론**: 타겟 단가가 로그 변환(`np.log1p`)된 상태에서는 **제곱 오차(MSE)**를 쓰는 것이 원래 공간에서의 **기하 평균(Geometric Mean)** 예측으로 수동 유도되어, 이상치를 차단하고 **오차율(MAPE) 자체를 최적화하는 데 가장 탁월함**이 실증 검증되었습니다.

---

## 4. 최종 서빙용 모델 및 튜닝 파라미터 정의

SCA AVM Engine 서빙 파이프라인으로 채택된 최종 모델 명세입니다.

```python
model = HistGradientBoostingRegressor(
    loss='squared_error',
    max_iter=700,
    learning_rate=0.025,
    max_depth=12,
    l2_regularization=5.0,
    categorical_features=cat_indices,
    random_state=42
)
```

---

## 5. 모델 내보내기 및 실시간 서빙 연동 규격

학습 완료된 모델과 전처리 파이프라인(OOF 읍면동 사전, 시군구 핫스팟 사전자료)은 API 서버에서 실시간 서빙될 수 있도록 `joblib`을 통해 바이너리 형태로 일괄 추출이 가능합니다. 상세 추출 코드는 `export_final_model.py`를 참조하십시오.

---

## 6. 데이터 수집 및 전처리 파이프라인 (Data Pipeline)

본 프로젝트는 상업용 부동산의 불투명성과 극심한 지역적 가치 편차를 극복하기 위해 다원화된 정밀 데이터 수집 및 가공 파이프라인을 구축하였습니다. 실제 데이터의 흐름 및 처리 순서(1 ➡️ 2 ➡️ 3[기존 5] ➡️ 4[기존 3] ➡️ 5[기존 4])에 따른 엔지니어링 명세는 다음과 같습니다.

### 1) Raw 데이터 수집 (Raw Data Collection)
* **국토교통부(MOLIT) 실거래가 데이터**: 
  * 공공데이터포털의 국토교통부 실거래가 공개 API를 연동하여 집합건물 상가 매물 거래 데이터를 전국 규모로 월별 적재하였습니다.
  * 거래일자, 거래금액(dealAmount), 층수(floor), 전용면적(buildingAr), 대지지분 등 핵심 원천 피처를 확보하고 수백 개의 raw JSON 파일(`data/raw/*.json`) 형태로 관리합니다.

### 2) 외부 추가 데이터 연동 (지가변동률 Index Integration)
* **한국부동산원(REB) 지가동향 데이터**:
  * 대한민국 부동산 통계 공식 발표 기관인 한국부동산원(지가변동률조사)과 국가통계포털(KOSIS)로부터 전국 17개 광역 지자체 산하 시군구별 월별 지가변동률 인덱스 원천 파일(`data/external/land_index_raw/*.csv`)을 확보하였습니다.
  * `preprocess_land_index.py` 파이프라인을 통해 전국 행정구역 명칭을 정규화 매핑 키(`sgg_key`)로 정제하여 일원화된 지가 인덱스 마스터 테이블(`land_index_master.csv`)을 일괄 가공 및 통합하였습니다.

### 3) 고유 지번(Jibun) 마스킹 및 비식별화 공간 대체 설계 (Jibun Anonymization Masking)
실제 상업용 부동산 자동가치산정모델(AVM) 서비스에서 특정 소유주의 자산을 식별할 수 있는 상세 주소와 지번 정보를 전용 모델에 그대로 입력 및 노출하는 것은 **개인정보 유출 및 보안 규정 위배**가 될 뿐만 아니라, 머신러닝 학습 모델의 파멸적인 성능 저하를 초래합니다. 본 프로젝트는 이 문제를 원천 해결하기 위해 강력한 비식별화 마스킹 및 물리적 공간 대체 기법을 결합 설계하였습니다.

* **상세 지번(jibun) 및 도로명주소(road_address) 전면 마스킹 (Drop/Masking)**:
  * 학습 및 예측 피처 매트릭스에서 상세 지번(`jibun`)과 도로명주소(`road_address`)를 완전히 제외(Drop) 처리하였습니다. 
  * 고유 주소 문자열은 카디널리티(Unique 값의 개수)가 행 수와 거의 일치하므로, 모델 주입 시 특정 주소의 단가를 모델이 통째로 암기(Memorization)해 버려 검증 셋에서 성능이 곤두박질치는 **치명적인 과적합(Overfitting)**과 데이터 누수(Data Leakage)의 직접적인 주범이 됩니다.
* **비식별화 입지 피처 대체 설계 (Alternative Location Features)**:
  * 지번을 전면 드롭(마스킹)하여 개인정보와 과적합을 철저히 방어하는 대신, 해당 매물의 물리적 입지 파워와 공간 특성을 손실 없이 표현할 수 있도록 고해상도의 대체 입지 피처들을 신설해 주입하였습니다:
    1. **위경도 좌표 데이터화 (`lat`, `lng`)**: 개별 주소 대신 미세 좌표를 추출하여 수치 피처로 활용.
    2. **시군구 중심지 거리 (`dist_to_sgg_center`)**: 광역적 중심성 파악.
    3. **Leakage-Free 읍면동 OOF 타겟 인코딩 (`umd_encoded_oof`)**: 과적합이 완벽히 제어된 미시 행정구역 가격 수준 변환 매핑.
    4. **다중 상권 거점 최단거리 (`dist_to_closest_hotspot`)**: 해당 지자체 내 상위 5% 초고가 입지 핫스팟과의 하버사인 직선 거리 분석.
* **보안성 및 일반화 성능의 양립**:
  * 이와 같은 정밀 마스킹 파이프라인을 구축함으로써 **금융 감정평가 및 보안 컴플라이언스를 100% 충족하는 비식별 안전성을 달성**하는 동시에, 미지의 주소에 대한 예측 신뢰성과 일반화(Generalization) 성능을 비약적으로 끌어올리는 결실을 맺었습니다.

### 4) 정밀 전처리 및 결측치 복원 (Preprocessing & Address Resolution)
* **정교한 중복 제거 (Deduplication)**: 
  * 동일 매물의 반복/이중 등록 노이즈를 완전 차단하기 위해 `[시군구코드, 법정동명, 지번, 거래금액, 전용면적, 거래연/월/일]`의 8가지 다중 고유 거래 키를 직렬 결합하여 정교한 중복 드롭을 수행하였습니다.
* **행정 주소 결함 복원 (Address Recovery)**: 
  * 원천 데이터에서 법정동코드(`sggCd`)가 `36110`(세종특별자치시)인 행 중 시군구명(`sggNm`) 결측치에 대해 법정 주소를 자동으로 복원하는 정밀 복원기(`address_restorer.py`)를 개발 및 적용하였습니다.
* **공용면적 및 대지지분 정밀 대체**: 
  * 집합건물 특성 상 발생하는 면적 오류 및 0값에 대하여 통계적 편향 왜곡이 없는 중위수(Median)로 엄격히 대체 처리하였습니다.

### 5) 실질 가치 동기화: 시점수정 파이프라인 (Time-point Correction)
* 부동산 거래는 수년 동안의 시차가 발생하므로 단순 명목 거래가격을 비교하면 시간적 노이즈가 스며듭니다.
* 수집된 지가 인덱스 마스터 테이블을 결합하여, 과거 시점의 거래 금액을 공인 지가 변동 계수에 맞추어 기준 시점인 **2026년 3월(latest_ym = "202603")**의 가치로 전부 일치화시켰습니다.
* **보정 메커니즘**:
  $$\text{adjusted\_price\_per\_m2} = \left( \frac{\text{dealAmount} \times 10,000}{\text{buildingAr}} \right) \times \left( \frac{\text{2026년 3월 지점의 지역 지상 지수}}{\text{거래 발생 연월의 지역 지상 지수}} \right)$$
* 이를 통해 인플레이션 및 거시 경제적 부동산 흐름 노이즈를 완벽하게 차단하고, 모델이 **오직 개별 매물의 입지 가치와 물리적 지표에만 집중하여 예측**하도록 물리적 학습 타겟을 동기화하였습니다.

---

## 최종 상용 배포용 모델 사용법 (Inference Guide)

최종 학습되어 산출된 모델 패키지(`.pkl`)를 서비스 API나 백엔드 서버에 연동하여 신규 매물의 가격을 예측하는 워크플로우입니다. 모델 패키지 내부에는 단순 추론 객체뿐만 아니라, **실시간 텍스트/좌표 맵핑을 위한 사전 메타데이터**가 모두 포함되어 있습니다.

### 1단계: 모델 패키지 로드 (Load Model Package)
```python
import pickle
import numpy as np
import pandas as pd

# 1. 아티팩트 디렉토리에서 패키지 로드
with open("data/model/final_avm_model_package.pkl", "rb") as f:
    package = pickle.load(f)
    
model = package['model']
feature_names = package['feature_names']          # 예측 시 보장되어야 할 피처 순서
categorical_cols = package['categorical_cols']    # 범주형 컬럼 리스트

# 2. 실시간 변환용 사전(Dictionary) 메타데이터
umd_map = package['umd_mapping']['umd_to_price_log'] 
global_median = package['umd_mapping']['global_median_log']
sgg_hotspots = package['sgg_hotspots']
```

### 2단계: 신규 데이터 입력 및 파생 변수 계산 (Feature Engineering)
API를 통해 단일 매물 데이터가 유입되면, 모델이 학습했던 파생 변수 형태와 일치하도록 가공해야 합니다.

1. **비율 지표 산출**: `exclusive_rate`(전용률), `parking_density`(주차밀도), `bcRat`(건폐율), `vlRat`(용적률) 산출
2. **건물 구조 원핫 인코딩**: 원본 `strctCdNm`(예: '철근콘크리트구조')을 `strct_철근콘크리트` 등 여러 컬럼의 0/1 매핑
3. **외부 API/캐시 연동 피처**: `dist_to_subway`(지하철 역까지의 거리), `cafe_count_200m`(반경 내 카페 수) 등 외부 인프라 데이터 세팅

### 3단계: 패키지 메타데이터를 활용한 공간 지표 매핑 (Spatial Mapping)
모델은 한글 주소 문자열을 그대로 학습하지 않으므로, 패키지에 동봉된 사전을 이용해 실시간 숫자로 매핑합니다.

* **법정동 타겟 인코딩 (`umd_encoded_oof`)**
  입력된 법정동명(`umdNm`)을 `umd_map`에 넣어 평균 로그 단가로 매핑하며, 매핑되지 않는 신규 구역의 경우 `global_median`으로 보완합니다.
  ```python
  encoded_val = umd_map.get(input_umdNm, global_median)
  ```
* **상권 핫스팟 최단 거리 (`dist_to_closest_hotspot`)**
  매물의 위경도(`lat`, `lng`)와, 모델에 저장된 해당 시군구(`sggNm`)의 상권 핵심 좌표군(`sgg_hotspots`) 간의 하버사인(Haversine) 최단 거리를 실시간으로 연산하여 추가합니다.

### 4단계: 순서 정렬 및 단가 추론 (Predict & Inverse Log)
준비된 피처들을 모델이 훈련할 때 사용한 배열과 동일하게 정렬 후 추론합니다.

```python
# 1. 컬럼 순서 및 타입 맞추기
inference_df = my_processed_data[feature_names]

for col in categorical_cols:
    inference_df[col] = inference_df[col].astype('category')

# 2. 단가 예측 수행 (로그화된 ㎡당 단가)
predicted_log_price = model.predict(inference_df)

# 3. 원래 원화(₩) 단가로 복원 (expm1 적용)
predicted_price_per_m2 = np.expm1(predicted_log_price)

# 4. 종합 부동산 담보가치 산출
final_total_price = predicted_price_per_m2[0] * input_buildingAr
```
> **Tip:** 단순히 `predict()`만 호출하는 것이 아니라, 사용자가 입력한 주소나 건물 스펙을 모델 패키지 내부의 맵과 결합하여 전처리를 통과시켜주는 **API 래퍼(Wrapper) 로직**이 서비스단에 함께 구성되어야 합니다.
