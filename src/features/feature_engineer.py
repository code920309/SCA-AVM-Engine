import os
import time
import requests
import logging
import pandas as pd
from dotenv import load_dotenv

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FeatureEngineer:
    """
    정제된 실거래가 데이터(CSV)의 특성(Feature)을 머신러닝 학습에 적합하게 수치화 및 변환하는 클래스
    """
    def __init__(self, data_path: str = "data/processed/training_set.csv", save_path: str = "data/processed/encoded_training_set.csv"):
        self.data_path = data_path
        self.save_path = save_path
        
        # .env 파일에서 카카오 REST API 키 로드
        load_dotenv()
        self.kakao_api_key = os.getenv("KAKAO_REST_API_KEY")

    def calculate_numeric_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """수치형 파생 변수(건물령, 단위면적당 가격)를 계산합니다."""
        logger.info("수치형 파생 변수 생성 (건물령, 단위면적당 가격)")
        
        # 1. 건물령(Age) = 계약년도(dealYear) - 건축년도(buildYear)
        if 'buildYear' in df.columns and 'dealYear' in df.columns:
            df['buildYear_num'] = pd.to_numeric(df['buildYear'], errors='coerce')
            df['dealYear_num'] = pd.to_numeric(df['dealYear'], errors='coerce')
            
            # 차이값 계산 후, 재건축/선분양 등으로 인한 음수는 0으로 보정
            df['age'] = df['dealYear_num'] - df['buildYear_num']
            df['age'] = df['age'].apply(lambda x: x if x >= 0 else 0)
            
            # 연산용 임시 컬럼 제거
            df = df.drop(columns=['buildYear_num', 'dealYear_num'])
            
        # 2. 단위면적당 가격(Price per m²) = 거래금액(dealAmount) / 건물면적(buildingAr)
        if 'dealAmount' in df.columns and 'buildingAr' in df.columns:
            df['dealAmount_num'] = df['dealAmount'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')
            df['buildingAr_num'] = df['buildingAr'].apply(pd.to_numeric, errors='coerce')
            
            valid_area_mask = df['buildingAr_num'] > 0
            df.loc[valid_area_mask, 'price_per_m2'] = df.loc[valid_area_mask, 'dealAmount_num'] / df.loc[valid_area_mask, 'buildingAr_num']
            
            df = df.drop(columns=['dealAmount_num', 'buildingAr_num'])
            
        return df

    def encode_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        """범주형 변수(건물용도, 건물유형 등)를 인코딩합니다."""
        logger.info("범주형 변수 인코딩 (One-Hot / Label Encoding)")
        
        # 1. 건물주용도(buildingUse), 용도지역(landUse) -> One-Hot Encoding
        one_hot_cols = [col for col in ['buildingUse', 'landUse'] if col in df.columns]
        if one_hot_cols:
            df = pd.get_dummies(df, columns=one_hot_cols, drop_first=False)
            
        # 2. 건물유형(buildingType - 일반/집합 등 구조적 특성) -> Label Encoding (Pandas factorize 활용)
        if 'buildingType' in df.columns:
            df['buildingType'] = df['buildingType'].fillna('알수없음')
            # factorize는 고유값 배열과 인코딩된 정수 배열을 반환함
            df['buildingType_encoded'], _ = pd.factorize(df['buildingType'])
            df = df.drop(columns=['buildingType'])
            
        return df

    def get_coordinates_from_kakao(self, address: str) -> tuple:
        """카카오 로컬 API를 호출하여 문자열 주소를 위경도(Lat, Lon)로 변환합니다."""
        if not self.kakao_api_key:
            return None, None
            
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {"Authorization": f"KakaoAK {self.kakao_api_key}"}
        params = {"query": address}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=5)
            if response.status_code == 200:
                documents = response.json().get('documents')
                if documents:
                    # 카카오 API는 y를 위도(latitude), x를 경도(longitude)로 반환
                    return float(documents[0]['y']), float(documents[0]['x'])
        except Exception as e:
            logger.error(f"카카오 좌표 변환 API 오류 ({address}): {str(e)}")
            
        return None, None

    def fill_missing_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """주소 조합을 통해 누락된 위경도 좌표를 채워넣습니다."""
        logger.info("카카오 API를 통한 위경도 결측치 보완")
        
        # 위경도 컬럼 사전 정의
        if 'latitude' not in df.columns: df['latitude'] = None
        if 'longitude' not in df.columns: df['longitude'] = None
            
        # sggNm(시군구), umdNm(법정동), jibun(지번) 존재 여부 확인
        if all(col in df.columns for col in ['sggNm', 'umdNm', 'jibun']):
            # 좌표가 비어있는 행 탐색
            missing_mask = df['latitude'].isna() | df['longitude'].isna()
            missing_indices = df[missing_mask].index
            
            if not missing_indices.empty and self.kakao_api_key:
                logger.info(f"총 {len(missing_indices)}건의 주소에 대해 좌표 변환을 시도합니다.")
                for idx in missing_indices:
                    row = df.loc[idx]
                    # 주소 텍스트 조합 (예: "서울특별시 종로구 사직동 12-3")
                    address = f"{row['sggNm']} {row['umdNm']} {row['jibun']}".strip()
                    
                    lat, lon = self.get_coordinates_from_kakao(address)
                    df.at[idx, 'latitude'] = lat
                    df.at[idx, 'longitude'] = lon
                    
                    # API 호출 제한 속도 준수 (초당 30건 이하 등)
                    time.sleep(0.1)
            elif not self.kakao_api_key:
                logger.warning(".env 파일에 KAKAO_REST_API_KEY가 없어 좌표 보완을 건너뜁니다.")
                
        return df

    def apply_time_correction(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        거래 시점의 지가변동률을 반영하여 시점 수정치를 자동 계산합니다.
        (추후 한국부동산원 지가변동률 API 연동 필요)
        """
        logger.info("거래 시점 지가변동률 반영 (시점 수정치 계산 - API 연동 예정)")
        
        # TODO: 추후 API 연동 시 거래연월(dealYear, dealMonth) 기준으로 지가변동률을 조회하여 시점 수정치 적용
        if 'time_correction_rate' not in df.columns:
            df['time_correction_rate'] = 1.0  # 기본값: 1.0 (보정 없음)
            
        return df

    def process(self):
        """전체 특성 공학 파이프라인을 실행하고 결과를 저장합니다."""
        if not os.path.exists(self.data_path):
            logger.error(f"원본 데이터를 찾을 수 없습니다: {self.data_path}")
            return
            
        logger.info("Feature Engineering 파이프라인 시작...")
        df = pd.read_csv(self.data_path)
        
        # 1. 수치형 파생 변수 계산
        df = self.calculate_numeric_features(df)
        
        # 2. 범주형 변수 인코딩
        df = self.encode_categorical(df)
        
        # 3. 주소 기반 위경도 추출 (카카오 API)
        df = self.fill_missing_coordinates(df)
        
        # 4. 시점 수정치 자동 계산 (지가변동률 반영)
        df = self.apply_time_correction(df)
        
        # One-Hot 인코딩으로 생성된 boolean 타입(True/False)을 정수형(1/0)으로 일괄 변환 (머신러닝 입력용)
        for col in df.columns:
            if df[col].dtype == 'bool':
                df[col] = df[col].astype(int)
                
        # 결과 저장
        os.makedirs(os.path.dirname(self.save_path), exist_ok=True)
        df.to_csv(self.save_path, index=False, encoding='utf-8-sig')
        logger.info(f"특성 공학 완료. 최종 데이터 저장: {self.save_path}")

if __name__ == "__main__":
    engineer = FeatureEngineer()
    engineer.process()
