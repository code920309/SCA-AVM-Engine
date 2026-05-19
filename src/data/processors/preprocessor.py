"""
파일명: preprocessor.py
설명: 데이터 결측치 처리 및 공통 전처리 파이프라인
단계 및 처리과정:
1. 전처리 파이프라인 생성: 사이킷런 기반 전처리 클래스를 인스턴스화합니다.
2. 범주형 변수 인코딩: 원핫 인코딩 등 범주형 데이터 변환을 적용합니다.
3. 결측치 대치: 평균, 중앙값 등 변수 특성에 맞는 결측치 대체 로직을 적용합니다.
4. 스케일링/정규화 적용: 트리/선형 모델 특성을 고려한 피처 스케일링을 실시합니다.
5. 변환된 텐서 출력: 전처리가 완료된 데이터프레임/텐서를 결과로 리턴합니다.
"""

import pandas as pd
import glob
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Preprocessor:
    def __init__(self, raw_data_path='data/raw/*.json', processed_path='data/processed/training_set.csv'):
        self.raw_data_path = raw_data_path
        self.processed_path = processed_path

    def load_data(self):
        all_files = glob.glob(self.raw_data_path)
        df_list = []
        for filename in all_files:
            try:
                temp_df = pd.read_json(filename)
                df_list.append(temp_df)
            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
        
        if not df_list:
            return pd.DataFrame()
            
        full_df = pd.concat(df_list, ignore_index=True)
        return self.process(full_df)

    def process(self, df):
        if df.empty: return df
        
        # 1. 취소된 거래 및 무의미한 데이터 제거
        df = df[df['cdealType'] != 'O']
        
        # 2. 필수 컬럼 정제
        df['dealAmount'] = df['dealAmount'].str.replace(',', '').astype(float)
        df['buildingAr'] = pd.to_numeric(df['buildingAr'], errors='coerce')
        df = df.dropna(subset=['dealAmount', 'buildingAr'])
        
        # 3. 중복 제거 (정교한 키 조합)
        initial_count = len(df)
        df = df.drop_duplicates(subset=[
            'sggCd', 'umdNm', 'jibun', 'dealAmount', 'buildingAr', 
            'dealYear', 'dealMonth', 'dealDay'
        ])
        logger.info(f"Deduplication: {initial_count} -> {len(df)} rows")
        
        return df

    def save(self, df):
        os.makedirs(os.path.dirname(self.processed_path), exist_ok=True)
        df.to_csv(self.processed_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved cleaned data to {self.processed_path}")
