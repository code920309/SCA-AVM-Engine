import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseDatasetGenerator:
    def __init__(self):
        self.train_path = "data/processed/training_set.csv"
        self.index_path = "data/processed/land_index_master.csv"
        self.output_path = "data/processed/national_base_dataset_v1.csv"

    def normalize_sgg_name(self, name):
        if not isinstance(name, str): return name
        for suffix in ['특별시', '광역시', '특별자치시', '특별자치도', '시', '군', '구']:
            name = name.replace(suffix, '')
        return name.replace(' ', '')

    def run(self):
        logger.info("=== Phase 1, 2, 4 통합: 전국 기초 데이터셋 구축 시작 ===")
        
        if not os.path.exists(self.train_path) or not os.path.exists(self.index_path):
            logger.error("필수 전처리 파일이 없습니다. Phase 1, 2를 먼저 실행하세요.")
            return

        # 1. 데이터 로드
        df = pd.read_csv(self.train_path)
        
        # 데이터 타입 정제 (금액의 콤마 제거 및 숫자 변환)
        if df['dealAmount'].dtype == object:
            df['dealAmount'] = df['dealAmount'].str.replace(',', '').astype(float)
        df['buildingAr'] = pd.to_numeric(df['buildingAr'], errors='coerce')
        
        index_df = pd.read_csv(self.index_path)
        index_df['deal_ym'] = index_df['deal_ym'].astype(str)
        
        # 2. 정규화 키 생성 (매핑용)
        df['sgg_key'] = df['sggNm'].apply(self.normalize_sgg_name)
        df['deal_ym'] = df['dealYear'].astype(str) + df['dealMonth'].astype(str).str.zfill(2)
        
        # 3. 지수 매핑
        logger.info("지가변동률 지수 매핑 중...")
        df = pd.merge(df, index_df[['sgg_key', 'deal_ym', 'land_index']], 
                     on=['sgg_key', 'deal_ym'], how='left')
        
        # 4. 시점 수정 (2026.03 기준)
        logger.info("시점 수정 가격 계산 중...")
        latest_ym = "202603"
        latest_indices = index_df[index_df['deal_ym'] == latest_ym][['sgg_key', 'land_index']]
        latest_indices.columns = ['sgg_key', 'latest_index']
        
        df = pd.merge(df, latest_indices, on='sgg_key', how='left')
        
        def calculate_adjusted_price(row):
            if pd.isna(row['land_index']) or pd.isna(row['latest_index']):
                return None
            correction_factor = row['latest_index'] / row['land_index']
            price_per_m2 = (row['dealAmount'] * 10000) / row['buildingAr']
            return price_per_m2 * correction_factor

        df['adjusted_price_per_m2'] = df.apply(calculate_adjusted_price, axis=1)
        
        # 5. 결과 저장
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        logger.info(f"성공! 전국 데이터셋 구축 완료: {self.output_path}")

if __name__ == "__main__":
    generator = BaseDatasetGenerator()
    generator.run()
