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
