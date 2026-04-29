import pandas as pd
import os

def normalize_sgg_name(name):
    """행정구역 명칭 정규화 (시, 군, 구 접미사 제거하여 매핑 확률 극대화)"""
    if not isinstance(name, str): return name
    for suffix in ['특별시', '광역시', '특별자치시', '특별자치도', '시', '군', '구']:
        name = name.replace(suffix, '')
    return name.replace(' ', '')

def preprocess_land_index(input_path='data/raw/land_index_raw.csv', output_path='data/processed/land_index_master.csv'):
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    
    # 지역명 정규화
    df['sgg_key'] = df['sgg_name'].apply(normalize_sgg_name)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Normalized land index saved to {output_path}")

if __name__ == "__main__":
    preprocess_land_index()
