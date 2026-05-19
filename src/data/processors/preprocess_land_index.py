"""
파일명: preprocess_land_index.py
설명: 토지 지표(Land Index) 관련 데이터 전처리
단계 및 처리과정:
1. 토지대장 데이터 스캔: 공시지가 및 토지 이용 데이터를 불러옵니다.
2. 면적 및 용도지역 변환: 면적 단위 변환 및 용도지역 카테고리를 통합합니다.
3. 지수화(Index) 로직: 토지의 복합적 가치를 단일 수치 지수로 환산합니다.
4. 파생 지표 매핑: 토지 지표를 실제 좌표 데이터에 매핑합니다.
5. 전처리 지표 병합: 계산된 토지 지표 데이터를 기준 데이터프레임에 통합합니다.
"""

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
