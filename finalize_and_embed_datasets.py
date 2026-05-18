import pandas as pd
import numpy as np
import os
import sys
import time

# UTF-8 출력 강제 설정 (Windows 콘솔 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def apply_master_pipeline(df, name):
    print(f"\n--- [{name} 데이터셋 마스터 파이프라인 처리 시작] ---")
    initial_shape = df.shape
    
    # 1. 불필요 컬럼 제거 (Drop Target)
    cols_to_drop = ['buildingType', 'estateAgentSggNm', 'sggCd', 'buildingName']
    existing_drops = [col for col in cols_to_drop if col in df.columns]
    df = df.drop(columns=existing_drops)
    print(f"  * [Step 1] 불필요 컬럼 제거 완료: {existing_drops}")
    
    # 2. 공공데이터 타포(Typo) 오류 정밀 스케일링 (Clamping)
    clamped_bc = 0
    clamped_vl = 0
    
    if 'bcRat' in df.columns:
        # 정상 범위 데이터(bcRat <= 100)의 용도지역별 중간값 계산
        bc_medians = df[df['bcRat'] <= 100.0].groupby('landUse')['bcRat'].median()
        global_bc = df[df['bcRat'] <= 100.0]['bcRat'].median()
        
        bc_outlier_mask = df['bcRat'] > 100.0
        clamped_bc = bc_outlier_mask.sum()
        
        if clamped_bc > 0:
            df.loc[bc_outlier_mask, 'bcRat'] = df[bc_outlier_mask].apply(
                lambda row: bc_medians.get(row['landUse'], global_bc), axis=1
            )
            
    if 'vlRat' in df.columns:
        # 정상 범위 데이터(vlRat <= 2000)의 용도지역별 중간값 계산
        vl_medians = df[df['vlRat'] <= 2000.0].groupby('landUse')['vlRat'].median()
        global_vl = df[df['vlRat'] <= 2000.0]['vlRat'].median()
        
        vl_outlier_mask = df['vlRat'] > 2000.0
        clamped_vl = vl_outlier_mask.sum()
        
        if clamped_vl > 0:
            df.loc[vl_outlier_mask, 'vlRat'] = df[vl_outlier_mask].apply(
                lambda row: vl_medians.get(row['landUse'], global_vl), axis=1
            )
            
    print(f"  * [Step 2] 타포 오류 Clamping 완료: 건폐율 {clamped_bc}건, 용적률 {clamped_vl}건 보정")
    
    # 3. 미세 결측치 최종 방어 조치 (Residual Imputation)
    if 'buildYear' in df.columns:
        # numeric 변환 후 결측 채우기
        df['buildYear_num'] = pd.to_numeric(df['buildYear'], errors='coerce')
        sgg_medians = df.groupby('sggNm')['buildYear_num'].median()
        global_year_median = df['buildYear_num'].median()
        
        missing_by_mask = df['buildYear_num'].isna()
        df.loc[missing_by_mask, 'buildYear_num'] = df.loc[missing_by_mask, 'sggNm'].map(sgg_medians).fillna(global_year_median)
        df['buildYear'] = df['buildYear_num'].round(0).astype(int)
        df = df.drop(columns=['buildYear_num'])
        
    if 'lat' in df.columns and 'lng' in df.columns:
        umd_lat = df.groupby('umdNm')['lat'].median()
        umd_lng = df.groupby('umdNm')['lng'].median()
        global_lat = df['lat'].median()
        global_lng = df['lng'].median()
        
        df['lat'] = df['lat'].fillna(df['umdNm'].map(umd_lat).fillna(global_lat))
        df['lng'] = df['lng'].fillna(df['umdNm'].map(umd_lng).fillna(global_lng))
        
    print("  * [Step 3] 미세 결측치 최종 방어 완료 (buildYear 및 lat/lng 보정)")
    
    # 4. 고차원 및 문자열 변수 임베딩 (Feature Encoding)
    # 타겟 인코딩
    umd_price_means = df.groupby('umdNm')['adjusted_price_per_m2'].mean()
    global_price = df['adjusted_price_per_m2'].mean()
    df['umd_encoded'] = df['umdNm'].map(umd_price_means).fillna(global_price).round(2)
    
    # strctCdNm 원-핫 인코딩
    if 'strctCdNm' in df.columns:
        df['strctCdNm'] = df['strctCdNm'].fillna('').astype(str).str.strip()
        top5_strcts = ['철근콘크리트구조', '철골철근콘크리트구조', '일반철골구조', '벽돌구조', '프리케스트콘크리트구조']
        df['strct_grouped'] = df['strctCdNm'].apply(lambda x: x if x in top5_strcts else '기타구조')
        
        dummies = pd.get_dummies(df['strct_grouped'], prefix='strct', dtype=int)
        df = pd.concat([df, dummies], axis=1)
        df = df.drop(columns=['strct_grouped'])
        
    print("  * [Step 4] umdNm 타겟 인코딩 및 strctCdNm 원-핫 인코딩 완료")
    
    # 6. 무비용 고효율 도메인 파생변수 생성
    df['is_corporate_deal'] = ((df['buyerGbn'] == '법인') | (df['slerGbn'] == '법인')).astype(bool)
    print("  * [Step 6] 법인 주도 거래 변수 'is_corporate_deal' 신설 완료")
    
    final_shape = df.shape
    print(f"  * [{name} 처리 결과] 셰이프 변화: {initial_shape} -> {final_shape}")
    
    return df, clamped_bc, clamped_vl

def main():
    start_time = time.time()
    
    enriched_path = "data/processed/avm_precision_set_enriched.csv"
    clean_path = "data/processed/avm_precision_set_clean.csv"
    raw_path = "data/processed/avm_precision_set.csv"
    
    # 결과 파일 저장 경로 정의
    final_enriched_out = "data/processed/avm_precision_set_final.csv"
    final_clean_out = "data/processed/avm_precision_set_clean_final.csv"
    final_raw_out = "data/processed/avm_precision_set_raw_final.csv"
    
    print("=" * 80)
    print(" [SCA AVM Engine - 마스터 정제 & 최종 인코딩 파이프라인 가동]")
    print("=" * 80)
    
    # 1. Enriched 데이터셋 처리 (최종 메인 데이터셋)
    if os.path.exists(enriched_path):
        df_enriched = pd.read_csv(enriched_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
        df_enriched_final, e_bc, e_vl = apply_master_pipeline(df_enriched, "Enriched")
        df_enriched_final.to_csv(final_enriched_out, index=False, encoding='utf-8-sig')
        print(f"    -> 저장 완료: {final_enriched_out}")
    else:
        print(f"오류: {enriched_path} 파일이 존재하지 않습니다.")
        return
        
    # 2. Clean 데이터셋 처리 (베이스라인 비교용)
    if os.path.exists(clean_path):
        df_clean = pd.read_csv(clean_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
        df_clean_final, c_bc, c_vl = apply_master_pipeline(df_clean, "Clean")
        df_clean_final.to_csv(final_clean_out, index=False, encoding='utf-8-sig')
        print(f"    -> 저장 완료: {final_clean_out}")
        
    # 3. Raw 데이터셋 처리 (순수 오리지널 비교용)
    if os.path.exists(raw_path):
        df_raw = pd.read_csv(raw_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
        df_raw_final, r_bc, r_vl = apply_master_pipeline(df_raw, "Raw (Original)")
        df_raw_final.to_csv(final_raw_out, index=False, encoding='utf-8-sig')
        print(f"    -> 저장 완료: {final_raw_out}")
        
    # 통계 검증을 위한 지방 샘플 데이터 추출 및 출력
    print("\n" + "=" * 80)
    print(" 📊 전국구 핵심 샘플 데이터 시공간/인코딩 임베딩 정밀 검증")
    print("=" * 80)
    
    # 경기(41), 부산(26), 대구(27) 샘플 확인
    # sggNm에 '부산', '대구', '경기'가 포함된 행들을 기준으로 검증
    sample_df = df_enriched_final[df_enriched_final['sggNm'].astype(str).str.contains('부산|대구|경기|수원|해운대|성남|달서', na=False)]
    
    if not sample_df.empty:
        print(f"전국구 지방 샘플 수량: {len(sample_df):,}건 추출 완료")
        cols_to_show = ['sggNm', 'umdNm', 'umd_encoded', 'dist_to_sgg_center', 'spatial_knn_price', 'price_momentum', 'is_corporate_deal']
        existing_cols = [c for c in cols_to_show if c in sample_df.columns]
        print(sample_df[existing_cols].drop_duplicates(subset=['sggNm', 'umdNm']).head(12).to_string(index=False))
    else:
        # sggNm 한글 깨짐 방지용 sggCd 매칭 검증
        print("sggNm 문자열 매칭 실패로, sggCd(41, 26, 27 등) 매칭 시도...")
        
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(" 📊 최종 데이터 마스터 정제 및 인코딩 완료 리포트")
    print("=" * 80)
    print(f"  * 총 소요 시간: {elapsed:.2f} 초")
    print(f"  * 마스터 데이터 저장 완료: {final_enriched_out}")
    print(f"  * 클린 데이터 저장 완료: {final_clean_out}")
    print(f"  * 원본 데이터 저장 완료: {final_raw_out}")
    print("=" * 80)

if __name__ == "__main__":
    main()
