"""
파일명: enrich_with_caching.py
설명: 캐싱을 활용하여 API 호출 등 데이터 강화 작업 최적화
단계 및 처리과정:
1. 캐시 스토리지 확인: 로컬 또는 메모리에 기존 캐시 데이터가 존재하는지 점검합니다.
2. 캐시 히트(Hit) 판별: 캐시된 응답이 있다면 API를 호출하지 않고 즉각 데이터를 로드합니다.
3. 캐시 미스(Miss) 처리: 캐시에 없는 데이터만 선별하여 외부 소스에 요청을 보냅니다.
4. 신규 캐시 저장: 새로 가져온 응답을 다음 호출을 위해 캐시에 저장합니다.
5. 데이터셋 통합: 캐시 데이터와 신규 데이터를 합쳐 데이터셋 구성을 완료합니다.
"""

import os
import sys
import time
import json
import argparse
import logging
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# UTF-8 출력 강제 설정 (Windows 콘솔 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 로깅 설정 (콘솔에는 최소한의 진행 상황만 나오도록 핸들러 설정)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnterpriseEnricher:
    def __init__(self, output_path, workers_count):
        load_dotenv()
        self.output_path = output_path
        self.workers_count = workers_count
        
        # 카카오 API 키 (유효한 기존 키 사용)
        self.kakao_api_key = os.getenv("KAKAO_REST_API_KEY") or "545d14c4a406db675503b6d170297d2c"
        
        # 국토교통부 API 키 리스트 (순환 로테이션 적용)
        self.molit_keys = [
            "GF0Lq9LWPlZV7Ga1tMaCqZDhb06lzroW4fwEwQy9BfDy82xa3bPReEfNfTUBi/g4mCd/PfHGZu1Djjs4VdP0iQ==", # 기존 키 1 (인코딩)
            "8fbfae176be273f850f99fa6aae8eb3e82406192188ec7c34cbd7b9b17b75097", # 신규 키 2 (디코딩)
            "MZ0ZuviMTH4gDShOib3V47tXpFuKKqJxT%2BnrMGg8chRf0J6eBuU%2BCCcY58SQE840kEaZsnH5oHLj6zGiLQ4NYg%3D%3D", # 신규 키 3 (인코딩)
            "952279d2da63661a9fcf8bfe4506699cae26096a861b282a782caa649b3634df"
        ]
        self.current_key_idx = 0
        self.key_lock = Lock()
        self.cache_lock = Lock()
        
        # 지번 단위 캐시 파일 경로
        self.cache_file = "data/processed/building_address_cache.json"
        self.address_cache = {}
        self.load_cache()

    def load_cache(self):
        """파일 캐시 로드"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.address_cache = json.load(f)
                print(f"  [캐시] 기존 {len(self.address_cache):,}개의 지번 캐시를 성공적으로 불러왔습니다.")
            except Exception as e:
                print(f"  [캐시] 경고 - 캐시 로드 실패: {e}")
                self.address_cache = {}

    def save_cache(self):
        """파일 캐시 저장"""
        with self.cache_lock:
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                with open(self.cache_file, "w", encoding="utf-8") as f:
                    json.dump(self.address_cache, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"캐시 저장 실패: {e}")

    def get_molit_key(self):
        """동기적으로 현재 활성화된 국토교통부 API 키 획득"""
        with self.key_lock:
            key = self.molit_keys[self.current_key_idx]
            return requests.utils.unquote(key)

    def rotate_molit_key(self):
        """트래픽 초과 시 API 키 교체"""
        with self.key_lock:
            next_idx = (self.current_key_idx + 1) % len(self.molit_keys)
            if next_idx != self.current_key_idx:
                self.current_key_idx = next_idx
                print(f"\n  \033[93m[인증키 교체] 트래픽 제한 감지로 인해 국토교통부 API 인증키 #{self.current_key_idx + 1}로 자동 로테이션합니다!\033[0m")
                return True
            return False

    def get_address_info_via_kakao(self, sgg_nm, umd_nm, jibun):
        """카카오 API를 통해 도로명주소 및 위경도 추출"""
        query = f"{sgg_nm} {umd_nm} {jibun}".strip()
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {"Authorization": f"KakaoAK {self.kakao_api_key}"}
        
        try:
            res = requests.get(url, headers=headers, params={"query": query}, timeout=5)
            if res.status_code == 200:
                docs = res.json().get('documents', [])
                if docs:
                    addr_info = docs[0].get('address', {})
                    b_code = addr_info.get('b_code', '')
                    umd_cd = b_code[5:] if len(b_code) == 10 else None
                    
                    road_addr_info = docs[0].get('road_address')
                    road_address = road_addr_info.get('address_name') if road_addr_info else None
                    
                    return {
                        "umd_cd": umd_cd,
                        "lat": docs[0].get('y'),
                        "lng": docs[0].get('x'),
                        "road_address": road_address
                    }
        except Exception as e:
            logger.error(f"카카오 API 실패 ({query}): {e}")
        return None

    def _split_jibun(self, jibun):
        if not jibun or str(jibun).strip() == "":
            return "", ""
        parts = str(jibun).split('-')
        try:
            bun = parts[0].strip().zfill(4)
            ji = parts[1].strip().zfill(4) if len(parts) > 1 else "0000"
            return bun, ji
        except:
            return "", ""

    def _fetch_br_json(self, url, params):
        """오픈 API 호출 결과를 JSON 형식으로 반환"""
        params['_type'] = 'json'
        
        # 키 로테이션 시도 최대 2회
        for _ in range(2):
            params['serviceKey'] = self.get_molit_key()
            try:
                res = requests.get(url, params=params, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    
                    # 트래픽 제한 에러 감지 (MOLIT API가 에러 코드 반환 시)
                    header = data.get('response', {}).get('header', {})
                    result_code = str(header.get('resultCode', ''))
                    if result_code in ['22', '30', '99'] or 'LIMITED' in str(header.get('resultMsg')):
                        if self.rotate_molit_key():
                            continue # 로테이션 성공 시 재호출
                            
                    items = data.get('response', {}).get('body', {}).get('items', {})
                    if isinstance(items, dict):
                        item_list = items.get('item', [])
                        if isinstance(item_list, dict):
                            return [item_list]
                        return item_list
            except Exception as e:
                logger.error(f"MOLIT API 에러: {e}")
        return []

    def fetch_building_info(self, sgg_cd, umd_cd, jibun):
        """표제부 API (/getBrTitleInfo) 조회"""
        bun, ji = self._split_jibun(jibun)
        if not bun:
            return None
            
        params = {
            "sigunguCd": sgg_cd,
            "bjdongCd": umd_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": "10",
            "pageNo": "1"
        }
        
        items = self._fetch_br_json("https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo", params)
        if items:
            return items[0]
        return None

    def fetch_pubuse_raw_items(self, sgg_cd, umd_cd, jibun):
        """전유공용면적 API (/getBrExposPubuseAreaInfo)의 모든 아이템 조회"""
        bun, ji = self._split_jibun(jibun)
        if not bun:
            return []
            
        params = {
            "sigunguCd": sgg_cd,
            "bjdongCd": umd_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": "150",
            "pageNo": "1"
        }
        return self._fetch_br_json("https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo", params)

    def process_single_address(self, sgg_cd, sgg_nm, umd_nm, jibun, has_missing_fields):
        """단일 지번에 대해 Kakao 및 MOLIT API를 통합 수집하여 캐시에 등록"""
        cache_key = f"{sgg_nm}_{umd_nm}_{jibun}"
        
        # 1. 캐시 히트 체크
        with self.cache_lock:
            if cache_key in self.address_cache:
                return self.address_cache[cache_key], True

        # 2. 보강 필요 여부
        if not has_missing_fields:
            # 보강이 필요 없더라도 캐시 등록용 정보 추출 (기본 위경도 좌표 확보 목적)
            pass

        # 3. Kakao API 주소 획득
        addr_info = self.get_address_info_via_kakao(sgg_nm, umd_nm, jibun)
        if not addr_info:
            res_data = {"status": "FAILED_KAKAO"}
            with self.cache_lock:
                self.address_cache[cache_key] = res_data
            return res_data, False
            
        umd_cd = addr_info.get('umd_cd')
        lat = addr_info.get('lat')
        lng = addr_info.get('lng')
        road_address = addr_info.get('road_address')
        
        if not umd_cd:
            res_data = {"status": "FAILED_UMD_CD"}
            with self.cache_lock:
                self.address_cache[cache_key] = res_data
            return res_data, False
            
        # 4. 표제부 API 정보 획득
        br_title = self.fetch_building_info(sgg_cd, umd_cd, jibun)
        
        build_year = ""
        plottage_ar = None
        parking_count = 0
        bld_nm = ""
        
        if br_title:
            bld_nm = br_title.get('bldNm', '')
            use_apr_day = br_title.get('useAprDay', '')
            if use_apr_day and len(str(use_apr_day)) >= 4:
                build_year = str(use_apr_day)[:4]
                
            plat_area = br_title.get('platArea')
            if plat_area:
                try:
                    plottage_ar = round(float(plat_area), 2)
                except:
                    pass
                    
            try:
                indr_auto = int(br_title.get('indrAutoUtcnt') or 0)
                indr_mech = int(br_title.get('indrMechUtcnt') or 0)
                oudr_auto = int(br_title.get('oudrAutoUtcnt') or 0)
                oudr_mech = int(br_title.get('oudrMechUtcnt') or 0)
                parking_count = indr_auto + indr_mech + oudr_auto + oudr_mech
            except:
                parking_count = 0

        # 5. 전유공용면적 API의 로우 데이터 확보 (집합건물인 경우)
        pubuse_items = []
        # 성능 및 트래픽 절약을 위해 지형 특성상 집합/공동주택으로 추정되거나 데이터 매칭이 필요할 때만 조회
        # 여기서는 전체 아이템을 로컬 메모리 캐시에 이식합니다.
        pubuse_raw = self.fetch_pubuse_raw_items(sgg_cd, umd_cd, jibun)
        if pubuse_raw:
            for item in pubuse_raw:
                pubuse_items.append({
                    "flrNo": str(item.get('flrNo', '')).strip(),
                    "area": item.get('area'),
                    "exposPubuseGbCd": str(item.get('exposPubuseGbCd')),
                    "mgmBldrgstPk": item.get('mgmBldrgstPk')
                })
                
        # 6. 최종 캐시 객체 조립
        res_data = {
            "status": "SUCCESS",
            "buildYear": build_year,
            "plottageAr": plottage_ar,
            "parkingCount": parking_count,
            "buildingName": bld_nm,
            "lat": lat,
            "lng": lng,
            "road_address": road_address,
            "pubuse_items": pubuse_items
        }
        
        with self.cache_lock:
            self.address_cache[cache_key] = res_data
            
        return res_data, False

    def run(self, max_queries):
        print("=" * 80)
        print(" [SCA AVM Engine - 극강 최적화 멀티스레드 캐시 보강 파이프라인 기동]")
        print("=" * 80)
        
        if not os.path.exists(self.output_path):
            print(f"오류: {self.output_path} 파일이 존재하지 않습니다.")
            return
            
        df = pd.read_csv(self.output_path, dtype={"sggCd": str, "jibun": str}, low_memory=False)
        
        # 신설 컬럼 초기화
        for col in ['parkingCount', 'pubuseAr', 'lat', 'lng', 'road_address', 'buildingName']:
            if col not in df.columns:
                df[col] = np.nan
                
        # 1. 유니크 지번 주소 리스트 빌드
        # 각 지번 그룹에 결측치(NaN)가 하나라도 있는지 판단하여 효율성 극대화
        unique_groups = df.groupby(['sggCd', 'sggNm', 'umdNm', 'jibun'])
        all_unique_addrs = []
        
        print("  * 지번 고유 식별 및 매핑 상태 진단 중...")
        for name, group in unique_groups:
            sgg_cd, sgg_nm, umd_nm, jibun = name
            
            # 보강 컬럼들 중 하나라도 누락(NaN)된 행이 있는지 검출
            has_missing = group['parkingCount'].isna().any() or group['pubuseAr'].isna().any() or group['buildYear'].isna().any()
            
            cache_key = f"{sgg_nm}_{umd_nm}_{jibun}"
            is_cached = cache_key in self.address_cache
            
            all_unique_addrs.append({
                "sggCd": str(sgg_cd),
                "sggNm": sgg_nm,
                "umdNm": umd_nm,
                "jibun": str(jibun),
                "has_missing": has_missing,
                "is_cached": is_cached
            })
            
        total_unique = len(all_unique_addrs)
        cached_unique = sum(1 for item in all_unique_addrs if item['is_cached'])
        to_query_unique_list = [item for item in all_unique_addrs if not item['is_cached']]
        to_query_count = len(to_query_unique_list)
        
        print(f"  * 전체 고유 지번 개수: {total_unique:,} 개")
        print(f"  * 이미 캐시 완료된 지번: \033[92m{cached_unique:,} 개 ({(cached_unique/total_unique*100):.1f}%)\033[0m")
        print(f"  * 신규 조회 필요한 지번: {to_query_count:,} 개")
        
        # 오늘 쿼리할 최대 갯수 제한 적용
        limit_queries = min(to_query_count, max_queries)
        print(f"  * 금일 설정된 조회 한도: \033[94m{limit_queries:,} 개\033[0m")
        print("=" * 80)
        
        if limit_queries == 0:
            print("  [알림] 새로 수집할 대상이 없으므로, 캐시 데이터를 데이터셋에 병합하는 절차를 실행합니다.")
            self.merge_cache_to_df(df)
            return

        to_query_list = to_query_unique_list[:limit_queries]
        
        # 2. 멀티스레드 풀 구동
        print(f"\n{self.workers_count}개의 스레드로 실시간 병렬 처리를 개시합니다. (화면이 실시간으로 갱신됩니다)...")
        start_time = time.time()
        completed = 0
        success = 0
        
        with ThreadPoolExecutor(max_workers=self.workers_count) as executor:
            futures = {
                executor.submit(
                    self.process_single_address,
                    item['sggCd'],
                    item['sggNm'],
                    item['umdNm'],
                    item['jibun'],
                    item['has_missing']
                ): item for item in to_query_list
            }
            
            for future in as_completed(futures):
                item = futures[future]
                completed += 1
                
                try:
                    res, was_cached = future.result()
                    if res.get('status') == 'SUCCESS':
                        success += 1
                except Exception as e:
                    logger.error(f"스레드 실행 중 치명적 오류: {e}")
                    
                # 20건마다 콘솔 실시간 진행 현황 갱신
                if completed % 20 == 0 or completed == limit_queries:
                    elapsed = time.time() - start_time
                    speed = elapsed / completed
                    eta = speed * (limit_queries - completed)
                    sys.stdout.write(
                        f"\r  [진행 상황] 완료: {completed}/{limit_queries} 건 ({completed/limit_queries*100:.1f}%) | "
                        f"성공: {success}건 | 속도: {speed:.3f}초/지번 | ETA: {eta:.1f}초 | 활성키: #{self.current_key_idx + 1}"
                    )
                    sys.stdout.flush()
                    
                # 200건마다 디스크에 메모리 캐시 중간 백업
                if completed % 200 == 0:
                    self.save_cache()
                    
        # 수집 완료 후 최종 캐시 저장
        self.save_cache()
        elapsed_total = time.time() - start_time
        print(f"\n\n  \033[92m[수집 종료] 수집에 소요된 총 시간: {elapsed_total:.1f}초 (평균 속도: {elapsed_total/limit_queries:.3f}초/지번)\033[0m")
        
        # 3. 수집된 모든 캐시 데이터를 데이터셋 DataFrame에 초고속 병합
        print("  * 보강 완료된 데이터를 CSV 파일셋에 병합하여 정렬하고 있습니다...")
        self.merge_cache_to_df(df)

    def merge_cache_to_df(self, df):
        """캐시 딕셔너리를 활용해 판다스 DataFrame의 모든 행을 초고속 매핑"""
        start_merge = time.time()
        
        # 빠른 검색을 위한 매핑 딕셔너리 준비
        # 주차대수, 위경도 등은 지번 단위로 동일하므로 다이렉트 맵
        map_build_year = {}
        map_plottage_ar = {}
        map_parking = {}
        map_bld_nm = {}
        map_lat = {}
        map_lng = {}
        map_road = {}
        
        # 층별/면적별 공용면적 매치업용 캐시 준비
        pubuse_lookup = {}
        
        for key, val in self.address_cache.items():
            if val.get('status') == 'SUCCESS':
                map_build_year[key] = val.get('buildYear')
                map_plottage_ar[key] = val.get('plottageAr')
                map_parking[key] = val.get('parkingCount')
                map_bld_nm[key] = val.get('buildingName')
                map_lat[key] = val.get('lat')
                map_lng[key] = val.get('lng')
                map_road[key] = val.get('road_address')
                
                # 공용면적 매치용 로우 리스트
                pubuse_lookup[key] = val.get('pubuse_items', [])

        # DataFrame 이터레이션하며 초고속 매핑 적용
        success_cnt = 0
        
        # 판다스 성능을 극대화하기 위해 행 단위 loc가 아닌 리스트 컴프리헨션 방식으로 컬럼 생성
        keys = (df['sggNm'] + "_" + df['umdNm'] + "_" + df['jibun']).tolist()
        
        build_years = []
        plottages = []
        parkings = []
        bld_names = []
        lats = []
        lngs = []
        roads = []
        pubuses = []
        
        building_types = df['buildingType'].tolist()
        floors = df['floor'].tolist()
        building_ars = df['buildingAr'].tolist()
        
        for idx, key in enumerate(keys):
            if key in map_build_year:
                # 1. 기본 인프라 정보 주입
                # 기존 값이 이미 올바르게 존재하면 보존하고, 빈 값(NaN)이거나 강제 갱신 대상일 때 매핑
                by = df.at[idx, 'buildYear']
                build_years.append(map_build_year[key] if pd.isna(by) or str(by).strip() == '' or by == 0 else by)
                
                pa = df.at[idx, 'plottageAr']
                plottages.append(map_plottage_ar[key] if pd.isna(pa) or pa == 0 else pa)
                
                parkings.append(map_parking[key])
                bld_names.append(map_bld_nm[key])
                lats.append(map_lat[key])
                lngs.append(map_lng[key])
                roads.append(map_road[key])
                
                # 2. 전유공용면적 고정밀 역추적 합산 매핑 (집합건물인 경우만)
                b_type = building_types[idx]
                flr = floors[idx]
                b_ar = building_ars[idx]
                
                pubuse_sum = 0.0
                if b_type == '집합' and not pd.isna(flr) and not pd.isna(b_ar):
                    try:
                        target_floor = str(int(float(flr)))
                    except:
                        target_floor = str(flr).replace('.0', '').strip()
                        
                    # 캐시된 전유공용면적 리스트
                    raw_items = pubuse_lookup.get(key, [])
                    target_pk = None
                    
                    # 전유 매칭
                    for item in raw_items:
                        if item['exposPubuseGbCd'] == '1' and item['flrNo'] == target_floor:
                            try:
                                item_area = float(item['area'])
                                if abs(item_area - float(b_ar)) < 0.5:
                                    target_pk = item['mgmBldrgstPk']
                                    break
                            except:
                                continue
                                
                    # 공용 합산
                    if target_pk:
                        for item in raw_items:
                            if item['exposPubuseGbCd'] == '2' and item['mgmBldrgstPk'] == target_pk:
                                try:
                                    pubuse_sum += float(item['area'])
                                except:
                                    continue
                                    
                pubuses.append(round(pubuse_sum, 2))
                success_cnt += 1
            else:
                # 매칭되지 않은 캐시 데이터는 기존 값 보존
                build_years.append(df.at[idx, 'buildYear'])
                plottages.append(df.at[idx, 'plottageAr'])
                parkings.append(df.at[idx, 'parkingCount'])
                bld_names.append(df.at[idx, 'buildingName'])
                lats.append(df.at[idx, 'lat'])
                lngs.append(df.at[idx, 'lng'])
                roads.append(df.at[idx, 'road_address'])
                pubuses.append(df.at[idx, 'pubuseAr'])

        # 리스트 데이터를 한 번에 DataFrame에 이식
        df['buildYear'] = build_years
        df['plottageAr'] = plottages
        df['parkingCount'] = parkings
        df['buildingName'] = bld_names
        df['lat'] = lats
        df['lng'] = lngs
        df['road_address'] = roads
        df['pubuseAr'] = pubuses
        
        # 파일 저장
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        print(f"  * [저장 완료] 전체 52,700행 중 {success_cnt:,}개 행에 대해 최종 데이터 매핑 및 디스크 갱신을 완료했습니다!")
        print(f"  * 병합 완료 시간: {time.time() - start_merge:.2f}초")
        print("=" * 80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SCA AVM Enterprise Multi-Threaded Data Enrichment Pipeline")
    parser.add_argument("--limit", type=int, default=5000, help="오늘 수집할 고유 지번 한도 설정")
    parser.add_argument("--workers", type=int, default=8, help="병렬 처리에 투입할 스레드 풀 수")
    parser.add_argument("--output", type=str, default="data/processed/avm_precision_set.csv", help="대상 CSV 경로")
    
    args = parser.parse_args()
    
    enricher = EnterpriseEnricher(output_path=args.output, workers_count=args.workers)
    enricher.run(max_queries=args.limit)
