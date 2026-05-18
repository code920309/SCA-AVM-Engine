import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressRestorer:
    """
    avm_precision_set.csv의 미마스킹 지번 데이터를 기반으로
    정확한 주소 정보를 획득하고 건축물대장 API를 통해 다음을 보강하는 모듈:
    1. 준공년도 (buildYear)
    2. 대지면적 (plottageAr)
    3. 총 주차대수 (parkingCount)
    4. 공용면적 (pubuseAr)
    """
    def __init__(self):
        load_dotenv()
        self.kakao_api_key = os.getenv("KAKAO_REST_API_KEY")
        self.molit_api_key = os.getenv("MOLIT_API_KEY")
        
        # 국토교통부_건축HUB_건축물대장정보 서비스 오픈 API 엔드포인트
        self.br_title_url = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
        self.br_pubuse_url = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo"

    def get_address_info_via_kakao(self, sgg_nm, umd_nm, jibun):
        """
        카카오 주소 검색 API를 호출하여 법정동 코드, 위경도 좌표, 도로명 주소를 정확히 가져옵니다.
        """
        if not self.kakao_api_key:
            logger.error("KAKAO_REST_API_KEY가 설정되지 않았습니다.")
            return None
            
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
                    
                    # 10자리 법정동 코드에서 앞 5자리는 sigunguCd, 뒤 5자리는 bjdongCd
                    umd_cd = b_code[5:] if len(b_code) == 10 else None
                    lat = docs[0].get('y')
                    lng = docs[0].get('x')
                    
                    road_addr_info = docs[0].get('road_address')
                    road_address = road_addr_info.get('address_name') if road_addr_info else None
                    
                    return {
                        "umd_cd": umd_cd,
                        "lat": lat,
                        "lng": lng,
                        "road_address": road_address
                    }
        except Exception as e:
            logger.error(f"카카오 API 호출 오류: {e}")
        return None

    def _split_jibun_for_api(self, jibun):
        """지번 텍스트(예: 123-4)를 본번 4자리, 부번 4자리 포맷으로 변환합니다."""
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
        """오픈 API 호출 결과를 JSON 형식으로 파싱하여 반환합니다."""
        params['_type'] = 'json'
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200:
                data = res.json()
                items = data.get('response', {}).get('body', {}).get('items', {})
                if isinstance(items, dict):
                    item_list = items.get('item', [])
                    if isinstance(item_list, dict):
                        return [item_list]
                    return item_list
        except Exception as e:
            logger.error(f"오픈 API 호출 오류: {e}")
        return []

    def fetch_br_title(self, sgg_cd, umd_cd, jibun):
        """표제부 API(/getBrTitleInfo)를 사용하여 건물 기본 및 대지면적, 주차대수 정보를 획득합니다."""
        bun, ji = self._split_jibun_for_api(jibun)
        if not bun: 
            return None
        
        params = {
            "serviceKey": requests.utils.unquote(self.molit_api_key),
            "sigunguCd": sgg_cd,
            "bjdongCd": umd_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": "10",
            "pageNo": "1"
        }
        
        items = self._fetch_br_json(self.br_title_url, params)
        if items:
            return items[0]
        return None

    def fetch_br_pubuse(self, sgg_cd, umd_cd, jibun, floor_val, building_ar):
        """
        전유공용면적 API(/getBrExposPubuseAreaInfo)를 활용하여
        해당 층(floor)과 전용면적(buildingAr)에 대응되는 호실의 공용면적 합계를 계산합니다.
        """
        bun, ji = self._split_jibun_for_api(jibun)
        if not bun: 
            return 0.0
            
        params = {
            "serviceKey": requests.utils.unquote(self.molit_api_key),
            "sigunguCd": sgg_cd,
            "bjdongCd": umd_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": "100",  # 한 층당 호실이 많을 수 있으므로 넉넉하게 조회
            "pageNo": "1"
        }
        
        items = self._fetch_br_json(self.br_pubuse_url, params)
        if not items:
            return 0.0
            
        try:
            # floor_val 파싱 (예: '5.0' -> 5, 5 -> 5)
            target_floor = str(int(float(floor_val))) if floor_val and not pd.isna(floor_val) else None
        except:
            target_floor = str(floor_val).replace('.0', '').strip() if floor_val else None

        target_pk = None
        
        # 1단계: 층과 전용면적이 일치하는 호실의 mgmBldrgstPk(건축물대장 고유키) 탐색
        for item in items:
            # exposPubuseGbCd == '1' (전유)
            is_priv = str(item.get('exposPubuseGbCd')) == '1'
            item_floor = str(item.get('flrNo')).strip()
            
            try:
                item_area = float(item.get('area') or 0)
                target_area = float(building_ar)
                area_match = abs(item_area - target_area) < 0.5  # 오차 범위 0.5m2 이내 매칭
            except:
                area_match = False
                
            if is_priv and item_floor == target_floor and area_match:
                target_pk = item.get('mgmBldrgstPk')
                break
                
        # 2단계: 동일한 고유키(mgmBldrgstPk)를 갖는 호실의 공용(exposPubuseGbCd == '2') 면적 합산
        if target_pk:
            pubuse_sum = 0.0
            for item in items:
                is_pub = str(item.get('exposPubuseGbCd')) == '2'
                is_same_pk = item.get('mgmBldrgstPk') == target_pk
                if is_pub and is_same_pk:
                    try:
                        pubuse_sum += float(item.get('area') or 0)
                    except:
                        continue
            return round(pubuse_sum, 2)
            
        return 0.0

    def restore_and_enrich(self, item_data):
        """
        메인 파이프라인:
        지번 조건에 근거하여 안전하고 정확하게 건축물대장의 핵심 지표를 매핑 보강합니다.
        """
        sgg_cd = item_data.get('sggCd')
        sgg_nm = item_data.get('sggNm')
        umd_nm = item_data.get('umdNm')
        jibun = item_data.get('jibun')
        
        if not sgg_nm or not umd_nm or not jibun:
            return item_data
            
        # 1. 카카오 API로 법정동 코드 및 주소 위치 정보 획득
        addr_info = self.get_address_info_via_kakao(sgg_nm, umd_nm, jibun)
        if not addr_info:
            return item_data
            
        umd_cd = addr_info.get('umd_cd')
        item_data['lat'] = addr_info.get('lat')
        item_data['lng'] = addr_info.get('lng')
        item_data['road_address'] = addr_info.get('road_address')
        
        if not umd_cd:
            return item_data
            
        # 2. 표제부 API 호출 -> 준공년도, 대지면적, 주차대수 수집
        br_title = self.fetch_br_title(sgg_cd, umd_cd, jibun)
        if br_title:
            item_data['buildingName'] = br_title.get('bldNm')
            item_data['mainPurpsCdNm'] = br_title.get('mainPurpsCdNm')
            
            # 건축 준공년도(buildYear) 보강
            use_apr_day = br_title.get('useAprDay')
            if use_apr_day and str(use_apr_day).strip() and len(str(use_apr_day)) >= 4:
                item_data['buildYear'] = str(use_apr_day)[:4]
                
            # 대지면적(plottageAr) 보강
            plat_area = br_title.get('platArea')
            if plat_area:
                try:
                    item_data['plottageAr'] = round(float(plat_area), 2)
                except:
                    pass
                    
            # 총 주차대수(parkingCount) 보강 (옥내/외 자주식/기계식 합계)
            try:
                indr_auto = int(br_title.get('indrAutoUtcnt') or 0)
                indr_mech = int(br_title.get('indrMechUtcnt') or 0)
                oudr_auto = int(br_title.get('oudrAutoUtcnt') or 0)
                oudr_mech = int(br_title.get('oudrMechUtcnt') or 0)
                item_data['parkingCount'] = indr_auto + indr_mech + oudr_auto + oudr_mech
            except:
                item_data['parkingCount'] = 0
        else:
            item_data['parkingCount'] = 0

        # 3. 전유공용면적 API 호출 -> 공용면적(pubuseAr) 수집
        # 집합건물인 경우에만 정밀 조회를 시도하고, 일반 건물일 경우 0.0 설정
        building_type = item_data.get('buildingType')
        floor_val = item_data.get('floor')
        building_ar = item_data.get('buildingAr')
        
        if building_type == '집합' and floor_val and building_ar:
            pubuse_ar = self.fetch_br_pubuse(sgg_cd, umd_cd, jibun, floor_val, building_ar)
            item_data['pubuseAr'] = pubuse_ar
        else:
            item_data['pubuseAr'] = 0.0
            
        return item_data
