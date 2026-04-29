import os
import time
import logging
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressRestorer:
    """
    마스킹된 지번을 복원하고, 건축물대장 API를 통해 데이터를 보강하는 모듈
    우선순위:
    1. 카카오 API를 활용한 지번 복원 시도
    2. 복원 성공 시 -> 건축물대장 '단건 조회' (트래픽 최소화)
    3. 복원 실패 시 -> 건축물대장 '동 단위 전체 스캔' 및 교차 검증 (면적 등 비교)
    """
    def __init__(self):
        load_dotenv()
        self.kakao_api_key = os.getenv("KAKAO_REST_API_KEY")
        self.molit_api_key = os.getenv("MOLIT_API_KEY")
        
        # 국토교통부_건축HUB_건축물대장정보 서비스 (표제부) 오픈 API
        self.br_base_url = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"

    def restore_via_kakao(self, sgg_nm, umd_nm, jibun):
        """
        카카오 로컬 API를 활용하여 마스킹된 지번 복원 및 법정동 코드(umd_cd)를 추출합니다.
        반환값: (복원된_지번, 법정동_코드_5자리)
        """
        if not self.kakao_api_key:
            logger.error("KAKAO_REST_API_KEY가 설정되지 않았습니다.")
            return None, None
            
        # 검색어 구성 (예: 종로구 인의동 1**)
        query = f"{sgg_nm} {umd_nm} {jibun}".replace('**', '').strip()
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        headers = {"Authorization": f"KakaoAK {self.kakao_api_key}"}
        
        umd_cd = None
        restored_jibun = None
        
        try:
            res = requests.get(url, headers=headers, params={"query": query}, timeout=5)
            docs = []
            if res.status_code == 200:
                docs = res.json().get('documents', [])
                
                if not docs:
                    fallback_query = f"{sgg_nm} {umd_nm}"
                    res_fallback = requests.get(url, headers=headers, params={"query": fallback_query}, timeout=5)
                    if res_fallback.status_code == 200:
                        docs = res_fallback.json().get('documents', [])
                        
                if len(docs) > 0:
                    first_doc = docs[0]
                    
                    # 위경도 좌표 추출 (키워드 검색은 최상위에 x, y 존재)
                    lat = first_doc.get('y')
                    lng = first_doc.get('x')
                    road_address = first_doc.get('road_address_name')
                    
                    # 법정동 코드 추출 (키워드 검색 결과에서 주소 정보 파싱)
                    # 키워드 검색 결과에는 b_code가 바로 없을 수 있으므로 address_name을 사용해 재조회하거나 유추
                    address_name = first_doc.get('address_name', '')
                    
                    # 1. 법정동 코드 확보를 위해 주소 정보가 있는 경우 활용
                    if address_name:
                        # 주소 검색 API로 법정동 코드를 가져오기 위한 재시도 (캐시 효과)
                        addr_url = "https://dapi.kakao.com/v2/local/search/address.json"
                        res_addr = requests.get(addr_url, headers=headers, params={"query": address_name}, timeout=5)
                        if res_addr.status_code == 200:
                            addr_docs = res_addr.json().get('documents', [])
                            if addr_docs:
                                addr_info = addr_docs[0].get('address', {})
                                b_code = addr_info.get('b_code', '')
                                umd_cd = b_code[5:] if len(b_code) == 10 else None
                                
                                # 지번 복원 시도
                                if len(addr_docs) == 1:
                                    main_no = addr_info.get('main_address_no', '')
                                    sub_no = addr_info.get('sub_address_no', '')
                                    if main_no:
                                        restored_jibun = f"{main_no}-{sub_no}" if sub_no else main_no

                    return restored_jibun, umd_cd, lat, lng, road_address
        except Exception as e:
            logger.error(f"카카오 API 호출 오류: {e}")
            
        if jibun and '*' not in str(jibun):
            restored_jibun = jibun
            
        return restored_jibun, umd_cd, None, None, None

    def _split_jibun_for_api(self, jibun):
        """지번 텍스트(예: 123-4)를 본번 4자리, 부번 4자리 포맷으로 변환합니다."""
        if not jibun or '*' in str(jibun) or str(jibun).strip() == "":
            return "", ""
        parts = str(jibun).split('-')
        try:
            bun = parts[0].strip().zfill(4)
            ji = parts[1].strip().zfill(4) if len(parts) > 1 else "0000"
            return bun, ji
        except:
            return "", ""

    def fetch_br_exact(self, sgg_cd, umd_cd, jibun):
        """1단계: 정확한 지번으로 건축물대장 단건 조회를 수행합니다."""
        bun, ji = self._split_jibun_for_api(jibun)
        if not bun: return None
        
        params = {
            "serviceKey": requests.utils.unquote(self.molit_api_key),
            "sigunguCd": sgg_cd,
            "bjdongCd": umd_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": "10",
            "pageNo": "1"
        }
        
        try:
            res = requests.get(self.br_base_url, params=params, timeout=10)
            if res.status_code == 200:
                items = self._parse_br_xml(res.text)
                if items:
                    return items[0]
        except Exception as e:
            logger.error(f"단건 조회 오류: {e}")
        return None

    def fetch_br_dong_scan(self, sgg_cd, umd_cd, target_ar, target_year):
        """2단계: 지번 복원 실패 시 동 단위 스캔을 통한 교차 검증을 수행합니다."""
        logger.info(f"2단계 동 단위 스캔 시작 (지역코드: {sgg_cd}{umd_cd})")
        
        page_no = 1
        max_pages = 20
        
        while page_no <= max_pages:
            params = {
                "serviceKey": requests.utils.unquote(self.molit_api_key),
                "sigunguCd": sgg_cd,
                "bjdongCd": umd_cd,
                "numOfRows": "100",
                "pageNo": str(page_no)
            }
            
            try:
                res = requests.get(self.br_base_url, params=params, timeout=10)
                items = self._parse_br_xml(res.text)
                if not items: break
                    
                for item in items:
                    # [개선] 안전한 숫자 변환
                    try:
                        br_ar = float(item.get('totArea') or 0)
                        tr_ar = float(target_ar or 0)
                        
                        br_use_date = str(item.get('useAprvDe') or "").strip()
                        br_year = int(br_use_date[:4]) if len(br_use_date) >= 4 and br_use_date[:4].isdigit() else 0
                        
                        tr_year_str = str(target_year or "0").strip()
                        tr_year = int(tr_year_str) if tr_year_str.isdigit() else 0
                        
                        area_match = (tr_ar > 0 and abs(br_ar - tr_ar) < 1.0)
                        year_match = (tr_year > 0 and br_year > 0 and abs(br_year - tr_year) <= 1)
                        
                        if area_match and year_match:
                            return item
                    except (ValueError, TypeError):
                        continue
                        
                page_no += 1
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"동 단위 스캔 오류: {e}")
                break
        return None

    def _parse_br_xml(self, xml_text):
        """XML 파싱 유틸리티"""
        try:
            root = ET.fromstring(xml_text)
            items = []
            for item in root.findall(".//item"):
                items.append({child.tag: child.text for child in item})
            return items
        except:
            return []

    def restore_and_enrich(self, item_data):
        """메인 파이프라인: 주어진 실거래가 데이터의 위치를 추적하고 보강합니다."""
        sgg_cd = item_data.get('sggCd')
        sgg_nm = item_data.get('sggNm')
        umd_nm = item_data.get('umdNm')
        jibun = item_data.get('jibun')
        
        # 1. 카카오 API로 주소 정보 추출
        restored_jibun, umd_cd, lat, lng, road_address = self.restore_via_kakao(sgg_nm, umd_nm, jibun)
        
        # 결과값 업데이트 (기본 정보)
        item_data['lat'] = lat
        item_data['lng'] = lng
        item_data['road_address'] = road_address
        
        if not umd_cd:
            return item_data
            
        # 2. 우선순위 1: 단건 조회
        br_data = None
        if restored_jibun and '*' not in restored_jibun:
            br_data = self.fetch_br_exact(sgg_cd, umd_cd, restored_jibun)
                
        # 3. 우선순위 2: 동 단위 스캔
        if not br_data:
            target_ar = item_data.get('buildingAr')
            target_year = item_data.get('buildYear')
            br_data = self.fetch_br_dong_scan(sgg_cd, umd_cd, target_ar, target_year)
        
        if br_data:
            item_data['buildingName'] = br_data.get('bldNm')
            item_data['mainPurpsCdNm'] = br_data.get('mainPurpsCdNm')
            # 지번 복원 성공 시 업데이트
            if not restored_jibun or '*' in restored_jibun:
                rbun = br_data.get('bun', '').lstrip('0')
                rji = br_data.get('ji', '').lstrip('0')
                item_data['restored_jibun'] = f"{rbun}-{rji}" if rji != "0" else rbun
            else:
                item_data['restored_jibun'] = restored_jibun
            
        return item_data

if __name__ == "__main__":
    # 스모크 테스트용 코드
    restorer = AddressRestorer()
    
    # 예시 1: 마스킹된 거래 데이터
    test_item = {
        "sggCd": "11110",
        "sggNm": "종로구",
        "umdNm": "인의동",
        "jibun": "1**",
        "buildingAr": "17098.51",
        "buildYear": "1982"
    }
    
    enriched = restorer.restore_and_enrich(test_item)
    print("최종 보강된 데이터:", enriched)
