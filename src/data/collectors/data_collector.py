import os
import time
import json
import logging
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import xml.etree.ElementTree as ET

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCollector:
    """
    국토교통부 상업업무용 실거래가 API 데이터를 수집하는 클래스
    """
    def __init__(self, api_key: str, data_dir: str = "data/raw"):
        self.api_key = api_key
        self.data_dir = data_dir
        self.state_file = os.path.join(self.data_dir, "collection_state.json")
        self.base_url = "http://apis.data.go.kr/1613000/RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade"
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def _get_target_months(self, n_months: int) -> list:
        """현재 달로부터 과거 n개월 간의 YYYYMM 리스트를 생성하여 반환합니다."""
        target_months = []
        today = datetime.today()
        for i in range(n_months):
            target_date = today - relativedelta(months=i)
            target_months.append(target_date.strftime("%Y%m"))
        return target_months

    def _load_state(self) -> dict:
        """이전 수집 완료 상태를 파일에서 불러옵니다."""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"completed": []}

    def _save_state(self, state: dict):
        """수집 완료 상태를 파일에 저장합니다."""
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=4)

    def _parse_xml_to_dict(self, xml_string: str) -> dict:
        """XML 형태의 API 응답을 파싱하여 딕셔너리로 변환합니다."""
        root = ET.fromstring(xml_string)
        header = root.find("header")
        result_code = header.findtext("resultCode") if header is not None else None
        result_msg = header.findtext("resultMsg") if header is not None else None
        
        items_element = root.find(".//items")
        items = []
        if items_element is not None:
            for item in items_element.findall("item"):
                item_dict = {child.tag: child.text for child in item}
                items.append(item_dict)
                
        return {
            "resultCode": result_code,
            "resultMsg": result_msg,
            "items": items
        }

    def collect(self, lawd_cd: str, n_months: int):
        """특정 법정동 코드(LAWD_CD)의 데이터를 n개월치 수집합니다."""
        target_months = self._get_target_months(n_months)
        state = self._load_state()
        
        logger.info(f"데이터 수집 시작: 지역코드 {lawd_cd}, {n_months}개월치")
        
        for yyyymm in target_months:
            task_id = f"{lawd_cd}_{yyyymm}"
            if task_id in state["completed"]:
                logger.info(f"이미 수집 완료된 데이터 건너뜀: {task_id}")
                continue
                
            # 디코딩된 API Key 사용을 위해 requests.get 파라미터 전달 시 주의 (공공데이터포털 특징)
            params = {
                "serviceKey": requests.utils.unquote(self.api_key),
                "LAWD_CD": lawd_cd,
                "DEAL_YMD": yyyymm,
                "numOfRows": "1000",
                "pageNo": "1"
            }
            
            try:
                logger.info(f"API 요청 중: {task_id}")
                response = requests.get(self.base_url, params=params, timeout=30)
                
                # 공공데이터 API 트래픽 제한(10,000건) 초과 에러 메시지 확인
                if "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR" in response.text or "LIMITED" in response.text:
                    logger.error("일일 트래픽 제한(10,000건)을 초과했습니다. 수집을 중단합니다.")
                    break
                    
                if response.status_code != 200:
                    logger.error(f"HTTP 에러 발생 (코드 {response.status_code}). 응답: {response.text[:200]}")
                    break

                # XML 응답 파싱
                parsed_data = self._parse_xml_to_dict(response.text)
                
                # API 결과 코드 확인 (00, 000이 정상)
                if parsed_data.get("resultCode") not in ["00", "000"]:
                    msg = parsed_data.get("resultMsg")
                    logger.error(f"API 에러 응답: {msg} (코드: {parsed_data.get('resultCode')}). 수집 중단.")
                    if parsed_data.get("resultCode") in ["22", "99"]: # 22: 요청건수 초과 등 API별 정의 상이할 수 있음
                        break
                    break
                
                # JSON 형태로 데이터 저장
                save_path = os.path.join(self.data_dir, f"{lawd_cd}_{yyyymm}.json")
                with open(save_path, "w", encoding="utf-8") as f:
                    json.dump(parsed_data, f, ensure_ascii=False, indent=4)
                    
                logger.info(f"데이터 저장 성공: {save_path} (수집 건수: {len(parsed_data.get('items', []))})")
                
                # 수집 상태 파일 업데이트 (중단 시점 이후부터 재개하기 위함)
                state["completed"].append(task_id)
                self._save_state(state)
                
                # 트래픽 제한 방지 및 서버 부하 조절을 위한 지연 (1.5초)
                time.sleep(1.5)
                
            except ET.ParseError:
                logger.error(f"XML 파싱 에러: 응답이 올바른 XML 형태가 아닙니다. ({task_id})")
                logger.error(f"응답 내용: {response.text[:200]}")
                break
            except Exception as e:
                logger.error(f"데이터 수집 중 오류 발생 ({task_id}): {str(e)}")
                break
                
        logger.info("데이터 수집 프로세스 종료.")

if __name__ == "__main__":
    # .env 파일에서 환경 변수 로드
    load_dotenv()
    
    # 공공데이터포털에서 발급받은 API Key (디코딩된 키 기준)
    API_KEY = os.getenv("MOLIT_API_KEY")
    
    if not API_KEY:
        logger.error(".env 파일에 MOLIT_API_KEY가 설정되지 않았습니다.")
    else:
        # 강남구(11680), 최근 12개월 데이터 수집 예시
        collector = DataCollector(api_key=API_KEY)
        collector.collect(lawd_cd="11680", n_months=12)
