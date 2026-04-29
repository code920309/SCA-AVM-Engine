import os
import sys
import json
import time
import logging
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
from dotenv import load_dotenv

# sys.path 설정하여 data_collector 및 코드 리스트 임포트
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_collector import DataCollector
from all_korea_lawd_codes import LAWD_CODES

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InitialNationalCollector(DataCollector):
    """
    전국 시·군·구 과거 60개월 실거래가 전수 수집 클래스
    배치 모드, 연속성 보장, 강건한 에외 처리 포함
    """
    def __init__(self, api_key: str, data_dir: str = "data/raw"):
        super().__init__(api_key, data_dir)
        self.national_codes = LAWD_CODES

    def run_collection(self, n_months: int = 60, batch_size: int = 50):
        """
        전국 데이터를 n개월치 수집합니다.
        batch_size: 한 번의 실행에서 처리할 최대 시군구 코드 수 (일일 트래픽 고려)
        """
        target_months = self._get_target_months(n_months)
        state = self._load_state()
        
        logger.info(f"전국 데이터 수집 시작 (전체 {len(self.national_codes)}개 지역 중 {batch_size}개 지역 처리 예정)")
        
        processed_districts = 0
        consecutive_failures = 0
        stop_all = False
        
        for lawd_cd in self.national_codes:
            if stop_all:
                break
            
            # 해당 지역의 모든 달이 완료되었는지 확인
            district_tasks = [f"{lawd_cd}_{m}" for m in target_months]
            if all(t in state.get("completed", []) for t in district_tasks):
                continue
            
            # 배치 크기 도달 시 종료
            if processed_districts >= batch_size:
                logger.info(f"배치 제한({batch_size}개 지역)에 도달하여 안전하게 수집을 중단합니다.")
                break
                
            logger.info(f"=== [지역코드: {lawd_cd}] 수집 시작 (배치 진행도: {processed_districts + 1}/{batch_size}) ===")
            
            for yyyymm in target_months:
                task_id = f"{lawd_cd}_{yyyymm}"
                if task_id in state.get("completed", []):
                    continue
                
                # API 요청 파라미터
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
                    
                    # 1. API 트래픽 제한 감지
                    if "LIMITED NUMBER" in response.text or "LIMITED" in response.text:
                        logger.error("일일 트래픽 제한(10,000건)을 초과했습니다. 종료합니다.")
                        stop_all = True
                        break
                    
                    # 2. HTTP 에러 및 연속 실패 카운트
                    if response.status_code != 200:
                        logger.error(f"HTTP 에러: {response.status_code}")
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logger.error("연속 3회 서버 응답 실패. 안전 종료합니다.")
                            stop_all = True
                            break
                        continue
                    else:
                        consecutive_failures = 0 # 성공 시 초기화

                    # 3. 데이터 파싱
                    parsed_data = self._parse_xml_to_dict(response.text)
                    
                    # API 결과 코드 확인
                    if parsed_data.get("resultCode") not in ["00", "000"]:
                        msg = parsed_data.get("resultMsg")
                        logger.error(f"API 에러 응답 ({task_id}): {msg} (코드: {parsed_data.get('resultCode')})")
                        
                        if parsed_data.get("resultCode") in ["22", "99", "30", "01"]:
                            stop_all = True
                            break
                        continue
                    
                    # 4. 저장 및 상태 업데이트
                    save_path = os.path.join(self.data_dir, f"{lawd_cd}_{yyyymm}.json")
                    with open(save_path, "w", encoding="utf-8") as f:
                        json.dump(parsed_data, f, ensure_ascii=False, indent=4)
                    
                    if "completed" not in state:
                        state["completed"] = []
                    state["completed"].append(task_id)
                    self._save_state(state)
                    
                    logger.info(f"데이터 저장 성공: {save_path} ({len(parsed_data.get('items', []))}건)")
                    
                    # 간격 유지 (1.2초)
                    time.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"예외 발생 ({task_id}): {str(e)}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        logger.error("연속 3회 예외 발생. 안전 종료합니다.")
                        stop_all = True
                        break
                    continue
            
            processed_districts += 1
            
        logger.info("수집 프로세스 종료 및 현재 상태 저장 완료.")

if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("MOLIT_API_KEY")
    
    if not API_KEY:
        logger.error(".env 파일에 MOLIT_API_KEY가 설정되지 않았습니다.")
    else:
        collector = InitialNationalCollector(api_key=API_KEY)
        # 60개월치 수집, 남은 모든 지역(약 110개)을 커버할 수 있도록 배치 크기 상향
        collector.run_collection(n_months=60, batch_size=200)
