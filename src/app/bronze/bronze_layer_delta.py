"""
Bronze Layer Delta Lake 기반 관리 클래스 - 원천 데이터만 저장
"""

import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
import logging
import time
from google.cloud import storage
from dotenv import load_dotenv

# .env 파일 로드
try:
    load_dotenv()
except Exception:
    pass

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLayerDelta:
    """Bronze Layer Delta Lake 기반 관리 클래스 - 원천 데이터만 저장"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        """
        Bronze Layer 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 경로
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        
        # GCS 클라이언트 초기화
        self.client = storage.Client()
        
        # Delta Table 경로
        self.price_table_path = f"gs://{gcs_bucket}/{gcs_path}/bronze_price_daily"
        self.dividend_table_path = f"gs://{gcs_bucket}/{gcs_path}/bronze_dividend_events"
    
    def to_yahoo_symbol(self, symbol: str) -> str:
        """심볼을 Yahoo Finance 형식으로 변환"""
        return symbol.strip().replace('.', '-').upper()
    
    def get_sp500_from_wikipedia(self, max_retries: int = 3) -> pd.DataFrame:
        """Wikipedia에서 S&P 500 데이터 수집"""
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Wikipedia에서 S&P 500 데이터 수집 시도 {attempt + 1}/{max_retries}")
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # HTML 테이블 파싱
                tables = pd.read_html(response.text)
                sp500_df = tables[0]  # 첫 번째 테이블이 S&P 500 리스트
                
                # 필요한 컬럼만 선택
                required_columns = ['Symbol', 'Security', 'GICS Sector']
                if all(col in sp500_df.columns for col in required_columns):
                    sp500_df = sp500_df[required_columns]
                    
                    # 심볼 정규화
                    sp500_df = self.normalize_symbols(sp500_df)
                    
                    logger.info(f"✅ S&P 500 데이터 수집 성공: {len(sp500_df)}개 종목")
                    return sp500_df
                else:
                    logger.warning(f"필요한 컬럼이 없습니다: {sp500_df.columns.tolist()}")
                    
            except Exception as e:
                logger.warning(f"Wikipedia 파싱 실패 (시도 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise RuntimeError(f"Wikipedia 파싱 최종 실패: {e}")
        
        raise RuntimeError("Wikipedia 파싱 최종 실패")
    
    def normalize_symbols(self, df: pd.DataFrame) -> pd.DataFrame:
        """심볼 정규화"""
        df['Symbol'] = df['Symbol'].apply(self.to_yahoo_symbol)
        return df
    
    def get_daily_data_for_tickers(self, tickers: List[str], target_date: date) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """여러 티커의 일일 데이터 수집"""
        all_data = []
        successful = []
        failed = []
        
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"데이터 수집 중: {ticker} ({i+1}/{len(tickers)})")
                
                yf_ticker = yf.Ticker(ticker)
                hist = yf_ticker.history(start=target_date, end=target_date + timedelta(days=1))
                
                if not hist.empty:
                    hist_df = hist.reset_index()
                    hist_df['ticker'] = ticker
                    hist_df['date'] = target_date
                    hist_df['ingest_at'] = datetime.now()
                    
                    all_data.append(hist_df)
                    successful.append(ticker)
                    logger.info(f"✅ {ticker} 데이터 수집 성공")
                else:
                    failed.append(ticker)
                    logger.warning(f"⚠️ {ticker} 데이터 없음")
                
                # API 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                failed.append(ticker)
                logger.error(f"❌ {ticker} 데이터 수집 실패: {e}")
        
        logger.info(f"데이터 수집 완료: 성공 {len(successful)}개, 실패 {len(failed)}개")
        return all_data, successful, failed
    
    def get_dividend_info_for_tickers(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """여러 티커의 배당 정보 수집"""
        dividend_info = []
        
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"배당 정보 수집 중: {ticker} ({i+1}/{len(tickers)})")
                
                yf_ticker = yf.Ticker(ticker)
                info = yf_ticker.info
                
                # 배당 정보 추출
                dividend_data = {
                    'ticker': ticker,
                    'company_name': info.get('longName', ''),
                    'sector': info.get('sector', ''),
                    'has_dividend': info.get('dividendYield', 0) > 0,
                    'dividend_yield': info.get('dividendYield', 0),
                    'dividend_rate': info.get('dividendRate', 0),
                    'ex_dividend_date': info.get('exDividendDate'),
                    'payment_date': info.get('dividendDate'),
                    'dividend_frequency': info.get('dividendFrequency'),
                    'market_cap': info.get('marketCap', 0),
                    'last_price': info.get('currentPrice', 0),
                    'ingest_at': datetime.now()
                }
                
                dividend_info.append(dividend_data)
                logger.info(f"✅ {ticker} 배당 정보 수집 성공")
                
                # API 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ {ticker} 배당 정보 수집 실패: {e}")
        
        return dividend_info
    
    def run_daily_collection(self, target_date: Optional[date] = None):
        """일일 데이터 수집 실행"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info(" Bronze Layer 일일 데이터 수집 시작")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            logger.info(f"\n1️⃣ S&P 500 종목 리스트 수집...")
            sp500_df = self.get_sp500_from_wikipedia()
            tickers = sp500_df['Symbol'].tolist()
            logger.info(f"✅ S&P 500 종목 수집 완료: {len(tickers)}개")
            
            # 2. 일일 가격 데이터 수집
            logger.info(f"\n2️⃣ 일일 가격 데이터 수집...")
            all_data, successful, failed = self.get_daily_data_for_tickers(tickers, target_date)
            
            if all_data:
                # 3. 가격 데이터 저장
                logger.info(f"\n3️⃣ 가격 데이터 저장...")
                from src.utils.data_storage import DeltaStorageManager
                storage_manager = DeltaStorageManager(self.gcs_bucket, self.gcs_path)
                storage_manager.save_price_data_to_delta(all_data, target_date)
                logger.info(f"✅ 가격 데이터 저장 완료: {len(all_data)}개")
            
            # 4. 배당 정보 수집
            logger.info(f"\n4️⃣ 배당 정보 수집...")
            dividend_info = self.get_dividend_info_for_tickers(successful)
            
            if dividend_info:
                # 5. 배당 정보 저장
                logger.info(f"\n5️⃣ 배당 정보 저장...")
                from src.utils.data_storage import DeltaStorageManager
                storage_manager = DeltaStorageManager(self.gcs_bucket, self.gcs_path)
                storage_manager.save_dividend_data_to_delta(dividend_info, target_date)
                logger.info(f"✅ 배당 정보 저장 완료: {len(dividend_info)}개")
            
            # 6. 최종 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Bronze Layer 수집 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 수집 날짜: {target_date}")
            logger.info(f"📊 전체 종목 수: {len(tickers)}개")
            logger.info(f"📊 성공한 종목 수: {len(successful)}개")
            logger.info(f"📊 실패한 종목 수: {len(failed)}개")
            logger.info(f"📊 배당 정보 수집: {len(dividend_info)}개")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Bronze Layer 수집 실패: {e}")
            raise Exception(f"Bronze Layer 수집 실패: {e}") from e

def main():
    """메인 실행 함수"""
    import os
    from dotenv import load_dotenv
    
    # .env 파일 로드 (선택적)
    try:
        load_dotenv()
    except Exception:
        pass
    
    # GCS 설정 (환경변수에서 가져오기)
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    bronze_layer = BronzeLayerDelta(gcs_bucket=gcs_bucket)
    
    try:
        # Bronze Layer 원천 데이터 수집 실행
        bronze_layer.run_daily_collection()
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
