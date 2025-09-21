"""
Bronze Layer - Delta Lake 기반 원시 데이터 수집 및 저장
GCS에 Delta Table 형태로 S&P 500 데이터와 배당 데이터를 저장
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import requests
from io import StringIO
from typing import List, Tuple, Optional
import os
import logging
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from google.cloud import storage

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

class BronzeLayerDelta:
    """Bronze Layer Delta Lake 기반 관리 클래스"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        """
        Bronze Layer 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 내 경로
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(gcs_bucket)
        
        # Delta Table 경로 설정
        self.price_table_path = f"gs://{gcs_bucket}/{gcs_path}/sp500_daily_prices"
        self.dividend_table_path = f"gs://{gcs_bucket}/{gcs_path}/sp500_dividend_info"
    
    def to_yahoo_symbol(self, sym: str) -> str:
        """클래스 주식 표기: BRK.B -> BRK-B"""
        return sym.strip().upper().replace(".", "-")
    
    def get_sp500_from_wikipedia(self, max_retries: int = 3, timeout: int = 15) -> pd.DataFrame:
        """Wikipedia에서 S&P500 구성종목 테이블 파싱"""
        headers_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
        ]
        last_err = None

        for i in range(max_retries):
            try:
                logger.info(f"Wikipedia 접근 시도 {i+1}/{max_retries}...")
                headers = {
                    "User-Agent": random.choice(headers_pool),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.7",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
                resp = requests.get(WIKI_URL, headers=headers, timeout=timeout)
                resp.raise_for_status()
                
                tables = pd.read_html(StringIO(resp.text))
                spx = tables[0]
                
                if "Symbol" not in spx.columns:
                    candidates = [c for c in spx.columns if "symbol" in c.lower() or "ticker" in c.lower()]
                    if candidates:
                        spx = spx.rename(columns={candidates[0]: "Symbol"})
                    else:
                        raise ValueError("Symbol 컬럼을 찾지 못했습니다.")
                
                logger.info(f"✅ Wikipedia에서 S&P 500 데이터 수집 성공: {len(spx)}개 종목")
                return spx
                
            except Exception as e:
                last_err = e
                logger.error(f"❌ Wikipedia 접근 실패 (시도 {i+1}): {e}")
                if i < max_retries - 1:
                    wait_time = 1.5 * (i + 1)
                    logger.info(f"⏳ {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
        
        raise RuntimeError(f"Wikipedia 파싱 최종 실패: {last_err}")
    
    def normalize_symbols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Yahoo 형식으로 심볼 정규화"""
        df = df.copy()
        df["Symbol"] = df["Symbol"].astype(str).map(self.to_yahoo_symbol)
        return df
    
    def get_daily_data_for_tickers(self, tickers: List[str], target_date: datetime.date) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """전체 S&P 500의 하루치 데이터를 조회합니다."""
        logger.info(f" {target_date} 하루치 데이터 수집 시작...")
        logger.info(f" 전체 종목 수: {len(tickers)}개")
        
        all_daily_data = []
        successful_tickers = []
        failed_tickers = []
        
        for i, ticker in enumerate(tickers):
            logger.info(f"  처리 중: {ticker} ({i+1}/{len(tickers)})")
            
            try:
                stock = yf.Ticker(ticker)
                
                # 하루치 데이터 가져오기
                start_date = target_date
                end_date = target_date + timedelta(days=1)
                
                hist = stock.history(start=start_date, end=end_date)
                
                if not hist.empty and hist['Close'].notna().any():
                    # 데이터 처리
                    hist['ticker'] = ticker
                    hist['date'] = hist.index
                    hist = hist.reset_index(drop=True)
                    all_daily_data.append(hist)
                    successful_tickers.append(ticker)
                    
                    logger.info(f"    ✅ {ticker}: ${hist['Close'].iloc[-1]:.2f}")
                else:
                    failed_tickers.append(ticker)
                    logger.info(f"    ❌ {ticker}: 데이터 없음")
                    
            except Exception as e:
                failed_tickers.append(ticker)
                logger.error(f"    ❌ {ticker}: {e}")
            
            # API 제한 고려한 딜레이
            time.sleep(0.5)
            
            # 진행 상황 표시 (50개마다)
            if (i + 1) % 50 == 0:
                logger.info(f"    📊 진행률: {i+1}/{len(tickers)} ({((i+1)/len(tickers)*100):.1f}%)")
                logger.info(f"    ✅ 성공: {len(successful_tickers)}개, ❌ 실패: {len(failed_tickers)}개")
        
        logger.info(f"\n📈 최종 수집 결과:")
        logger.info(f"  ✅ 성공: {len(successful_tickers)}개")
        logger.info(f"  ❌ 실패: {len(failed_tickers)}개")
        logger.info(f"  데이터 포인트: {sum(len(df) for df in all_daily_data)}개")
        
        return all_daily_data, successful_tickers, failed_tickers
    
    def get_dividend_info_for_tickers(self, tickers: List[str], sample_size: Optional[int] = None) -> List[dict]:
        """배당 정보 수집"""
        if sample_size is None:
            sample_size = len(tickers)
        
        logger.info(f"\n💰 배당 정보 수집 중... (상위 {min(sample_size, len(tickers))}개 종목)")
        
        dividend_info = []
        successful_count = 0
        
        for i, ticker in enumerate(tickers[:sample_size]):
            logger.info(f"  처리 중: {ticker} ({i+1}/{min(sample_size, len(tickers))})")
            
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 기본 정보 수집
                dividend_yield = info.get('dividendYield', 0) or 0
                dividend_rate = info.get('dividendRate', 0) or 0
                ex_dividend_date = info.get('exDividendDate', None)
                payment_date = info.get('dividendDate', None)
                dividend_frequency = info.get('dividendFrequency', None)
                
                # 배당주 여부 판단
                has_dividend = dividend_yield > 0 or dividend_rate > 0
                
                dividend_info.append({
                    'ticker': ticker,
                    'company_name': info.get('longName', 'N/A'),
                    'sector': info.get('sector', 'N/A'),
                    'has_dividend': has_dividend,
                    'dividend_yield': dividend_yield,
                    'dividend_yield_percent': dividend_yield * 100 if dividend_yield else 0,
                    'dividend_rate': dividend_rate,
                    'ex_dividend_date': ex_dividend_date,
                    'payment_date': payment_date,
                    'dividend_frequency': dividend_frequency,
                    'market_cap': info.get('marketCap', 0),
                    'last_price': info.get('currentPrice', 0)
                })
                
                successful_count += 1
                logger.info(f"    ✅ {info.get('longName', 'N/A')[:30]}")
                if has_dividend:
                    logger.info(f"    💰 배당수익률: {dividend_yield:.2%}")
                
            except Exception as e:
                logger.error(f"    ❌ {ticker}: {e}")
                # 실패한 경우에도 기본 정보는 추가
                dividend_info.append({
                    'ticker': ticker,
                    'company_name': 'N/A',
                    'sector': 'N/A',
                    'has_dividend': False,
                    'dividend_yield': 0,
                    'dividend_yield_percent': 0,
                    'dividend_rate': 0,
                    'ex_dividend_date': None,
                    'payment_date': None,
                    'dividend_frequency': None,
                    'market_cap': 0,
                    'last_price': 0
                })
            
            # API 제한 고려
            time.sleep(0.3)
            
            # 진행 상황 표시 (50개마다)
            if (i + 1) % 50 == 0:
                logger.info(f"    📊 진행률: {i+1}/{min(sample_size, len(tickers))} ({((i+1)/min(sample_size, len(tickers))*100):.1f}%)")
                logger.info(f"    ✅ 성공: {successful_count}개")
        
        return dividend_info
    
    def save_price_data_to_delta(self, all_daily_data: List[pd.DataFrame], target_date: datetime.date):
        """가격 데이터를 Delta Table에 저장"""
        logger.info(f"\n�� 가격 데이터를 Delta Table에 저장 중...")
        
        if not all_daily_data:
            logger.warning("저장할 가격 데이터가 없습니다.")
            return
        
        # pandas DataFrame 결합
        combined_df = pd.concat(all_daily_data, ignore_index=True)
        
        # 데이터 정리
        combined_df['date'] = pd.to_datetime(combined_df['date']).dt.date
        combined_df['ingestion_timestamp'] = datetime.now()
        
        try:
            # Delta Table이 존재하는지 확인
            delta_table = DeltaTable(self.price_table_path)
            mode = "append"
            logger.info("✅ 기존 Delta Table에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("🆕 새로운 Delta Table 생성")
        
        # Delta Table에 저장
        write_deltalake(
            self.price_table_path,
            combined_df,
            mode=mode,
            partition_by=["date"],  # 날짜별 파티셔닝
            engine="pyarrow"
        )
        
        logger.info(f"✅ 가격 데이터 저장 완료: {len(combined_df)}행")
        logger.info(f"📍 저장 위치: {self.price_table_path}")
    
    def save_dividend_data_to_delta(self, dividend_info: List[dict], target_date: datetime.date):
        """배당 정보를 Delta Table에 저장"""
        logger.info(f"\n💾 배당 정보를 Delta Table에 저장 중...")
        
        if not dividend_info:
            logger.warning("저장할 배당 정보가 없습니다.")
            return
        
        # pandas DataFrame으로 변환
        dividend_df = pd.DataFrame(dividend_info)
        dividend_df['ingestion_timestamp'] = datetime.now()
        
        try:
            # Delta Table이 존재하는지 확인
            delta_table = DeltaTable(self.dividend_table_path)
            mode = "append"
            logger.info("✅ 기존 Delta Table에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("🆕 새로운 Delta Table 생성")
        
        # Delta Table에 저장
        write_deltalake(
            self.dividend_table_path,
            dividend_df,
            mode=mode,
            partition_by=["has_dividend"],  # 배당주 여부별 파티셔닝
            engine="pyarrow"
        )
        
        logger.info(f"✅ 배당 정보 저장 완료: {len(dividend_df)}행")
        logger.info(f"📍 저장 위치: {self.dividend_table_path}")
    
    def run_daily_collection(self, target_date: Optional[datetime.date] = None):
        """일일 데이터 수집 실행"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info(" S&P 500 Bronze Layer 일일 데이터 수집 (Delta Lake)")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            logger.info("\n1️⃣ S&P 500 종목 리스트 수집...")
            spx_raw = self.get_sp500_from_wikipedia()
            spx = self.normalize_symbols(spx_raw)
            tickers = spx["Symbol"].dropna().unique().tolist()
            logger.info(f"✅ 수집 완료: {len(tickers)}개 종목")
            
            # 2. 가격 데이터 수집
            logger.info(f"\n2️⃣ 가격 데이터 수집...")
            all_daily_data, successful_tickers, failed_tickers = self.get_daily_data_for_tickers(tickers, target_date)
            
            if all_daily_data:
                self.save_price_data_to_delta(all_daily_data, target_date)
            else:
                logger.error("❌ 가격 데이터 수집에 실패했습니다.")
                return
            
            # 3. 배당 정보 수집
            logger.info(f"\n3️⃣ 배당 정보 수집...")
            dividend_info = self.get_dividend_info_for_tickers(successful_tickers, sample_size=200)
            self.save_dividend_data_to_delta(dividend_info, target_date)
            
            # 4. 최종 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Bronze Layer 수집 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 수집 날짜: {target_date}")
            logger.info(f" 전체 종목 수: {len(tickers)}개")
            logger.info(f"✅ 성공한 종목: {len(successful_tickers)}개")
            logger.info(f"❌ 실패한 종목: {len(failed_tickers)}개")
            logger.info(f"💰 배당주 종목: {len([d for d in dividend_info if d['has_dividend']])}개")
            logger.info(f" 저장된 Delta Table:")
            logger.info(f"  - {self.price_table_path}")
            logger.info(f"  - {self.dividend_table_path}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Bronze Layer 수집 실패: {e}")
            raise

def main():
    """메인 실행 함수"""
    # GCS 설정 (환경변수에서 가져오기)
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    bronze_layer = BronzeLayerDelta(gcs_bucket=gcs_bucket)
    
    try:
        bronze_layer.run_daily_collection()
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
