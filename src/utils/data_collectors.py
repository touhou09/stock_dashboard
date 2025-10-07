"""
Bronze Layer 데이터 수집 모듈
- Wikipedia에서 S&P 500 종목 리스트 수집
- yfinance API를 통한 가격 및 배당 데이터 수집
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone, date
import time
import random
import requests
from io import StringIO
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

class SP500Collector:
    """S&P 500 종목 리스트 수집기 - 날짜별 지원"""
    
    WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    def __init__(self):
        self.headers_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
        ]
        # 연도별 S&P 500 목록 캐시
        self._yearly_cache = {}
        self._current_sp500_df = None
    
    def to_yahoo_symbol(self, sym: str) -> str:
        """클래스 주식 표기: BRK.B -> BRK-B"""
        return sym.strip().upper().replace(".", "-")
    
    def get_sp500_from_wikipedia(self, max_retries: int = 3, timeout: int = 15) -> pd.DataFrame:
        """Wikipedia에서 S&P500 구성종목 테이블 파싱"""
        last_err = None

        for i in range(max_retries):
            try:
                logger.info(f"Wikipedia 접근 시도 {i+1}/{max_retries}...")
                headers = {
                    "User-Agent": random.choice(self.headers_pool),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.7",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
                resp = requests.get(self.WIKI_URL, headers=headers, timeout=timeout)
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
    
    def get_current_sp500_dataframe(self) -> pd.DataFrame:
        """현재 S&P 500 DataFrame 반환 (캐싱)"""
        if self._current_sp500_df is None:
            logger.info("📊 현재 S&P 500 데이터 수집 중...")
            self._current_sp500_df = self.get_sp500_from_wikipedia()
            # 편입일 컬럼 추가
            if 'Date added' in self._current_sp500_df.columns:
                self._current_sp500_df['Date added'] = pd.to_datetime(
                    self._current_sp500_df['Date added'], errors='coerce'
                )
        return self._current_sp500_df
    
    def get_sp500_tickers_for_date(self, target_date: datetime.date) -> List[str]:
        """
        특정 날짜의 S&P 500 구성 종목 반환
        
        Args:
            target_date: 대상 날짜
            
        Returns:
            List[str]: 해당 날짜의 S&P 500 티커 리스트
        """
        logger.info(f"📋 {target_date} S&P 500 구성 종목 조회 중...")
        
        # 현재 S&P 500 데이터 가져오기
        sp500_df = self.get_current_sp500_dataframe()
        
        if 'Date added' not in sp500_df.columns:
            logger.warning("⚠️ 편입일 정보가 없어서 현재 S&P 500 목록을 반환합니다.")
            return sp500_df['Symbol'].dropna().unique().tolist()
        
        # 편입일이 대상 날짜 이전인 종목들만 필터링
        valid_tickers = sp500_df[
            sp500_df['Date added'] <= pd.Timestamp(target_date)
        ]['Symbol'].dropna().unique().tolist()
        
        # Yahoo Finance용 심볼로 변환
        valid_tickers = [self.to_yahoo_symbol(ticker) for ticker in valid_tickers]
        
        logger.info(f"✅ {target_date} S&P 500 구성 종목: {len(valid_tickers)}개")
        return valid_tickers
    
    def get_sp500_tickers_for_year(self, year: int) -> List[str]:
        """
        특정 연도의 S&P 500 구성 종목 반환 (캐싱)
        
        Args:
            year: 대상 연도
            
        Returns:
            List[str]: 해당 연도의 S&P 500 티커 리스트
        """
        if year not in self._yearly_cache:
            logger.info(f"📋 {year}년 S&P 500 구성 종목 캐싱 중...")
            
            # 연도 마지막 날짜 기준으로 조회
            year_end = date(year, 12, 31)
            self._yearly_cache[year] = self.get_sp500_tickers_for_date(year_end)
            
            logger.info(f"✅ {year}년 S&P 500 구성 종목 캐싱 완료: {len(self._yearly_cache[year])}개")
        
        return self._yearly_cache[year]
    
    def get_sp500_tickers_smart(self, start_date: datetime.date, end_date: datetime.date) -> List[str]:
        """
        백필 기간에 맞는 S&P 500 구성 종목 반환 (스마트)
        
        Args:
            start_date: 백필 시작 날짜
            end_date: 백필 종료 날짜
            
        Returns:
            List[str]: 백필에 적합한 S&P 500 티커 리스트
        """
        logger.info(f"📋 백필 기간 {start_date} ~ {end_date} S&P 500 구성 종목 조회 중...")
        
        # 같은 연도인 경우 연도별 캐싱 사용
        if start_date.year == end_date.year:
            return self.get_sp500_tickers_for_year(start_date.year)
        
        # 다른 연도인 경우 시작일 기준으로 조회
        return self.get_sp500_tickers_for_date(start_date)
    
    def get_historical_major_stocks_2000(self) -> List[str]:
        """
        2000년에 있었을 것으로 추정되는 주요 S&P 500 종목들
        Wikipedia 데이터가 부정확한 경우를 위한 대안
        """
        return [
            # 기술주
            'MSFT', 'IBM', 'INTC', 'CSCO', 'ORCL', 'SUNW', 'DELL', 'HPQ',
            
            # 금융주
            'JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'AXP', 'USB', 'PNC', 'TFC',
            
            # 제조업
            'GE', 'BA', 'CAT', 'MMM', 'HON', 'UTX', 'EMR', 'ITW', 'ETN', 'PH',
            
            # 소비재
            'WMT', 'HD', 'PG', 'JNJ', 'KO', 'PEP', 'MCD', 'NKE', 'DIS', 'MCD',
            
            # 에너지
            'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PXD', 'KMI', 'WMB', 'OKE', 'EPD',
            
            # 헬스케어
            'PFE', 'MRK', 'JNJ', 'ABT', 'TMO', 'DHR', 'BMY', 'LLY', 'AMGN', 'GILD',
            
            # 통신
            'T', 'VZ', 'CMCSA', 'VZ', 'T', 'VZ', 'CMCSA', 'VZ', 'T', 'VZ',
            
            # 유틸리티
            'SO', 'DUK', 'NEE', 'AEP', 'EXC', 'XEL', 'WEC', 'ES', 'PEG', 'ED',
            
            # 기타
            'MCD', 'DIS', 'NKE', 'SBUX', 'COST', 'TGT', 'LOW', 'HD', 'WMT', 'PG'
        ]

class PriceDataCollector:
    """가격 데이터 수집기"""
    
    def get_daily_data_for_tickers(self, tickers: List[str], target_date: datetime.date) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """전체 S&P 500의 하루치 데이터를 조회합니다."""
        logger.info(f"📊 {target_date} 하루치 데이터 수집 시작...")
        logger.info(f"📊 전체 종목 수: {len(tickers)}개")
        
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
                    # 데이터 처리 - Bronze 스키마에 맞게 정규화
                    hist['ticker'] = ticker
                    hist['date'] = hist.index.date  # 날짜만 추출
                    hist = hist.reset_index(drop=True)
                    
                    # 컬럼명 정규화 (Bronze 스키마)
                    # [수정] Adj Close가 있을 때만 rename
                    rename_dict = {
                        'Open': 'open',
                        'High': 'high', 
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    }
                    
                    if 'Adj Close' in hist.columns:
                        rename_dict['Adj Close'] = 'adj_close'
                    
                    hist = hist.rename(columns=rename_dict)
                    
                    # [수정] adj_close가 없으면 close 값으로 대체
                    if 'adj_close' not in hist.columns:
                        hist['adj_close'] = hist['close']
                    
                    # ingest_at 타임스탬프 추가
                    hist['ingest_at'] = datetime.now(timezone.utc)
                    
                    all_daily_data.append(hist)
                    successful_tickers.append(ticker)
                    
                    logger.info(f"    ✅ {ticker}: ${hist['close'].iloc[-1]:.2f}")
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

class DividendDataCollector:
    """배당 데이터 수집기"""
    
    def fetch_dividend_events_for_tickers(self, tickers: List[str], since: datetime.date, until: datetime.date, collection_date: datetime.date = None) -> pd.DataFrame:
        """
        [Bronze] yfinance 배당 이벤트를 원천 그대로 적재용 DF로 수집합니다.
        입력: tickers, since(포함), until(포함)
        출력: columns = [ex_date, ticker, amount, date, ingest_at]
        """
        logger.info(f"\n💰 배당 이벤트 수집 중... ({since} ~ {until})")
        logger.info(f"💰 처리할 종목 수: {len(tickers)}개")
        
        # 수집일 설정 (기본값: 오늘)
        if collection_date is None:
            collection_date = datetime.now().date()
        
        rows = []
        processed_count = 0
        
        for i, ticker in enumerate(tickers, 1):
            if i % 50 == 0 or i == 1:
                logger.info(f"  📊 진행률: {i}/{len(tickers)} ({((i)/len(tickers)*100):.1f}%)")
            
            try:
                tk = yf.Ticker(ticker)
                divs = tk.dividends  # Series(index=ex-date, value=amount)
                
                if divs is None or divs.empty:
                    continue

                s = divs.copy()
                # 인덱스 tz 정규화
                if hasattr(s.index, "tz"):
                    s.index = s.index.tz_convert("UTC").tz_localize(None)

                # 기간 필터
                mask = (s.index.date >= since) & (s.index.date <= until)
                s = s[mask]
                if s.empty:
                    continue

                for idx, amt in s.items():
                    rows.append({
                        "ex_date": idx.date(),
                        "ticker": ticker,
                        "amount": float(amt),
                        "date": collection_date,  # 수집일 추가
                        "ingest_at": datetime.now(timezone.utc)
                    })
                    
            except Exception as e:
                logger.error(f"    ❌ {ticker}: {e}")
                continue
            
            # API 제한 고려
            time.sleep(0.3)
            processed_count += 1
        
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["ex_date", "ticker"]).reset_index(drop=True)
            logger.info(f"✅ 배당 이벤트 수집 완료: {len(df)}개 이벤트")
        else:
            logger.info("✅ 배당 이벤트 수집 완료: 0개 이벤트")
            
        return df
