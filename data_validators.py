"""
Bronze Layer 데이터 검증 모듈
- 데이터 품질 검증
- 중복 데이터 체크
- 거래일 검증
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """데이터 검증기"""
    
    def is_trading_day(self, date: datetime.date) -> bool:
        """주식 거래일인지 확인 (주말 제외)"""
        # 주말 체크 (토요일=5, 일요일=6)
        return date.weekday() < 5
    
    def validate_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """가격 데이터 검증"""
        if df.empty:
            logger.warning("⚠️ 가격 데이터가 비어있습니다.")
            return df
        
        # 필수 컬럼 확인
        required_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_columns}")
        
        # 가격 데이터 검증
        invalid_rows = df[
            (df['open'] <= 0) | 
            (df['high'] <= 0) | 
            (df['low'] <= 0) | 
            (df['close'] <= 0) |
            (df['volume'] < 0)
        ]
        
        if not invalid_rows.empty:
            logger.warning(f"⚠️ {len(invalid_rows)}개의 잘못된 가격 데이터 발견")
        
        # OHLC 논리 검증
        ohlc_invalid = df[
            (df['high'] < df['open']) |
            (df['high'] < df['close']) |
            (df['low'] > df['open']) |
            (df['low'] > df['close']) |
            (df['high'] < df['low'])
        ]
        
        if not ohlc_invalid.empty:
            logger.warning(f"⚠️ {len(ohlc_invalid)}개의 OHLC 논리 오류 발견")
        
        return df
    
    def validate_dividend_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """배당 데이터 검증"""
        if df.empty:
            logger.info("📊 배당 데이터가 비어있습니다.")
            return df
        
        # 필수 컬럼 확인
        required_columns = ['ex_date', 'ticker', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_columns}")
        
        # 배당금액 검증
        invalid_amounts = df[df['amount'] <= 0]
        if not invalid_amounts.empty:
            logger.warning(f"⚠️ {len(invalid_amounts)}개의 잘못된 배당금액 발견")
        
        return df

class BackfillValidator:
    """Backfill 관련 검증기"""
    
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
    
    def find_earliest_missing_date(self, start_date: datetime.date, end_date: datetime.date) -> Optional[datetime.date]:
        """지정된 기간에서 가장 이른 누락된 날짜를 찾습니다."""
        logger.info(f"🔍 누락된 날짜 검색 중... ({start_date} ~ {end_date})")
        
        try:
            from deltalake import DeltaTable
            delta_table = DeltaTable(self.storage_manager.price_table_path)
            existing_df = delta_table.to_pandas()
            
            if existing_df.empty:
                logger.info("📅 기존 데이터가 없습니다. 가장 이른 날짜부터 시작합니다.")
                return start_date
            
            # 날짜 컬럼이 있는지 확인
            if 'date' not in existing_df.columns:
                logger.info("📅 날짜 컬럼이 없습니다. 가장 이른 날짜부터 시작합니다.")
                return start_date
            
            # 날짜 형식 통일
            existing_df['date'] = pd.to_datetime(existing_df['date']).dt.date
            existing_dates = set(existing_df['date'].unique())
            
            # 주말 제외한 영업일만 생성
            validator = DataValidator()
            current_date = start_date
            while current_date <= end_date:
                # 주말이 아닌 경우만 체크 (월요일=0, 일요일=6)
                if validator.is_trading_day(current_date):
                    if current_date not in existing_dates:
                        logger.info(f"📅 가장 이른 누락된 날짜 발견: {current_date}")
                        return current_date
                current_date += timedelta(days=1)
            
            logger.info("📅 지정된 기간에 누락된 날짜가 없습니다.")
            return None
            
        except Exception as e:
            logger.info(f"📅 기존 데이터 확인 실패 (테이블이 없을 수 있음): {e}")
            return start_date
    
    def generate_trading_dates(self, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
        """영업일 목록 생성"""
        validator = DataValidator()
        trading_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if validator.is_trading_day(current_date):
                trading_dates.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_dates
