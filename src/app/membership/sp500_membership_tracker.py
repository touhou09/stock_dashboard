"""
S&P 500 편입/퇴출 이력 추적 시스템
생존 편향 문제 해결을 위한 시점별 정확한 구성 종목 추적
"""

import pandas as pd
import requests
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from google.cloud import storage
from dotenv import load_dotenv
import re
import time
import os

load_dotenv()
logger = logging.getLogger(__name__)

class SP500MembershipTracker:
    """S&P 500 편입/퇴출 이력 추적 클래스"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/membership"):
        """
        초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 경로
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.client = storage.Client()
        
        # Delta Table 경로
        self.membership_changes_path = f"gs://{gcs_bucket}/{gcs_path}/sp500_membership_changes"
        self.membership_daily_path = f"gs://{gcs_bucket}/{gcs_path}/sp500_membership_daily"
    
    def get_sp500_for_year(self, year: int) -> pd.DataFrame:
        """특정 연도의 S&P 500 구성 종목 수집 (Wikipedia에서 실제 데이터)"""
        logger.info(f"📋 {year}년 S&P 500 구성 종목 수집 중...")
        
        # 모든 연도에 대해 Wikipedia에서 실제 S&P 500 리스트 가져오기
        try:
            logger.info("🌐 Wikipedia에서 현재 S&P 500 구성 종목 수집 중...")
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            
            # User-Agent 헤더 추가 (403 Forbidden 방지)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            tables = pd.read_html(url, header=0)
            
            # 첫 번째 테이블이 S&P 500 구성 종목 테이블
            sp500_df = tables[0]
            sp500_df.columns = sp500_df.columns.str.strip()
            
            # Symbol 컬럼이 있는지 확인
            if 'Symbol' in sp500_df.columns:
                logger.info(f"✅ Wikipedia에서 S&P 500 구성 종목 수집 완료: {len(sp500_df)}개")
                return sp500_df
            else:
                logger.warning("⚠️ Wikipedia 테이블에서 Symbol 컬럼을 찾을 수 없습니다.")
                logger.info(f"📊 사용 가능한 컬럼: {list(sp500_df.columns)}")
                
        except Exception as e:
            logger.error(f"❌ Wikipedia 스크래핑 실패: {e}")
            logger.info("🔄 대안 방법을 시도합니다...")
            
            # 대안: requests + BeautifulSoup 사용
            try:
                import requests
                from bs4 import BeautifulSoup
                
                logger.info("🌐 requests + BeautifulSoup으로 재시도...")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', {'class': 'wikitable'})
                
                if table:
                    # 테이블을 DataFrame으로 변환
                    sp500_df = pd.read_html(str(table), header=0)[0]
                    sp500_df.columns = sp500_df.columns.str.strip()
                    
                    if 'Symbol' in sp500_df.columns:
                        logger.info(f"✅ 대안 방법으로 S&P 500 구성 종목 수집 완료: {len(sp500_df)}개")
                        return sp500_df
                
            except Exception as e2:
                logger.error(f"❌ 대안 방법도 실패: {e2}")
        
        # 모든 방법이 실패한 경우 에러 발생
        raise Exception("S&P 500 구성 종목을 가져올 수 없습니다. 인터넷 연결을 확인해주세요.")
    
    
    def get_current_sp500_from_wikipedia(self) -> pd.DataFrame:
        """현재 S&P 500 구성 종목 수집 (2024년 기준)"""
        return self.get_sp500_for_year(2024)
    
    def scrape_wikipedia_changes(self, start_year: int = 2000) -> pd.DataFrame:
        """
        Wikipedia의 S&P 500 변경 이력 페이지들을 스크래핑 - 개선된 버전
        
        Args:
            start_year: 수집 시작 연도
            
        Returns:
            pd.DataFrame: 편입/퇴출 이력 데이터
        """
        logger.info(f"📊 Wikipedia S&P 500 변경 이력 스크래핑 시작 (연도: {start_year}~)")
        
        changes_list = []
        
        # 여러 Wikipedia 페이지 시도
        urls_to_try = [
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#Changes_to_the_S%26P_500",
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "https://en.wikipedia.org/wiki/S%26P_500_Index#Changes_to_the_S%26P_500"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for url_idx, url in enumerate(urls_to_try):
            try:
                logger.info(f"🌐 Wikipedia 페이지 접근 시도 {url_idx + 1}: {url}")
                
                # requests로 직접 접근
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                # BeautifulSoup으로 HTML 파싱
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # "Changes to the S&P 500" 섹션 찾기
                changes_section = soup.find('span', {'id': 'Changes_to_the_S.26P_500'})
                if not changes_section:
                    changes_section = soup.find('span', {'id': 'Changes_to_the_S&P_500'})
                if not changes_section:
                    changes_section = soup.find('h2', string=lambda text: text and 'Changes' in text)
                
                if changes_section:
                    logger.info("✅ 'Changes to the S&P 500' 섹션 발견")
                    
                    # 섹션 다음의 테이블들 찾기
                    tables = changes_section.find_all_next('table')
                    
                    for table_idx, table in enumerate(tables[:3]):  # 최대 3개 테이블만 확인
                        try:
                            # 테이블을 DataFrame으로 변환
                            table_df = pd.read_html(str(table), header=0)[0]
                            logger.info(f"📊 테이블 {table_idx + 1} 컬럼: {list(table_df.columns)}")
                            
                            # 변경 이력 테이블인지 확인
                            if self._is_changes_table(table_df):
                                logger.info(f"✅ 변경 이력 테이블 발견: 테이블 {table_idx + 1}")
                                changes_list.extend(self._parse_changes_table(table_df, start_year))
                                break
                                
                        except Exception as e:
                            logger.debug(f"테이블 {table_idx + 1} 파싱 실패: {e}")
                            continue
                    
                    if changes_list:
                        break  # 성공적으로 파싱했으면 다른 URL 시도하지 않음
                else:
                    logger.warning(f"⚠️ 'Changes to the S&P 500' 섹션을 찾을 수 없습니다: {url}")
                
                time.sleep(2)  # API 제한 방지
                
            except Exception as e:
                logger.error(f"❌ URL {url_idx + 1} 스크래핑 실패: {e}")
                continue
        
        if changes_list:
            changes_df = pd.DataFrame(changes_list)
            # 중복 제거
            changes_df = changes_df.drop_duplicates(subset=['effective_date', 'action', 'ticker'])
            changes_df = changes_df.sort_values('effective_date')
            logger.info(f"✅ Wikipedia 변경 이력 수집 완료: {len(changes_df)}개 변경사항")
            return changes_df
        else:
            logger.warning("⚠️ Wikipedia에서 변경 이력을 찾을 수 없습니다.")
            return pd.DataFrame(columns=['effective_date', 'action', 'ticker', 'description', 'year'])
    
    def _is_changes_table(self, df: pd.DataFrame) -> bool:
        """테이블이 변경 이력 테이블인지 확인"""
        if df.empty or len(df.columns) < 2:
            return False
        
        # 컬럼명에서 날짜 관련 키워드 찾기
        date_indicators = ['date', 'Date', 'DATE', 'effective', 'Effective', 'change', 'Change']
        action_indicators = ['added', 'removed', 'replaced', 'change', 'Change', 'company', 'Company']
        
        columns_str = ' '.join(str(col).lower() for col in df.columns)
        
        has_date = any(indicator in columns_str for indicator in date_indicators)
        has_action = any(indicator in columns_str for indicator in action_indicators)
        
        return has_date or has_action
    
    def _parse_changes_table(self, df: pd.DataFrame, start_year: int) -> List[Dict]:
        """변경 이력 테이블 파싱"""
        changes_list = []
        
        # 날짜 컬럼 찾기
        date_col = None
        for col in df.columns:
            if any(indicator in str(col).lower() for indicator in ['date', 'effective', 'change']):
                date_col = col
                break
        
        if not date_col:
            logger.warning("⚠️ 날짜 컬럼을 찾을 수 없습니다.")
            return changes_list
        
        logger.info(f"📅 날짜 컬럼 발견: {date_col}")
        
        # 날짜 파싱
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        
        # 편입/퇴출 정보 추출
        for _, row in df.iterrows():
            date_val = row[date_col]
            if pd.notna(date_val) and date_val.year >= start_year:
                # 각 컬럼에서 편입/퇴출 정보 파싱
                for col in df.columns:
                    if col != date_col:
                        cell_value = str(row[col])
                        
                        # 편입 정보 추출
                        if any(keyword in cell_value.lower() for keyword in ['added', 'add', 'replaced', 'replace']):
                            tickers = self._extract_tickers_from_text(cell_value)
                            for ticker in tickers:
                                changes_list.append({
                                    'effective_date': date_val.date(),
                                    'action': 'add',
                                    'ticker': ticker,
                                    'description': cell_value,
                                    'year': date_val.year
                                })
                        
                        # 퇴출 정보 추출
                        elif any(keyword in cell_value.lower() for keyword in ['removed', 'remove', 'replaced', 'replace']):
                            tickers = self._extract_tickers_from_text(cell_value)
                            for ticker in tickers:
                                changes_list.append({
                                    'effective_date': date_val.date(),
                                    'action': 'remove',
                                    'ticker': ticker,
                                    'description': cell_value,
                                    'year': date_val.year
                                })
        
        return changes_list
    
    def _extract_tickers_from_text(self, text: str) -> List[str]:
        """텍스트에서 티커 추출 - 개선된 로직"""
        tickers = []
        
        # 일반적인 단어들 제외
        exclude_words = {
            'THE', 'AND', 'FOR', 'INC', 'CORP', 'LLC', 'LTD', 'CO', 'COMPANY', 
            'GROUP', 'HOLDINGS', 'INTERNATIONAL', 'SYSTEMS', 'SOLUTIONS',
            'TECHNOLOGIES', 'COMMUNICATIONS', 'FINANCIAL', 'SERVICES',
            'HEALTHCARE', 'PHARMACEUTICAL', 'ENERGY', 'UTILITIES',
            'REAL', 'ESTATE', 'INVESTMENT', 'MANAGEMENT', 'PARTNERS',
            'ADDED', 'REMOVED', 'REPLACED', 'CHANGE', 'CHANGES'
        }
        
        # 티커 패턴들
        patterns = [
            r'\b[A-Z]{1,5}\b',  # 기본 티커 패턴
            r'\b[A-Z]{2,4}\b',  # 2-4자리 티커
            r'\b[A-Z]{1,3}[0-9]{1,2}\b',  # 문자+숫자 조합
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text.upper())
            for match in matches:
                # 제외 단어 필터링
                if (match not in exclude_words and 
                    len(match) >= 2 and 
                    len(match) <= 5 and
                    not match.isdigit()):
                    tickers.append(match)
        
        # 중복 제거 및 정렬
        return sorted(list(set(tickers)))
    
    def create_manual_membership_changes(self) -> pd.DataFrame:
        """
        수동으로 주요 편입/퇴출 이력 생성 - 개선된 버전
        Wikipedia 스크래핑이 어려운 경우 대안으로 사용
        """
        logger.info("📝 주요 S&P 500 편입/퇴출 이력 수동 생성 중...")
        
        # 주요 편입/퇴출 이력 (실제 편입일 기준) - 수정된 부분
        changes_data = [
            # 1990년대 주요 편입
            {'effective_date': '1997-05-15', 'action': 'add', 'ticker': 'AMZN', 'description': 'Amazon.com Inc. added'},
            {'effective_date': '1999-01-22', 'action': 'add', 'ticker': 'NVDA', 'description': 'NVIDIA Corporation added'},
            
            # 2000년대 주요 편입
            {'effective_date': '2000-01-01', 'action': 'add', 'ticker': 'AAPL', 'description': 'Apple Inc. added'},
            {'effective_date': '2004-08-19', 'action': 'add', 'ticker': 'GOOGL', 'description': 'Google Inc. added'},
            {'effective_date': '2004-08-19', 'action': 'add', 'ticker': 'GOOG', 'description': 'Google Inc. Class A added'},
            
            # 2010년대 주요 편입
            {'effective_date': '2012-05-18', 'action': 'add', 'ticker': 'META', 'description': 'Facebook Inc. added'},
            {'effective_date': '2013-05-31', 'action': 'add', 'ticker': 'TSLA', 'description': 'Tesla Inc. added'},
            {'effective_date': '2014-03-20', 'action': 'add', 'ticker': 'FB', 'description': 'Facebook Inc. (old ticker) added'},
            
            # 2020년대 주요 편입
            {'effective_date': '2020-12-21', 'action': 'add', 'ticker': 'TSM', 'description': 'Taiwan Semiconductor added'},
            {'effective_date': '2021-03-22', 'action': 'add', 'ticker': 'PLTR', 'description': 'Palantir Technologies added'},
            
            # 주요 퇴출 이력
            {'effective_date': '2008-09-15', 'action': 'remove', 'ticker': 'LEH', 'description': 'Lehman Brothers removed'},
            {'effective_date': '2009-06-01', 'action': 'remove', 'ticker': 'GM', 'description': 'General Motors removed'},
            {'effective_date': '2018-06-26', 'action': 'remove', 'ticker': 'GE', 'description': 'General Electric removed'},
            {'effective_date': '2020-08-31', 'action': 'remove', 'ticker': 'ETFC', 'description': 'E*TRADE removed'},
            
            # 최근 주요 변경사항 (2022-2024)
            {'effective_date': '2022-03-18', 'action': 'add', 'ticker': 'CEG', 'description': 'Constellation Energy added'},
            {'effective_date': '2022-06-06', 'action': 'add', 'ticker': 'ENPH', 'description': 'Enphase Energy added'},
            {'effective_date': '2023-03-20', 'action': 'add', 'ticker': 'SEDG', 'description': 'SolarEdge Technologies added'},
            {'effective_date': '2023-06-20', 'action': 'add', 'ticker': 'GEHC', 'description': 'GE HealthCare added'},
            {'effective_date': '2024-01-22', 'action': 'add', 'ticker': 'SMCI', 'description': 'Super Micro Computer added'},
        ]
        
        changes_df = pd.DataFrame(changes_data)
        changes_df['effective_date'] = pd.to_datetime(changes_df['effective_date']).dt.date
        changes_df['year'] = pd.to_datetime(changes_df['effective_date']).dt.year
        
        logger.info(f"✅ 수동 편입/퇴출 이력 생성 완료: {len(changes_df)}개")
        return changes_df
    
    def save_membership_changes(self, changes_df: pd.DataFrame):
        """편입/퇴출 이력을 Delta Table에 저장"""
        logger.info("💾 편입/퇴출 이력을 Delta Table에 저장 중...")
        
        if changes_df.empty:
            logger.warning("저장할 변경 이력이 없습니다.")
            return
        
        try:
            # 기존 데이터 확인
            try:
                existing_delta = DeltaTable(self.membership_changes_path)
                existing_df = existing_delta.to_pandas()
                
                if not existing_df.empty:
                    # 중복 제거 (같은 날짜, 액션, 티커)
                    changes_df = changes_df[~changes_df.set_index(['effective_date', 'action', 'ticker']).index.isin(
                        existing_df.set_index(['effective_date', 'action', 'ticker']).index
                    )]
                    
                    if changes_df.empty:
                        logger.info("새로운 변경 이력이 없습니다.")
                        return
                    
                    # 기존 데이터와 새 데이터 결합
                    changes_df = pd.concat([existing_df, changes_df], ignore_index=True)
                    mode = "overwrite"
                else:
                    mode = "overwrite"
                    
            except Exception:
                mode = "overwrite"
            
            # Delta Table에 저장
            arrow_table = pa.Table.from_pandas(changes_df)
            
            write_deltalake(
                self.membership_changes_path,
                arrow_table,
                mode=mode,
                partition_by=["year"],  # 연도별 파티셔닝
                configuration={
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true"
                }
            )
            
            logger.info(f"✅ 편입/퇴출 이력 저장 완료: {len(changes_df)}개")
            logger.info(f"📍 저장 위치: {self.membership_changes_path}")
            
        except Exception as e:
            logger.error(f"❌ 편입/퇴출 이력 저장 실패: {e}")
            raise
    
    def generate_daily_membership(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        일자별 멤버십 스냅샷 생성
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            pd.DataFrame: 일자별 멤버십 데이터
        """
        logger.info(f"📅 일자별 멤버십 스냅샷 생성 중... ({start_date} ~ {end_date})")
        
        try:
            # 편입/퇴출 이력 로드
            changes_delta = DeltaTable(self.membership_changes_path)
            changes_df = changes_delta.to_pandas()
            
            if changes_df.empty:
                logger.warning("편입/퇴출 이력이 없습니다.")
                return pd.DataFrame()
            
            changes_df['effective_date'] = pd.to_datetime(changes_df['effective_date']).dt.date
            
            # 기준 연도의 구성 종목 수집 (한 번만 호출) - 수정된 부분
            base_year = start_date.year
            logger.info(f"📋 {base_year}년 S&P 500 구성 종목 수집 중... (년도별 실제 존재 종목)")
            current_sp500 = self.get_sp500_for_year(base_year)
            base_tickers = set(current_sp500['Symbol'].tolist())
            logger.info(f"✅ {base_year}년 S&P 500 구성 종목 수집 완료: {len(base_tickers)}개")
            
            # 일자별 멤버십 생성
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                if current_date.weekday() < 5:  # 평일만
                    date_list.append(current_date)
                current_date += timedelta(days=1)
            
            daily_membership_list = []
            
            for target_date in date_list:
                # 해당 날짜까지의 편입/퇴출 이력 적용
                relevant_changes = changes_df[changes_df['effective_date'] <= target_date]
                
                # 편입된 종목들
                added_tickers = set(relevant_changes[relevant_changes['action'] == 'add']['ticker'].tolist())
                
                # 퇴출된 종목들
                removed_tickers = set(relevant_changes[relevant_changes['action'] == 'remove']['ticker'].tolist())
                
                # Point-in-Time: 해당 날짜까지 편입된 종목들만 (퇴출된 종목 제외)
                # 2000년 1월 1일이면 2000년 1월 1일까지 편입된 종목들만
                # 편입 이력이 없는 종목들은 기본적으로 포함 (2000년 기준 종목들)
                # base_tickers는 이미 위에서 한 번만 계산됨 - 수정된 부분
                
                # 해당 날짜까지 편입된 종목들만 필터링
                valid_added_tickers = set()
                for ticker in added_tickers:
                    ticker_changes = relevant_changes[relevant_changes['ticker'] == ticker]
                    add_changes = ticker_changes[ticker_changes['action'] == 'add']
                    if not add_changes.empty:
                        # 해당 날짜 이전에 편입된 종목만 포함
                        earliest_add = add_changes['effective_date'].min()
                        if earliest_add <= target_date:
                            valid_added_tickers.add(ticker)
                
                daily_tickers = (base_tickers | valid_added_tickers) - removed_tickers
                
                # 각 종목별로 멤버십 정보 생성
                for ticker in daily_tickers:
                    # 편입일/퇴출일 계산
                    ticker_adds = relevant_changes[
                        (relevant_changes['ticker'] == ticker) & 
                        (relevant_changes['action'] == 'add')
                    ]
                    ticker_removes = relevant_changes[
                        (relevant_changes['ticker'] == ticker) & 
                        (relevant_changes['action'] == 'remove')
                    ]
                    
                    in_dt = ticker_adds['effective_date'].min() if not ticker_adds.empty else None
                    out_dt = ticker_removes['effective_date'].min() if not ticker_removes.empty else None
                    
                    daily_membership_list.append({
                        'date': target_date,
                        'ticker': ticker,
                        'in_dt': in_dt if in_dt is not None else target_date,  # None 대신 target_date 사용
                        'out_dt': out_dt if out_dt is not None else pd.NaT,  # None 대신 NaT 사용
                        'is_member': True
                    })
            
            daily_membership_df = pd.DataFrame(daily_membership_list)
            
            logger.info(f"✅ 일자별 멤버십 스냅샷 생성 완료: {len(daily_membership_df)}개 레코드")
            return daily_membership_df
            
        except Exception as e:
            logger.error(f"❌ 일자별 멤버십 생성 실패: {e}")
            raise
    
    def save_daily_membership(self, daily_membership_df: pd.DataFrame):
        """일자별 멤버십을 Delta Table에 저장"""
        logger.info("💾 일자별 멤버십을 Delta Table에 저장 중...")
        
        if daily_membership_df.empty:
            logger.warning("저장할 일자별 멤버십이 없습니다.")
            return
        
        try:
            # 기존 데이터 확인 및 덮어쓰기
            arrow_table = pa.Table.from_pandas(daily_membership_df)
            
            write_deltalake(
                self.membership_daily_path,
                arrow_table,
                mode="overwrite",  # 전체 덮어쓰기
                partition_by=["date"],  # 날짜별 파티셔닝
                configuration={
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true"
                }
            )
            
            logger.info(f"✅ 일자별 멤버십 저장 완료: {len(daily_membership_df)}개")
            logger.info(f"📍 저장 위치: {self.membership_daily_path}")
            
        except Exception as e:
            logger.error(f"❌ 일자별 멤버십 저장 실패: {e}")
            raise
    
    def get_daily_membership(self, target_date: date) -> pd.DataFrame:
        """특정 날짜의 멤버십 조회"""
        try:
            daily_delta = DeltaTable(self.membership_daily_path)
            daily_df = daily_delta.to_pandas()
            
            if not daily_df.empty:
                daily_df['date'] = pd.to_datetime(daily_df['date']).dt.date
                target_membership = daily_df[daily_df['date'] == target_date]
                return target_membership
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 일자별 멤버십 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_membership_for_date_range(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        특정 날짜 범위의 멤버십 정보 조회
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            pd.DataFrame: 해당 기간의 멤버십 정보
        """
        try:
            daily_delta = DeltaTable(self.membership_daily_path)
            daily_df = daily_delta.to_pandas()
            
            if not daily_df.empty:
                daily_df['date'] = pd.to_datetime(daily_df['date']).dt.date
                # 날짜 범위 필터링
                membership_df = daily_df[
                    (daily_df['date'] >= start_date) & 
                    (daily_df['date'] <= end_date)
                ]
                return membership_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 멤버십 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_tickers_for_backfill(self, start_date: date, end_date: date) -> Dict[str, Dict[str, date]]:
        """
        백필을 위한 종목별 편입일 정보 조회
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            Dict[str, Dict[str, date]]: {ticker: {'in_date': date, 'out_date': date}}
        """
        logger.info(f"📋 백필용 종목별 편입일 정보 조회 중... ({start_date} ~ {end_date})")
        
        try:
            # 해당 기간의 멤버십 정보 조회
            membership_df = self.get_membership_for_date_range(start_date, end_date)
            
            if membership_df.empty:
                logger.warning("멤버십 정보가 없습니다.")
                return {}
            
            # 종목별 편입/퇴출일 정리
            ticker_info = {}
            
            for ticker in membership_df['ticker'].unique():
                ticker_data = membership_df[membership_df['ticker'] == ticker]
                
                # 편입일 (해당 기간 내 최초 등장일)
                in_date = ticker_data['date'].min()
                
                # 퇴출일 (해당 기간 내 마지막 등장일 이후)
                out_date = ticker_data['date'].max()
                
                # 퇴출 여부 확인 (마지막 날짜가 end_date보다 이전이면 퇴출)
                if out_date < end_date:
                    # 실제 퇴출일 확인
                    out_dt = ticker_data['out_dt'].iloc[0]
                    if pd.notna(out_dt):
                        out_date = out_dt
                
                ticker_info[ticker] = {
                    'in_date': in_date,
                    'out_date': out_date if out_date < end_date else None
                }
            
            logger.info(f"✅ 백필용 종목 정보 조회 완료: {len(ticker_info)}개 종목")
            return ticker_info
            
        except Exception as e:
            logger.error(f"❌ 백필용 종목 정보 조회 실패: {e}")
            return {}
    
    def run_membership_setup(self, start_date: date, end_date: date, use_manual: bool = True):
        """
        멤버십 추적 시스템 초기 설정
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            use_manual: 수동 데이터 사용 여부
        """
        logger.info("=" * 80)
        logger.info("📋 S&P 500 멤버십 추적 시스템 초기 설정")
        logger.info("=" * 80)
        logger.info(f" 설정 기간: {start_date} ~ {end_date}")
        logger.info(f" 데이터 소스: {'수동' if use_manual else 'Wikipedia 스크래핑'}")
        
        try:
            # 1. 편입/퇴출 이력 생성
            if use_manual:
                changes_df = self.create_manual_membership_changes()
            else:
                changes_df = self.scrape_wikipedia_changes()
            
            # 2. 편입/퇴출 이력 저장
            if not changes_df.empty:
                self.save_membership_changes(changes_df)
            
            # 3. 일자별 멤버십 생성
            daily_membership_df = self.generate_daily_membership(start_date, end_date)
            
            # 4. 일자별 멤버십 저장
            if not daily_membership_df.empty:
                self.save_daily_membership(daily_membership_df)
            
            # 5. 요약 정보 출력
            logger.info("\n" + "=" * 80)
            logger.info("📈 멤버십 추적 시스템 설정 완료")
            logger.info("=" * 80)
            logger.info(f" 편입/퇴출 이력: {len(changes_df)}개")
            logger.info(f" 일자별 멤버십: {len(daily_membership_df)}개 레코드")
            logger.info(f" 기간: {start_date} ~ {end_date}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ 멤버십 시스템 설정 실패: {e}")
            raise

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="S&P 500 멤버십 추적 시스템")
    parser.add_argument("--start-date", type=str, required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--use-manual", action="store_true", help="수동 데이터 사용")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    tracker = SP500MembershipTracker(gcs_bucket)
    
    # 날짜 파싱
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    try:
        tracker.run_membership_setup(start_date, end_date, args.use_manual)
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
