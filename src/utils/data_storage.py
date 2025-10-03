"""
Bronze Layer 데이터 저장 모듈
- Delta Lake 테이블 저장
- GCS 연동
"""

import pandas as pd
from datetime import datetime, date
from typing import List
import logging
from deltalake import DeltaTable, write_deltalake, WriterProperties
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

class DeltaStorageManager:
    """Delta Lake 저장 관리자"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        """
        저장 관리자 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 내 경로
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(gcs_bucket)
        
        # Delta Table 경로 설정
        self.price_table_path = f"gs://{gcs_bucket}/{gcs_path}/bronze_price_daily"
        self.dividend_events_table_path = f"gs://{gcs_bucket}/{gcs_path}/bronze_dividend_events"
    
    def check_existing_data(self, table_path: str, target_date: datetime.date) -> bool:
        """특정 날짜의 데이터가 이미 존재하는지 확인"""
        try:
            delta_table = DeltaTable(table_path)
            existing_df = delta_table.to_pandas()
            
            # 날짜 컬럼이 있는지 확인
            if 'date' in existing_df.columns:
                # 날짜 형식 통일
                existing_df['date'] = pd.to_datetime(existing_df['date']).dt.date
                target_date_str = target_date.strftime('%Y-%m-%d')
                
                # 해당 날짜의 데이터가 있는지 확인
                has_data = (existing_df['date'] == target_date).any()
                
                if has_data:
                    count = (existing_df['date'] == target_date).sum()
                    logger.info(f"📅 {target_date_str} 날짜의 데이터가 이미 존재합니다: {count}행")
                    return True
                else:
                    logger.info(f"📅 {target_date_str} 날짜의 데이터가 없습니다.")
                    return False
            else:
                logger.info("📅 날짜 컬럼이 없습니다.")
                return False
                
        except Exception as e:
            logger.info(f"📅 기존 데이터 확인 실패 (테이블이 없을 수 있음): {e}")
            return False
    
    def save_price_data_to_delta(self, all_daily_data: List[pd.DataFrame], target_date: datetime.date):
        """가격 데이터를 Delta Table에 저장 (Bronze 스키마)"""
        logger.info(f"\n💾 가격 데이터를 Bronze Delta Table에 저장 중...")
        
        if not all_daily_data:
            logger.warning("저장할 가격 데이터가 없습니다.")
            return
        
        # 날짜 중복 확인
        if self.check_existing_data(self.price_table_path, target_date):
            logger.warning(f"⚠️ {target_date} 날짜의 가격 데이터가 이미 존재합니다. 건너뜁니다.")
            return
        
        # pandas DataFrame 결합
        combined_df = pd.concat(all_daily_data, ignore_index=True)
        
        # 필요한 컬럼만 선택 (Bronze 스키마)
        bronze_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'adj_close', 'ingest_at']
        combined_df = combined_df[bronze_columns]
        
        try:
            # Delta Table이 존재하는지 확인
            delta_table = DeltaTable(self.price_table_path)
            mode = "append"
            logger.info("✅ 기존 Bronze 가격 테이블에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("🆕 새로운 Bronze 가격 테이블 생성")
        
        # [수정] deltalake 1.0+ WriterProperties로 zstd 압축 설정
        arrow_table = pa.Table.from_pandas(combined_df)
        
        # zstd 압축 설정
        writer_props = WriterProperties(
            compression='ZSTD',
            compression_level=5
        )
        
        # Delta Table에 저장
        write_deltalake(
            self.price_table_path,
            arrow_table,
            mode=mode,
            partition_by=["date"],  # 날짜별 파티셔닝
            writer_properties=writer_props,  # [수정] zstd 압축 적용
            configuration={
                "delta.dataSkippingStatsColumns": "ticker,close",  # 통계 최적화
                "delta.autoOptimize.optimizeWrite": "true",        # 자동 최적화
                "delta.autoOptimize.autoCompact": "true"           # 자동 압축
            }
        )
        
        logger.info(f"✅ Bronze 가격 데이터 저장 완료: {len(combined_df)}행")
        logger.info(f"📍 저장 위치: {self.price_table_path}")
    
    def save_dividend_events_to_delta(self, dividend_events_df: pd.DataFrame):
        """배당 이벤트를 Delta Table에 저장 (Bronze 스키마)"""
        logger.info(f"\n💾 배당 이벤트를 Bronze Delta Table에 저장 중...")
        
        if dividend_events_df.empty:
            logger.warning("저장할 배당 이벤트가 없습니다.")
            return
        
        
        try:
            # Delta Table이 존재하는지 확인
            delta_table = DeltaTable(self.dividend_events_table_path)
            mode = "append"
            logger.info("✅ 기존 Bronze 배당 이벤트 테이블에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("🆕 새로운 Bronze 배당 이벤트 테이블 생성")
        
        # [수정] deltalake 1.0+ WriterProperties로 zstd 압축 설정
        arrow_table = pa.Table.from_pandas(dividend_events_df)
        
        # zstd 압축 설정
        writer_props = WriterProperties(
            compression='ZSTD',
            compression_level=5
        )
        
        # Delta Table에 저장
        write_deltalake(
            self.dividend_events_table_path,
            arrow_table,
            mode=mode,
            partition_by=["date"],  # 날짜별 파티셔닝
            writer_properties=writer_props,  # [수정] zstd 압축 적용
            configuration={
                "delta.dataSkippingStatsColumns": "ticker,amount",  # 통계 최적화
                "delta.autoOptimize.optimizeWrite": "true",         # 자동 최적화
                "delta.autoOptimize.autoCompact": "true"            # 자동 압축
            }
        )
        
        logger.info(f"✅ Bronze 배당 이벤트 저장 완료: {len(dividend_events_df)}행")
        logger.info(f"📍 저장 위치: {self.dividend_events_table_path}")
    
    def save_data_to_parquet_zstd(self, df: pd.DataFrame, parquet_path: str, partition_cols: List[str] = None):
        """
        데이터를 zstd 압축된 Parquet 파일로 저장 (BigQuery 직접 연동용)
        
        Args:
            df: 저장할 DataFrame
            parquet_path: 저장 경로 (gs://bucket/path)
            partition_cols: 파티션 컬럼 리스트
        """
        logger.info(f"\n💾 데이터를 zstd 압축 Parquet으로 저장 중...")
        
        if df.empty:
            logger.warning("저장할 데이터가 없습니다.")
            return
        
        try:
            # pandas DataFrame을 PyArrow Table로 변환
            table = pa.Table.from_pandas(df)
            
            if partition_cols:
                # 파티션된 Parquet 저장 (zstd 압축)
                pq.write_to_dataset(
                    table,
                    root_path=parquet_path,
                    partition_cols=partition_cols,
                    compression="zstd",           # [수정] zstd 압축 적용
                    compression_level=5,          # [수정] 압축 수준 (1-22)
                    use_deprecated_int96_timestamps=False  # [수정] BigQuery 호환성
                )
                logger.info(f"✅ 파티션된 zstd Parquet 저장 완료: {len(df)}행")
            else:
                # 단일 Parquet 파일 저장 (zstd 압축)
                pq.write_table(
                    table,
                    where=parquet_path,
                    compression="zstd",           # [수정] zstd 압축 적용
                    compression_level=5,          # [수정] 압축 수준
                    use_deprecated_int96_timestamps=False  # [수정] BigQuery 호환성
                )
                logger.info(f"✅ zstd Parquet 저장 완료: {len(df)}행")
            
            logger.info(f"📍 저장 위치: {parquet_path}")
            
        except Exception as e:
            logger.error(f"❌ zstd Parquet 저장 실패: {e}")
            raise Exception(f"zstd Parquet 저장 실패: {e}") from e
