import os
import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine, Column, Integer, Date, REAL
from sqlalchemy.orm import declarative_base, sessionmaker

# 데이터베이스 파일 경로 설정
DB_FOLDER = "data"
DB_NAME = "kospi200.db"
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# 데이터베이스 폴더 생성 (없을 경우)
if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

# 데이터베이스 연결
engine = create_engine(f'sqlite:///{DB_PATH}')
Session = sessionmaker(bind=engine)
session = Session()

# ORM 베이스 정의
Base = declarative_base()

# 지수 데이터 테이블 모델 정의 (매수·매도 신호 및 보류 상태 추가)
class IndexData(Base):
    __tablename__ = 'index_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, index=True)
    open = Column(REAL)
    high = Column(REAL)
    low = Column(REAL)
    close = Column(REAL)
    volume = Column(Integer)
    change = Column(REAL)
    buy_signal = Column(Integer, default=0)  # 매수 신호 (1: 매수, 0: 없음)
    sell_signal = Column(Integer, default=0)  # 매도 신호 (1: 매도, 0: 없음)
    pending_buy = Column(Integer, default=0)  # 매수 보류 상태 (1: 보류, 0: 없음)
    pending_sell = Column(Integer, default=0)  # 매도 보류 상태 (1: 보류, 0: 없음)

# 데이터베이스 생성 함수
def create_database():
    """
    데이터베이스와 테이블을 생성합니다.
    """
    Base.metadata.create_all(engine)
    print(f"'{DB_PATH}'에 데이터베이스와 테이블이 생성되었습니다.")

# KOSPI 200 데이터 가져오기 및 저장
def fetch_and_store_kospi200(start_date, end_date):
    df = fdr.DataReader('KS200', start_date, end_date)
    df.reset_index(inplace=True)
    df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume', 'Change': 'change'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])

    # 매수·매도 신호 추가
    df['buy_signal'] = 0
    df['sell_signal'] = 0
    df['pending_buy'] = 0
    df['pending_sell'] = 0

    df.to_sql('index_data', engine, if_exists='replace', index=False)
    print("KOSPI 200 데이터가 'index_data' 테이블에 저장되었습니다.")

if __name__ == '__main__':
    start_date = '2010-01-02'
    end_date = '2025-02-07'
    create_database()
    fetch_and_store_kospi200(start_date, end_date)
