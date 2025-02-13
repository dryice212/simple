import os
import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine, Column, Integer, Date, REAL
from sqlalchemy.orm import declarative_base, sessionmaker
import matplotlib.pyplot as plt
import mplcursors
from datetime import datetime

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

# 매수·매도 신호 추가 함수
def add_signals_to_db(cci_period=7, buy_threshold=100, sell_threshold=-100):
    df = pd.read_sql('index_data', engine)

    # 매수·매도 신호 추가
    df['buy_signal'] = 0
    df['sell_signal'] = 0

    # CCI 계산
    df['TP'] = (df['high'] + df['low'] + df['close']) / 3
    df['CCI'] = (df['TP'] - df['TP'].rolling(cci_period).mean()) / (0.015 * df['TP'].rolling(cci_period).std())

    # 현재 포지션 (0: 없음, 1: 매수, -1: 매도)
    current_position = 0

    for i in range(cci_period, len(df)):
        cci = df.loc[i, 'CCI']
        
        if current_position == 1:
            if cci < sell_threshold:
                df.at[i, 'sell_signal'] = 1
                current_position = -1
            elif cci > buy_threshold:
                df.at[i, 'sell_signal'] = 1
                current_position = -1
        elif current_position == -1:
            if cci < sell_threshold:
                df.at[i, 'buy_signal'] = 1
                current_position = 1
            elif cci > buy_threshold:
                df.at[i, 'buy_signal'] = 1
                current_position = 1
        else:
            if cci > buy_threshold:
                df.at[i, 'buy_signal'] = 1
                current_position = 1
            elif cci < sell_threshold:
                df.at[i, 'sell_signal'] = 1
                current_position = -1

    df.to_sql('index_data', engine, if_exists='replace', index=False)
    print("매수·매도 신호가 데이터베이스에 추가되었습니다.")

# 수익률 테이블 생성 및 그래프 표시 함수
def generate_profit_and_plot():
    # 지수 데이터 불러오기
    df = pd.read_sql('SELECT date, close, buy_signal, sell_signal FROM index_data', engine)
    df['date'] = pd.to_datetime(df['date'])

    # 수익률 테이블 생성
    trade_history = []
    position = None  # 현재 포지션 (None, 'buy', 'sell')

    for i, row in df.iterrows():
        if row['buy_signal'] == 1:
            position = 'buy'
            if trade_history and trade_history[-1]['sell_price'] is not None and trade_history[-1]['buy_price'] is None:
                trade_history[-1]['buy_price'] = row['close']  # 이전 매도 거래의 매수 가격 채우기
                trade_history[-1]['profit'] = trade_history[-1]['sell_price'] - trade_history[-1]['buy_price']
                trade_history[-1]['cumulative_profit'] = sum([t['profit'] for t in trade_history if t['profit'] is not None])
                trade_history.append({'date': row['date'], 'buy_price':  row['close'], 'sell_price':None, 'profit': None, 'cumulative_profit': None})
            else:
                trade_history.append({'date': row['date'], 'buy_price': row['close'], 'sell_price': None, 'profit': None, 'cumulative_profit': None})
        elif row['sell_signal'] == 1:
            position = 'sell'
            if trade_history and trade_history[-1]['buy_price'] is not None and trade_history[-1]['sell_price'] is None:
                trade_history[-1]['sell_price'] = row['close']  # 이전 매수 거래의 매도 가격 채우기
                trade_history[-1]['profit'] = trade_history[-1]['sell_price'] - trade_history[-1]['buy_price']
                trade_history[-1]['cumulative_profit'] = sum([t['profit'] for t in trade_history if t['profit'] is not None])
                trade_history.append({'date': row['date'], 'buy_price': None, 'sell_price': row['close'], 'profit': None, 'cumulative_profit': None})
            else:
                trade_history.append({'date': row['date'], 'buy_price': row['close'], 'sell_price': None, 'profit': None, 'cumulative_profit': None})

    # 데이터프레임 변환
    trade_df = pd.DataFrame(trade_history)
    trade_df.to_sql('returns_data', engine, if_exists='replace', index=False)

    # 현재 날짜의 buy_signal과 sell_signal을 확인
    today = datetime.today().strftime('%Y-%m-%d')
    today_signals = df[df['date'] == today]
    buy_signal_today = today_signals['buy_signal'].sum()
    sell_signal_today = today_signals['sell_signal'].sum()

    # 1. Figure와 Subplots 생성
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)  # x축 공유

    # 2. KOSPI 200 그래프 (ax1에 그림)
    ax1.plot(df['date'], df['close'], label='KOSPI 200', color='blue')
    ax1.scatter(df['date'][df['buy_signal'] == 1], df['close'][df['buy_signal'] == 1], marker='^', color='green', label='Buy Signal', alpha=1)
    ax1.scatter(df['date'][df['sell_signal'] == 1], df['close'][df['sell_signal'] == 1], marker='v', color='red', label='Sell Signal', alpha=1)
    ax1.set_title(f"KOSPI 200 with Buy/Sell Signals\nBuy Signal Today: {buy_signal_today}, Sell Signal Today: {sell_signal_today}")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Index Price")
    ax1.legend()
    ax1.grid(True)  # Grid 추가

    # 3. 누적 수익 그래프 (ax2에 그림)
    ax2.plot(trade_df['date'], trade_df['cumulative_profit'], label='Cumulative Profit', color='purple')
    ax2.set_title("Cumulative Profit Over Time")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Cumulative Profit")
    ax2.legend()
    ax2.grid(True)  # Grid 추가

    # 4. 레이아웃 조정 (필수!)
    plt.tight_layout() # 서브플롯 간 간격 자동 조정

    # 5. x축 공유 설정 (매우 중요!)
    plt.setp(ax1.get_xticklabels(), visible=False) # ax1의 x축 레이블 숨김

    # 6. mplcursors 활성화 (필요한 경우)
    mplcursors.cursor(ax1, hover=True)
    mplcursors.cursor(ax2, hover=True)

    # 7. 그래프 표시
    plt.show()

if __name__ == '__main__':
    start_date = '2010-01-02'
    end_date = datetime.today().strftime('%Y-%m-%d')
    create_database()
    fetch_and_store_kospi200(start_date, end_date)
    add_signals_to_db(cci_period=7, buy_threshold=100, sell_threshold=-100)
    generate_profit_and_plot()
