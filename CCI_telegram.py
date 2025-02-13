# -*- coding: utf-8 -*-

import os
import requests  # 텔레그램 API 호출을 위해 추가
import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine, Column, Integer, Date, REAL
from sqlalchemy.orm import declarative_base, sessionmaker
#import mplcursors
from datetime import datetime

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = "7471637657:AAEXcgfXTwnprqk2F6Ux5x_tlZ09armk9tY"  # 본인의 BotFather 토큰 입력
TELEGRAM_CHAT_ID = "5349738900"  # 본인의 chat_id 입력

# 데이터베이스 파일 경로 설정
DB_FOLDER = "data"
DB_NAME = "kospi200.db"
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

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


# 📌 텔레그램 메시지 전송 함수
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print("✅ 텔레그램 메시지 전송 성공!")
    else:
        print(f"❌ 메시지 전송 실패: {response.text}")

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

    # 현재 시간 추가
    now = datetime.now().strftime('%H:%M:%S')  # 시간, 분, 초
    message = f"📢 <b>CCI Signal Alert</b>\n🗓 날짜: {today} {now}\n" # 날짜와 시간 함께 표시
    message += f"✅ 매수 신호 발생 ({buy_signal_today}건)\n"
    message += f"❌ 매도 신호 발생 ({sell_signal_today}건)\n"
    send_telegram_message(message)

if __name__ == '__main__':
    cci_period = 9
    buy_threshold = 130
    sell_threshold = -145
    start_date = '2010-01-02'
    end_date = datetime.today().strftime('%Y-%m-%d')
    create_database()
    fetch_and_store_kospi200(start_date, end_date)
    add_signals_to_db(cci_period=cci_period, buy_threshold=buy_threshold, sell_threshold=sell_threshold)
    generate_profit_and_plot()

