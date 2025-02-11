import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from kiwoom_api import get_option_price  # 가정: 키움 API에서 옵션 가격을 가져오는 함수

# 데이터베이스 연결
DB_PATH = "data/kospi200.db"
engine = create_engine(f'sqlite:///{DB_PATH}')

# 설정 변수
POINT_DIFF = 10  # 행사가격 조정 값
STRIKE_INTERVAL = 2.5  # 옵션 행사가격 간격

# returns_data 테이블 불러오기
df = pd.read_sql('SELECT * FROM returns_data', engine)

# 옵션 거래 기록 리스트
option_trades = []

for _, row in df.iterrows():
    if row['type'] == 'buy':
        option_type = 'call'
        strike_price = np.ceil((row['buy_price'] + POINT_DIFF) / STRIKE_INTERVAL) * STRIKE_INTERVAL
        entry_price = get_option_price(row['buy_date'], option_type, strike_price)  # 키움 API에서 매수가 조회
        exit_price = get_option_price(row['sell_date'], option_type, strike_price)  # 키움 API에서 청산가 조회
        profit = exit_price - entry_price if entry_price and exit_price else None
        option_trades.append({
            'trade_type': 'buy',
            'option_type': option_type,
            'trade_date': row['buy_date'],
            'settlement_date': row['sell_date'],
            'strike_price': strike_price,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'profit': profit
        })
    elif row['type'] == 'sell':
        option_type = 'put'
        strike_price = np.floor((row['sell_price'] - POINT_DIFF) / STRIKE_INTERVAL) * STRIKE_INTERVAL
        entry_price = get_option_price(row['sell_date'], option_type, strike_price)  # 키움 API에서 매수가 조회
        exit_price = get_option_price(row['buy_date'], option_type, strike_price)  # 키움 API에서 청산가 조회
        profit = entry_price - exit_price if entry_price and exit_price else None
        option_trades.append({
            'trade_type': 'sell',
            'option_type': option_type,
            'trade_date': row['sell_date'],
            'settlement_date': row['buy_date'],
            'strike_price': strike_price,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'profit': profit
        })

# 데이터프레임 변환 및 데이터베이스 저장
option_df = pd.DataFrame(option_trades)
option_df.to_sql('option_trades', engine, if_exists='replace', index=False)

print("옵션 거래 시뮬레이션 데이터 생성 및 가격 업데이트 완료!")
