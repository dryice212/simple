import os
import pandas as pd
from sqlalchemy import create_engine

# 데이터베이스 파일 경로 설정
DB_FOLDER = "data"
DB_NAME = "kospi200.db"
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# 데이터베이스 연결
engine = create_engine(f'sqlite:///{DB_PATH}')

def add_signals_to_db(cci_period=9, buy_threshold=100, sell_threshold=-100):
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

if __name__ == '__main__':
    add_signals_to_db()
