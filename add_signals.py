import os
import pandas as pd
from sqlalchemy import create_engine

# 데이터베이스 파일 경로 설정
DB_FOLDER = "data"
DB_NAME = "kospi200.db"
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# 데이터베이스 연결
engine = create_engine(f'sqlite:///{DB_PATH}')

def add_signals_to_db():
    df = pd.read_sql('index_data', engine)

    # 매수·매도 신호 추가
    df['buy_signal'] = 0
    df['sell_signal'] = 0
    df['pending_buy'] = 0
    df['pending_sell'] = 0

    # 가격 변동폭 임계값 (조절 가능)
    price_diff_threshold = 4  # 예시: 4 포인트

    # 이전 신호 상태 (0: 없음, 1: 매수, -1: 매도)
    prev_signal = 0

    for i in range(2, len(df)):
        prev2, prev1, today = df.loc[i-2, 'close'], df.loc[i-1, 'close'], df.loc[i, 'close']
        price_diff_buy = abs(today - prev1)
        price_diff_sell = abs(prev1 - today)

        # 매수 신호 규칙
        if prev_signal != 1:  # 이전 신호가 매수 신호가 아니어야 함
            if prev2 > prev1 < today and price_diff_buy >= price_diff_threshold:
                df.at[i, 'buy_signal'] = 1
                prev_signal = 1  # 매수 신호 발생, prev_signal 갱신
                continue  # 매도 신호 확인 건너뜀

        # 매도 신호 규칙
        if prev_signal != -1:  # 이전 신호가 매도 신호가 아니어야 함
            if prev2 < prev1 > today and price_diff_sell >= price_diff_threshold:
                df.at[i, 'sell_signal'] = 1
                prev_signal = -1  # 매도 신호 발생, prev_signal 갱신
                continue  # 매수 신호 확인 건너뜀

        # 보류 상태 처리 (매수/매도 모두)
        if prev_signal == 1:  # 매수 신호가 보류 중인 경우
            if df.loc[i-1, 'pending_buy'] == 1 and today > prev1 and price_diff_buy >= price_diff_threshold:
                df.at[i, 'buy_signal'] = 1
                df.at[i-1, 'pending_buy'] = 0
                prev_signal = 1
            elif prev2 > prev1 == today and price_diff_buy < price_diff_threshold:
                df.at[i, 'pending_buy'] = 1
            else:
                df.at[i, 'pending_buy'] = 0

        elif prev_signal == -1:  # 매도 신호가 보류 중인 경우
            if df.loc[i-1, 'pending_sell'] == 1 and today < prev1 and price_diff_sell >= price_diff_threshold:
                df.at[i, 'sell_signal'] = 1
                df.at[i-1, 'pending_sell'] = 0
                prev_signal = -1
            elif prev2 < prev1 == today and price_diff_sell < price_diff_threshold:
                df.at[i, 'pending_sell'] = 1
            else:
                df.at[i, 'pending_sell'] = 0
        else:
            df.at[i, 'pending_buy'] = 0
            df.at[i, 'pending_sell'] = 0

    df.to_sql('index_data', engine, if_exists='replace', index=False)
    print("매수·매도 신호가 데이터베이스에 추가되었습니다.")

if __name__ == '__main__':
    add_signals_to_db()

