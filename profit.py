import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import mplcursors
from datetime import datetime

# 데이터베이스 연결
DB_PATH = "data/kospi200.db"
engine = create_engine(f'sqlite:///{DB_PATH}')

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
ax1.set_title(f"KOSPI 200 with Buy/Sell Signals\nCCI Period: {cci_period}, Buy Threshold: {buy_threshold}, Sell Threshold: {sell_threshold}\nBuy Signal Today: {buy_signal_today}, Sell Signal Today: {sell_signal_today}")
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
