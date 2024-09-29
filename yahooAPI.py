import yfinance as yf
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017/")  # URL 수정 필요
db = client["stock_data"]  # 데이터베이스 이름
collection = db["intel"]   # 컬렉션 이름

# 현재 날짜와 1년 전 날짜 계산
end_date = datetime.now()
start_date = end_date - timedelta(days=365)

# 하루 단위로 데이터를 수집
current_date = start_date
while current_date <= end_date:
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")
    
    # 30일 이내의 데이터는 1분 간격으로 요청
    if current_date >= end_date - timedelta(days=30):
        data = yf.download("INTC", start=current_date.strftime('%Y-%m-%d'), 
                           end=(current_date + timedelta(days=1)).strftime('%Y-%m-%d'), 
                           interval="1m")
    else:
        # 30일 이전의 데이터는 하루치로 요청
        data = yf.download("INTC", start=current_date.strftime('%Y-%m-%d'), 
                           end=(current_date + timedelta(days=1)).strftime('%Y-%m-%d'), 
                           interval="1d")
    
    # 데이터가 비어있지 않은지 확인 후 MongoDB에 저장
    if not data.empty:
        for index, row in data.iterrows():
            record = {
                "timestamp": index,
                "open": row["Open"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "volume": row["Volume"]
            }
            collection.insert_one(record)
            print(f"Inserted: {record}")

    # 다음 날짜로 이동
    current_date += timedelta(days=1)

print("Data collection complete.")
