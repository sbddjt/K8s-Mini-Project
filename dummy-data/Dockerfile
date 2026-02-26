# 1. 베이스 이미지 설정
FROM python:3.9-slim

# 2. 환경 변수 최적화
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. 작업 폴더 설정
WORKDIR /app

# 4. 종속성 설치 (httpx 등 포함)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 소스 코드 및 데이터 프로필(JSON) 복사
COPY dummy_car_server.py .
COPY *.json . 

# 6. 비관리자 계정으로 보안 강화
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# 7. 더미 서버용 포트 노출
EXPOSE 8001

# 8. 더미 서버 실행 명령
CMD ["uvicorn", "dummy_car_server:app", "--host", "0.0.0.0", "--port", "8001"]
