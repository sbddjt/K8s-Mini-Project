# 1. 베이스 이미지: 파이썬 3.9 슬림 버전 (용량 최소화 및 보안 강화)
FROM python:3.9-slim

# 2. 파이썬 환경 설정
# .pyc 파일 생성 방지 및 로그 즉시 출력을 위해 버퍼링 해제
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. 컨테이너 내 작업 디렉토리 설정
WORKDIR /app

# 4. 라이브러리 설치 (레이어 캐싱을 위해 코드 복사 전 수행)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 실행 코드 복사
COPY ingest_server.py .

# 6. 보안을 위한 비관리자 계정 설정
# 루트 권한으로 실행되는 것을 막아 해킹 시 피해를 최소화함
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# 7. 서비스 포트 노출 (FastAPI 기본 포트)
EXPOSE 8000

# 8. 서버 실행 명령
CMD ["uvicorn", "ingest_server:app", "--host", "0.0.0.0", "--port", "8000"]
