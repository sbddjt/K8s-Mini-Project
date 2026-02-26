# 1. 현준님의 개발 환경과 동일한 파이썬 버전 사용
FROM python:3.12-slim

# 2. 컨테이너 내 작업 디렉토리 설정
WORKDIR /app

# 3. 환경 변수 설정
# .pyc 파일을 생성하지 않도록 설정 (이미지 경량화)
ENV PYTHONDONTWRITEBYTECODE=1
# 로그가 버퍼링 없이 즉시 출력되도록 설정 (디버깅 용이)
ENV PYTHONUNBUFFERED=1

# 4. 의존성 설치
# requirements.txt만 먼저 복사하여 캐시 효율을 높입니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 소스 코드 전체 복사
# .dockerignore에서 제외한 파일들(venv 등)을 제외하고 복사됩니다.
COPY . .

# 6. API 포트 개방 (현재 .env 설정인 8000번 반영)
EXPOSE 8000

# 7. 실행 명령어
# 현준님의 구조(api/main.py)에 맞춰 모듈 형태로 실행
CMD ["python", "-m", "api.main"]