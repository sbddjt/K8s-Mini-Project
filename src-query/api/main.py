import os
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from database import redis_client

# .env 파일의 환경 변수를 읽어옵니다.
load_dotenv()

app = FastAPI(title="Connected Car Query API")

# 프론트엔드 협업을 위한 CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """시스템 상태 및 Redis 연결 확인"""
    try:
        redis_client.ping()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis connection failed: {str(e)}")

@app.get("/api/vehicles")
async def get_all_vehicles():
    """모든 차량의 최신 상태 조회"""
    try:
        # 환경 변수에서 키 패턴을 가져오거나 기본값 사용
        key_pattern = os.getenv("REDIS_KEY_PATTERN", "vehicle:*:latest")
        keys = redis_client.keys(key_pattern)
        
        if not keys:
            return {"count": 0, "vehicles": []}
        
        vehicles = [json.loads(redis_client.get(k)) for k in keys if redis_client.get(k)]
        return {"count": len(vehicles), "vehicles": vehicles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/vehicles/{vehicle_id}")
async def get_vehicle_by_id(vehicle_id: str):
    """특정 차량 상세 조회"""
    key = f"vehicle:{vehicle_id}:latest"
    data = redis_client.get(key)
    if not data:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return json.loads(data)

if __name__ == "__main__":
    import uvicorn
    
    # [핵심] 포트 번호를 환경 변수에서 가져오되, 없으면 30003을 기본으로 사용
    # 이렇게 하면 코드를 건드리지 않고도 외부에서 포트를 바꿀 수 있습니다.
    app_port = int(os.getenv("QUERY_API_PORT", 30003))
    app_host = os.getenv("QUERY_API_HOST", "0.0.0.0")
    
    print(f"🚀 Query API 서버를 시작합니다... (Port: {app_port})")
    
    uvicorn.run(
        "main:app", 
        host=app_host, 
        port=app_port, 
        reload=True  # 개발 단계에서는 코드 수정 시 자동 재시작
    )