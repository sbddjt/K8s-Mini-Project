import json
import os
from datetime import datetime
from database import (
    redis_client, 
    get_vehicle_telemetry, 
    VehicleTelemetry
)
from dotenv import load_dotenv

load_dotenv()

def get_all_vehicles_summary() -> dict:
    """모든 차량 요약 (속도, SOC, 위치)"""
    keys = redis_client.keys("vehicle:*:latest")
    summary = {}
    for key in keys:
        vehicle_id = key.split(":")[1]
        data = redis_client.get(key)
        if data:
            parsed = json.loads(data)
            summary[vehicle_id] = {
                "speed": parsed["telemetry"]["speed_kmh"],
                "soc": parsed["telemetry"]["battery_soc_pct"],
                "city": parsed["location"]["city"],
                "timestamp": parsed["timestamp"]
            }
    return summary

def get_recent_vehicles(limit: int = 5) -> dict:
    """최근 업데이트 차량 상세"""
    recent = redis_client.zrevrange("vehicles:recent", 0, limit-1, withscores=True)
    vehicles = {}
    for vehicle_id_bytes, score in recent:
        vehicle_id = vehicle_id_bytes.decode()
        telemetry = get_vehicle_telemetry(vehicle_id)
        if telemetry:
            vehicles[vehicle_id] = {
                "data": telemetry.model_dump(),
                "updated_at": datetime.fromtimestamp(score).strftime("%Y-%m-%d %H:%M:%S")
            }
    return vehicles

def print_vehicle_status(vehicle: VehicleTelemetry):
    """차량 상태 예쁘게 출력"""
    tel = vehicle.telemetry
    loc = vehicle.location
    print(f"🚗 {vehicle.model} ({vehicle.vehicle_id})")
    print(f"   📍 {loc['city']} | 속도: {tel['speed_kmh']}km/h | SOC: {tel['battery_soc_pct']:.1f}%")
    print(f"   👤 {vehicle.driver} | {vehicle.timestamp}")
    print(f"   ⚡ {vehicle.status['driving_mode']} 모드 | 기어: {vehicle.status['gear']}")
    print(f"   📊 타이어: FL:{vehicle.diagnostics['tire_pressure_psi']['front_left']:.1f} FR:{vehicle.diagnostics['tire_pressure_psi']['front_right']:.1f}")
    print()

def main():
    print("🔴 Redis 차량 관제 대시보드")
    print("=" * 60)
    
    # 1. 전체 차량 현황
    summary = get_all_vehicles_summary()
    print(f"\n📊 실시간 차량 {len(summary)}대")
    for vid, info in summary.items():
        print(f"  {vid}: {info['speed']}km/h ({info['city']}, SOC {info['soc']:.0f}%)")
    
    # 2. 최근 활동 차량 상세
    print(f"\n⏰ 최근 {len(get_recent_vehicles())}대 상세")
    recent = get_recent_vehicles(3)
    for vid, info in recent.items():
        print_vehicle_status(VehicleTelemetry(**info["data"]))
    
    # 3. 특정 차량 (CAR-1003)
    print("\n🎯 CAR-1003 상세 조회")
    car = get_vehicle_telemetry("CAR-1003")
    if car:
        print_vehicle_status(car)
    else:
        print("  📭 데이터 없음")

if __name__ == "__main__":
    main()