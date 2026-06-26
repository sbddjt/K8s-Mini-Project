# ☁️ Connected Car Telemetry

**현대오토에버 모빌리티스쿨 클라우드트랙 3기** 쿠버네티스 미니프로젝트 작업 공간입니다.

## 🗓️ 프로젝트 개요
- **진행 기간:** 2026.02.24 ~ 2026.03.13 (약 3주)
- **팀 구성:** 성윤, 중훈, 현준, 준태
- **핵심 목표:** 애플리케이션에 CQRS 패턴을 적용하고, K8s 클러스터 환경에 안정적으로 배포 및 운영하기.

<br>

## 🛠️ Tech Stack

| 분류 | 기술 |
| :--- | :--- |
| **Infrastructure** | Kubernetes, Docker, Harbor Registry, Helm, NFS |
| **Backend (Command)** | Python 3.11, FastAPI, kafka-python, pymongo |
| **Backend (Query)** | Python 3.12, FastAPI, kafka-python, redis-py |
| **Frontend** | React 19, TypeScript, Vite, Leaflet (react-leaflet) |
| **Database** | MongoDB 7 (telemetry 이력), Redis 7 (차량 최신 상태 캐시) |
| **Messaging** | Apache Kafka 3.9 (KRaft 모드, 주키퍼 없음) |
| **CI/CD** | Jenkins, Helm |
| **Monitoring** | Prometheus, Grafana, Alertmanager (kube-prometheus-stack) |
| **Collaboration** | GitHub, Notion, JIRA / Slack |

<br>

## 🏗️ 시스템 아키텍처

### 전체 데이터 흐름

```
Dummy Car Server (Python/FastAPI)
    │  POST /api/query/telemetry
    ▼
[Command] Ingest API (FastAPI)
    │  Kafka Produce → car.telemetry.events
    ▼
Apache Kafka (KRaft)
    ├──▶ [Command] MongoDB Consumer → MongoDB (telemetry_history 영구 저장)
    └──▶ [Query]   Redis Consumer   → Redis   (vehicle:*:latest 캐시, TTL 60s)
                                                    │
                                          [Query] Query API (FastAPI)
                                                    │  GET /api/vehicles
                                                    ▼
                                              Frontend (React + Leaflet)
```

### CQRS 패턴 적용

| 구분 | 서비스 | 역할 |
| :--- | :--- | :--- |
| **Command** | `telemetry-ingest-api` | 차량 텔레메트리 수신 및 Kafka 발행 |
| **Command** | `telemetry-mongo-consumer` | Kafka 이벤트 → MongoDB 영구 저장 |
| **Query** | `mobility-query-api` | Redis 조회, 차량 목록/상세 API 제공 |
| **Query** | `mobility-query-consumer` | Kafka 이벤트 → Redis 최신 상태 업데이트 |

<br>

## 🎯 핵심 구현 요구사항

- [x] **CQRS 패턴 적용:** Command(명령)와 Query(조회)의 책임 분리 및 아키텍처 설계
- [x] **Dockerizing:** 애플리케이션 이미지 빌드 및 Harbor Registry 업로드
- [x] **K8s 배포 (Deployment):** Helm Chart를 통한 파드(Pod) 배포 및 관리
- [x] **외부 통신 (Service):** Ingress + NodePort를 활용한 외부 포트 노출
- [x] **데이터 영속성 (Volume):** NFS PersistentVolume으로 MongoDB / Redis / Kafka 데이터 보존
- [x] **설정 관리 (Config):** ConfigMap(비민감 설정), Secret(인증정보·API 키) 분리 관리
- [x] **자동 스케일링 (HPA):** Command API / Query API CPU 기반 HorizontalPodAutoscaler
- [x] **데이터 보존 정책 (CronJob):** MongoDB TTL 정리(15분마다), Redis 고아 키 정리(10분마다)
- [x] **보안 강화:** MongoDB `--auth`, Redis `--requirepass`, VWorld API 키 nginx proxy_pass 처리
- [x] **모니터링:** Prometheus + Grafana + Alertmanager, Discord 웹훅 알림

<br>

## 🌐 포트 및 인프라 컨벤션

Ingress를 활용한 **단일 외부 진입점** 패턴을 적용하였으며, 내부 서비스는 ClusterIP를 통해 통신합니다.

| Service Name | Port (Type) | Description |
| :--- | :--- | :--- |
| **Ingress Controller** | `80/443` (LoadBalancer) | 외부 접속 통합 게이트웨이 |
| **Frontend** | `30001` (NodePort) | 관제 웹 인터페이스 (직접 접속/테스트용) |
| **Command API** | `80` (ClusterIP) | 텔레메트리 수신 (`POST /api/query/telemetry`) |
| **Query API** | `80` (ClusterIP) | 차량 상태 조회 (`GET /api/vehicles`) |
| **MongoDB** | `27017` (ClusterIP) | telemetry_history 영구 저장 |
| **Redis** | `6379` (ClusterIP) | vehicle:*:latest 인메모리 캐시 |
| **Kafka** | `9092` (ClusterIP) | Command-Query 데이터 동기화 |

### Ingress 라우팅 규칙

| 경로 | 백엔드 서비스 |
| :--- | :--- |
| `/api/map/` | frontend-svc (nginx → VWorld proxy_pass) |
| `/api/query/telemetry` | command-svc |
| `/api/vehicles` | query-svc |
| `/health`, `/api/health` | query-svc |
| `/` | frontend-svc |

> 💡 **VWorld API 키 보안:** 브라우저가 `/api/map/tiles/{z}/{y}/{x}` 로 요청하면 frontend nginx가 `${VWORLD_API_KEY}`를 주입해 VWorld로 프록시합니다. API 키는 네트워크 탭에 노출되지 않습니다.

<br>

## 📁 디렉토리 구조

```text
📦 K8s-Mini-Project
 ┣ 📂 k8s-manifests
 ┃  ┣ 📂 mobility-app          # Helm Chart (배포 핵심)
 ┃  ┃  ┣ 📂 templates
 ┃  ┃  ┃  ┣ command-deployment.yaml
 ┃  ┃  ┃  ┣ query-deployment.yaml
 ┃  ┃  ┃  ┣ frontend-deployment.yaml
 ┃  ┃  ┃  ┣ mongo.yaml          # StatefulSet + CronJob (TTL 정리)
 ┃  ┃  ┃  ┣ redis.yaml          # StatefulSet + CronJob (고아 키 정리)
 ┃  ┃  ┃  ┣ kafka.yaml          # StatefulSet (KRaft)
 ┃  ┃  ┃  ┣ ingress.yaml
 ┃  ┃  ┃  ┣ hpa.yaml
 ┃  ┃  ┃  ┣ secret.yaml
 ┃  ┃  ┃  └ configmap.yaml
 ┃  ┃  ┣ values.yaml            # 기본값 (이미지 태그 등)
 ┃  ┃  └ Chart.yaml
 ┃  ┣ nfs-storage.yaml          # PersistentVolume (NFS)
 ┃  ┣ mobility-alert-rules.yaml # PrometheusRule
 ┃  ┣ miniproject-servicemonitor.yaml
 ┃  ┣ miniproject-podmonitor.yaml
 ┃  ┣ monitoring-values.yaml    # kube-prometheus-stack Helm values
 ┃  └ mobility-monitoring-ingress.yaml
 ┣ 📂 src-command
 ┃  ┣ 📂 ingest                 # Ingest API (FastAPI, Kafka Producer)
 ┃  └ 📂 consumer               # MongoDB Consumer (kafka-python)
 ┣ 📂 src-query
 ┃  ┣ 📂 api                    # Query API (FastAPI, Redis)
 ┃  └ 📂 consumer               # Redis Consumer (kafka-python)
 ┣ 📂 dummy-data                # 차량 시뮬레이터 (FastAPI + httpx)
 ┣ 📂 frontend                  # React + Vite + Leaflet
 ┣ 📂 docs
 ┃  ┣ secret-setup.md           # Secret 설정 가이드
 ┃  └ deploy.sh                 # 수동 재배포 스크립트
 ┣ 📜 Jenkinsfile               # CI/CD 파이프라인
 ┣ 📜 docker-compose.yaml       # 로컬 개발 환경
 └ 📜 README.md
```

<br>

## 🚀 배포

### CI/CD (Jenkins)

`Jenkinsfile`에 정의된 파이프라인이 자동으로 실행됩니다.

```
1. Checkout        - 소스 코드 체크아웃
2. Set image tag   - {BUILD_NUMBER}-{git shortSHA} 로 이미지 태그 생성
3. Docker Build & Push
   - telemetry-ingest-api
   - telemetry-mongo-consumer
   - mobility-query-api
   - mobility-query-consumer
   - k8s-mini-frontend
   → Harbor Registry (10.0.2.111:80/k8s-mini)
4. Deploy to Kubernetes
   - helm upgrade --install mobility-app
   - --atomic --cleanup-on-fail
```

### Helm 수동 배포

```bash
# 시크릿 values 파일 준비 (docs/secret-setup.md 참고)
helm upgrade --install mobility-app k8s-manifests/mobility-app \
  -n miniproject --create-namespace \
  --wait --timeout 10m --atomic \
  -f secret-values.yaml
```

### 로컬 개발 환경

```bash
docker compose up -d
```

| 서비스 | 로컬 접속 주소 |
| :--- | :--- |
| Ingest API | http://localhost:8080 |
| Query API | http://localhost:8000 |
| Frontend | http://localhost:3000 |
| MongoDB | localhost:27017 |
| Redis | localhost:6379 |
| Kafka | localhost:9092 |

<br>

## 🔒 Secret 관리

민감 정보는 `mobility-secret` K8s Secret으로 관리하며 `values.yaml`에 평문 저장하지 않습니다.

| 키 | 분류 | 설명 |
| :--- | :--- | :--- |
| `MONGO_INITDB_ROOT_USERNAME` | Secret | MongoDB root 계정 |
| `MONGO_INITDB_ROOT_PASSWORD` | Secret | MongoDB root 비밀번호 |
| `MONGO_URI` | Secret | 인증 포함 MongoDB URI |
| `REDIS_PASSWORD` | Secret | Redis requirepass |
| `VWORLD_API_KEY` | Secret | VWorld 지도 API 키 |

> **주의:** `secret-values.yaml`은 절대 Git에 커밋하지 마세요.  
> 상세 설정 방법은 `docs/secret-setup.md`를 참고하세요.

<br>

## 📊 모니터링

kube-prometheus-stack을 활용하며 Ingress를 통해 접근합니다.

| 서비스 | 경로 |
| :--- | :--- |
| Grafana | `/grafana` |
| Prometheus | `/prometheus` |
| Alertmanager | `/alertmanager` |

### 알림 규칙 (`mobility-alert-rules.yaml`)

| 알림명 | 조건 | 심각도 |
| :--- | :--- | :--- |
| MiniprojectPodNotReady | Pod Ready=false 2분 이상 | warning |
| MiniprojectPodNotRunning | Pod Running 아님 3분 이상 | warning |
| MiniprojectPodHighCPUUsage | CPU > 0.8 cores (5분 avg) | warning |
| MiniprojectPodHighMemoryUsage | 메모리 > 700MB (5분 avg) | warning |
| MiniprojectPodRestartingTooOften | 15분 내 재시작 3회 초과 | critical |

알림은 **Discord 웹훅**으로 전송됩니다.

<br>

## 💾 데이터 보존 정책

### MongoDB CronJob (매 15분)
- `telemetry_history` 컬렉션에서 `MONGO_TTL_DAYS`(기본 7일) 이전 데이터 삭제
- `created_at`, `received_at`, `_consumed_at` 필드 기준

### Redis CronJob (매 10분)
- `vehicle:*:latest` 키 중 TTL이 없는 고아 키(legacy 데이터) 자동 삭제

<br>

## 🚗 차량 시뮬레이터 (Dummy Data)

`dummy-data/dummy_car_server.py`는 가상 차량 데이터를 생성해 Ingest API로 전송하는 FastAPI 서버입니다.

```bash
# 주요 환경변수
INGEST_SERVER_URL=http://<ingest-api>/api/query/telemetry
DUMMY_SEED_COUNT=100       # 생성할 차량 수
INGEST_BURST_SIZE=100      # 한 주기에 보낼 요청 수
INGEST_CONCURRENCY=50      # 동시 전송 상한
INGEST_INTERVAL_MIN=0.5    # 전송 최소 간격(초)
INGEST_INTERVAL_MAX=1.0    # 전송 최대 간격(초)
```

차량은 한국 주요 도시(서울, 부산, 대전 등 20개 앵커) 반경 내에서 DRIVE / IDLE / PARK / CHARGE 상태를 시뮬레이션하며 실시간 이동합니다.

<br>

## 🗃️ DB 볼륨 마운트 경로

| DB | 컨테이너 내부 경로 | NFS 경로 |
| :--- | :--- | :--- |
| MongoDB | `/data/db` | `/srv/nfs/mongo` |
| Redis | `/data` | `/srv/nfs/redis` |
| Kafka | `/tmp/kraft-combined-logs` | `/srv/nfs/kafka` |

NFS 서버: `10.0.2.100`
