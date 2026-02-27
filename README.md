# ☁️ K8s Mini Project

**현대오토에버 모빌리티스쿨 클라우드트랙 3기** 쿠버네티스 미니프로젝트 작업 공간입니다.

## 🗓️ 프로젝트 개요
- **진행 기간:** 2026.02.24 ~ 2026.03.13 (약 3주)
- **팀 구성:** 성윤, 중훈, 현준, 준태
- **핵심 목표:** 애플리케이션에 CQRS 패턴을 적용하고, K8s 클러스터 환경에 안정적으로 배포 및 운영하기.

<br>

## 🛠️ Tech Stack
- **Infrastructure:** Kubernetes, Docker, Docker Hub
- **Backend:** (Python, Fast API)
- **Database:** (MongoDB, Redis)
- **Collaboration:** GitHub, Notion, JIRA / Slack

<br>

## 🎯 핵심 구현 요구사항
- [x] **CQRS 패턴 적용:** Command(명령)와 Query(조회)의 책임 분리 및 아키텍처 설계
- [x] **Dockerizing:** 애플리케이션 이미지 빌드 및 Docker Hub 업로드
- [ ] **K8s 배포 (Deployment):** 작성한 이미지를 기반으로 파드(Pod) 배포
- [ ] **외부 통신 (Service):** LoadBalancer를 활용한 외부 포트 노출
- [ ] **데이터 영속성 (Volume):** HostPath 등을 활용한 DB 데이터 볼륨 마운트 (경로 확보)
- [ ] **설정 관리 (Config):** ConfigMap, Secret, Downward API 등을 활용한 유연한 환경 설정

<br>

## 🌐 포트 및 인프라 컨벤션 (규칙)
K8s 클러스터 내부 및 포트 포워딩을 위해 아래와 같이 포트를 분배하여 사용합니다. (총 10개 포트 할당)

| Service Name | Port Number | Description | 담당자 |
| :--- | :--- | :--- | :--- |
| Frontend | `30001` | 사용자 웹 인터페이스 | - |
| Command API | `30002` | 데이터 변경(CUD) 처리 API | - |
| Query API | `30003` | 데이터 조회(R) 처리 API | - |
| Command DB | `30004` | Command 서비스용 DB | - |
| Query DB | `30005` | Query 서비스용 DB | - |
| ... | ... | ... | ... |

> 💡 **DB Volume Mount Path:** `(사용하는 DB의 실제 저장 경로 작성, 예: /var/lib/mysql)`

<br>

## 📁 디렉토리 구조
```text
📦 K8s-Mini-Project
 ┣ 📂 k8s-manifests     # Kubernetes 배포를 위한 YAML 파일 모음 (Deployment, Service, ConfigMap 등)
 ┣ 📂 src-command       # Command API 소스 코드
 ┣ 📂 src-query         # Query API 소스 코드
 ┗ 📜 README.md
