# KRaft — ZooKeeper에서 KRaft로

## 개념

카프카 클러스터는 메시지 데이터 외에 **메타데이터**를 관리해야 한다.

- 어떤 브로커가 살아 있는지
- 어떤 토픽과 파티션이 있는지, 설정은 무엇인지
- 각 파티션의 리더와 ISR이 누구인지
- ACL(권한), 쿼터 등

이 메타데이터를 관리하고, 리더 선출 같은 결정을 내리는 역할이 **Controller**다.

### ZooKeeper 모드 (과거)
- **Apache ZooKeeper**라는 별도의 분산 코디네이션 시스템에 메타데이터를 저장했다.
- 브로커 중 하나가 Controller로 선출되어 ZooKeeper를 보며 클러스터를 관리했다.

### KRaft 모드 (현재)
- **KRaft = Kafka + Raft.** ZooKeeper 없이 카프카가 **Raft 합의 알고리즘**으로 직접 메타데이터를 관리한다.
- 메타데이터를 카프카 내부 토픽 **`__cluster_metadata`** 에 이벤트 로그로 저장한다.
- 제안: KIP-500 ("Replace ZooKeeper with a Self-Managed Metadata Quorum")

### 버전별 흐름

| 버전 | 시기 | 변화 |
|---|---|---|
| 2.8 | 2021 | KRaft 얼리 액세스 (테스트용) |
| 3.3 | 2022 | KRaft **프로덕션 사용 가능** |
| 3.5 | 2023 | ZooKeeper 모드 **deprecated** |
| 3.9 | 2024 | ZooKeeper를 지원하는 마지막 버전 (ZK → KRaft 마이그레이션용) |
| **4.0** | 2025 | **ZooKeeper 완전 제거.** KRaft만 지원 |

## 동작 원리

### 1. ZooKeeper 모드 구조와 문제점

```
       ┌──────────── ZooKeeper 앙상블 (3~5대) ───────────┐
       │   ZK1        ZK2        ZK3                    │
       │   (메타데이터 저장, 브로커 생존 감시)               │
       └───────────────▲──────────────────────────────────┘
                       │ 읽기/쓰기, watch
       ┌───────────────┴─────────── Kafka Cluster ───────┐
       │  Broker1 (Controller)   Broker2     Broker3     │
       └─────────────────────────────────────────────────┘
```

**문제점**
1. **두 개의 분산 시스템 운영**: 카프카와 ZooKeeper를 따로 설치·설정·모니터링·보안 설정해야 한다.
2. **Controller 장애 복구가 느리다**: 새 Controller가 선출되면 ZooKeeper에서 **전체 메타데이터를 다시 읽어야** 한다. 파티션이 많으면 수 분이 걸리고, 그동안 리더 선출이 멈춘다.
3. **파티션 수 한계**: 메타데이터 로딩 비용 때문에 클러스터당 파티션 수가 약 20만 개 수준으로 제한됐다.
4. **메타데이터 불일치**: Controller의 메모리, ZooKeeper, 각 브로커의 메타데이터가 서로 어긋날 수 있었다.

### 2. KRaft 모드 구조

```
       ┌────────── Controller Quorum (3 또는 5대) ──────────┐
       │  Controller1      Controller2       Controller3    │
       │  (Active/Leader)  (Follower)        (Follower)     │
       │       └──── __cluster_metadata 로그를 Raft로 복제 ──┘
       └──────────────────────┬──────────────────────────────┘
                              │ 메타데이터 변경분 fetch
       ┌──────────────────────▼──────────── Kafka Cluster ───┐
       │      Broker1          Broker2          Broker3      │
       │   (메타데이터 캐시)  (메타데이터 캐시)  (메타데이터 캐시) │
       └──────────────────────────────────────────────────────┘
```

- **Controller Quorum**: Controller 역할을 하는 노드들의 그룹. 그중 **하나가 Active Controller(Raft 리더)** 이고, 나머지는 대기 상태로 로그를 복제한다.
- 모든 메타데이터 변경은 **`__cluster_metadata` 토픽에 이벤트로 기록**된다 (예: "토픽 생성", "P0 리더가 B2로 변경").
- 브로커들은 이 로그를 **fetch해서 메모리에 메타데이터를 반영**한다 → 컨슈머가 토픽을 읽는 것과 같은 방식.
- 메타데이터 로그가 계속 커지지 않도록 주기적으로 **스냅샷**을 만든다.

### 3. Raft 합의 알고리즘 — 간단히

Raft는 여러 노드가 **같은 로그를 같은 순서로** 갖도록 합의하는 알고리즘.

1. **리더 선출**: 리더의 heartbeat가 끊기면 팔로워가 후보가 되어 투표를 요청한다. **과반수 표**를 얻으면 새 리더.
2. **로그 복제**: 모든 변경은 리더가 받아서 팔로워들에게 복제한다.
3. **커밋**: **과반수**가 저장하면 그 변경은 확정(commit)된다.

```
Controller 3대 → 과반수 2 → 1대 장애까지 견딤
Controller 5대 → 과반수 3 → 2대 장애까지 견딤
```

→ Controller 수는 **홀수(3 또는 5)** 로 둔다. 4대는 3대와 견디는 장애 수(1대)가 같아서 의미가 없다.

> 데이터 파티션 복제(ISR 방식)와 메타데이터 복제(Raft, 과반수 방식)는 다르다. [05-replication.md](05-replication.md#카프카-복제-vs-일반적인-다수결quorum-복제) 참고.

### 4. 노드 역할 설정: process.roles

```properties
# 브로커 전용
process.roles=broker

# 컨트롤러 전용
process.roles=controller

# 둘 다 (combined mode)
process.roles=broker,controller

# 컨트롤러 쿼럼 지정
controller.quorum.bootstrap.servers=controller1:9093,controller2:9093,controller3:9093
```

| 모드 | 구성 | 적합한 환경 |
|---|---|---|
| **Isolated** | 컨트롤러와 브로커를 **다른 노드**에서 실행 | 프로덕션 (권장). 브로커 부하가 컨트롤러에 영향을 주지 않음 |
| **Combined** | 한 노드가 브로커와 컨트롤러를 **같이** 수행 | 개발, 테스트, 소규모 클러스터 |

### 5. 장애 복구가 빨라진 이유

```
ZooKeeper 모드: Controller 장애 → 새 Controller 선출 → ZK에서 메타데이터 전체 로딩 (느림) → 동작
KRaft 모드:    Controller 장애 → 대기 Controller가 이미 같은 로그를 갖고 있음 → 바로 동작 (빠름)
```

- 대기 중인 Controller들이 **항상 최신 메타데이터 로그를 복제**하고 있어서 리더가 바뀌어도 다시 읽을 필요가 없다.
- 브로커도 전체 메타데이터를 다시 받지 않고 **마지막으로 읽은 offset 이후 변경분만** 가져온다.

## 장단점 / 비교

### ZooKeeper vs KRaft

| | ZooKeeper 모드 | KRaft 모드 |
|---|---|---|
| **구성 요소** | Kafka + ZooKeeper (2개 시스템) | Kafka만 |
| **메타데이터 저장** | ZooKeeper (znode) | `__cluster_metadata` 토픽 (이벤트 로그) |
| **합의 방식** | ZooKeeper의 ZAB 프로토콜 | Raft |
| **Controller 장애 복구** | 느림 (메타데이터 전체 재로딩) | 빠름 (대기 Controller가 이미 최신 상태) |
| **최대 파티션 수** | 약 20만 개 | 수백만 개까지 가능 |
| **운영/보안 설정** | 두 시스템 각각 | 하나로 통일 |
| **지원 상태** | 4.0에서 제거 | 4.0부터 유일한 방식 |

### KRaft의 장점
- **운영 단순화**: ZooKeeper를 따로 설치·관리할 필요가 없다. 설정, 보안, 모니터링 대상이 하나로 줄어든다.
- **확장성**: 클러스터당 수백만 개 파티션까지 지원.
- **빠른 장애 복구**: Controller 교체와 브로커 재시작이 빨라진다.
- **일관된 모델**: 메타데이터도 카프카의 핵심 개념인 "로그"로 관리 → 브로커가 같은 방식으로 따라 읽는다.

### KRaft의 주의점
- **마이그레이션 필요**: 기존 ZooKeeper 클러스터는 3.x(3.9 권장)에서 KRaft로 마이그레이션한 뒤에 4.0으로 올릴 수 있다.
- **Controller 쿼럼 과반수 유지 필수**: 과반수가 죽으면 메타데이터 변경(리더 선출, 토픽 생성 등)이 불가능해진다.
- **ZooKeeper 기반 도구 사용 불가**: `--zookeeper` 옵션을 쓰던 스크립트나 ZK에 직접 접근하던 도구는 `--bootstrap-server` 방식으로 바꿔야 한다.

## 예상 질문
-

## 참고 자료
-
