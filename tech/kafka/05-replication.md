# 복제 — 리더/팔로워, ISR

## 개념

**복제(Replication)** 는 파티션 데이터를 여러 브로커에 복사해 두는 것.
브로커 하나가 죽어도 **데이터가 유실되지 않고, 서비스가 계속되게** 하기 위한 장치다.

### 핵심 용어

| 용어 | 설명 |
|---|---|
| **Replica** | 파티션의 복제본. 리더와 팔로워를 모두 포함 |
| **Replication Factor** | 파티션당 복제본 개수 (보통 3) |
| **Leader** | 해당 파티션의 읽기/쓰기를 처리하는 복제본 |
| **Follower** | 리더의 데이터를 가져와 복사만 하는 복제본 |
| **ISR** (In-Sync Replicas) | 리더와 충분히 동기화된 복제본 목록 (리더 포함) |
| **LEO** (Log End Offset) | 각 복제본에 마지막으로 쓰인 메시지 다음 offset |
| **HW** (High Watermark) | ISR 전체에 복제가 끝난 위치. 컨슈머는 여기까지만 읽을 수 있다 |

### 복제 배치 예시

```
Topic: orders (파티션 3개, replication.factor=3)

           Broker 1        Broker 2        Broker 3
P0      [Leader]        [Follower]      [Follower]
P1      [Follower]      [Leader]        [Follower]
P2      [Follower]      [Follower]      [Leader]
```

- 같은 파티션의 복제본은 **서로 다른 브로커**에 배치된다.
- 리더가 여러 브로커에 고르게 퍼져 있어서 **부하가 분산**된다.
- `replication.factor=N`이면 브로커 **N−1대가 죽어도** 데이터가 남아 있다.

## 동작 원리

### 1. 복제 흐름

```
Producer ──write──▶ Leader (P0)
                      ▲    ▲
          fetch 요청   │    │  fetch 요청
                Follower  Follower
```

1. Producer는 **리더에게만** 메시지를 보낸다.
2. 팔로워는 컨슈머처럼 리더에게 **fetch 요청을 보내 데이터를 가져간다** (pull 방식).
3. 리더는 팔로워들의 fetch 요청을 보고 **각 팔로워가 어디까지 복제했는지** 파악한다.
4. ISR 전체가 복제를 마친 위치까지 **High Watermark**를 올린다.

> 기본적으로 컨슈머도 리더에게서 읽는다. 단, 카프카 2.4+에서는 `client.rack` 설정으로 **가까운 팔로워에서 읽는 것**도 가능하다 (데이터센터 간 네트워크 비용 절감).

### 2. ISR (In-Sync Replicas)

**ISR = 리더를 잘 따라오고 있는 복제본들의 목록.**

- 팔로워가 `replica.lag.time.max.ms` (기본 30초) 동안 리더를 따라잡지 못하면 **ISR에서 빠진다.**
- 다시 따라잡으면 **ISR에 복귀**한다.
- 새 리더는 원칙적으로 **ISR 안에서만** 선출된다 → 데이터가 충분히 복제된 복제본만 리더가 될 수 있다.

```
ISR = [B1(Leader), B2, B3]
  B3가 네트워크 문제로 30초 이상 뒤처짐
ISR = [B1(Leader), B2]        ← B3 제외
  B3가 다시 따라잡음
ISR = [B1(Leader), B2, B3]    ← B3 복귀
```

### 3. High Watermark — 컨슈머는 어디까지 읽을 수 있나

```
            offset:  0  1  2  3  4  5  6
Leader   (LEO=7):   [■][■][■][■][■][■][■]
Follower1 (LEO=6):  [■][■][■][■][■][■]
Follower2 (LEO=5):  [■][■][■][■][■]
                                    ▲
                           High Watermark = 5
                (offset 0~4는 ISR 전체에 복제 완료)
```

- 컨슈머는 **HW 이전 메시지(0~4)만** 읽을 수 있다. 5, 6은 아직 안 보인다.
- **이유**: 리더에만 있는 메시지를 컨슈머가 읽었는데 리더가 죽으면, 새 리더에는 그 메시지가 없다.
  → 컨슈머는 읽었는데 카프카에서는 사라진 **"유령 메시지"** 가 생긴다. HW가 이것을 막는다.

### 4. acks와 복제의 관계

| acks | 응답 시점 | 리더가 바로 죽으면? |
|---|---|---|
| `0` | 보내자마자 | 유실 가능 |
| `1` | 리더 저장 직후 | 팔로워에 복제되기 전이면 **유실** |
| `all` | **ISR 전체** 복제 후 (= HW가 올라간 후) | 유실 없음 (ISR 중 하나가 새 리더) |

**`min.insync.replicas`** (브로커/토픽 설정)
- `acks=all`일 때 **ISR이 최소 몇 개 이상이어야 쓰기를 받을지** 정한다.
- ISR이 이보다 적으면 Producer에 `NotEnoughReplicasException`을 돌려준다.
- 표준 조합: `replication.factor=3`, `min.insync.replicas=2`, `acks=all`
  → 브로커 1대 장애: 쓰기 계속 가능 / 2대 장애: 쓰기 거부 (유실 대신 실패를 택함)

> Producer 쪽 설명은 [03-producer.md](03-producer.md#2-acks--어디까지-저장되면-성공으로-볼-것인가)에서.

### 5. 리더 장애와 재선출

```
1. Broker 1 (P0 리더) 장애
2. Controller가 감지 (heartbeat 끊김)
3. P0의 ISR [B1, B2, B3] 중 살아 있는 B2를 새 리더로 선출
4. 메타데이터 변경을 모든 브로커에 전파
5. Producer/Consumer는 메타데이터를 갱신하고 B2로 요청
```

- 리더 선출은 **Controller**가 담당한다 ([07-kraft.md](07-kraft.md)).
- **Leader Epoch**: 리더가 바뀔 때마다 증가하는 번호. 이전 리더가 복구됐을 때 새 리더와 맞지 않는 데이터를 **잘라내서(truncate)** 복제본 간 데이터를 일치시킨다.

#### Unclean Leader Election

ISR이 전부 죽고 **ISR 밖의 뒤처진 복제본만 살아 있다면?**

| `unclean.leader.election.enable` | 동작 | 결과 |
|---|---|---|
| `false` (기본) | ISR 복제본이 돌아올 때까지 기다림 | **가용성 ↓**, 유실 없음 |
| `true` | 뒤처진 복제본을 리더로 선출 | **유실 발생**, 서비스는 계속 |

→ **일관성(Consistency) vs 가용성(Availability)** 의 트레이드오프. 기본값은 유실을 막는 쪽.

### 6. Preferred Leader와 Rack Awareness

- **Preferred Leader**: 파티션의 복제본 목록에서 첫 번째 브로커. 장애 복구 후 리더가 한 브로커에 몰리지 않도록 원래 리더로 되돌린다 (`auto.leader.rebalance.enable=true`).
- **Rack Awareness** (`broker.rack`): 복제본을 서로 다른 랙/가용 영역(AZ)에 배치 → 랙이나 AZ 전체가 죽어도 데이터가 남는다.

## 장단점 / 비교

### 복제 구조의 장점
- **내구성**: 브로커 N−1대가 죽어도 데이터가 남는다.
- **고가용성**: 리더 장애 시 ISR에서 자동으로 새 리더를 선출해 서비스가 이어진다.
- **유연한 조절**: `acks`, `min.insync.replicas`로 사용 사례에 맞게 내구성과 성능을 조절.
- **일관성 보장**: HW 덕분에 컨슈머는 유실될 수 있는 메시지를 읽지 않는다.

### 단점 / 비용
- **저장 공간 N배**: replication factor 3이면 디스크도 3배 필요.
- **네트워크 트래픽 증가**: 브로커 간 복제 트래픽이 발생한다.
- **지연시간 증가**: `acks=all`이면 가장 느린 ISR 복제본을 기다려야 한다.
- **설정 실수 위험**: `min.insync.replicas`를 replication factor와 같게 두면 브로커 1대만 죽어도 쓰기가 막힌다.

### 설정 조합별 비교 (replication.factor=3 기준)

| min.insync.replicas | acks | 1대 장애 시 쓰기 | 유실 위험 | 비고 |
|---|---|---|---|---|
| 1 | all | 가능 | 있음 (ISR이 리더 혼자일 수 있음) | |
| **2** | **all** | **가능** | **거의 없음** | **권장** |
| 3 | all | **불가** | 없음 | 가용성 너무 낮음 |
| — | 1 | 가능 | 있음 | 성능 우선 |

### 카프카 복제 vs 일반적인 다수결(Quorum) 복제
- 다수결 방식(예: Raft)은 **과반수**가 저장하면 성공 → 복제본 2f+1개로 f개 장애를 견딤.
- 카프카 ISR 방식은 **ISR 전체**가 저장하면 성공 → 복제본 f+1개로 f개 장애를 견딤 (더 적은 복제본으로 같은 내구성).
- 대신 느린 복제본을 ISR에서 빼서 전체가 느려지지 않게 한다.

## 예상 질문
-

## 참고 자료
-
