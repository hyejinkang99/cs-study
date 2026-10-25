# Consumer — Consumer Group, 리밸런싱, 오프셋 커밋

## 개념

**Consumer**는 토픽에서 메시지를 읽어 처리하는 클라이언트.
카프카는 브로커가 밀어주는(push) 방식이 아니라 **컨슈머가 직접 가져가는(pull) 방식**이다.

### 핵심 용어

| 용어 | 설명 |
|---|---|
| **Consumer Group** | 같은 `group.id`를 가진 컨슈머들의 묶음. 토픽의 파티션을 나눠서 처리 |
| **Group Coordinator** | 그룹을 관리하는 브로커. 멤버 관리, 파티션 할당, offset 저장 담당 |
| **Rebalance** | 그룹 멤버가 바뀔 때 파티션을 다시 나눠 갖는 과정 |
| **Committed Offset** | "이 파티션은 여기까지 처리했다"고 기록한 위치 |
| **Consumer Lag** | 마지막으로 쓰인 offset − 커밋된 offset. 밀린 메시지 수 |

### Pull 방식의 이유
- 컨슈머가 **자기 처리 속도에 맞춰** 가져갈 수 있다 → 느린 컨슈머가 압도당하지 않는다.
- 한 번에 여러 메시지를 **배치로** 가져와 효율적이다.
- 원하는 offset으로 돌아가 **다시 읽기(replay)** 가 쉽다.
- 단점: 데이터가 없을 때 빈 요청을 반복할 수 있다 → `fetch.min.bytes`, `fetch.max.wait.ms`로 브로커가 데이터가 쌓일 때까지 잠시 기다렸다 응답하게 해서(long polling) 해결.

### Consumer Group 규칙

```
Topic: order-created (P0, P1, P2, P3)

Group "payment" (컨슈머 2개)      Group "notification" (컨슈머 1개)
  C1 ← P0, P1                       C1 ← P0, P1, P2, P3
  C2 ← P2, P3
```

1. **파티션 하나는 그룹 안의 컨슈머 하나에만** 할당된다 → 그룹 내에서 같은 메시지를 중복 처리하지 않는다.
2. 한 컨슈머는 **여러 파티션**을 맡을 수 있다.
3. **그룹이 다르면 서로 독립적**이다. 각 그룹이 자기 offset을 따로 관리하며 같은 메시지를 각자 읽는다.
4. 컨슈머 수가 파티션 수보다 많으면 **남는 컨슈머는 놀게 된다.**

→ 같은 그룹 = **작업 분담 (Queue처럼)**, 다른 그룹 = **각자 전부 받음 (Pub/Sub처럼)**

## 동작 원리

### 1. Poll 루프

```java
consumer.subscribe(List.of("order-created"));

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        process(record);   // 비즈니스 로직
    }
    consumer.commitSync(); // 처리 완료 후 offset 커밋 (수동 커밋인 경우)
}
```

- `poll()`은 할당된 파티션들에서 메시지를 **한 번에 여러 개** 가져온다 (`max.poll.records`, 기본 500).
- `poll()` 호출 자체가 "나 살아 있고 처리 중이야"라는 신호 역할도 한다.
- ⚠️ `KafkaConsumer`는 **thread-safe하지 않다.** 컨슈머 하나는 스레드 하나에서만 사용해야 한다.

### 2. 컨슈머 생존 확인: Heartbeat와 Poll 간격

코디네이터는 두 가지로 컨슈머가 정상인지 판단한다.

| 설정 | 의미 | 기본값 | 넘기면? |
|---|---|---|---|
| `heartbeat.interval.ms` | 백그라운드 스레드가 heartbeat를 보내는 주기 | 3초 | — |
| `session.timeout.ms` | 이 시간 동안 heartbeat가 없으면 죽은 것으로 판단 | 45초 | 그룹에서 제외 → 리밸런싱 |
| `max.poll.interval.ms` | `poll()` 호출 사이의 최대 간격 | 5분 | 처리가 너무 느리다고 보고 그룹에서 제외 → 리밸런싱 |

- heartbeat는 **프로세스가 살아 있는지**, poll 간격은 **실제로 처리를 진행하고 있는지**를 본다.
- 메시지 하나 처리가 오래 걸리면(외부 API 호출 등) `max.poll.interval.ms`를 넘겨 **리밸런싱이 반복**될 수 있다.
  → `max.poll.records`를 줄이거나 `max.poll.interval.ms`를 늘려서 해결.

### 3. 리밸런싱 (Rebalance)

**언제 일어나나?**
- 컨슈머가 그룹에 새로 들어올 때 (스케일 아웃, 재배포)
- 컨슈머가 나가거나 죽었을 때 (session timeout, poll 간격 초과)
- 구독 중인 토픽의 파티션 수가 바뀔 때

**Eager vs Cooperative 리밸런싱**

```
Eager (전통 방식): 모두 멈추고 전부 다시 분배
  C1: P0,P1 ──▶ (전부 반납, 처리 중단 ⏸) ──▶ C1: P0,P1
  C2: P2,P3 ──▶ (전부 반납, 처리 중단 ⏸) ──▶ C2: P2
  C3 합류                                    C3: P3

Cooperative (점진적): 옮겨야 하는 파티션만 이동
  C1: P0,P1 ──▶ 계속 처리 ▶ ──────────────▶ C1: P0,P1
  C2: P2,P3 ──▶ P3만 반납, P2는 계속 처리 ──▶ C2: P2
  C3 합류                                    C3: P3
```

- **Eager**: 리밸런싱 동안 그룹 전체가 처리를 멈춘다 (**Stop-the-world**).
- **Cooperative**: 이동이 필요한 파티션만 반납하고 나머지는 계속 처리 → 중단 시간이 크게 줄어든다.

**파티션 할당 전략 (`partition.assignment.strategy`)**

| 전략 | 방식 | 리밸런싱 |
|---|---|---|
| `RangeAssignor` | 토픽별로 파티션을 연속 구간으로 나눔 | Eager |
| `RoundRobinAssignor` | 모든 파티션을 돌아가며 하나씩 배분 | Eager |
| `StickyAssignor` | 균등 분배 + 기존 할당을 최대한 유지 | Eager |
| `CooperativeStickyAssignor` | Sticky + 점진적 리밸런싱 | **Cooperative** |

**리밸런싱을 줄이는 방법**
- **Static Membership** (`group.instance.id` 지정): 컨슈머가 재시작해도 같은 멤버로 인정 → `session.timeout.ms` 안에 돌아오면 리밸런싱이 일어나지 않는다. 롤링 배포에 유용.
- **새 Consumer Group 프로토콜** (`group.protocol=consumer`, 카프카 4.0 정식 지원, KIP-848): 할당 계산을 클라이언트가 아니라 **브로커(코디네이터)가 하고**, 컨슈머별로 점진적으로 반영 → 그룹 전체가 멈추는 일이 사라진다.

### 4. Offset 커밋

컨슈머는 처리한 위치를 `__consumer_offsets` 토픽에 커밋한다.
재시작하거나 리밸런싱으로 파티션 주인이 바뀌면 **커밋된 offset부터** 다시 읽는다.

> 커밋하는 offset은 "다음에 읽을 위치"다. offset 5까지 처리했으면 6을 커밋한다.

**① 자동 커밋 (기본값)**
- `enable.auto.commit=true`, `auto.commit.interval.ms=5000`
- `poll()`을 호출할 때 주기가 지났으면 **이전 poll에서 받은 offset까지** 자동으로 커밋.
- 간편하지만 커밋 시점과 실제 처리 완료 시점이 달라 **중복이나 유실**이 생길 수 있다.

**② 수동 커밋** (`enable.auto.commit=false`)

| 방식 | 특징 |
|---|---|
| `commitSync()` | 커밋이 끝날 때까지 블록. 실패하면 재시도. 안전하지만 느림 |
| `commitAsync()` | 블록하지 않음. 재시도 안 함 (순서가 꼬일 수 있어서). 빠름 |

실무 패턴: 평소에는 `commitAsync()`, **종료 직전이나 리밸런싱 직전에는 `commitSync()`** 로 확실하게 커밋.

```java
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets); // 파티션 뺏기기 전에 커밋
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
});
```

**커밋 시점에 따른 결과**

```
처리 → 커밋 순서:  처리 ✅ → (여기서 죽음) → 커밋 ✗  → 재시작 후 다시 처리 → 중복 (at-least-once)
커밋 → 처리 순서:  커밋 ✅ → (여기서 죽음) → 처리 ✗  → 재시작 후 건너뜀   → 유실 (at-most-once)
```

→ 보통 **처리 후 커밋 (at-least-once)** 을 쓰고, 중복은 컨슈머가 **멱등하게** 처리해서 막는다. [06-delivery.md](06-delivery.md)

### 5. 커밋된 offset이 없을 때: auto.offset.reset

새 그룹이거나, 커밋된 offset이 보관 기간이 지나 삭제된 경우.

| 값 | 동작 |
|---|---|
| `latest` (기본) | 가장 최신 위치부터 → 이전 메시지는 안 읽음 |
| `earliest` | 가장 처음부터 → 남아 있는 메시지 전부 읽음 |
| `none` | 예외 발생 |

⚠️ 새 서비스를 붙였는데 과거 메시지가 안 읽힌다면 대부분 `latest` 때문이다.

## 장단점 / 비교

### Consumer Group 구조의 장점
- **수평 확장**: 컨슈머를 늘리면 파티션이 자동으로 재분배된다 (파티션 수까지).
- **자동 장애 조치**: 컨슈머가 죽으면 리밸런싱으로 다른 컨슈머가 이어서 처리한다.
- **독립적인 소비**: 여러 서비스가 같은 토픽을 각자 자기 속도로 읽는다.
- **재처리 가능**: offset을 되돌려 과거 메시지를 다시 처리할 수 있다 (`kafka-consumer-groups.sh --reset-offsets`).

### 단점 / 주의점
- **병렬성이 파티션 수에 묶인다**: 파티션보다 많은 컨슈머는 의미가 없다.
- **리밸런싱 비용**: Eager 방식에서는 처리 전체가 멈추고, 잦은 리밸런싱은 장애처럼 보일 수 있다.
- **중복 처리**: 기본이 at-least-once라 리밸런싱이나 재시작 시 같은 메시지가 다시 올 수 있다.
- **느린 처리 = 리밸런싱 위험**: 처리 시간이 `max.poll.interval.ms`를 넘기면 그룹에서 쫓겨난다.
- **파티션 내 순차 처리**: 한 파티션의 메시지는 순서대로 처리되므로, 메시지 하나가 막히면 그 파티션 전체가 밀린다.

### 자동 커밋 vs 수동 커밋

| | 자동 커밋 | 수동 커밋 |
|---|---|---|
| 구현 | 간단 | 커밋 로직 직접 작성 |
| 정확성 | 중복/유실 가능 | 커밋 시점을 제어해 at-least-once 보장 가능 |
| 적합한 경우 | 일부 유실/중복이 괜찮은 로그 처리 | 주문, 결제 등 정확성이 중요한 처리 |

## 예상 질문
-

## 참고 자료
-
