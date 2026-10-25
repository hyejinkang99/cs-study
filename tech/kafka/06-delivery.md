# 전달 보장 — at-most / at-least / exactly-once

## 개념

**전달 보장(Delivery Semantics)** 은 장애가 나도 메시지가 **몇 번 처리되는지**에 대한 약속.
네트워크는 언제든 끊길 수 있고, 프로세스는 언제든 죽을 수 있기 때문에 이 약속이 필요하다.

| 보장 수준 | 의미 | 유실 | 중복 |
|---|---|---|---|
| **At-most-once** | 최대 한 번. 실패하면 그냥 버림 | ⭕ 가능 | ❌ 없음 |
| **At-least-once** | 최소 한 번. 실패하면 다시 시도 | ❌ 없음 | ⭕ 가능 |
| **Exactly-once** | 정확히 한 번 | ❌ 없음 | ❌ 없음 |

전달 보장은 **Producer → Broker** 구간과 **Broker → Consumer** 구간을 각각 따져야 한다.

```
[Producer] ──①──▶ [Broker] ──②──▶ [Consumer] ──③──▶ [처리 결과 (DB 등)]
  ① 전송 중 유실/중복?    ② 읽고 처리하는 중 유실/중복?
```

## 동작 원리

### 1. At-most-once

**Producer**: `acks=0` 또는 재시도 없음 → 실패해도 다시 보내지 않음
**Consumer**: **커밋 먼저, 처리 나중**

```
poll() → 커밋 ✅ → 처리 중 죽음 💥 → 재시작 → 커밋된 다음부터 읽음 → 해당 메시지 유실
```

- 사용 사례: 일부 유실돼도 괜찮은 메트릭, 로그 수집

### 2. At-least-once (카프카의 일반적인 기본 동작)

**Producer**: `acks=all` + 재시도 → 응답을 못 받으면 다시 보냄 (중복 가능)
**Consumer**: **처리 먼저, 커밋 나중**

```
poll() → 처리 ✅ → 커밋 전에 죽음 💥 → 재시작 → 마지막 커밋부터 다시 읽음 → 같은 메시지 다시 처리
```

- 리밸런싱 때도 같은 일이 생긴다: 처리는 했지만 커밋 전에 파티션이 다른 컨슈머로 넘어가면 그쪽에서 다시 처리.
- 사용 사례: 대부분의 실무 시스템. **중복은 컨슈머의 멱등 처리로 해결**한다.

### 3. Exactly-once (EOS)

카프카의 Exactly-once는 두 가지 기능으로 구성된다.

#### ① 멱등성 Producer — Producer → Broker 구간의 중복 제거

- `enable.idempotence=true` (3.0부터 기본값)
- PID + Sequence Number로 브로커가 재전송된 중복 메시지를 걸러낸다.
- 한계: **한 파티션, 한 Producer 세션 안에서만** 보장. → [03-producer.md](03-producer.md#5-재시도의-부작용과-멱등성-producer)

#### ② 트랜잭션 — 여러 파티션 쓰기 + offset 커밋을 원자적으로

**"읽고 → 처리하고 → 다른 토픽에 쓰는" (consume-transform-produce)** 과정 전체를 하나의 단위로 묶는다.

```
input-topic ──▶ [ 읽기 → 변환 → 쓰기 ] ──▶ output-topic
                         + offset 커밋
                └──── 하나의 트랜잭션 ────┘
          모두 성공(commit) 하거나 모두 취소(abort)
```

```java
producer.initTransactions();   // transactional.id 등록, 이전 세션의 미완료 트랜잭션 정리

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    producer.beginTransaction();
    try {
        for (ConsumerRecord<String, String> record : records) {
            producer.send(new ProducerRecord<>("output-topic", record.key(), transform(record.value())));
        }
        // 컨슈머 offset 커밋도 트랜잭션에 포함
        producer.sendOffsetsToTransaction(currentOffsets(records), consumer.groupMetadata());
        producer.commitTransaction();
    } catch (KafkaException e) {
        producer.abortTransaction();  // 쓴 메시지와 offset 커밋 모두 취소
    }
}
```

**트랜잭션 동작 방식**

| 구성 요소 | 역할 |
|---|---|
| `transactional.id` | Producer를 식별하는 고정 ID. 재시작해도 같은 ID를 쓰면 이전 트랜잭션을 이어받아 정리 |
| **Transaction Coordinator** | 트랜잭션 상태를 관리하는 브로커 |
| `__transaction_state` | 트랜잭션 상태가 저장되는 내부 토픽 |
| **Control Marker** | 각 파티션에 기록되는 COMMIT / ABORT 표시 |
| **Producer Epoch** | 같은 `transactional.id`로 새 Producer가 뜨면 증가 → 이전(좀비) Producer의 쓰기를 차단 (**Zombie Fencing**) |

1. `beginTransaction()` 후 보낸 메시지는 **일단 파티션에 바로 기록**된다 (아직 미확정 상태).
2. `commitTransaction()` 하면 코디네이터가 관련된 모든 파티션에 **COMMIT 마커**를 쓴다.
3. 실패하면 **ABORT 마커**를 쓴다. 메시지는 남아 있지만 읽을 때 무시된다.

#### ③ Consumer 쪽: isolation.level

| `isolation.level` | 동작 |
|---|---|
| `read_uncommitted` (기본) | 트랜잭션 미완료/취소된 메시지까지 모두 읽음 |
| `read_committed` | **커밋된 트랜잭션의 메시지만** 읽음 |

- `read_committed` 컨슈머는 **LSO (Last Stable Offset)** 까지만 읽는다. LSO는 아직 진행 중인 트랜잭션 중 가장 앞선 위치.
- ⚠️ 트랜잭션을 쓰는 Producer가 있다면 **Consumer도 반드시 `read_committed`** 로 설정해야 EOS가 완성된다.

#### Kafka Streams에서는

설정 하나로 위의 모든 것이 적용된다.
```properties
processing.guarantee=exactly_once_v2
```

### 4. Exactly-once의 한계 — 외부 시스템이 끼면?

카프카 트랜잭션은 **카프카 안에서만** (카프카 → 처리 → 카프카) exactly-once를 보장한다.
**DB 저장, 외부 API 호출, 이메일 발송** 같은 외부 작업은 카프카 트랜잭션으로 되돌릴 수 없다.

```
Consumer → DB에 결제 저장 ✅ → offset 커밋 전 죽음 💥 → 재처리 → 결제 중복 저장 ❗
```

**해결 방법**

| 방법 | 설명 |
|---|---|
| **멱등 Consumer** | 메시지에 고유 ID를 두고 이미 처리한 ID면 건너뜀 (처리 이력 테이블, DB unique 제약) |
| **Upsert** | `INSERT ... ON DUPLICATE KEY UPDATE`처럼 여러 번 실행해도 결과가 같은 연산 사용 |
| **offset을 DB에 함께 저장** | 처리 결과와 offset을 같은 DB 트랜잭션으로 저장. 재시작 시 DB의 offset부터 `seek()` |
| **Transactional Outbox** (Producer 쪽) | DB 변경과 "보낼 이벤트"를 같은 DB 트랜잭션으로 outbox 테이블에 저장 → 별도 프로세스(CDC 등)가 카프카로 발행 |

```java
// 멱등 Consumer 예시
@Transactional
public void handle(OrderEvent event) {
    if (processedEventRepository.existsById(event.getEventId())) {
        return; // 이미 처리한 메시지 → 무시
    }
    paymentService.pay(event);
    processedEventRepository.save(new ProcessedEvent(event.getEventId()));
}
```

> 실무에서 흔히 말하는 "exactly-once"는 대부분 **at-least-once 전달 + 멱등 처리 = 결과적으로 정확히 한 번 (effectively-once)** 이다.

## 장단점 / 비교

### 보장 수준별 비교

| | At-most-once | At-least-once | Exactly-once |
|---|---|---|---|
| **Producer 설정** | `acks=0`, 재시도 X | `acks=all`, 재시도 O | 멱등성 + 트랜잭션 |
| **Consumer 커밋** | 처리 전 커밋 | 처리 후 커밋 | 트랜잭션에 offset 포함 |
| **Consumer 설정** | — | — | `read_committed` |
| **성능** | 가장 빠름 | 빠름 | 느림 (트랜잭션 오버헤드) |
| **구현 난이도** | 쉬움 | 보통 (멱등 처리 필요) | 어려움 |
| **사용 사례** | 메트릭, 로그 | 대부분의 서비스 | 스트림 처리, 금융성 집계 |

### Exactly-once의 장점
- 카프카 → 카프카 파이프라인에서 **중복/유실 없이** 처리 가능.
- 애플리케이션이 중복 처리 로직을 직접 짜지 않아도 된다 (Kafka Streams).
- Zombie Fencing으로 장애 후 이전 인스턴스가 끼어드는 문제도 막는다.

### Exactly-once의 단점
- **성능 오버헤드**: 트랜잭션 마커 기록, 코디네이터 통신이 추가된다.
- **지연시간 증가**: `read_committed` 컨슈머는 트랜잭션이 커밋될 때까지 메시지를 볼 수 없다.
- **적용 범위 제한**: 외부 시스템(DB, API)은 보장하지 않는다.
- **복잡도**: `transactional.id` 관리, 예외 처리가 까다롭다.

### 정리
- **기본 전략**: at-least-once + 멱등 Consumer
- **카프카 → 카프카 스트림 처리**: Kafka Streams의 `exactly_once_v2`
- **DB 변경 → 이벤트 발행**: Transactional Outbox

## 예상 질문
-

## 참고 자료
-
