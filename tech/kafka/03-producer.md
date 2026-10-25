# Producer — acks, 파티셔너, 배치

## 개념

**Producer**는 카프카 토픽에 메시지(Record)를 보내는 클라이언트.
단순히 "보내기"만 하는 것 같지만, 내부적으로는 **직렬화 → 파티션 결정 → 배치로 모으기 → 전송 → 응답 확인/재시도**를 거친다.

### Record 구성

| 필드 | 설명 |
|---|---|
| Topic | 보낼 토픽 (필수) |
| Value | 실제 데이터 (필수) |
| Key | 파티션 결정과 순서 보장에 사용 (선택) |
| Partition | 파티션 직접 지정 (선택) |
| Timestamp | 생성 시각 (기본: 보낸 시각) |
| Headers | 메타데이터 (추적 ID 등) |

### Producer 내부 구성 요소

```
 send(record)
     │
     ▼
┌─────────────┐   ┌─────────────┐   ┌──────────────────────────┐      ┌─────────┐
│ Serializer  │ → │ Partitioner │ → │ Record Accumulator       │      │ Broker  │
│ (객체→byte) │   │ (파티션 결정) │   │  P0: [batch][batch]      │      │         │
└─────────────┘   └─────────────┘   │  P1: [batch]             │      │         │
                                    │  P2: [batch][batch]      │      │         │
                                    └────────────┬─────────────┘      │         │
                                                 │ Sender Thread      │         │
                                                 └───────────────────▶│         │
                                                   (배치 단위 전송)      └─────────┘
```

| 구성 요소 | 역할 |
|---|---|
| **Serializer** | key/value 객체를 byte 배열로 변환 (String, JSON, Avro, Protobuf 등) |
| **Partitioner** | 메시지를 보낼 파티션 결정 ([02-architecture.md](02-architecture.md#1-메시지가-어느-파티션으로-가는가-파티셔닝)) |
| **Record Accumulator** | 파티션별로 메시지를 **배치(batch)** 로 모아두는 버퍼 |
| **Sender Thread** | 별도 스레드에서 배치를 브로커로 전송하고 응답 처리 |

→ `send()`는 메시지를 **버퍼에 넣고 바로 리턴**한다. 실제 네트워크 전송은 Sender 스레드가 비동기로 한다.

## 동작 원리

### 1. 전송 방식: Fire-and-forget / 동기 / 비동기

```java
// 1) Fire-and-forget: 결과 확인 안 함 → 유실돼도 모름
producer.send(record);

// 2) 동기: 응답 올 때까지 대기 → 안전하지만 느림
producer.send(record).get();

// 3) 비동기 + 콜백: 실무에서 주로 사용
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        log.error("전송 실패", exception);
    } else {
        log.info("partition={}, offset={}", metadata.partition(), metadata.offset());
    }
});
```

### 2. acks — 어디까지 저장되면 성공으로 볼 것인가

Producer가 브로커로부터 **"저장 완료" 응답을 언제 받을지** 정하는 설정. 내구성과 속도의 트레이드오프.

| acks | 성공 기준 | 속도 | 유실 가능성 |
|---|---|---|---|
| `0` | 응답을 기다리지 않음 | 가장 빠름 | 높음 (브로커가 못 받아도 모름) |
| `1` | **리더**가 저장하면 성공 | 빠름 | 있음 (리더가 복제 전에 죽으면 유실) |
| `all` (`-1`) | **ISR 전체**가 저장하면 성공 | 느림 | 가장 낮음 |

```
acks=1                              acks=all
Producer → Leader ✅ 응답            Producer → Leader → Follower1 ✅
             ↓ (복제는 나중에)                         → Follower2 ✅
          Follower                                   ↓ 모두 저장 후 응답
리더가 복제 전에 죽으면 유실 ❗          
```

#### acks=all + min.insync.replicas

`acks=all`이어도 ISR에 리더 혼자만 남으면 사실상 `acks=1`과 같다.
그래서 브로커/토픽 설정 `min.insync.replicas`와 함께 쓴다.

- `replication.factor=3`, `min.insync.replicas=2`, `acks=all` ← **가장 많이 쓰는 조합**
- ISR이 2개 미만이면 쓰기를 거부(`NotEnoughReplicasException`) → 유실 대신 실패를 택함
- 브로커 1대가 죽어도 쓰기는 계속 가능

> ISR(In-Sync Replicas): 리더와 동기화가 잘 되고 있는 복제본 목록. [05-replication.md](05-replication.md)에서 자세히.

> 카프카 3.0부터 Producer 기본값이 `acks=all`, `enable.idempotence=true`로 바뀌었다.

### 3. 배치(Batch) — 모아서 보내기

메시지를 하나씩 보내면 네트워크 왕복이 너무 많아진다. 그래서 **파티션별로 모아서 한 번에** 보낸다.

| 설정 | 의미 | 기본값 |
|---|---|---|
| `batch.size` | 배치 하나의 최대 크기 | 16KB |
| `linger.ms` | 배치가 다 안 차도 이 시간만큼 기다렸다 전송 | 5ms (카프카 4.0 이전: 0ms) |
| `buffer.memory` | Accumulator 전체 버퍼 크기 | 32MB |
| `compression.type` | 배치 압축 방식 (`none`, `gzip`, `snappy`, `lz4`, `zstd`) | none |

- **배치가 `batch.size`만큼 차거나, `linger.ms`가 지나면** 전송한다.
- `linger.ms`를 늘리면 → 배치가 커져 **처리량↑, 지연시간↑**
- 압축은 배치 단위로 하기 때문에 배치가 클수록 압축 효율이 좋다.
- 버퍼가 가득 차면 `send()`가 `max.block.ms`(기본 60초)만큼 블록되고, 그래도 안 되면 예외 발생.

### 4. 재시도와 타임아웃

일시적인 오류(리더 변경, 네트워크 끊김 등)가 나면 Producer가 자동으로 재시도한다.

| 설정 | 의미 | 기본값 |
|---|---|---|
| `retries` | 재시도 횟수 | 사실상 무한 (`Integer.MAX_VALUE`) |
| `retry.backoff.ms` | 재시도 간격 | 100ms |
| `delivery.timeout.ms` | `send()` 후 성공/실패가 확정될 때까지 최대 시간 | 120초 |

→ 실무에서는 `retries`보다 **`delivery.timeout.ms`로 전체 시간을 제어**하는 게 권장된다.

### 5. 재시도의 부작용과 멱등성 Producer

재시도를 하면 두 가지 문제가 생길 수 있다.

**① 중복**
```
Producer → 메시지 A → Broker 저장 ✅
Producer ← (응답이 네트워크에서 유실) ✗
Producer → 메시지 A 재전송 → Broker 또 저장 ❗ (중복)
```

**② 순서 뒤바뀜**
```
batch1 전송 → 실패
batch2 전송 → 성공
batch1 재전송 → 성공      → 저장 순서: batch2, batch1 ❗
```
(`max.in.flight.requests.per.connection`: 응답을 안 받고 동시에 보낼 수 있는 요청 수, 기본 5)

**해결: 멱등성 Producer (`enable.idempotence=true`)**
- Producer마다 **Producer ID(PID)** 를, 메시지 배치마다 파티션별 **Sequence Number** 를 붙인다.
- 브로커는 이미 받은 sequence가 다시 오면 **저장하지 않고 성공 응답만** 준다 → 중복 제거.
- sequence가 건너뛰면 거부 → 순서 보장 (in-flight 5개 이하일 때).
- 조건: `acks=all`, `retries > 0`, `max.in.flight.requests.per.connection ≤ 5`

⚠️ 멱등성은 **하나의 Producer 세션, 하나의 파티션 안에서만** 보장된다. Producer가 재시작되면 PID가 바뀐다.
여러 파티션에 걸친 원자적 쓰기는 **트랜잭션**이 필요하다 → [06-delivery.md](06-delivery.md)

## 장단점 / 비교

### 설정별 트레이드오프

| 목표 | 설정 방향 | 대가 |
|---|---|---|
| **유실 방지 (내구성)** | `acks=all`, `min.insync.replicas=2`, 멱등성 ON | 지연시간 증가 |
| **처리량 극대화** | `linger.ms`↑, `batch.size`↑, 압축 사용 | 지연시간 증가, 메모리 사용 |
| **낮은 지연시간** | `linger.ms=0`, 작은 배치 | 처리량 감소, 요청 수 증가 |
| **유실 허용, 최고 속도** (로그, 메트릭) | `acks=0` 또는 `1` | 데이터 유실 가능 |

### 압축 방식 비교

| 방식 | 압축률 | CPU 사용 | 특징 |
|---|---|---|---|
| gzip | 높음 | 높음 | 네트워크 대역폭이 부족할 때 |
| snappy | 보통 | 낮음 | 균형형, 많이 사용 |
| lz4 | 보통 | 매우 낮음 | 속도 우선 |
| zstd | 높음 | 보통 | 압축률과 속도 모두 좋음 (2.1+) |

### Producer 설계의 장점
- **비동기 + 배치**로 높은 처리량을 낸다.
- 자동 재시도와 멱등성으로 일시적인 장애에도 **중복/유실 없이** 전송할 수 있다.
- 설정만으로 내구성과 성능 사이를 **사용 사례에 맞게** 조절할 수 있다.

### 주의점
- `send()`가 바로 리턴되기 때문에 **콜백으로 실패를 반드시 처리**해야 한다. 안 하면 유실돼도 모른다.
- 애플리케이션 종료 시 `flush()` / `close()`를 호출하지 않으면 버퍼에 남은 메시지가 유실된다.
- 멱등성은 Producer 측 중복만 막는다. **Consumer 측 중복 처리는 별도로** 대비해야 한다.
- 순서가 중요하면 key 설계 + 멱등성 ON을 함께 고려해야 한다.

## 예상 질문
-

## 참고 자료
-
