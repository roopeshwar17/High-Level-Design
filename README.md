# Notification Service — Senior Software Engineer System Design

A GitHub-ready system-design reference for preparing for SDE-2 / Senior Software Engineer interviews at product-based companies. The design focuses on asynchronous delivery, Kafka and queues, retries, DLQs, idempotency, provider failures, and delivery tracking.

## Contents

- [1. Problem and requirements](#1-problem-and-requirements)
- [2. Capacity estimation](#2-capacity-estimation)
- [3. High-level architecture](#3-high-level-architecture)
- [4. End-to-end flow](#4-end-to-end-flow)
- [5. API design](#5-api-design)
- [6. Kafka and queue design](#6-kafka-and-queue-design)
- [7. Retries and dead-letter queues](#7-retries-and-dead-letter-queues)
- [8. Delivery tracking](#8-delivery-tracking)
- [9. Idempotency and duplicate prevention](#9-idempotency-and-duplicate-prevention)
- [10. Data model and storage](#10-data-model-and-storage)
- [11. Scaling and failure handling](#11-scaling-and-failure-handling)
- [12. Monitoring and security](#12-monitoring-and-security)
- [13. Senior-level interview questions](#13-senior-level-interview-questions)
- [14. Common design mistakes](#14-common-design-mistakes)
- [15. A 45-minute interview plan](#15-a-45-minute-interview-plan)
- [16. One-minute summary](#16-one-minute-summary)
- [References](#references)

---

## 1. Problem and requirements

Design a notification platform used by multiple product services to send **email, SMS, push, and in-app notifications** reliably, including during traffic spikes or external provider outages.

### Functional requirements

| Requirement | What the system supports |
|---|---|
| Notification ingestion | Internal services submit notification requests. |
| Multiple channels | Email, SMS, mobile push, and in-app notifications. |
| Templates | A notification type and its variables produce channel-specific content. |
| User preferences | Channel/category opt-outs and applicable quiet hours. |
| Priority | OTPs and security alerts should not wait behind marketing campaigns. |
| Reliable processing | Transient failures retry; exhausted or poison messages can be inspected and replayed. |
| Delivery tracking | Status per channel and destination, not only one status for the whole request. |
| In-app inbox | Paginated notification history, read state, and unread counts. |
| Scheduling and bulk delivery | Future reminders and large campaigns without overwhelming providers. |

### Non-functional requirements

- **Availability:** The API should remain available when an external delivery provider is down.
- **Durability:** Once a request is accepted, it should not silently disappear.
- **Scalability:** Support high write throughput and bursts.
- **Latency:** Critical messages should normally be dispatched within seconds; bulk campaigns may have a longer delivery window.
- **Consistency:** Eventual consistency is acceptable for delivery status and inbox updates. Acceptance, deduplication, and state transitions must be reliable.
- **Isolation:** A failure in one delivery channel should not stop the other channels.
- **Security:** Protect recipient data, provider credentials, and user preferences.

**Scope boundary:** Focus on the delivery pipeline, correctness, recovery, and scaling. Designing the marketing UI or the internals of email/SMS/push providers is usually unnecessary.

## 2. Capacity estimation

State assumptions, then calculate request and delivery-job volume. These example numbers are illustrative, not mandatory requirements.

Assume 50 million daily active users, five logical notifications per user per day, and an average of 1.5 delivery channels per notification.

| Metric | Estimate |
|---|---:|
| Logical notifications/day | 250 million |
| Individual channel-delivery jobs/day | 375 million |
| Average logical notification rate | ~2,900/sec |
| Average delivery-job rate | ~4,300/sec |
| Peak delivery rate at 5× average | ~22,000/sec |

**Important insight:** One logical notification can create multiple delivery jobs. A notification routed through push, email, and in-app creates three channel jobs. Multiple registered devices can create additional push-delivery jobs.

At this scale, direct synchronous calls to providers make the API slow and fragile. Durable queues absorb bursts, while independent workers process each channel at its sustainable rate.

## 3. High-level architecture

![Notification Service High-Level Architecture](images/notification-service-architecture.png)

[Open the scalable SVG diagram](images/notification-service-architecture.svg) · [View the diagram generator](diagrams/build_diagram.py)

### Main components

1. **API Gateway:** Authentication, validation, per-producer rate limits, and request-size limits.
2. **Notification API:** Validates the notification type and required variables, handles idempotency, creates the notification ID, and returns `202 Accepted` after durable acceptance.
3. **Request database + transactional outbox:** When the request must be recorded in a database before processing, commit the request and an outbox event in one database transaction.
4. **Outbox publisher:** Publishes outbox events to Kafka. If the system chooses Kafka as the initial durable acceptance boundary, it may publish directly and omit the outbox database path.
5. **Kafka incoming topic:** Decouples product services from delivery workers and enables buffering, independent consumers, and replay.
6. **Processing/fan-out workers:** Resolve templates, preferences, endpoint/device tokens, quiet hours, policy checks, and eligible channels.
7. **Channel queues and workers:** Email, SMS, push, and in-app work independently, with channel-specific concurrency, retries, and provider rate limits.
8. **Delivery status service:** Records attempts, processes provider callbacks, deduplicates events, and maintains the delivery lifecycle.
9. **Retry topics and DLQs:** Make transient retry scheduling durable and preserve failed messages for diagnosis and controlled replay.
10. **Scheduler:** Enqueues future-dated work when due and enforces expiry rules.

### The four core design decisions

- The API does **not** deliver messages synchronously. It durably accepts the request and returns a notification ID.
- Kafka decouples ingestion from processing.
- Each channel has independent queues/workers, so a slow SMS provider does not stop email or push.
- Delivery tracking is separate: provider acceptance is not the same as confirmed delivery.

## 4. End-to-end flow

1. A producer (Order Service, Payment Service, Security Service, etc.) calls `POST /v1/notifications`.
2. The API authenticates the producer, validates the request, and checks the idempotency key.
3. The service durably accepts the request and returns `202 Accepted` with a `notification_id`. This means **accepted for processing**, not delivered.
4. The incoming event is published to Kafka. Consumers process it asynchronously.
5. Workers resolve the template and preferences, check opt-outs/quiet hours, apply rate limits, and determine eligible destinations.
6. Fan-out creates a delivery job for each eligible channel and, where needed, each recipient endpoint/device.
7. Channel workers call the external provider or store the in-app notification in the inbox database.
8. Synchronous provider responses and asynchronous webhooks update delivery status and attempt history.
9. Transient errors go through a bounded retry mechanism. Messages that exhaust their retry budget go to a DLQ.
10. Delivery-status APIs expose the current per-channel outcome. In-app read state is tracked separately.

## 5. API design

Keep the API surface small and make the semantics explicit.

### A. Submit a notification

`POST /v1/notifications`

Request example:

```json
{
  "idempotency_key": "order-789-shipped",
  "user_id": "user-123",
  "type": "ORDER_SHIPPED",
  "priority": "HIGH",
  "channels": ["PUSH", "EMAIL", "IN_APP"],
  "data": {
    "order_id": "789",
    "carrier": "FedEx"
  }
}
```

Response example:

```json
{
  "notification_id": "notif-abc",
  "status": "ACCEPTED"
}
```

Return `202 Accepted` because delivery is asynchronous. Do not return success if durable acceptance failed. Validate notification type, required template variables, allowed channels, expiry, and producer permissions.

### B. Get notification delivery status

`GET /v1/notifications/{notification_id}`

```json
{
  "notification_id": "notif-abc",
  "deliveries": [
    { "channel": "PUSH", "status": "DELIVERED" },
    { "channel": "EMAIL", "status": "PROVIDER_ACCEPTED" },
    { "channel": "IN_APP", "status": "READ" }
  ]
}
```

This is an example of independent per-channel status. Email may be accepted by a provider but not yet confirmed delivered, while the in-app notification may already have been read.

### C. User-facing APIs

| API | Purpose |
|---|---|
| `GET /v1/users/{userId}/notifications` | Fetch inbox with cursor-based pagination. |
| `PATCH /v1/notifications/{notificationId}` | Mark an in-app notification as read. |
| `PUT /v1/users/{userId}/notification-preferences` | Update channel/category preferences. |
| `POST /v1/notifications/bulk` | Optional endpoint to create a bulk job and return a `batch_id`. |

A bulk API must not synchronously expand millions of recipients or send every message inside the HTTP request. Process recipients incrementally in chunks.

## 6. Kafka and queue design

### Why a queue?

Without a queue:

`Notification API → Email/SMS/Push provider`

The request thread waits for external network calls. Provider latency and failure can exhaust API capacity.

With a queue:

`Notification API → Kafka → Workers → Provider`

The API's responsibility ends at durable acceptance. Kafka buffers traffic, decouples services, and retains events for recovery/replay.

### Suggested topics/queues

| Topic / queue | Purpose |
|---|---|
| `notification.requested` | Incoming logical notification requests. |
| `notification.email` | Email delivery jobs. |
| `notification.sms` | SMS delivery jobs. |
| `notification.push` | Push delivery jobs. |
| `notification.in_app` | In-app inbox writes. |
| `notification.retry.*` | Delayed retry stages or scheduled retry events. |
| `notification.dlq.*` | Poison messages and exhausted deliveries. |
| `notification.delivery-events` | Attempt results, provider responses, and status updates. |

Separate high-priority and bulk topics/queues—or equivalent isolated worker capacity—when critical traffic must be protected from bulk campaigns. A priority field alone does not help if all messages still compete for the same constrained worker pool.

### Partitions, keys, and ordering

- **Partitions provide parallelism.** Consumers in the same group share partition work; adding consumers helps only up to useful partition parallelism.
- Choose `user_id` as the key if per-user ordering within a topic is required.
- Choose `notification_id` or `delivery_id` if ordering is unnecessary and spreading independent jobs is more important.
- Kafka ordering is per partition, not global across partitions or topics. A hot key can become a bottleneck.

### Durability and consumer processing

For a high-reliability Kafka deployment, consider replication factor 3, producer `acks=all`, and an appropriate `min.insync.replicas` policy. Set values based on the deployment's durability/availability needs.

Commit consumer offsets **after the relevant downstream action or handoff is durable**. If the worker crashes before offset commit, Kafka may redeliver the event, so handlers must be idempotent.

For Kafka-to-Kafka processing, Kafka transactions can atomically publish output events and commit consumed offsets. If a database or an external provider is involved, extra idempotency and recovery are still required.

### Kafka vs. SQS vs. RabbitMQ

| Technology | Choose it when |
|---|---|
| **Kafka** | High-throughput event pipelines, replay/retention, several independent consumer groups, and stream processing are important. |
| **Amazon SQS** | A managed work queue, simpler operations, acknowledgements/visibility timeouts, and managed DLQs are attractive. |
| **RabbitMQ** | Traditional message-queue semantics, flexible routing, acknowledgements, and work distribution fit the workload. |

For this design, Kafka is a reasonable default for the high-volume event pipeline. In an AWS-centric system with simpler replay needs, SQS may be operationally simpler. Avoid combining Kafka and SQS without a specific requirement.

## 7. Retries and dead-letter queues

**Do not retry every error.** Classify the failure first.

| Failure type | Handling |
|---|---|
| Transient: network issue, timeout, provider HTTP 5xx | Retry with exponential backoff and jitter. |
| Provider throttling / HTTP 429 | Respect `Retry-After` where supplied and apply provider-aware rate limiting. |
| Permanent: invalid email/phone, permanently invalid token, opt-out | Do not retry. Record the failure or skip reason. Invalidate bad endpoints where appropriate. |
| Ambiguous timeout | Provider may already have accepted the message. Retry only with care; use provider idempotency or reconcile where possible. |

### Backoff

A simplified retry delay is:

\[
d_n = \min(d_{\max}, d_0 \times 2^n) + \text{jitter}
\]

For example, the retry delays could be approximately 1 second, 5 seconds, 30 seconds, and 2 minutes, depending on the total retry window, message expiry, and provider policy. These values are examples, not universal defaults.

Jitter prevents many failed messages from retrying simultaneously.

### Retry flow with Kafka

Kafka is not itself a general-purpose delayed-message queue. Do not sleep for minutes inside a consumer while blocking useful work.

1. On a transient failure, publish the job to a retry topic or delayed-retry mechanism.
2. Store metadata such as `delivery_id`, attempt number, next retry time, failure reason, and expiry.
3. Make the job eligible for processing when its retry time arrives.
4. Once the retry budget is exhausted, route the job to a DLQ.
5. Acknowledge/commit the original event only after the recovery path is durably recorded.

**Crash window to discuss:** If a worker publishes a retry event and crashes before committing the original offset, duplicate retry events may result. Use Kafka transactions for Kafka-to-Kafka forwarding where appropriate, or make retry consumers idempotent.

### What to store in the DLQ

Preserve the original payload (or a secure reference), `notification_id`, `delivery_id`, channel, attempt count, timestamps, correlation ID, provider, and last error code/message. Alert on DLQ growth, investigate the root cause, and replay only after the issue is fixed. Replayed jobs must go through normal deduplication rules.

## 8. Delivery tracking

A provider API response that indicates acceptance does **not** necessarily mean the user received the message.

### Suggested lifecycle states

| Status | Meaning |
|---|---|
| `ACCEPTED` | The platform durably accepted the original request. |
| `QUEUED` | A channel delivery job is waiting for processing. |
| `PROCESSING` | A worker is processing a delivery. |
| `RETRY_SCHEDULED` | A transient failure occurred and another attempt is scheduled. |
| `PROVIDER_ACCEPTED` | The external provider accepted the send request. |
| `DELIVERED` | A provider receipt or channel-specific mechanism confirmed delivery. |
| `FAILED` | A permanent error occurred or the retry policy was exhausted. |
| `BOUNCED` | The email provider reported a bounce. |
| `SKIPPED` | Delivery was not attempted, for example due to opt-out or no eligible endpoint. |
| `READ` | The user read the in-app notification. This is distinct from provider delivery. |

Not all providers support all statuses. For example, an email provider may accept a send request and emit a delivery or bounce webhook later. A push provider accepting a request does not prove that the OS displayed the notification on the device.

### Track at channel and destination level

Track each delivery independently. One phone/device may succeed while another endpoint fails; email may be accepted while push is delivered. Keep parent request status separate from each delivery's state.

### Processing and webhook flow

1. Record or identify the delivery attempt.
2. Call the provider.
3. Persist the synchronous response and provider message ID when available.
4. Receive asynchronous provider webhooks when supported.
5. Verify/validate the callback, deduplicate it using provider event ID when available, and update the delivery lifecycle.
6. Keep an append-only attempt history alongside the current state.

Make webhooks idempotent because providers may redeliver them. Validate state transitions so stale callbacks do not overwrite a newer status.

### Do not lose delivery results

A worker could send a message successfully and crash before persisting success. A robust design uses durable result events (for example, a `notification.delivery-events` topic) and a separate status consumer, with retries and deduplication. The provider call and the internal database update generally cannot be committed as one atomic transaction.

## 9. Idempotency and duplicate prevention

This is a high-value senior-level topic.

Kafka processing is commonly designed around **at-least-once delivery/processing**. A worker may perform an operation and crash before committing its offset, so the message can be replayed.

Use multiple safeguards:

1. **Producer idempotency:** Accept an idempotency key tied to the originating business event, such as `order-789-shipped`.
2. **Durable deduplication:** Enforce uniqueness of the key within the appropriate producer/tenant scope using a unique database constraint or durable deduplication store.
3. **Stable delivery identity:** Assign a `delivery_id` to each channel/destination job. Reprocessing must not create a second internal delivery or inbox item.
4. **Provider idempotency:** Use a provider-supported idempotency key where available.
5. **Idempotent status handling:** Duplicate events must not double-count metrics or repeatedly mutate counters.
6. **State-transition validation:** Ignore or reconcile stale/duplicate updates instead of blindly overwriting the current state.

### Can we guarantee exactly-once delivery?

**Not universally end-to-end.** Kafka transactions can make Kafka-to-Kafka processing atomic, but they cannot atomically commit both an external SMS/email side effect and an internal database transaction.

Example:

1. The worker calls an SMS provider.
2. The provider accepts the SMS.
3. The worker crashes before recording success or committing its Kafka offset.
4. The job is processed again.

A duplicate SMS is possible unless the provider supplies a suitable idempotency mechanism. The realistic goal is durable at-least-once processing, idempotent internal operations, and best-effort duplicate prevention at the provider boundary. Do not promise universal exactly-once user-visible delivery.

## 10. Data model and storage

Keep the original request, per-destination delivery state, and attempt history separate.

| Entity | Important fields | Purpose |
|---|---|---|
| `notification_request` | `notification_id`, `idempotency_key`, `user_id`, `type`, `priority`, `created_at`, `expires_at` | Original logical request. |
| `notification_delivery` | `delivery_id`, `notification_id`, `channel`, `destination_id`, `status`, `attempt_count`, `next_attempt_at`, `provider_message_id` | Current state for each delivery destination. |
| `delivery_attempt` | `delivery_id`, `attempt_number`, `provider`, `result`, `error_code`, `attempted_at` | Attempt audit history. |
| `user_preferences` | `user_id`, channel preferences, category preferences, quiet hours, timezone | Consent, category, and timing policies. |
| `notification_template` | `type`, `channel`, `template_version`, `content` | Channel-specific message rendering. |
| `notification_inbox` | `user_id`, `notification_id`, `created_at`, `read_at`, content or reference | In-app history/read state. |
| `outbox_event` | `event_id`, `aggregate_id`, `event_type`, `payload`, `published_at` | Reliable event publication from a database transaction. |

### Database selection

| Data | Sensible starting choice | At larger scale |
|---|---|---|
| Requests, templates, preferences, scheduling metadata | PostgreSQL or MySQL | Partition or scale reads/writes when needed. |
| In-app inbox | PostgreSQL for moderate workload | DynamoDB/Cassandra for high-volume, predictable user-keyed access. |
| Delivery state and attempt history | Relational database initially | Partitioned history tables or a write-optimized store. |
| Preferences cache, device-token cache, rate-limit counters | Redis where low latency is useful | Clustered Redis or equivalent distributed cache. |
| Events | Kafka | Scale partitions and consumers based on throughput/lag. |

Design inbox access around the common query: “fetch the latest 20 notifications for this user.” Use cursor pagination and an index/partition strategy aligned to user ID and creation time. Design attempt-history lookups around actual query patterns such as `notification_id` and time range.

### Retention

Delivery attempts grow faster than logical requests because each request can have multiple channels and retries. Define retention policies for inbox items, current status, detailed attempt logs, DLQ messages, and audit records. Archive or expire operational history according to product and compliance requirements.

## 11. Scaling and failure handling

### A. Traffic spikes and bulk campaigns

A campaign to millions of users must not starve transactional notifications.

- Use a separate bulk-processing path and chunked fan-out.
- Resolve recipient lists incrementally and fetch preferences in batches.
- Publish delivery jobs in controlled batches.
- Apply per-provider rate limits.
- Reserve capacity for critical traffic using separate priority queues or worker quotas.
- Pause or throttle marketing traffic first during overload.

### B. Third-party provider outage

If SMS becomes unavailable:

1. Detect elevated errors and open a circuit breaker.
2. Stop making futile provider calls while it is unhealthy.
3. Keep jobs durably queued or in controlled retry processing.
4. Allow email, push, and in-app delivery to continue independently.
5. After recovery, resume at a controlled rate.

A fallback provider is justified only if the business requirement warrants its added complexity. An ambiguous timeout is not automatically safe for provider failover, because the original provider may already have accepted the message.

### C. Kafka backlog

A backlog is not automatically data loss, but it can violate delivery-latency targets. Monitor consumer lag and oldest-message age. Add consumers when partition parallelism and downstream capacity allow; throttle producers if needed. More workers can worsen rate limiting when the provider is the bottleneck.

### D. Database or cache failure

Use replicated databases and stateless API/worker instances across availability zones. Define behavior when Redis is unavailable: for example, fallback to the authoritative preferences database within its capacity, or apply a controlled fail-closed policy for security-sensitive consent decisions. Cache data must not be treated as the only durable source of truth.

### E. Scheduling and expiry

Store future-dated requests in scheduler metadata/storage and enqueue when due. Quiet hours may defer noncritical notifications to the allowed window. Time-sensitive notifications such as OTPs must include an expiry deadline; expired messages should be discarded rather than sent late.

## 12. Monitoring and security

### Monitoring

Track:

- Ingestion rate, API latency, and acceptance/error rates.
- Queue depth, consumer lag, and oldest queued-message age.
- Delivery success and permanent-failure rates by channel/provider.
- End-to-end latency and time spent queued.
- Retry counts, provider throttling, and circuit-breaker state.
- DLQ count, age, and replay outcomes.
- Delivery-status update lag and webhook failures.

Alert on user-impacting symptoms: critical-message latency, sustained provider failures, increasing queue age, and rapid DLQ growth.

### Security and abuse prevention

- Authenticate internal producers and authorize which notification types they may send.
- Enforce authentication/authorization on inbox and preference APIs; one user must not be able to access another user's notifications.
- Protect provider secrets, encrypt data in transit and at rest, and minimize personal data in Kafka payloads and logs.
- Enforce per-producer quotas, user-level notification limits, and marketing opt-outs.
- Validate provider webhooks and prevent spoofed callback events.

## 13. Senior-level interview questions

### Q1. Why not call the provider directly from the API?

Queues decouple latency, absorb bursts, and allow independent delivery retries and worker scaling. The producer does not depend on the external provider completing its request.

### Q2. Why have an incoming topic and channel-specific queues?

Incoming events centralize request processing and fan-out. Channel queues isolate provider-specific throughput, retries, and failures.

### Q3. What if a consumer crashes after sending an SMS but before committing its offset?

The event may replay. Use stable delivery identities and provider idempotency where supported. Universal exactly-once external delivery cannot be guaranteed.

### Q4. What goes into a DLQ?

Poison messages and deliveries that have exhausted the retry policy. Permanent recipient/token errors should not be pointlessly retried. DLQ messages need alerts, diagnosis, and controlled replay.

### Q5. How do you keep marketing campaigns from delaying OTPs?

Isolate priority lanes, reserve worker capacity for critical traffic, and throttle bulk campaigns.

### Q6. What does a successful provider API response mean?

Often only that the provider accepted the request. Confirmed delivery requires a receipt or channel-specific confirmation, where available.

### Q7. Why do we need the transactional outbox?

When the request database and Kafka must both reflect one logical operation, the outbox prevents an unsafe database-write-plus-publish dual write. The publisher and consumers still require deduplication.

### Q8. How do you scale to tens of thousands of deliveries per second?

Estimate fan-out, increase partitions/consumer parallelism where useful, partition high-write data, batch bulk work, and identify whether Kafka, the database, or provider quotas are the real bottleneck.

### Q9. What if provider success is received but the status DB is unavailable?

Use a durable delivery-result event/record and retry status persistence. Do not assume the send and database update are atomic. Make result processing idempotent and reconcile where provider APIs allow it.

### Q10. How do you choose the Kafka partition key?

Use `user_id` if per-user ordering is a hard requirement, or a stable notification/delivery key when order is unnecessary and spreading load matters more. Explain hot-key risk and the fact that Kafka ordering is per partition.

## 14. Common design mistakes

| Mistake | Better approach |
|---|---|
| Claiming `202 Accepted` means delivered | Separate acceptance, provider acceptance, confirmed delivery, and read state. |
| Claiming Kafka guarantees exactly-once delivery to a phone | Explain the external side-effect boundary and use idempotency where supported. |
| Retrying all failures forever | Classify errors, use bounded backoff, and route exhausted messages to a DLQ. |
| Using one shared queue for all channels/priorities | Isolate channel failures and protect critical traffic from campaigns. |
| Committing offsets before downstream work is durable | Commit after durable handoff and make replay safe. |
| Assuming a provider send and DB update are atomic | Handle the crash window with durable attempt/result handling and provider idempotency. |
| Introducing many databases and queues without a requirement | Start from access patterns and scale only genuine bottlenecks. |
| Treating provider acceptance as user receipt | Use provider receipts/webhooks only where supported and document status semantics. |

## 15. A 45-minute interview plan

| Time | Discussion |
|---|---|
| 0–5 min | Requirements, assumptions, and capacity estimation. |
| 5–12 min | High-level architecture and end-to-end flow. |
| 12–20 min | Kafka, channel queues, fan-out, and data model. |
| 20–30 min | Retries, DLQs, delivery tracking, and idempotency. |
| 30–38 min | Provider failures, overload, scaling, and bulk traffic. |
| 38–45 min | Trade-offs and interviewer follow-ups. |

The senior-level distinction is to explain **what happens when an operation fails between two steps**, not just name components. Explain exactly when offsets are committed, how retry work becomes durable, when a message enters a DLQ, how duplicate sends are mitigated, and what status an API reports when provider confirmation is missing.

## 16. One-minute summary

“I would build the Notification Service as a durable asynchronous pipeline. The API validates and idempotently accepts requests, then uses Kafka to decouple ingestion from delivery. Fan-out workers apply templates and preferences and create channel-specific jobs. Isolated email, SMS, push, and in-app worker pools protect channels from each other's failures. Transient failures use bounded exponential backoff with jitter; permanent failures are not retried, and exhausted jobs go to DLQs with controlled replay. Delivery status is tracked per channel and destination, with provider webhooks processed idempotently. Since the external provider send and our database commit cannot generally be atomic, I would design for at-least-once processing, durable state, and best-effort duplicate prevention rather than promise universal exactly-once delivery.”

## References

- [AlgoMaster — Design Notification Service](https://algomaster.io/learn/system-design-interviews/design-notification-service)
- [YouTube video 1](https://www.youtube.com/watch?v=Yv9q8d11Y7k)
- [YouTube video 2](https://www.youtube.com/watch?v=IScGMqojqKE)

---

### Repository files

- `README.md` — the complete interview notes.
- `images/notification-service-architecture.png` — architecture diagram optimized for viewing in GitHub.
- `images/notification-service-architecture.svg` — scalable vector version of the diagram.
- `diagrams/build_diagram.py` — script used to regenerate the diagram (requires Python and CairoSVG).
