- Start the containers with `docker compose up`
- visit the [Kafka UI](http://localhost:8080)
- make a `POST` request to `http://localhost:8000` (webhook handler app) with this payload:

```json
{
  "timestamp": 1,
  "description": "OOPS!",
  "severity": 5
}
```

Webhook Handler app accepts requests, filters out low severity, then sends messages on the `webhooks` topic. The Alerts app consumes messages from the `webhooks` topic, transforms the messages, and sends them over the `alerts` topic. The Notifications app consumes events from the `alerts` topic.
