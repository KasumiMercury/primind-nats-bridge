# nats-bridge

NATS JetStreamからHTTPへのイベント転送ブリッジ

- NATS JetStreamイベントの購読
- HTTPリクエストへの変換・転送
- W3C Trace Context伝播
- メッセージAck/Nack処理

## ルート設定

設定ファイル: `nats-bridge.json`

- Subject: `remind.cancelled` → Endpoint: `http://throttling:8080/api/v1/remind/cancel`

## 設定ファイル

```json
{
  "nats": {
    "url": "nats://nats:4222",
    "stream": "REMIND_EVENTS",
    "consumer_group": "nats-http-bridge"
  },
  "http": {
    "timeout": "30s"
  },
  "routes": [
    {
      "subject": "remind.cancelled",
      "method": "POST",
      "endpoint": "http://throttling:8080/api/v1/remind/cancel",
      "content_type": "application/json"
    }
  ]
}
```

## メッセージ処理フロー

1. NATS JetStreamからメッセージ受信
2. W3C Trace Contextの抽出
3. HTTPリクエストの構築
   - Body: NATSメッセージペイロード
   - Headers: Content-Type, x-request-id, traceparent, tracestate, X-NATS-*
4. HTTPリクエスト送信
5. レスポンス処理
   - 2xx-3xx: メッセージAck
   - 4xx-5xx: メッセージNack（リトライ）

## 依存サービス

- NATS 2.10 (JetStream)
