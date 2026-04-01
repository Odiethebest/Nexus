# Nexus in Plain English

## What this project does

Nexus is a notification hub.

Your app sends one event, for example order paid, and Nexus handles delivery to multiple channels such as email, in app updates, and webhooks.

You can think of it as a single place that manages notification delivery for your system.

## Why people use it

When teams send notifications directly from business code, problems show up quickly.

- Third party services can be slow and block requests
- Temporary failures can cause missed notifications
- Retries can create duplicate sends

Nexus is built to reduce that risk and make delivery behavior predictable.

## How the dashboard works

The page has two main areas.

On the left, you publish an event.

- Type: what happened, such as order, payment, alert
- Priority: high, normal, or low
- Payload: event details in JSON format
- Publish button: sends the event

Use this sample payload for a first test.

```json
{"user_id":"u123","email":"user@example.com"}
```

On the right, you see the live event feed.

After publishing, a new card should appear with the event type, priority, payload, and time marker.

If the top right status says Connected, live updates are active.

## First run in two minutes

1. Set Type to order
2. Choose high priority
3. Paste the sample payload
4. Click Publish
5. Confirm a new item appears in the live feed
6. Repeat with normal priority
7. Repeat with low priority

After this, you have already tested the full basic flow.

## If something feels wrong

- Nothing appears after publish: check whether payload is valid JSON and uses double quotes
- Status shows disconnected: publish can still work, but live updates may not appear immediately
- Not sure which type to use: start with order and then try payment or refund

## What you get from Nexus

- One event can be delivered to multiple channels
- Webhook delivery has automatic retry with backoff
- Duplicate processing is reduced with Redis idempotency keys
- Delivery history is stored for troubleshooting and auditing

## A simple real world example

In an ecommerce app, when an order is paid:

- The customer receives an email receipt
- The app can show a new in app message
- A CRM system can receive a webhook

All of this starts from one published event.
