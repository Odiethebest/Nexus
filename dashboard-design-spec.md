# Nexus Dashboard — Design Spec

## Design Philosophy

Flat, minimal, data-first。去除一切装饰性元素，用留白和层次结构承载信息。配色克制，只在需要传达语义时引入颜色。

---

## Layout

双栏 Grid，左窄右宽（`1fr : 1.6fr`），顶部 Header 横跨全宽。

```
┌─────────────────────────────────────────────┐
│  Nexus Dashboard              ● disconnected │
├──────────────────┬──────────────────────────┤
│                  │                           │
│  Publish Event   │   Live Notifications      │
│                  │                           │
│  [ Type       ]  │   ┌─────────────────────┐│
│  [ Priority ▼ ]  │   │ order  [high] 2m ago ││
│  [ Payload    ]  │   │ {"user_id":"u123"}   ││
│                  │   └─────────────────────┘│
│  [  Publish   ]  │   ...                     │
└──────────────────┴──────────────────────────┘
```

- 两个面板等高，内容各自滚动
- 通知列表 `max-height: 300px`，超出内部滚动，不撑开面板

---

## Color System

### Semantic Colors（用 CSS 变量，自动适配深浅模式）

| 用途 | Token |
|------|-------|
| 页面背景 | `--color-background-tertiary` |
| 面板背景 | `--color-background-primary` |
| 输入框背景 | `--color-background-secondary` |
| 主文字 | `--color-text-primary` |
| 次要文字 | `--color-text-secondary` |
| 占位/提示文字 | `--color-text-tertiary` |
| 默认边框 | `--color-border-tertiary`（0.5px） |
| 悬浮/强调边框 | `--color-border-secondary` |

### Priority Badge Colors（固定色，语义明确）

| Priority | Background | Text |
|----------|-----------|------|
| high | `#FAECE7` | `#993C1D` |
| medium | `#FAEEDA` | `#854F0B` |
| low | `#EAF3DE` | `#3B6D11` |

### Accent

Publish 按钮使用 Purple ramp：`#534AB7` → hover `#3C3489`

---

## Typography

| 元素 | Size | Weight | Color |
|------|------|--------|-------|
| 页面标题 | 18px | 500 | text-primary |
| 面板标签（PANEL TITLE） | 13px | 500 | text-secondary，`letter-spacing: 0.06em`，uppercase |
| 字段 label | 12px | 500 | text-secondary |
| 输入框文字 | 14px | 400 | text-primary |
| Payload textarea | 13px | 400 | text-primary，`font-family: mono` |
| 通知类型 | 12px | 500 | text-primary，mono |
| 通知 payload | 12px | 400 | text-secondary，mono |
| 时间戳 | 11px | 400 | text-tertiary |
| Badge 文字 | 11px | 500 | 对应语义色 |

---

## Components

### Panel

```css
background: var(--color-background-primary);
border: 0.5px solid var(--color-border-tertiary);
border-radius: var(--border-radius-lg);   /* 12px */
padding: 20px;
```

### Input / Select / Textarea

```css
background: var(--color-background-secondary);
border: 0.5px solid var(--color-border-secondary);
border-radius: var(--border-radius-md);   /* 8px */
padding: 9px 12px;
transition: border-color 0.15s;

:focus {
  border-color: var(--color-border-primary);
  box-shadow: 0 0 0 3px rgba(83, 74, 183, 0.08);
}
```

### Priority Badge

```css
display: inline-flex;
align-items: center;
padding: 3px 8px;
border-radius: 20px;   /* pill */
font-size: 11px;
font-weight: 500;
```

Badge 跟随 select 实时变色，在 label 行右侧展示当前值。

### Publish Button

```css
background: #534AB7;
color: #fff;
border: none;
border-radius: var(--border-radius-md);
padding: 10px 16px;
font-size: 14px;
font-weight: 500;
transition: background 0.15s, transform 0.1s;

:hover  { background: #3C3489; }
:active { transform: scale(0.98); }
```

### Notification Card

```css
background: var(--color-background-secondary);
border: 0.5px solid var(--color-border-tertiary);
border-radius: var(--border-radius-md);
padding: 12px 14px;
animation: slideIn 0.2s ease;

@keyframes slideIn {
  from { opacity: 0; transform: translateY(-6px); }
  to   { opacity: 1; transform: translateY(0); }
}
```

卡片头部：左侧 event type（mono），右侧 priority badge + 时间戳。

### Empty State

无通知时居中展示，包含：
- 圆形图标容器（`background-secondary`，36px）
- bell SVG icon（`var(--color-text-tertiary)`）
- 标题 + 说明文字

### Connection Status

Header 右侧，圆点 + 文字。

```
disconnected → dot: #E24B4A
connected    → dot: #639922
```

---

## Interaction Spec

| 动作 | 行为 |
|------|------|
| 修改 Priority select | Badge 实时更新颜色和文字 |
| 点击 Publish | 解析 JSON payload，生成事件卡片插入通知列表顶部，计数 +1 |
| JSON 解析失败 | payload 作为字符串原样展示，不报错 |
| 通知列表 > 0 | 隐藏 empty state，显示卡片列表 |
| 事件计数 | `N event / N events`，显示在通知面板标题旁 |

---

## WebSocket 接入指南

当前 demo 为纯前端模拟。接入真实 WS 只需替换两处：

**发送事件（替换 publishEvent 内容）：**
```js
ws.send(JSON.stringify({ type, priority, payload: parsed }));
```

**接收事件（替换 renderEvents 触发方式）：**
```js
const ws = new WebSocket('wss://your-server/ws');
ws.onopen  = () => setStatus('connected');
ws.onclose = () => setStatus('disconnected');
ws.onmessage = (e) => {
  events.unshift({ ...JSON.parse(e.data), time: new Date() });
  renderEvents();
};
```