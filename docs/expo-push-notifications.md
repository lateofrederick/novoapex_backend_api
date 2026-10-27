# Expo Push Notifications Integration Guide for Vendor Mobile App (`asa-dwaso-shop`)

This document specifies the backend API contracts, payload formats, Android notification channels, and client-side handling patterns for push notifications in the vendor mobile app.

---

## 1. Device Token Management

Vendors register their Expo Push Token upon logging in and permission grant, and unregister on logout.

### 1.1 Register / Upsert Push Token

- **Endpoint**: `POST /devices/push-token`
- **Auth**: Required (`Authorization: Bearer <sessionToken>`)
- **Headers**: `Content-Type: application/json`

#### Request Body
```json
{
  "token": "ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]",
  "platform": "ios", // "ios" | "android" | "web"
  "deviceName": "Ama's iPhone 15 Pro" // optional, max 120 chars
}
```

#### Response (`200 OK`)
```json
{
  "success": true,
  "message": "Push token registered successfully"
}
```

---

### 1.2 Remove / Deregister Push Token

Call this immediately upon vendor logout so the device stops receiving business alerts.

- **Endpoint**: `DELETE /devices/push-token`
- **Auth**: Required (`Authorization: Bearer <sessionToken>`)
- **Headers**: `Content-Type: application/json`

#### Request Body
```json
{
  "token": "ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]"
}
```

#### Response (`200 OK`)
```json
{
  "success": true,
  "message": "Push token removed successfully"
}
```

---

## 2. Notification Event Payloads

Every push notification sent by NovoApex follows the standard Expo Push Notification Envelope.

### Expo Message Structure
```json
{
  "to": "ExponentPushToken[...]",
  "title": "🎉 New Order Placed!",
  "subtitle": "Order #f3acb7ee",
  "body": "Ama Mensah ordered 2 items totaling GHS 90.00.",
  "sound": "default",
  "badge": 1,
  "channelId": "orders",
  "priority": "high",
  "data": {
    "type": "ORDER_CREATED",
    "url": "/orders/f3acb7ee-7617-4dea-8bb7-8370f4ae3cce",
    "entityId": "f3acb7ee-7617-4dea-8bb7-8370f4ae3cce",
    "timestamp": "2026-10-27T13:40:00Z",
    "metadata": {
      "orderId": "f3acb7ee-7617-4dea-8bb7-8370f4ae3cce",
      "customerPhone": "233244111222",
      "customerName": "Ama Mensah",
      "totalAmount": "90.00",
      "currency": "GHS",
      "itemCount": 2
    }
  }
}
```

---

## 3. Supported Event Types & Metadata

| Event Type | Channel ID | Deep Link (`data.url`) | Description |
| :--- | :--- | :--- | :--- |
| `ORDER_CREATED` | `orders` | `/orders/{orderId}` | WhatsApp bot confirmed customer order turn |
| `ORDER_PAID` | `orders` | `/orders/{orderId}` | Paystack charge settled and verified |
| `ORDER_CANCELLED` | `orders` | `/orders/{orderId}` | Unpaid invoice expired or customer cancelled |
| `HANDOFF_REQUESTED`| `handoffs`| `/inbox/{conversationId}`| Bot requested human assist (low confidence / complex inquiry) |
| `HANDOFF_ESCALATED`| `handoffs`| `/inbox/{conversationId}`| Customer triggered escalation keyword (e.g. refund/agent) |
| `INVENTORY_LOW_STOCK`| `alerts` | `/inventory/{productId}` | Stock dropped to $\le 3$ units |

---

## 4. TypeScript Definitions

```typescript
export type PushEventType =
  | 'ORDER_CREATED'
  | 'ORDER_PAID'
  | 'ORDER_CANCELLED'
  | 'HANDOFF_REQUESTED'
  | 'HANDOFF_ESCALATED'
  | 'INVENTORY_LOW_STOCK';

export type NotificationChannelId = 'orders' | 'handoffs' | 'alerts' | 'default';

export interface PushNotificationData<T = Record<string, unknown>> {
  type: PushEventType;
  url: string;            // Deep link for Expo Router: router.push(data.url)
  entityId: string;       // orderId, conversationId, or productId
  timestamp: string;      // ISO 8601 UTC string
  metadata: T;
}

export interface OrderCreatedMetadata {
  orderId: string;
  customerPhone: string;
  customerName?: string;
  totalAmount: string;
  currency: string;
  itemCount: number;
}

export interface OrderPaidMetadata {
  orderId: string;
  customerPhone: string;
  amount: string;
  currency: string;
  channel?: string;
  paystackRef?: string;
}

export interface OrderCancelledMetadata {
  orderId: string;
  customerPhone: string;
  reason?: string;
}

export interface HandoffMetadata {
  conversationId: string;
  customerPhone: string;
  customerName?: string;
  triggerReason?: string;
  lastMessageSnippet?: string;
}

export interface LowStockMetadata {
  productId: string;
  productName: string;
  currentStock: number;
  threshold: number;
}
```

---

## 5. Android Notification Channels Setup

In Android 8.0+ (API level 26+), notification channels are required before push notifications can display.

```typescript
import * as Notifications from 'expo-notifications';
import { Platform } from 'react-native';

export async function setupAndroidNotificationChannels() {
  if (Platform.OS !== 'android') return;

  await Notifications.setNotificationChannelAsync('orders', {
    name: 'Customer Orders',
    description: 'Notifications for new customer orders and payments',
    importance: Notifications.AndroidImportance.HIGH,
    vibrationPattern: [0, 250, 250, 250],
    lightColor: '#25D366', // WhatsApp green accent
    sound: 'default',
    enableLights: true,
    enableVibrate: true,
  });

  await Notifications.setNotificationChannelAsync('handoffs', {
    name: 'Live Chat & Support Handoffs',
    description: 'Urgent customer requests requiring immediate human vendor response',
    importance: Notifications.AndroidImportance.MAX,
    vibrationPattern: [0, 500, 200, 500],
    lightColor: '#FF3B30',
    sound: 'default',
    enableLights: true,
    enableVibrate: true,
  });

  await Notifications.setNotificationChannelAsync('alerts', {
    name: 'Operational Alerts',
    description: 'Low stock and business alerts',
    importance: Notifications.AndroidImportance.HIGH,
    sound: 'default',
    enableVibrate: true,
  });
}
```

---

## 6. Token Registration & Deep-Link Handler

```typescript
import * as Notifications from 'expo-notifications';
import * as Device from 'expo-device';
import Constants from 'expo-constants';
import { router } from 'expo-router';
import { Platform } from 'react-native';

export async function registerForPushNotificationsAsync(apiClient: any): Promise<string | null> {
  if (!Device.isDevice) {
    console.warn('Must use physical device for Push Notifications');
    return null;
  }

  await setupAndroidNotificationChannels();

  const { status: existingStatus } = await Notifications.getPermissionsAsync();
  let finalStatus = existingStatus;

  if (existingStatus !== 'granted') {
    const { status } = await Notifications.requestPermissionsAsync();
    finalStatus = status;
  }

  if (finalStatus !== 'granted') {
    console.warn('Permission not granted for push notifications');
    return null;
  }

  const projectId = Constants?.expoConfig?.extra?.eas?.projectId ?? Constants?.easConfig?.projectId;
  const tokenData = await Notifications.getExpoPushTokenAsync({ projectId });
  const token = tokenData.data;

  // Register with NovoApex backend
  await apiClient.post('/devices/push-token', {
    token,
    platform: Platform.OS,
    deviceName: Device.modelName ?? undefined,
  });

  return token;
}

export function subscribeToNotificationNavigation() {
  // When vendor taps notification while app is running in background or closed
  const responseListener = Notifications.addNotificationResponseReceivedListener(response => {
    const data = response.notification.request.content.data as { url?: string };
    if (data?.url) {
      router.push(data.url as any);
    }
  });

  return () => {
    Notifications.removeNotificationSubscription(responseListener);
  };
}
```
