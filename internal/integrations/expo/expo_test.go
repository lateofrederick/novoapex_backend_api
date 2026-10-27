package expo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExpoSendPushNotifications(t *testing.T) {
	var receivedBody []PushMessage

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/--/api/v2/push/send" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected Content-Type: %s", r.Header.Get("Content-Type"))
		}

		if err := json.NewDecoder(r.Body).Decode(&receivedBody); err != nil {
			t.Errorf("decode error: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		resp := PushResponse{
			Data: []PushTicket{
				{Status: "ok", ID: "ticket-123"},
				{Status: "error", Message: "DeviceNotRegistered", Details: map[string]any{"error": "DeviceNotRegistered"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := New(Config{
		BaseURL: server.URL,
	})

	messages := []PushMessage{
		{To: "ExponentPushToken[valid-1]", Title: "Order Created", Body: "New order"},
		{To: "ExponentPushToken[dead-2]", Title: "Order Created", Body: "New order"},
	}

	tickets, deadTokens, err := client.SendPushNotifications(context.Background(), messages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tickets) != 2 {
		t.Fatalf("expected 2 tickets, got %d", len(tickets))
	}
	if tickets[0].Status != "ok" {
		t.Errorf("expected ticket 0 to be ok, got %s", tickets[0].Status)
	}
	if len(deadTokens) != 1 || deadTokens[0] != "ExponentPushToken[dead-2]" {
		t.Errorf("expected deadTokens [ExponentPushToken[dead-2]], got %v", deadTokens)
	}
	if len(receivedBody) != 2 {
		t.Errorf("server received %d messages, want 2", len(receivedBody))
	}
}
