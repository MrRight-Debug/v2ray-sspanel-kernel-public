package inbound

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/v2fly/v2ray-core/v5/common/protocol"
	"github.com/v2fly/v2ray-core/v5/proxy/vmess"
)

func TestSSPanelSyncUsersTrafficAndIPs(t *testing.T) {
	const testUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	var trafficPayload []byte
	var alivePayload []byte
	var userRequests atomic.Int32
	var payloadMu sync.Mutex

	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/mod_mu/nodes/21/info":
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"32"}}}`))
		case "/mod_mu/users":
			userRequests.Add(1)
			_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + testUUID + `"}]}`))
		case "/mod_mu/users/traffic":
			body, _ := io.ReadAll(request.Body)
			payloadMu.Lock()
			trafficPayload = body
			payloadMu.Unlock()
			_, _ = response.Write([]byte(`{"ret":1,"data":"ok"}`))
		case "/mod_mu/users/aliveip":
			body, _ := io.ReadAll(request.Body)
			payloadMu.Lock()
			alivePayload = body
			payloadMu.Unlock()
			_, _ = response.Write([]byte(`{"ret":1,"data":"ok"}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer panel.Close()

	clients := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	defer clients.Close()
	handler := &Handler{
		clients:      clients,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}
	sync := newSSPanelSync(handler, &SSPanelConfig{
		URL:        panel.URL,
		Key:        "test-key",
		NodeID:     21,
		Refresh:    60,
		ConnLimit:  800,
		ShareGroup: "single-test",
	})

	ctx := context.Background()
	sync.manager.acquire(ctx, 21, 60)
	defer sync.manager.release(21)
	sync.manager.waitReady(ctx)
	if err := sync.refreshNodeInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sync.syncUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if userRequests.Load() != 1 {
		t.Fatalf("unexpected user request count: %d", userRequests.Load())
	}
	user := sync.getUser(sync.emailForUser(42))
	if user == nil {
		t.Fatal("expected synchronized user")
	}
	if user.shared.UUID != testUUID || user.alterID != 32 {
		t.Fatalf("unexpected user: %+v", user)
	}
	memoryUser := user.memoryUser()
	account := memoryUser.Account.(*vmess.MemoryAccount)
	if len(account.AlterIDs) != 32 {
		t.Fatalf("unexpected alter ID count: %d", len(account.AlterIDs))
	}

	user.upload.Add(123)
	user.download.Add(456)
	user.enterIP("127.0.0.1")
	user.enterIP("2001:db8::1")
	if err := sync.reportTraffic(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sync.reportAliveIPs(ctx); err != nil {
		t.Fatal(err)
	}

	payloadMu.Lock()
	defer payloadMu.Unlock()
	if !strings.Contains(string(trafficPayload), `"u":123`) || !strings.Contains(string(trafficPayload), `"d":456`) {
		t.Fatalf("unexpected traffic payload: %s", trafficPayload)
	}
	var alive struct {
		Data []struct {
			UserID int64  `json:"user_id"`
			IP     string `json:"ip"`
		} `json:"data"`
	}
	if err := json.Unmarshal(alivePayload, &alive); err != nil {
		t.Fatal(err)
	}
	if len(alive.Data) != 2 || alive.Data[0].UserID != 42 {
		t.Fatalf("unexpected alive IP payload: %s", alivePayload)
	}
}

func TestSSPanelConnectionLimit(t *testing.T) {
	handler := &Handler{}
	sync := newSSPanelSync(handler, &SSPanelConfig{
		URL:        "http://127.0.0.1:1",
		Key:        "test-key",
		NodeID:     1,
		ShareGroup: "connection-limit-test",
		ConnLimit:  2,
	})
	if !sync.acquireConnection() || !sync.acquireConnection() {
		t.Fatal("expected first two connections to succeed")
	}
	if sync.acquireConnection() {
		t.Fatal("expected third connection to be rejected")
	}
	sync.releaseConnection()
	if !sync.acquireConnection() {
		t.Fatal("expected released slot to be reusable")
	}
}
