package inbound

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v2fly/v2ray-core/v5/common/protocol"
	"github.com/v2fly/v2ray-core/v5/proxy/vmess"
)

func TestSSPanelSyncUsersTrafficAndIPs(t *testing.T) {
	const testUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	var trafficPayload []byte
	var alivePayload []byte
	var userRequests atomic.Int32
	var userNotModified atomic.Int32
	var payloadMu sync.Mutex

	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/mod_mu/nodes/21/info":
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"32"}}}`))
		case "/mod_mu/users":
			userRequests.Add(1)
			if request.Header.Get("If-None-Match") == `"users-v1"` {
				userNotModified.Add(1)
				response.WriteHeader(http.StatusNotModified)
				return
			}
			response.Header().Set("ETag", `"users-v1"`)
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
		URL:       panel.URL,
		Key:       "test-key",
		NodeID:    21,
		Refresh:   60,
		ConnLimit: 800,
	})

	ctx := context.Background()
	if err := sync.refreshNodeInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sync.syncUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if userRequests.Load() != 1 {
		t.Fatalf("unexpected user request count: %d", userRequests.Load())
	}
	if err := sync.syncUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if userRequests.Load() != 2 || userNotModified.Load() != 1 {
		t.Fatalf("unexpected conditional counts: requests=%d not-modified=%d",
			userRequests.Load(), userNotModified.Load())
	}
	user := sync.getUser(sync.emailForUser(42))
	if user == nil {
		t.Fatal("expected synchronized user")
	}
	if user.uuid != testUUID || user.alterID != 32 {
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

func TestSSPanelAlterIDZeroAndThirtyTwo(t *testing.T) {
	const testUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/mod_mu/nodes/21/info":
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"0"}}}`))
		case "/mod_mu/nodes/22/info":
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"32"}}}`))
		case "/mod_mu/users":
			_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + testUUID + `"}]}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer panel.Close()

	for _, testCase := range []struct {
		nodeID      int64
		wantAlterID int
	}{
		{nodeID: 21, wantAlterID: 0},
		{nodeID: 22, wantAlterID: 32},
	} {
		clients := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
		defer clients.Close()
		handler := &Handler{
			clients:      clients,
			usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
		}
		sync := newSSPanelSync(handler, &SSPanelConfig{
			URL:       panel.URL,
			Key:       "test-key",
			NodeID:    testCase.nodeID,
			Refresh:   60,
			ConnLimit: 800,
		})
		ctx := context.Background()
		if err := sync.refreshNodeInfo(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sync.syncUsers(ctx); err != nil {
			t.Fatal(err)
		}
		user := sync.getUser(sync.emailForUser(42))
		if user == nil {
			t.Fatalf("node %d did not receive a user", testCase.nodeID)
		}
		memoryUser := user.memoryUser()
		account := memoryUser.Account.(*vmess.MemoryAccount)
		if len(account.AlterIDs) != testCase.wantAlterID {
			t.Fatalf("node %d alter IDs = %d, want %d",
				testCase.nodeID, len(account.AlterIDs), testCase.wantAlterID)
		}
		sync.close()
	}
}

func TestSSPanelEachInboundRequestsItsOwnHeartbeat(t *testing.T) {
	const testUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	var requestsByNode sync.Map
	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		nodeID := request.URL.Query().Get("node_id")
		switch {
		case request.URL.Path == "/mod_mu/users":
			value, _ := requestsByNode.LoadOrStore(nodeID, new(atomic.Int32))
			value.(*atomic.Int32).Add(1)
			response.Header().Set("ETag", `"users-`+nodeID+`"`)
			if request.Header.Get("If-None-Match") == `"users-`+nodeID+`"` {
				response.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + testUUID + `"}]}`))
		case strings.HasPrefix(request.URL.Path, "/mod_mu/nodes/") && strings.HasSuffix(request.URL.Path, "/info"):
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"32"}}}`))
		case strings.HasSuffix(request.URL.Path, "/traffic"), strings.HasSuffix(request.URL.Path, "/aliveip"):
			_, _ = response.Write([]byte(`{"ret":1,"data":"ok"}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer panel.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientsFirst := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	clientsSecond := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	defer clientsFirst.Close()
	defer clientsSecond.Close()
	handlerFirst := &Handler{
		clients:      clientsFirst,
		clientsOwned: true,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}
	handlerSecond := &Handler{
		clients:      clientsSecond,
		clientsOwned: true,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}
	first := newSSPanelSync(handlerFirst, &SSPanelConfig{
		URL: panel.URL, Key: "test-key", NodeID: 31, Refresh: 1, ConnLimit: 800,
	})
	second := newSSPanelSync(handlerSecond, &SSPanelConfig{
		URL: panel.URL, Key: "test-key", NodeID: 32, Refresh: 1, ConnLimit: 800,
	})
	first.start(ctx)
	second.start(ctx)
	defer first.close()
	defer second.close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		firstCount := atomicCount(&requestsByNode, "31")
		secondCount := atomicCount(&requestsByNode, "32")
		if firstCount >= 2 && secondCount >= 2 {
			if handlerFirst.currentClients() != handlerSecond.currentClients() {
				t.Fatal("identical user sets should share the immutable authentication index")
			}
			if first.getUser(first.emailForUser(42)) == second.getUser(second.emailForUser(42)) {
				t.Fatal("per-inbound user accounting objects must remain independent")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("independent heartbeat requests missing: node31=%d node32=%d",
		atomicCount(&requestsByNode, "31"), atomicCount(&requestsByNode, "32"))
}

func atomicCount(values *sync.Map, key string) int32 {
	value, found := values.Load(key)
	if !found {
		return 0
	}
	return value.(*atomic.Int32).Load()
}

func TestSSPanelDifferentUsersDoNotShareAuthIndex(t *testing.T) {
	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		nodeID := request.URL.Query().Get("node_id")
		switch {
		case request.URL.Path == "/mod_mu/users":
			userID := 42
			if nodeID == "32" {
				userID = 43
			}
			_, _ = response.Write([]byte(fmt.Sprintf(
				`{"ret":1,"data":[{"id":%d,"uuid":"4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"}]}`,
				userID,
			)))
		case strings.HasPrefix(request.URL.Path, "/mod_mu/nodes/") && strings.HasSuffix(request.URL.Path, "/info"):
			_, _ = response.Write([]byte(`{"ret":1,"data":{"custom_config":{"alter_id":"32"}}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer panel.Close()

	handlerA := &Handler{
		clients:      vmess.NewTimedUserValidator(protocol.DefaultIDHash),
		clientsOwned: true,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}
	handlerB := &Handler{
		clients:      vmess.NewTimedUserValidator(protocol.DefaultIDHash),
		clientsOwned: true,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}
	syncA := newSSPanelSync(handlerA, &SSPanelConfig{
		URL: panel.URL, Key: "test-key", NodeID: 31, Refresh: 60, ConnLimit: 800,
	})
	syncB := newSSPanelSync(handlerB, &SSPanelConfig{
		URL: panel.URL, Key: "test-key", NodeID: 32, Refresh: 60, ConnLimit: 800,
	})
	defer syncA.close()
	defer syncB.close()
	ctx := context.Background()
	for _, sync := range []*sspanelSync{syncA, syncB} {
		if err := sync.refreshNodeInfo(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sync.syncUsers(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if handlerA.currentClients() == handlerB.currentClients() {
		t.Fatal("different user sets must not share an authentication index")
	}
}

func TestSSPanelConnectionLimit(t *testing.T) {
	handler := &Handler{}
	sync := newSSPanelSync(handler, &SSPanelConfig{ConnLimit: 2})
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
