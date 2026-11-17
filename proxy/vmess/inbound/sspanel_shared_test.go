package inbound

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v2fly/v2ray-core/v5/common/protocol"
	"github.com/v2fly/v2ray-core/v5/proxy/vmess"
)

func TestSharedSSPanelETagAndResponseHashDedupe(t *testing.T) {
	const userUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	var requests atomic.Int32
	var notModified atomic.Int32
	var forceChangedETag atomic.Bool

	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/mod_mu/users" {
			http.NotFound(response, request)
			return
		}
		requests.Add(1)
		if request.Header.Get("If-None-Match") == `"users-v1"` && !forceChangedETag.Load() {
			notModified.Add(1)
			response.WriteHeader(http.StatusNotModified)
			return
		}
		etag := `"users-v1"`
		if forceChangedETag.Load() {
			etag = `"users-v2"`
		}
		response.Header().Set("ETag", etag)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + userUUID + `"}]}`))
	}))
	defer panel.Close()

	config := &SSPanelConfig{
		URL:        panel.URL,
		Key:        "shared-key",
		NodeID:     1,
		Refresh:    60,
		ShareGroup: "shared-test",
	}
	manager := sharedManagerFor(config)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager.acquire(ctx, 1, 60)
	manager.acquire(ctx, 2, 60)
	defer manager.release(1)
	defer manager.release(2)
	manager.waitReady(ctx)

	first := manager.snapshot.Load()
	if first == nil || len(first.Users) != 1 {
		t.Fatal("expected initial shared snapshot")
	}
	if first.Users[0].UUID != userUUID {
		t.Fatalf("unexpected shared user: %+v", first.Users[0])
	}

	if err := manager.fetchUserSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	second := manager.snapshot.Load()
	if second != first {
		t.Fatal("expected 304 to preserve the immutable snapshot")
	}
	if notModified.Load() != 1 {
		t.Fatalf("expected one conditional request, got %d", notModified.Load())
	}

	forceChangedETag.Store(true)
	if err := manager.fetchUserSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	third := manager.snapshot.Load()
	if third != second {
		t.Fatal("expected response hash dedupe to preserve immutable snapshot")
	}
	if third.Generation != first.Generation {
		t.Fatalf("generation changed unexpectedly: %d", third.Generation)
	}
	if requests.Load() != 3 {
		t.Fatalf("unexpected request count: %d", requests.Load())
	}
}

func TestSharedSSPanelProjectionKeepsPerInboundValidators(t *testing.T) {
	const userUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/mod_mu/users" {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("ETag", `"users-v1"`)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + userUUID + `"}]}`))
	}))
	defer panel.Close()

	configA := &SSPanelConfig{
		URL:        panel.URL,
		Key:        "shared-key",
		NodeID:     1,
		Refresh:    60,
		ShareGroup: "projection-test",
	}
	configB := *configA
	configB.NodeID = 2

	clientsA := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	defer clientsA.Close()
	clientsB := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	defer clientsB.Close()
	syncA := newSSPanelSync(&Handler{
		clients:      clientsA,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}, configA)
	syncB := newSSPanelSync(&Handler{
		clients:      clientsB,
		usersByEmail: newUserByEmail(&DefaultConfig{AlterId: 32}),
	}, &configB)
	if syncA.manager != syncB.manager {
		t.Fatal("expected one shared manager")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	syncA.manager.acquire(ctx, configA.NodeID, configA.Refresh)
	syncA.manager.acquire(ctx, configB.NodeID, configB.Refresh)
	defer syncA.manager.release(configA.NodeID)
	defer syncB.manager.release(configB.NodeID)
	syncA.manager.waitReady(ctx)
	if err := syncA.syncUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if err := syncB.syncUsers(ctx); err != nil {
		t.Fatal(err)
	}

	userA := syncA.getUser(syncA.emailForUser(42))
	userB := syncB.getUser(syncB.emailForUser(42))
	if userA == nil || userB == nil {
		t.Fatal("expected a user projection in each inbound")
	}
	if userA.shared != userB.shared {
		t.Fatal("expected shared immutable user metadata")
	}
	if userA.account != userB.account {
		t.Fatal("expected shared VMess account metadata for the same alter ID")
	}
	if fmt.Sprintf("%p", syncA.users) == fmt.Sprintf("%p", syncB.users) {
		t.Fatal("inbound projection maps must remain independent")
	}
	if !strings.Contains(userA.email, "-1-") || !strings.Contains(userB.email, "-2-") {
		t.Fatalf("unexpected projection emails: %s %s", userA.email, userB.email)
	}
}

func TestSharedSSPanelFailureRetainsLastSnapshot(t *testing.T) {
	const userUUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
	var failing atomic.Bool
	panel := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/mod_mu/users" {
			http.NotFound(response, request)
			return
		}
		if failing.Load() {
			http.Error(response, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("ETag", `"users-v1"`)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ret":1,"data":[{"id":42,"uuid":"` + userUUID + `"}]}`))
	}))
	defer panel.Close()

	manager := sharedManagerFor(&SSPanelConfig{
		URL:        panel.URL,
		Key:        "shared-key",
		NodeID:     1,
		Refresh:    60,
		ShareGroup: "retain-test",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager.acquire(ctx, 1, 60)
	defer manager.release(1)
	manager.waitReady(ctx)
	first := manager.snapshot.Load()
	if first == nil {
		t.Fatal("expected initial snapshot")
	}
	failing.Store(true)
	if err := manager.fetchUserSnapshot(ctx); err == nil {
		t.Fatal("expected refresh failure")
	}
	if manager.snapshot.Load() != first {
		t.Fatal("failed refresh must retain last valid snapshot")
	}
}
