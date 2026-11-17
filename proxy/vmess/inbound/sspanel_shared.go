package inbound

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/v2fly/v2ray-core/v5/proxy/vmess"
)

const (
	defaultSharedUsersRefresh = 60
	maxSharedUsersBackoff     = 5 * time.Minute
	sharedManagerStartupDelay = 100 * time.Millisecond
)

type sharedSSPanelUser struct {
	ID             int64
	UUID           string
	NodeSpeedlimit int
}

type sharedSSPanelSnapshot struct {
	Users      []sharedSSPanelUser
	ByID       map[int64]*sharedSSPanelUser
	Hash       [32]byte
	RawHash    [32]byte
	Generation uint64
	FetchedAt  time.Time
	SourceNode int64
}

type sharedSSPanelNodeCache struct {
	ETag         string
	LastModified string
	RawHash      [32]byte
}

type sharedSSPanelAccount struct {
	account *vmess.MemoryAccount
}

type sharedSSPanelManager struct {
	url        string
	key        string
	shareGroup string
	client     *http.Client

	mu          sync.Mutex
	refs        int
	nodes       map[int64]int
	sourceIndex int
	nodeCache   map[int64]sharedSSPanelNodeCache
	divergent   map[int64]string
	generation  uint64
	cancel      context.CancelFunc
	runDone     chan struct{}

	refresh atomic.Int64
	ready   chan struct{}
	readyMu sync.Once

	snapshot atomic.Pointer[sharedSSPanelSnapshot]
	accounts sync.Map
}

type sspanelHTTPMeta struct {
	ETag         string
	LastModified string
	NotModified  bool
}

var sharedSSPanelManagers = struct {
	sync.Mutex
	managers map[string]*sharedSSPanelManager
}{
	managers: make(map[string]*sharedSSPanelManager),
}

func sharedManagerIdentity(urlValue, key, shareGroup string) string {
	sum := sha256.Sum256([]byte(urlValue + "\x00" + key + "\x00" + shareGroup))
	return string(sum[:])
}

func sharedManagerFor(config *SSPanelConfig) *sharedSSPanelManager {
	identity := sharedManagerIdentity(config.URL, config.Key, config.ShareGroup)
	sharedSSPanelManagers.Lock()
	defer sharedSSPanelManagers.Unlock()
	if manager := sharedSSPanelManagers.managers[identity]; manager != nil {
		return manager
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		IdleConnTimeout:       90 * time.Second,
	}
	manager := &sharedSSPanelManager{
		url:        config.URL,
		key:        config.Key,
		shareGroup: config.ShareGroup,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		nodes:     make(map[int64]int),
		nodeCache: make(map[int64]sharedSSPanelNodeCache),
		divergent: make(map[int64]string),
		ready:     make(chan struct{}),
	}
	sharedSSPanelManagers.managers[identity] = manager
	return manager
}

func (m *sharedSSPanelManager) acquire(ctx context.Context, nodeID int64, refresh int) {
	m.mu.Lock()
	m.refs++
	m.nodes[nodeID]++
	if refresh <= 0 {
		refresh = defaultSharedUsersRefresh
	}
	currentRefresh := m.refresh.Load()
	if currentRefresh == 0 || int64(refresh) < currentRefresh {
		m.refresh.Store(int64(refresh))
	}
	if m.cancel == nil {
		runCtx, cancel := context.WithCancel(ctx)
		m.cancel = cancel
		done := make(chan struct{})
		m.runDone = done
		go func() {
			defer close(done)
			m.run(runCtx)
		}()
	}
	m.mu.Unlock()
}

func (m *sharedSSPanelManager) release(nodeID int64) {
	m.mu.Lock()
	if m.refs > 0 {
		m.refs--
	}
	if count := m.nodes[nodeID]; count <= 1 {
		delete(m.nodes, nodeID)
	} else {
		m.nodes[nodeID] = count - 1
	}
	last := m.refs == 0
	var done chan struct{}
	if last && m.cancel != nil {
		m.cancel()
		m.cancel = nil
		done = m.runDone
		m.runDone = nil
	}
	m.mu.Unlock()
	if last && done != nil {
		<-done
		if transport, ok := m.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

func (m *sharedSSPanelManager) markReady() {
	m.readyMu.Do(func() { close(m.ready) })
}

func (m *sharedSSPanelManager) waitReady(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.ready:
	}
}

func (m *sharedSSPanelManager) refreshInterval() time.Duration {
	refresh := m.refresh.Load()
	if refresh <= 0 {
		refresh = defaultSharedUsersRefresh
	}
	return time.Duration(refresh) * time.Second
}

func (m *sharedSSPanelManager) run(ctx context.Context) {
	failures := 0
	startup := time.NewTimer(sharedManagerStartupDelay)
	select {
	case <-ctx.Done():
		startup.Stop()
		return
	case <-startup.C:
	}
	for {
		successes, err := m.refreshUserSnapshots(ctx)
		m.markReady()
		delay := m.refreshInterval()
		if err != nil {
			newError("sspanel: shared user sync failed").Base(err).AtWarning().WriteToLog()
		}
		if successes == 0 {
			failures++
			delay = m.refreshInterval() * time.Duration(1<<minInt(failures, 4))
			if delay > maxSharedUsersBackoff {
				delay = maxSharedUsersBackoff
			}
		} else {
			failures = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func (m *sharedSSPanelManager) orderedNodes() ([]int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes := make([]int64, 0, len(m.nodes))
	for nodeID, count := range m.nodes {
		if count <= 0 {
			continue
		}
		nodes = append(nodes, nodeID)
	}
	if len(nodes) == 0 {
		return nil, 0
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	index := m.sourceIndex % len(nodes)
	source := nodes[index]
	ordered := make([]int64, 0, len(nodes))
	ordered = append(ordered, nodes[index:]...)
	ordered = append(ordered, nodes[:index]...)
	return ordered, source
}

func (m *sharedSSPanelManager) requestJSON(
	ctx context.Context,
	method, path string,
	query url.Values,
	payload, output interface{},
	headers http.Header,
) (*sspanelHTTPMeta, error) {
	requestURL := strings.TrimRight(m.url, "/") + "/mod_mu/" + strings.TrimLeft(path, "/")
	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return nil, newError("invalid sspanel API URL")
	}
	values := parsedURL.Query()
	values.Set("key", m.key)
	for key, entries := range query {
		for _, entry := range entries {
			values.Add(key, entry)
		}
	}
	parsedURL.RawQuery = values.Encode()

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, newError("failed to encode sspanel request")
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), body)
	if err != nil {
		return nil, newError("failed to create sspanel request")
	}
	request.Header.Set("User-Agent", "v2ray-sspanel/5.56.0")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}

	response, err := m.client.Do(request)
	if err != nil {
		var urlError *url.Error
		if errors.As(err, &urlError) {
			err = urlError.Err
		}
		return nil, newError("sspanel API ", path, " request failed").Base(err)
	}
	defer response.Body.Close()

	meta := &sspanelHTTPMeta{
		ETag:         response.Header.Get("ETag"),
		LastModified: response.Header.Get("Last-Modified"),
	}
	if response.StatusCode == http.StatusNotModified {
		meta.NotModified = true
		return meta, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, newError("sspanel API ", path, " returned HTTP ", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxSSPanelResponseSize))
	if err := decoder.Decode(output); err != nil {
		return nil, newError("failed to decode sspanel API ", path, " response").Base(err)
	}
	return meta, nil
}

func decodeSharedUsers(payload []byte) ([]sharedSSPanelUser, map[int64]*sharedSSPanelUser, [32]byte, error) {
	response := new(sspanelUsersResponse)
	if err := json.Unmarshal(payload, response); err != nil {
		return nil, nil, [32]byte{}, err
	}
	if response.Ret != 1 {
		return nil, nil, [32]byte{}, newError("sspanel rejected shared user list request")
	}
	byID := make(map[int64]sharedSSPanelUser, len(response.Data))
	for _, item := range response.Data {
		if item.ID <= 0 || item.UUID == "" {
			continue
		}
		byID[item.ID] = sharedSSPanelUser{
			ID:             item.ID,
			UUID:           item.UUID,
			NodeSpeedlimit: item.NodeSpeedlimit,
		}
	}
	users := make([]sharedSSPanelUser, 0, len(byID))
	for _, user := range byID {
		users = append(users, user)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	hash := hashSharedUsers(users)
	userByID := make(map[int64]*sharedSSPanelUser, len(users))
	for index := range users {
		userByID[users[index].ID] = &users[index]
	}
	return users, userByID, hash, nil
}

func (m *sharedSSPanelManager) requestUsers(
	ctx context.Context,
	nodeID int64,
	headers http.Header,
) ([]byte, *sspanelHTTPMeta, error) {
	requestURL := strings.TrimRight(m.url, "/") + "/mod_mu/users"
	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return nil, nil, newError("invalid sspanel API URL")
	}
	values := parsedURL.Query()
	values.Set("key", m.key)
	values.Set("node_id", strconv.FormatInt(nodeID, 10))
	parsedURL.RawQuery = values.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
	if err != nil {
		return nil, nil, newError("failed to create sspanel request")
	}
	request.Header.Set("User-Agent", "v2ray-sspanel/5.56.0")
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}

	response, err := m.client.Do(request)
	if err != nil {
		var urlError *url.Error
		if errors.As(err, &urlError) {
			err = urlError.Err
		}
		return nil, nil, newError("sspanel API users request failed").Base(err)
	}
	defer response.Body.Close()

	meta := &sspanelHTTPMeta{
		ETag:         response.Header.Get("ETag"),
		LastModified: response.Header.Get("Last-Modified"),
	}
	if response.StatusCode == http.StatusNotModified {
		meta.NotModified = true
		return nil, meta, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, nil, newError("sspanel API users returned HTTP ", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxSSPanelResponseSize+1))
	if err != nil {
		return nil, nil, newError("failed to read sspanel users response").Base(err)
	}
	if len(payload) > maxSSPanelResponseSize {
		return nil, nil, newError("sspanel users response exceeds size limit")
	}
	return payload, meta, nil
}

func (m *sharedSSPanelManager) refreshUserSnapshots(ctx context.Context) (int, error) {
	nodes, source := m.orderedNodes()
	if source == 0 {
		return 0, newError("no active shared user source")
	}

	current := m.snapshot.Load()
	successes := 0
	sourceOK := false
	var sourceRawHash [32]byte
	var candidate *sharedSSPanelSnapshot
	var firstErr error
	checked := make(map[int64]struct{}, len(nodes))
	divergent := make(map[int64]string)

	for _, nodeID := range nodes {
		m.mu.Lock()
		cache := m.nodeCache[nodeID]
		m.mu.Unlock()

		headers := make(http.Header)
		if cache.ETag != "" {
			headers.Set("If-None-Match", cache.ETag)
		}
		if cache.LastModified != "" {
			headers.Set("If-Modified-Since", cache.LastModified)
		}

		payload, meta, err := m.requestUsers(ctx, nodeID, headers)
		if err != nil {
			if firstErr == nil {
				firstErr = newError("node ", nodeID, ": ").Base(err)
			}
			continue
		}

		successes++
		checked[nodeID] = struct{}{}
		cacheUsable := true
		rawHash := cache.RawHash
		if meta.NotModified {
			if rawHash == ([32]byte{}) && current != nil {
				rawHash = current.RawHash
			}
		} else {
			rawHash = sha256.Sum256(payload)
		}

		if nodeID == source {
			if meta.NotModified {
				if current == nil {
					if firstErr == nil {
						firstErr = newError("node ", nodeID, ": received 304 without a shared snapshot")
					}
					cacheUsable = false
				} else {
					sourceOK = true
					sourceRawHash = current.RawHash
				}
			} else if current == nil || rawHash != current.RawHash {
				users, byID, hash, decodeErr := decodeSharedUsers(payload)
				if decodeErr != nil {
					if firstErr == nil {
						firstErr = newError("node ", nodeID, ": ").Base(decodeErr)
					}
					cacheUsable = false
				} else {
					sourceOK = true
					sourceRawHash = rawHash
					candidate = &sharedSSPanelSnapshot{
						Users:      users,
						ByID:       byID,
						Hash:       hash,
						RawHash:    rawHash,
						FetchedAt:  time.Now(),
						SourceNode: source,
					}
				}
			} else {
				sourceOK = true
				sourceRawHash = rawHash
			}
		} else if sourceOK && rawHash != sourceRawHash {
			divergent[nodeID] = "shared user response differs from source node"
		}

		if cacheUsable {
			m.mu.Lock()
			if !meta.NotModified {
				cache.ETag = meta.ETag
				cache.LastModified = meta.LastModified
			} else {
				if meta.ETag != "" {
					cache.ETag = meta.ETag
				}
				if meta.LastModified != "" {
					cache.LastModified = meta.LastModified
				}
			}
			cache.RawHash = rawHash
			m.nodeCache[nodeID] = cache
			m.mu.Unlock()
		}
	}

	var newlyDivergent []int64
	m.mu.Lock()
	for nodeID := range checked {
		reason, isDivergent := divergent[nodeID]
		_, wasDivergent := m.divergent[nodeID]
		switch {
		case isDivergent:
			m.divergent[nodeID] = reason
			if !wasDivergent {
				newlyDivergent = append(newlyDivergent, nodeID)
			}
		case wasDivergent:
			delete(m.divergent, nodeID)
		}
	}

	if candidate != nil {
		if current == nil || candidate.Hash != current.Hash {
			m.generation++
			candidate.Generation = m.generation
		} else {
			candidate.Generation = current.Generation
			candidate.Users = current.Users
			candidate.ByID = current.ByID
		}
		m.snapshot.Store(candidate)
	}
	if !sourceOK {
		m.sourceIndex++
	}
	m.mu.Unlock()

	for _, nodeID := range newlyDivergent {
		newError("sspanel: shared user snapshot mismatch for node ", nodeID).AtWarning().WriteToLog()
	}
	if candidate != nil {
		m.pruneAccounts(candidate.Users)
	}
	if firstErr != nil {
		return successes, firstErr
	}
	if !sourceOK {
		return successes, newError("shared source node did not provide a valid snapshot")
	}
	return successes, nil
}

func (m *sharedSSPanelManager) fetchUserSnapshot(ctx context.Context) error {
	_, err := m.refreshUserSnapshots(ctx)
	return err
}

func (m *sharedSSPanelManager) snapshotForNode(nodeID int64) (*sharedSSPanelSnapshot, error) {
	reason, divergent := m.divergenceForNode(nodeID)
	if divergent {
		return nil, newError("sspanel: shared user snapshot isolated for node ", nodeID).Base(errors.New(reason))
	}
	snapshot := m.snapshot.Load()
	if snapshot == nil {
		return nil, newError("shared sspanel user snapshot unavailable")
	}
	return snapshot, nil
}

func (m *sharedSSPanelManager) divergenceForNode(nodeID int64) (string, bool) {
	m.mu.Lock()
	reason, divergent := m.divergent[nodeID]
	m.mu.Unlock()
	return reason, divergent
}

func hashSharedUsers(users []sharedSSPanelUser) [32]byte {
	hasher := sha256.New()
	var numeric [8]byte
	for _, user := range users {
		binary.LittleEndian.PutUint64(numeric[:], uint64(user.ID))
		_, _ = hasher.Write(numeric[:])
		_, _ = hasher.Write([]byte(user.UUID))
		_, _ = hasher.Write([]byte{0})
		binary.LittleEndian.PutUint64(numeric[:], uint64(user.NodeSpeedlimit))
		_, _ = hasher.Write(numeric[:])
	}
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func (m *sharedSSPanelManager) accountFor(uuid string, alterID uint32) (*vmess.MemoryAccount, error) {
	cacheKey := uuid + ":" + strconv.FormatUint(uint64(alterID), 10)
	if cached, found := m.accounts.Load(cacheKey); found {
		return cached.(*sharedSSPanelAccount).account, nil
	}
	rawAccount := &vmess.Account{Id: uuid, AlterId: alterID}
	accountValue, err := rawAccount.AsAccount()
	if err != nil {
		return nil, err
	}
	account := accountValue.(*vmess.MemoryAccount)
	actual, _ := m.accounts.LoadOrStore(cacheKey, &sharedSSPanelAccount{account: account})
	return actual.(*sharedSSPanelAccount).account, nil
}

func (m *sharedSSPanelManager) pruneAccounts(users []sharedSSPanelUser) {
	active := make(map[string]struct{}, len(users))
	for _, user := range users {
		active[user.UUID] = struct{}{}
	}
	m.accounts.Range(func(key, _ interface{}) bool {
		cacheKey, ok := key.(string)
		if !ok {
			return true
		}
		separator := strings.LastIndexByte(cacheKey, ':')
		if separator <= 0 {
			m.accounts.Delete(key)
			return true
		}
		if _, found := active[cacheKey[:separator]]; !found {
			m.accounts.Delete(key)
		}
		return true
	})
}
