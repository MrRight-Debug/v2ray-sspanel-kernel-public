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
	Generation uint64
	FetchedAt  time.Time
	SourceNode int64
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
	etag        string
	lastModify  string
	etagSource  int64
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
		nodes: make(map[int64]int),
		ready: make(chan struct{}),
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
	for {
		err := m.fetchUserSnapshot(ctx)
		m.markReady()
		delay := m.refreshInterval()
		if err != nil {
			failures++
			newError("sspanel: shared user sync failed").Base(err).AtWarning().WriteToLog()
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

func (m *sharedSSPanelManager) sourceNode() int64 {
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
		return 0
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	nodeID := nodes[m.sourceIndex%len(nodes)]
	return nodeID
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

func (m *sharedSSPanelManager) fetchUserSnapshot(ctx context.Context) error {
	source := m.sourceNode()
	if source == 0 {
		return newError("no active shared user source")
	}
	m.mu.Lock()
	etag := m.etag
	lastModified := m.lastModify
	if m.etagSource != source {
		etag = ""
		lastModified = ""
	}
	m.mu.Unlock()

	headers := make(http.Header)
	if etag != "" {
		headers.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		headers.Set("If-Modified-Since", lastModified)
	}
	response := new(sspanelUsersResponse)
	query := url.Values{"node_id": []string{strconv.FormatInt(source, 10)}}
	meta, err := m.requestJSON(ctx, http.MethodGet, "users", query, nil, response, headers)
	if err != nil {
		m.mu.Lock()
		m.sourceIndex++
		m.etag = ""
		m.lastModify = ""
		m.etagSource = 0
		m.mu.Unlock()
		return err
	}
	if meta.NotModified {
		return nil
	}
	if response.Ret != 1 {
		m.mu.Lock()
		m.sourceIndex++
		m.etag = ""
		m.lastModify = ""
		m.etagSource = 0
		m.mu.Unlock()
		return newError("sspanel rejected shared user list request")
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
	current := m.snapshot.Load()
	if current != nil && current.Hash == hash {
		m.mu.Lock()
		m.etag = meta.ETag
		m.lastModify = meta.LastModified
		m.etagSource = source
		m.mu.Unlock()
		return nil
	}

	userByID := make(map[int64]*sharedSSPanelUser, len(users))
	for index := range users {
		userByID[users[index].ID] = &users[index]
	}
	m.mu.Lock()
	m.generation++
	generation := m.generation
	m.etag = meta.ETag
	m.lastModify = meta.LastModified
	m.etagSource = source
	m.mu.Unlock()
	m.snapshot.Store(&sharedSSPanelSnapshot{
		Users:      users,
		ByID:       userByID,
		Hash:       hash,
		Generation: generation,
		FetchedAt:  time.Now(),
		SourceNode: source,
	})
	m.pruneAccounts(users)
	return nil
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
