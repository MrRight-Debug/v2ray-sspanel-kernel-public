package inbound

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/v2fly/v2ray-core/v5/common/protocol"
	"github.com/v2fly/v2ray-core/v5/proxy/vmess"
	"github.com/v2fly/v2ray-core/v5/transport/internet"
)

const maxSSPanelResponseSize = 16 << 20

type flexibleUint32 uint32

func (v *flexibleUint32) UnmarshalJSON(data []byte) error {
	raw := strings.Trim(strings.TrimSpace(string(data)), `"`)
	if raw == "" || raw == "null" {
		*v = 0
		return nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return err
	}
	*v = flexibleUint32(parsed)
	return nil
}

type sspanelNodeInfoResponse struct {
	Ret  int `json:"ret"`
	Data struct {
		NodeSpeedlimit int `json:"node_speedlimit"`
		Sort           int `json:"sort"`
		CustomConfig   struct {
			AlterID *flexibleUint32 `json:"alter_id"`
		} `json:"custom_config"`
	} `json:"data"`
}

type sspanelUsersResponse struct {
	Ret  int `json:"ret"`
	Data []struct {
		ID             int64  `json:"id"`
		UUID           string `json:"uuid"`
		NodeSpeedlimit int    `json:"node_speedlimit"`
	} `json:"data"`
}

type sspanelUser struct {
	id         int64
	email      string
	uuid       string
	alterID    uint32
	level      uint32
	account    *vmess.MemoryAccount
	upload     atomic.Int64
	download   atomic.Int64
	reportedUp atomic.Int64
	reportedDn atomic.Int64

	ipMu     sync.Mutex
	activeIP map[string]int
}

type sharedAuthUser struct {
	id      int64
	email   string
	uuid    string
	alterID uint32
	level   uint32
}

type sharedAuthBucket struct {
	validator *vmess.TimedUserValidator
	users     map[string]*protocol.MemoryUser
	refs      int
}

var sharedAuthBuckets = struct {
	sync.Mutex
	buckets map[[32]byte]*sharedAuthBucket
}{
	buckets: make(map[[32]byte]*sharedAuthBucket),
}

func sharedAuthKey(users []sharedAuthUser) [32]byte {
	hasher := sha256.New()
	var numeric [8]byte
	for _, user := range users {
		binary.LittleEndian.PutUint64(numeric[:], uint64(user.id))
		_, _ = hasher.Write(numeric[:])
		_, _ = hasher.Write([]byte(user.email))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(user.uuid))
		_, _ = hasher.Write([]byte{0})
		binary.LittleEndian.PutUint64(numeric[:], uint64(user.alterID))
		_, _ = hasher.Write(numeric[:])
		binary.LittleEndian.PutUint64(numeric[:], uint64(user.level))
		_, _ = hasher.Write(numeric[:])
	}
	var key [32]byte
	copy(key[:], hasher.Sum(nil))
	return key
}

func acquireSharedAuth(users []sharedAuthUser) ([32]byte, *sharedAuthBucket, error) {
	sort.Slice(users, func(i, j int) bool { return users[i].email < users[j].email })
	key := sharedAuthKey(users)

	sharedAuthBuckets.Lock()
	defer sharedAuthBuckets.Unlock()
	if bucket := sharedAuthBuckets.buckets[key]; bucket != nil {
		bucket.refs++
		return key, bucket, nil
	}

	validator := vmess.NewTimedUserValidator(protocol.DefaultIDHash)
	memoryUsers := make(map[string]*protocol.MemoryUser, len(users))
	for _, item := range users {
		rawAccount := &vmess.Account{Id: item.uuid, AlterId: item.alterID}
		accountValue, err := rawAccount.AsAccount()
		if err != nil {
			validator.Close()
			return [32]byte{}, nil, err
		}
		memoryUser := &protocol.MemoryUser{
			Level:   item.level,
			Email:   item.email,
			Account: accountValue,
		}
		if err := validator.Add(memoryUser); err != nil {
			validator.Close()
			return [32]byte{}, nil, err
		}
		memoryUsers[item.email] = memoryUser
	}
	bucket := &sharedAuthBucket{
		validator: validator,
		users:     memoryUsers,
		refs:      1,
	}
	sharedAuthBuckets.buckets[key] = bucket
	return key, bucket, nil
}

func releaseSharedAuth(bucket *sharedAuthBucket) {
	if bucket == nil {
		return
	}
	sharedAuthBuckets.Lock()
	if bucket.refs > 0 {
		bucket.refs--
	}
	last := bucket.refs == 0
	if last {
		for key, candidate := range sharedAuthBuckets.buckets {
			if candidate == bucket {
				delete(sharedAuthBuckets.buckets, key)
				break
			}
		}
	}
	sharedAuthBuckets.Unlock()
	if last {
		bucket.validator.Close()
	}
}

func newSSPanelUser(id int64, email, uuid string, alterID, level uint32, account *vmess.MemoryAccount) *sspanelUser {
	return &sspanelUser{
		id:       id,
		email:    email,
		uuid:     uuid,
		alterID:  alterID,
		level:    level,
		account:  account,
		activeIP: make(map[string]int),
	}
}

func (u *sspanelUser) memoryUser() *protocol.MemoryUser {
	return &protocol.MemoryUser{
		Level:   u.level,
		Email:   u.email,
		Account: u.account,
	}
}

func (u *sspanelUser) enterIP(ip string) {
	if ip == "" {
		return
	}
	u.ipMu.Lock()
	u.activeIP[ip]++
	u.ipMu.Unlock()
}

func (u *sspanelUser) leaveIP(ip string) {
	if ip == "" {
		return
	}
	u.ipMu.Lock()
	if u.activeIP[ip] <= 1 {
		delete(u.activeIP, ip)
	} else {
		u.activeIP[ip]--
	}
	u.ipMu.Unlock()
}

func (u *sspanelUser) currentIPs() []string {
	u.ipMu.Lock()
	defer u.ipMu.Unlock()

	ips := make([]string, 0, len(u.activeIP))
	for ip := range u.activeIP {
		ips = append(ips, ip)
	}
	return ips
}

type sspanelSync struct {
	handler        *Handler
	config         SSPanelConfig
	client         *http.Client
	etag           string
	lastMod        string
	authKey        [32]byte
	auth           *sharedAuthBucket
	appliedAlterID uint32

	alterID   atomic.Uint32
	connLimit atomic.Int64
	active    atomic.Int64

	usersMu sync.RWMutex
	users   map[string]*sspanelUser

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSSPanelSync(handler *Handler, config *SSPanelConfig) *sspanelSync {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		IdleConnTimeout:       90 * time.Second,
	}
	s := &sspanelSync{
		handler: handler,
		config:  *config,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		users: make(map[string]*sspanelUser),
	}
	// A server-side VMess account with 32 alter IDs accepts both the main AEAD
	// identity (client alterId 0) and the generated legacy identities.
	s.alterID.Store(32)
	s.connLimit.Store(config.ConnLimit)
	return s
}

func (s *sspanelSync) start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(runCtx)
	}()
}

func (s *sspanelSync) close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	releaseSharedAuth(s.auth)
	s.auth = nil
	if transport, ok := s.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

func (s *sspanelSync) run(ctx context.Context) {
	s.tick(ctx)

	interval := time.Duration(s.config.Refresh) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *sspanelSync) tick(ctx context.Context) {
	if err := s.refreshNodeInfo(ctx); err != nil {
		newError("sspanel: failed to refresh node info").Base(err).AtWarning().WriteToLog()
	}
	if err := s.syncUsers(ctx); err != nil {
		newError("sspanel: failed to sync users").Base(err).AtWarning().WriteToLog()
	}
	if err := s.reportTraffic(ctx); err != nil {
		newError("sspanel: failed to report traffic").Base(err).AtWarning().WriteToLog()
	}
	if err := s.reportAliveIPs(ctx); err != nil {
		newError("sspanel: failed to report alive IPs").Base(err).AtWarning().WriteToLog()
	}
}

type sspanelHTTPMeta struct {
	ETag         string
	LastModified string
	NotModified  bool
}

func (s *sspanelSync) requestJSON(
	ctx context.Context,
	method, path string,
	query url.Values,
	payload, output interface{},
	headers http.Header,
) (*sspanelHTTPMeta, error) {
	requestURL := strings.TrimRight(s.config.URL, "/") + "/mod_mu/" + strings.TrimLeft(path, "/")
	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return nil, newError("invalid sspanel API URL")
	}
	values := parsedURL.Query()
	values.Set("key", s.config.Key)
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
	for name, entries := range headers {
		for _, entry := range entries {
			request.Header.Add(name, entry)
		}
	}

	response, err := s.client.Do(request)
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

func (s *sspanelSync) refreshNodeInfo(ctx context.Context) error {
	response := new(sspanelNodeInfoResponse)
	path := fmt.Sprintf("nodes/%d/info", s.config.NodeID)
	if _, err := s.requestJSON(ctx, http.MethodGet, path, nil, nil, response, nil); err != nil {
		return err
	}
	if response.Ret != 1 {
		return newError("sspanel rejected node info request")
	}
	if response.Data.CustomConfig.AlterID != nil {
		s.alterID.Store(uint32(*response.Data.CustomConfig.AlterID))
	}
	return nil
}

func (s *sspanelSync) emailForUser(userID int64) string {
	return fmt.Sprintf("sspanel-%d", userID)
}

func (s *sspanelSync) syncUsers(ctx context.Context) error {
	alterID := s.alterID.Load()
	response := new(sspanelUsersResponse)
	query := url.Values{"node_id": []string{strconv.FormatInt(s.config.NodeID, 10)}}
	headers := make(http.Header)
	alterChanged := s.auth == nil || s.appliedAlterID != alterID
	if !alterChanged && s.etag != "" {
		headers.Set("If-None-Match", s.etag)
	}
	if !alterChanged && s.lastMod != "" {
		headers.Set("If-Modified-Since", s.lastMod)
	}
	meta, err := s.requestJSON(ctx, http.MethodGet, "users", query, nil, response, headers)
	if err != nil {
		return err
	}
	if meta.NotModified && !alterChanged {
		return nil
	}
	if response.Ret != 1 {
		return newError("sspanel rejected user list request")
	}
	s.etag = meta.ETag
	s.lastMod = meta.LastModified

	desired := make([]sharedAuthUser, 0, len(response.Data))
	for _, item := range response.Data {
		if item.ID <= 0 || item.UUID == "" {
			continue
		}
		desired = append(desired, sharedAuthUser{
			id:      item.ID,
			email:   s.emailForUser(item.ID),
			uuid:    item.UUID,
			alterID: alterID,
			level:   0,
		})
	}

	key, bucket, err := acquireSharedAuth(desired)
	if err != nil {
		return newError("failed to build shared authentication index").Base(err)
	}
	if s.auth == bucket {
		releaseSharedAuth(bucket)
		return nil
	}

	nextUsers := make(map[string]*sspanelUser, len(desired))
	for _, item := range desired {
		memoryUser := bucket.users[item.email]
		account := memoryUser.Account.(*vmess.MemoryAccount)
		nextUsers[item.email] = newSSPanelUser(
			item.id, item.email, item.uuid, item.alterID, item.level, account,
		)
	}

	s.handler.replaceSSPanelAuth(bucket.validator, bucket.users)
	s.usersMu.Lock()
	s.users = nextUsers
	s.usersMu.Unlock()

	oldBucket := s.auth
	s.authKey = key
	s.auth = bucket
	s.appliedAlterID = alterID
	releaseSharedAuth(oldBucket)
	return nil
}

func (s *sspanelSync) reportTraffic(ctx context.Context) error {
	type trafficLog struct {
		UserID int64 `json:"user_id"`
		Upload int64 `json:"u"`
		Down   int64 `json:"d"`
	}
	type report struct {
		user   *sspanelUser
		upload int64
		down   int64
	}

	s.usersMu.RLock()
	users := make([]*sspanelUser, 0, len(s.users))
	for _, user := range s.users {
		users = append(users, user)
	}
	s.usersMu.RUnlock()

	reports := make([]report, 0, len(users))
	logs := make([]trafficLog, 0, len(users))
	for _, user := range users {
		upload := user.upload.Load()
		down := user.download.Load()
		uploadDelta := upload - user.reportedUp.Load()
		downDelta := down - user.reportedDn.Load()
		if uploadDelta <= 0 && downDelta <= 0 {
			continue
		}
		reports = append(reports, report{user: user, upload: upload, down: down})
		logs = append(logs, trafficLog{
			UserID: user.id,
			Upload: uploadDelta,
			Down:   downDelta,
		})
	}
	if len(logs) == 0 {
		return nil
	}

	payload := map[string]interface{}{"data": logs}
	query := url.Values{"node_id": []string{strconv.FormatInt(s.config.NodeID, 10)}}
	response := struct {
		Ret int `json:"ret"`
	}{}
	if _, err := s.requestJSON(ctx, http.MethodPost, "users/traffic", query, payload, &response, nil); err != nil {
		return err
	}
	if response.Ret != 1 {
		return newError("sspanel rejected traffic report")
	}

	for _, entry := range reports {
		entry.user.reportedUp.Store(entry.upload)
		entry.user.reportedDn.Store(entry.down)
	}
	return nil
}

func (s *sspanelSync) reportAliveIPs(ctx context.Context) error {
	type aliveIP struct {
		UserID int64  `json:"user_id"`
		IP     string `json:"ip"`
	}

	s.usersMu.RLock()
	users := make([]*sspanelUser, 0, len(s.users))
	for _, user := range s.users {
		users = append(users, user)
	}
	s.usersMu.RUnlock()

	ips := make([]aliveIP, 0)
	for _, user := range users {
		for _, ip := range user.currentIPs() {
			ips = append(ips, aliveIP{UserID: user.id, IP: ip})
		}
	}
	if len(ips) == 0 {
		return nil
	}

	payload := map[string]interface{}{"data": ips}
	query := url.Values{"node_id": []string{strconv.FormatInt(s.config.NodeID, 10)}}
	response := struct {
		Ret int `json:"ret"`
	}{}
	if _, err := s.requestJSON(ctx, http.MethodPost, "users/aliveip", query, payload, &response, nil); err != nil {
		return err
	}
	if response.Ret != 1 {
		return newError("sspanel rejected alive IP report")
	}
	return nil
}

func (s *sspanelSync) getUser(email string) *sspanelUser {
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	return s.users[email]
}

func (s *sspanelSync) acquireConnection() bool {
	limit := s.connLimit.Load()
	for {
		current := s.active.Load()
		if limit > 0 && current >= limit {
			return false
		}
		if s.active.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *sspanelSync) releaseConnection() {
	s.active.Add(-1)
}

type sspanelMeteredConnection struct {
	internet.Connection
	user *sspanelUser
}

func (c *sspanelMeteredConnection) Read(payload []byte) (int, error) {
	n, err := c.Connection.Read(payload)
	if n > 0 && c.user != nil {
		c.user.upload.Add(int64(n))
	}
	return n, err
}

func (c *sspanelMeteredConnection) Write(payload []byte) (int, error) {
	n, err := c.Connection.Write(payload)
	if n > 0 && c.user != nil {
		c.user.download.Add(int64(n))
	}
	return n, err
}

func remoteIP(address net.Addr) string {
	if address == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		host = address.String()
	}
	host = strings.Trim(host, "[]")
	parsed := net.ParseIP(host)
	if parsed == nil {
		return ""
	}
	if ipv4 := parsed.To4(); ipv4 != nil {
		return ipv4.String()
	}
	return parsed.String()
}
