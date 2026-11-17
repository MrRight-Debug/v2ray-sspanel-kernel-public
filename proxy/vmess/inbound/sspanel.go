package inbound

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
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
	shared     *sharedSSPanelUser
	account    *vmess.MemoryAccount
	email      string
	alterID    uint32
	level      uint32
	upload     atomic.Int64
	download   atomic.Int64
	reportedUp atomic.Int64
	reportedDn atomic.Int64

	ipMu    sync.Mutex
	activeIP map[string]int
}

func newSSPanelUser(
	shared *sharedSSPanelUser,
	email string,
	alterID, level uint32,
	account *vmess.MemoryAccount,
) *sspanelUser {
	return &sspanelUser{
		shared:   shared,
		account:  account,
		email:    email,
		alterID:  alterID,
		level:    level,
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
	handler *Handler
	config  SSPanelConfig
	manager *sharedSSPanelManager

	alterID   atomic.Uint32
	connLimit atomic.Int64
	active    atomic.Int64

	usersMu sync.RWMutex
	users   map[string]*sspanelUser
	appliedGeneration uint64
	appliedAlterID    uint32

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSSPanelSync(handler *Handler, config *SSPanelConfig) *sspanelSync {
	s := &sspanelSync{
		handler: handler,
		config:  *config,
		manager: sharedManagerFor(config),
		users:   make(map[string]*sspanelUser),
	}
	// A server-side VMess account with 32 alter IDs accepts both the main AEAD
	// identity (client alterId 0) and the generated legacy identities.
	s.alterID.Store(32)
	s.connLimit.Store(config.ConnLimit)
	return s
}

func (s *sspanelSync) start(ctx context.Context) {
	s.manager.acquire(ctx, s.config.NodeID, s.config.Refresh)
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
	s.manager.release(s.config.NodeID)
}

func (s *sspanelSync) run(ctx context.Context) {
	s.manager.waitReady(ctx)
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

func (s *sspanelSync) refreshNodeInfo(ctx context.Context) error {
	response := new(sspanelNodeInfoResponse)
	path := fmt.Sprintf("nodes/%d/info", s.config.NodeID)
	if _, err := s.manager.requestJSON(ctx, http.MethodGet, path, nil, nil, response, nil); err != nil {
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
	return fmt.Sprintf("sspanel-%d-%d", s.config.NodeID, userID)
}

func (s *sspanelSync) syncUsers(ctx context.Context) error {
	alterID := s.alterID.Load()
	snapshot := s.manager.snapshot.Load()
	if snapshot == nil {
		return newError("shared sspanel user snapshot unavailable")
	}

	s.usersMu.RLock()
	current := make(map[string]*sspanelUser, len(s.users))
	for email, user := range s.users {
		current[email] = user
	}
	upToDate := s.appliedGeneration == snapshot.Generation && s.appliedAlterID == alterID
	s.usersMu.RUnlock()
	if upToDate {
		return nil
	}

	desired := make(map[string]*sharedSSPanelUser, len(snapshot.Users))
	for index := range snapshot.Users {
		item := &snapshot.Users[index]
		desired[s.emailForUser(item.ID)] = item
	}

	for email := range current {
		if _, found := desired[email]; found {
			continue
		}
		if err := s.handler.RemoveUser(ctx, email); err != nil {
			newError("sspanel: failed to remove stale user").Base(err).AtDebug().WriteToLog()
		}
		s.usersMu.Lock()
		delete(s.users, email)
		s.usersMu.Unlock()
	}

	for email, desiredUser := range desired {
		if existing := current[email]; existing != nil &&
			existing.shared.UUID == desiredUser.UUID && existing.alterID == alterID {
			continue
		}
		account, err := s.manager.accountFor(desiredUser.UUID, alterID)
		if err != nil {
			newError("sspanel: invalid VMess user from panel").Base(err).AtWarning().WriteToLog()
			continue
		}
		user := newSSPanelUser(desiredUser, email, alterID, 0, account)
		if current[email] != nil {
			if err := s.handler.RemoveUser(ctx, email); err != nil {
				newError("sspanel: failed to replace changed user").Base(err).AtDebug().WriteToLog()
			}
		}
		if err := s.handler.AddUser(ctx, user.memoryUser()); err != nil {
			newError("sspanel: failed to add user").Base(err).AtWarning().WriteToLog()
			continue
		}
		s.usersMu.Lock()
		s.users[email] = user
		s.usersMu.Unlock()
	}
	s.usersMu.Lock()
	s.appliedGeneration = snapshot.Generation
	s.appliedAlterID = alterID
	s.usersMu.Unlock()
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
			UserID: user.shared.ID,
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
	if _, err := s.manager.requestJSON(ctx, http.MethodPost, "users/traffic", query, payload, &response, nil); err != nil {
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
			ips = append(ips, aliveIP{UserID: user.shared.ID, IP: ip})
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
	if _, err := s.manager.requestJSON(ctx, http.MethodPost, "users/aliveip", query, payload, &response, nil); err != nil {
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

