package inbound

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
)

// SSPanelConfig describes the native SSPanel UIM synchronization settings used
// by the VMess inbound. It is intentionally separate from the protobuf config so
// existing v4 JSON configurations remain compatible.
type SSPanelConfig struct {
	URL        string `json:"url"`
	Key        string `json:"key"`
	NodeID     int64  `json:"node_id"`
	Refresh    int    `json:"refresh"`
	ConnLimit  int64  `json:"conn_limit"`
	ShareGroup string `json:"share_group"`
}

func decodeFlexibleInt64(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	value := strings.TrimSpace(string(raw))
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func (c *SSPanelConfig) UnmarshalJSON(data []byte) error {
	raw := struct {
		URL       string          `json:"url"`
		Key       string          `json:"key"`
		NodeID    json.RawMessage `json:"node_id"`
		Refresh   json.RawMessage `json:"refresh"`
		ConnLimit json.RawMessage `json:"conn_limit"`
		ShareGroup string         `json:"share_group"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	nodeID, err := decodeFlexibleInt64(raw.NodeID)
	if err != nil {
		return fmt.Errorf("invalid node_id: %w", err)
	}
	refresh, err := decodeFlexibleInt64(raw.Refresh)
	if err != nil {
		return fmt.Errorf("invalid refresh: %w", err)
	}
	connLimit, err := decodeFlexibleInt64(raw.ConnLimit)
	if err != nil {
		return fmt.Errorf("invalid conn_limit: %w", err)
	}
	c.URL = raw.URL
	c.Key = raw.Key
	c.NodeID = nodeID
	c.Refresh = int(refresh)
	c.ConnLimit = connLimit
	c.ShareGroup = raw.ShareGroup
	return nil
}

type sspanelSettingsEnvelope struct {
	SSPanel json.RawMessage `json:"sspanel"`
}

var sspanelConfigRegistry = struct {
	sync.RWMutex
	configs map[string]*SSPanelConfig
}{
	configs: make(map[string]*SSPanelConfig),
}

func envInt(name string, current int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return current
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return current
	}
	return parsed
}

func envInt64(name string, current int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return current
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return current
	}
	return parsed
}

func (c *SSPanelConfig) normalize() error {
	if c.URL == "" {
		c.URL = strings.TrimSpace(os.Getenv("V2RAY_SSPANEL_URL"))
	}
	if c.Key == "" {
		c.Key = strings.TrimSpace(os.Getenv("V2RAY_SSPANEL_KEY"))
	}
	if c.NodeID == 0 {
		c.NodeID = envInt64("V2RAY_SSPANEL_NODE_ID", c.NodeID)
	}
	if c.Refresh == 0 {
		c.Refresh = envInt("V2RAY_SSPANEL_REFRESH", 60)
	}
	if c.ConnLimit == 0 {
		c.ConnLimit = envInt64("V2RAY_SSPANEL_CONN_LIMIT", 800)
	}
	if c.ShareGroup == "" {
		c.ShareGroup = strings.TrimSpace(os.Getenv("V2RAY_SSPANEL_SHARE_GROUP"))
	}
	if c.ShareGroup == "" {
		c.ShareGroup = "default"
	}

	c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if c.URL == "" || c.Key == "" || c.NodeID <= 0 {
		return fmt.Errorf("sspanel url, key and positive node_id are required")
	}
	if c.Refresh <= 0 {
		c.Refresh = 60
	}
	if c.ConnLimit < 0 {
		return fmt.Errorf("sspanel conn_limit must not be negative")
	}

	parsed, err := url.Parse(c.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("sspanel url must use http or https")
	}
	if parsed.User != nil {
		return fmt.Errorf("sspanel url must not contain user information")
	}
	return nil
}

// RegisterSSPanelConfig registers SSPanel settings for one inbound tag. The
// inbound tag is required because a single process may serve several VMess
// inbounds, each with a different panel node ID.
func RegisterSSPanelConfig(tag string, rawSettings []byte) error {
	if tag == "" || len(rawSettings) == 0 {
		return nil
	}

	envelope := sspanelSettingsEnvelope{}
	if err := json.Unmarshal(rawSettings, &envelope); err != nil {
		return newError("failed to parse VMess settings").Base(err)
	}
	if len(envelope.SSPanel) == 0 || string(envelope.SSPanel) == "null" {
		return nil
	}

	config := new(SSPanelConfig)
	if err := json.Unmarshal(envelope.SSPanel, config); err != nil {
		return newError("failed to parse sspanel settings").Base(err)
	}
	if err := config.normalize(); err != nil {
		return newError("invalid sspanel settings for inbound ", tag).Base(err)
	}

	copyConfig := *config
	sspanelConfigRegistry.Lock()
	sspanelConfigRegistry.configs[tag] = &copyConfig
	sspanelConfigRegistry.Unlock()
	return nil
}

func getSSPanelConfig(tag string) *SSPanelConfig {
	sspanelConfigRegistry.RLock()
	defer sspanelConfigRegistry.RUnlock()

	config := sspanelConfigRegistry.configs[tag]
	if config == nil {
		return nil
	}
	copyConfig := *config
	return &copyConfig
}
