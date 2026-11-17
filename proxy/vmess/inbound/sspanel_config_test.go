package inbound

import (
	"testing"
)

func TestRegisterSSPanelConfig(t *testing.T) {
	settings := []byte(`{
		"sspanel": {
			"url": "https://panel.example.test/base/",
			"key": "test-key",
			"node_id": "11",
			"refresh": "45",
			"conn_limit": "800"
		}
	}`)
	if err := RegisterSSPanelConfig("node-11", settings); err != nil {
		t.Fatal(err)
	}

	config := getSSPanelConfig("node-11")
	if config == nil {
		t.Fatal("expected registered config")
	}
	if config.URL != "https://panel.example.test/base" {
		t.Fatalf("unexpected URL: %s", config.URL)
	}
	if config.Key != "test-key" || config.NodeID != 11 || config.Refresh != 45 || config.ConnLimit != 800 {
		t.Fatalf("unexpected config: %+v", config)
	}
	if config.ShareGroup != "default" {
		t.Fatalf("unexpected share group: %s", config.ShareGroup)
	}
}

func TestRegisterSSPanelConfigUsesEnvironment(t *testing.T) {
	t.Setenv("V2RAY_SSPANEL_URL", "http://127.0.0.1:8080")
	t.Setenv("V2RAY_SSPANEL_KEY", "env-key")
	t.Setenv("V2RAY_SSPANEL_NODE_ID", "12")
	t.Setenv("V2RAY_SSPANEL_REFRESH", "30")
	t.Setenv("V2RAY_SSPANEL_CONN_LIMIT", "321")

	if err := RegisterSSPanelConfig("node-12", []byte(`{"sspanel":{}}`)); err != nil {
		t.Fatal(err)
	}
	config := getSSPanelConfig("node-12")
	if config == nil {
		t.Fatal("expected environment-backed config")
	}
	if config.Key != "env-key" || config.NodeID != 12 || config.Refresh != 30 || config.ConnLimit != 321 {
		t.Fatalf("unexpected environment config: %+v", config)
	}
}

func TestRegisterSSPanelConfigRejectsInvalidURL(t *testing.T) {
	settings := []byte(`{
		"sspanel": {
			"url": "file:///tmp/panel",
			"key": "test-key",
			"node_id": 11
		}
	}`)
	if err := RegisterSSPanelConfig("invalid", settings); err == nil {
		t.Fatal("expected invalid URL to fail")
	}
}

func TestRegisterSSPanelConfigShareGroup(t *testing.T) {
	settings := []byte(`{
		"sspanel": {
			"url": "https://panel.example.test",
			"key": "test-key",
			"node_id": 31,
			"share_group": "scope-a"
		}
	}`)
	if err := RegisterSSPanelConfig("scope-a", settings); err != nil {
		t.Fatal(err)
	}
	config := getSSPanelConfig("scope-a")
	if config == nil || config.ShareGroup != "scope-a" {
		t.Fatalf("unexpected share group: %+v", config)
	}
}

func TestRegisterSSPanelConfigIgnoresNonPanelSettings(t *testing.T) {
	if err := RegisterSSPanelConfig("plain", []byte(`{"clients":[]}`)); err != nil {
		t.Fatal(err)
	}
	if config := getSSPanelConfig("plain"); config != nil {
		t.Fatalf("unexpected config: %+v", config)
	}
}
