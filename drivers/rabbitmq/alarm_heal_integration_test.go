//go:build integration

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

type rabbitMQStatus struct {
	Alarms []struct {
		Resource string `json:"resource"`
	} `json:"alarms"`
	MemoryWatermark struct {
		Relative *float64 `json:"relative"`
	} `json:"vm_memory_high_watermark_setting"`
}

var packageHealRan bool

func TestMain(m *testing.M) {
	container := os.Getenv(alarmContainerEnv)
	if container != "" {
		packageHealRan = true
		healed, err := healDirtyAlarm(container)
		if err != nil {
			_, _ = os.Stderr.WriteString("RabbitMQ fixture heal failed: " + err.Error() + "\n")
			os.Exit(1)
		}
		if healed {
			_, _ = os.Stderr.WriteString("RabbitMQ fixture heal: cleared memory alarm left by a previous killed run\n")
		}
	}
	os.Exit(m.Run())
}

func readRabbitMQStatus(container string) (rabbitMQStatus, error) {
	output, err := rabbitmqctl(container, "status", "--formatter", "json")
	if err != nil {
		return rabbitMQStatus{}, err
	}
	var status rabbitMQStatus
	if err := json.Unmarshal([]byte(output), &status); err != nil {
		return rabbitMQStatus{}, fmt.Errorf("decode RabbitMQ status: %w", err)
	}
	if status.MemoryWatermark.Relative == nil {
		return rabbitMQStatus{}, fmt.Errorf("RabbitMQ status has no relative memory watermark")
	}
	return status, nil
}

func configuredMemoryWatermark(container string) (string, error) {
	output, err := rabbitmqctl(container, "eval", "application:get_env(rabbit, vm_memory_high_watermark).")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(output)
	const prefix = "{ok,"
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, "}") {
		return "", fmt.Errorf("unexpected RabbitMQ memory watermark setting %q", value)
	}
	value = strings.TrimSuffix(strings.TrimPrefix(value, prefix), "}")
	if _, err := strconv.ParseFloat(value, 64); err != nil {
		return "", fmt.Errorf("parse RabbitMQ memory watermark setting %q: %w", value, err)
	}
	return value, nil
}

func healDirtyAlarm(container string) (bool, error) {
	status, err := readRabbitMQStatus(container)
	if err != nil {
		return false, err
	}
	alarmValue, err := strconv.ParseFloat(alarmWatermark, 64)
	if err != nil {
		return false, fmt.Errorf("parse RabbitMQ alarm watermark %q: %w", alarmWatermark, err)
	}
	if *status.MemoryWatermark.Relative != alarmValue {
		return false, nil
	}
	configured, err := configuredMemoryWatermark(container)
	if err != nil {
		return false, err
	}
	configuredValue, err := strconv.ParseFloat(configured, 64)
	if err != nil {
		return false, err
	}
	if configuredValue == alarmValue {
		return false, fmt.Errorf("configured RabbitMQ memory watermark is the alarm watermark")
	}
	if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", configured); err != nil {
		return false, err
	}
	return true, nil
}

func rabbitMQMemoryAlarmActive(container string) (bool, error) {
	status, err := readRabbitMQStatus(container)
	if err != nil {
		return false, err
	}
	for _, alarm := range status.Alarms {
		if alarm.Resource == "memory" {
			return true, nil
		}
	}
	return false, nil
}

func TestRabbitMQPackageStartHeal(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)
	if !packageHealRan {
		t.Fatal("RabbitMQ package-start fixture heal did not run")
	}
	active, err := rabbitMQMemoryAlarmActive(container)
	if err != nil {
		t.Fatalf("check RabbitMQ memory alarm: %v", err)
	}
	if active {
		t.Fatal("RabbitMQ memory alarm remains active after package-start fixture heal")
	}
}

func TestRabbitMQHealDirtyAlarm(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)
	configured, err := configuredMemoryWatermark(container)
	if err != nil {
		t.Fatalf("read configured RabbitMQ memory watermark: %v", err)
	}
	t.Cleanup(func() {
		_, _ = rabbitmqctl(container, "set_vm_memory_high_watermark", configured)
	})
	if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", alarmWatermark); err != nil {
		t.Fatalf("set RabbitMQ memory alarm watermark: %v", err)
	}
	healed, err := healDirtyAlarm(container)
	if err != nil {
		t.Fatalf("heal RabbitMQ memory alarm: %v", err)
	}
	if !healed {
		t.Fatal("heal RabbitMQ memory alarm reported no change")
	}
	active, err := rabbitMQMemoryAlarmActive(container)
	if err != nil {
		t.Fatalf("check RabbitMQ memory alarm after healing: %v", err)
	}
	if active {
		t.Fatal("RabbitMQ memory alarm remains active after healing")
	}
}

func TestRabbitMQHealLeavesDifferentWatermark(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)
	configured, err := configuredMemoryWatermark(container)
	if err != nil {
		t.Fatalf("read configured RabbitMQ memory watermark: %v", err)
	}
	t.Cleanup(func() {
		_, _ = rabbitmqctl(container, "set_vm_memory_high_watermark", configured)
	})
	const runtimeWatermark = "0.61"
	if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", runtimeWatermark); err != nil {
		t.Fatalf("set RabbitMQ runtime memory watermark: %v", err)
	}
	healed, err := healDirtyAlarm(container)
	if err != nil {
		t.Fatalf("check RabbitMQ memory watermark: %v", err)
	}
	if healed {
		t.Fatal("heal RabbitMQ memory alarm changed a differing runtime watermark")
	}
	status, err := readRabbitMQStatus(container)
	if err != nil {
		t.Fatalf("read RabbitMQ memory watermark after no-op heal: %v", err)
	}
	got := strconv.FormatFloat(*status.MemoryWatermark.Relative, 'f', -1, 64)
	if got != runtimeWatermark {
		t.Fatalf("RabbitMQ memory watermark after no-op heal = %s, want %s", got, runtimeWatermark)
	}
}

func TestRaiseMemoryAlarmRestoresConfiguredWatermark(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)
	configured, err := configuredMemoryWatermark(container)
	if err != nil {
		t.Fatalf("read configured RabbitMQ memory watermark: %v", err)
	}
	t.Cleanup(func() {
		_, _ = rabbitmqctl(container, "set_vm_memory_high_watermark", configured)
	})
	if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", alarmWatermark); err != nil {
		t.Fatalf("set RabbitMQ memory alarm watermark: %v", err)
	}
	restore := raiseMemoryAlarm(t, container)
	restore()
	active, err := rabbitMQMemoryAlarmActive(container)
	if err != nil {
		t.Fatalf("check RabbitMQ memory alarm after restore: %v", err)
	}
	if active {
		t.Fatal("RabbitMQ memory alarm remains active after restore")
	}
	status, err := readRabbitMQStatus(container)
	if err != nil {
		t.Fatalf("read RabbitMQ memory watermark after restore: %v", err)
	}
	got := strconv.FormatFloat(*status.MemoryWatermark.Relative, 'f', -1, 64)
	if got != configured {
		t.Fatalf("RabbitMQ memory watermark after restore = %s, want configured %s", got, configured)
	}
}
