package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
)

func TestIsBaseDiskDevice(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{name: "sda", want: true},
		{name: "sda3", want: false},
		{name: "/dev/sda", want: true},
		{name: "/dev/sda3", want: false},
		{name: "xvda", want: true},
		{name: "xvda1", want: false},
		{name: "nvme0n1", want: true},
		{name: "nvme0n1p1", want: false},
		{name: "mmcblk0", want: true},
		{name: "mmcblk0p2", want: false},
		{name: "loop0", want: false},
		{name: "ram1", want: false},
		{name: "dm-0", want: true},
		{name: "md127", want: true},
		{name: "zd0", want: true},
		{name: "weird_device", want: true},
		{name: "", want: false},
		{name: "   ", want: false},
	}

	for _, tc := range cases {
		got := isBaseDiskDevice(tc.name)
		if got != tc.want {
			t.Fatalf("isBaseDiskDevice(%q)=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestDISKCollectorScrapeSkipsPartitions(t *testing.T) {
	collector := NewDISKCollector("disk")
	collector.now = func() time.Time {
		return time.Unix(1700000000, 0)
	}
	collector.readIO = func(_ context.Context, _ ...string) (map[string]disk.IOCountersStat, error) {
		return map[string]disk.IOCountersStat{
			"sda":          {},
			"sda1":         {},
			"nvme0n1":      {},
			"nvme0n1p1":    {},
			"mmcblk0":      {},
			"mmcblk0p1":    {},
			"dm-0":         {},
			"md127":        {},
			"loop0":        {},
			"ram0":         {},
			"weird_device": {},
		}, nil
	}

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error: %v", err)
	}

	got := make(map[string]struct{}, len(points))
	for _, point := range points {
		got[point.Key] = struct{}{}
	}

	mustContain := []string{
		"/dev/sda",
		"/dev/nvme0n1",
		"/dev/mmcblk0",
		"/dev/dm-0",
		"/dev/md127",
		"/dev/weird_device",
	}
	for _, key := range mustContain {
		if _, exists := got[key]; !exists {
			t.Fatalf("expected key %q to be present", key)
		}
	}

	mustNotContain := []string{
		"/dev/sda1",
		"/dev/nvme0n1p1",
		"/dev/mmcblk0p1",
		"/dev/loop0",
		"/dev/ram0",
	}
	for _, key := range mustNotContain {
		if _, exists := got[key]; exists {
			t.Fatalf("expected key %q to be filtered out", key)
		}
	}
}
