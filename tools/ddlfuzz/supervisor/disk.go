package main

import (
	"context"
	"fmt"
	"syscall"
	"time"
)

const GiB = int64(1024 * 1024 * 1024)

func FreeBytes(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

func DockerSystemDF(ctx context.Context, cfg Config) string {
	res, err := RunTimeout(ctx, cfg.Root, 30*time.Second, nil, "docker", "system", "df")
	if err != nil {
		return resultOutputTail(res, 4000)
	}
	return res.Stdout
}

func formatGiB(bytes int64) string {
	return fmt.Sprintf("%.1fGiB", float64(bytes)/float64(GiB))
}
