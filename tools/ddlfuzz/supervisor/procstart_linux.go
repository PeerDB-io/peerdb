package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processStartTime(pid int) (int64, error) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	text := string(stat)
	end := strings.LastIndex(text, ")")
	if end < 0 {
		return 0, fmt.Errorf("malformed proc stat for pid %d", pid)
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("short proc stat for pid %d", pid)
	}
	startTicks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, err
	}
	btime, err := procBootTime()
	if err != nil {
		return 0, err
	}
	return btime*1_000_000 + startTicks*10_000, nil
}

func procBootTime() (int64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "btime" {
			return strconv.ParseInt(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("btime missing from /proc/stat")
}
