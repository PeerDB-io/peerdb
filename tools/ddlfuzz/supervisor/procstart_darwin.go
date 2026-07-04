package main

import "golang.org/x/sys/unix"

func processStartTime(pid int) (int64, error) {
	proc, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	tv := proc.Proc.P_starttime
	return int64(tv.Sec)*1_000_000 + int64(tv.Usec), nil
}
