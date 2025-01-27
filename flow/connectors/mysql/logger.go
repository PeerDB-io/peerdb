package connmysql

import (
	"fmt"
	"os"

	"go.temporal.io/sdk/log"
)

// implement interface at https://github.com/siddontang/go-log
type BinlogLogger struct {
	log.Logger
}

func (l BinlogLogger) Fatal(args ...any) {
	l.Error(args...)
	os.Exit(1)
}

func (l BinlogLogger) Fatalf(format string, args ...any) {
	l.Errorf(format, args...)
	os.Exit(1)
}

func (l BinlogLogger) Fatalln(args ...any) {
	l.Errorln(args...)
	os.Exit(1)
}

func (l BinlogLogger) Panic(args ...any) {
	s := fmt.Sprint(args...)
	l.Logger.Error(s)
	panic(s)
}

func (l BinlogLogger) Panicf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Logger.Error(s)
	panic(s)
}

func (l BinlogLogger) Panicln(args ...any) {
	s := fmt.Sprintln(args...)
	l.Logger.Error(s)
	panic(s)
}

func (l BinlogLogger) Print(args ...any) {
	l.Info(args...)
}

func (l BinlogLogger) Printf(format string, args ...any) {
	l.Infof(format, args...)
}

func (l BinlogLogger) Println(args ...any) {
	l.Infoln(args...)
}

func (l BinlogLogger) Debug(args ...any) {
	s := fmt.Sprint(args...)
	l.Logger.Debug(s)
}

func (l BinlogLogger) Debugf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Logger.Debug(s)
}

func (l BinlogLogger) Debugln(args ...any) {
	s := fmt.Sprintln(args...)
	l.Logger.Debug(s)
}

func (l BinlogLogger) Error(args ...any) {
	s := fmt.Sprint(args...)
	l.Logger.Error(s)
}

func (l BinlogLogger) Errorf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Logger.Error(s)
}

func (l BinlogLogger) Errorln(args ...any) {
	s := fmt.Sprintln(args...)
	l.Logger.Error(s)
}

func (l BinlogLogger) Info(args ...any) {
	s := fmt.Sprint(args...)
	l.Logger.Info(s)
}

func (l BinlogLogger) Infof(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Logger.Info(s)
}

func (l BinlogLogger) Infoln(args ...any) {
	s := fmt.Sprintln(args...)
	l.Logger.Info(s)
}

func (l BinlogLogger) Warn(args ...any) {
	s := fmt.Sprint(args...)
	l.Logger.Warn(s)
}

func (l BinlogLogger) Warnf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Logger.Warn(s)
}

func (l BinlogLogger) Warnln(args ...any) {
	s := fmt.Sprintln(args...)
	l.Logger.Warn(s)
}
