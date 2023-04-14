package mylog

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type LogLevelT int

const (
	LEVEL_DEBUG LogLevelT = iota
	LEVEL_INFO
	LEVEL_WARN
	LEVEL_ERROR
	LEVEL_PANIC
	LEVEL_FATAL
)

var logLevel = LEVEL_DEBUG

func init() {
	log.SetFlags(log.LstdFlags)
	level, err := strconv.Atoi(os.Getenv("LOGLEVEL"))

	if err != nil {
		// log.Printf("[WARN ] cannot convert loglevel, use default %v\n", logLevel)
		return
	}
	if !checkLevel(level) {
		return
	}
	logLevel = LogLevelT(level)
}

func checkLevel(level int) bool {
	if !(int(LEVEL_DEBUG) >= level && level >= int(LEVEL_PANIC)) {
		log.Printf("[WARN ] wrong loglevel range, no effect %v\n", logLevel)
		return false
	}
	return true
}

func printLog(threshold LogLevelT, prefix, format string, args ...interface{}) {
	if logLevel > threshold {
		return
	}
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}
	_, file, line, ok := runtime.Caller(2)
	if ok {
		file = filepath.Base(file)
		log.Printf(fmt.Sprintf("%s:%d %s: ", file, line, prefix)+format, args...)
	} else {
		log.Printf(prefix+format, args...)
	}
}

func Warnf(format string, args ...interface{}) {
	printLog(LEVEL_WARN, "[WARN] ", format, args...)
}

func Infof(format string, args ...interface{}) {
	printLog(LEVEL_INFO, "[INFO] ", format, args...)
}

func Debugf(format string, args ...interface{}) {
	printLog(LEVEL_DEBUG, "[DEBUG]", format, args...)
}

func Errorf(format string, args ...interface{}) {
	printLog(LEVEL_ERROR, "[ERROR]", format, args...)
}

func Panicf(format string, args ...interface{}) {
	printLog(LEVEL_PANIC, "[PANIC]", format, args...)
	panic(fmt.Errorf(format, args...))
}

func Fatalf(format string, args ...interface{}) {
	printLog(LEVEL_FATAL, "[FATAL]", format, args...)
	log.Fatal(fmt.Errorf(format, args...))
}
