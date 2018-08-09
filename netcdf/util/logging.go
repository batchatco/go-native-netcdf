package util

import (
	"log"
	"os"
	"sync"
)

type Logger struct {
	logLevel int
	logger   *log.Logger
	lock     sync.Mutex
}

const (
	// error levels that should almost always be printed
	LevelFatal = iota // error that must stop the program (panics)
	LevelError        // error that does not need to stop execution

	// debugging levels, okay to disable
	LevelWarn // something may be wrong, but not necessarily an error
	LevelInfo // nothing wrong, informational only

	// Production code by default only shows warnings and above.
	LogLevelDefault = LevelWarn

	// min, max levels for setting print level
	levelMin = LevelFatal
	levelMax = LevelInfo
)

var (
	levelToPrefix = []string{
		"FATAL ",
		"ERROR ",
		"WARN ",
		"INFO ",
	}
)

func NewLogger() *Logger {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	return &Logger{logLevel: LogLevelDefault, logger: logger, lock: sync.Mutex{}}
}

func (l *Logger) LogLevel() int {
	return l.logLevel
}

func (l *Logger) SetLogLevel(level int) {
	if level < levelMin || level > levelMax {
		panic("trying to set invalid log level")
	}
	l.logLevel = level
}

func (l *Logger) output(level int, f func(...interface{}), v ...interface{}) {
	if level > l.logLevel {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	l.logger.SetPrefix(levelToPrefix[level])
	f(v...)
}

func (l *Logger) outputf(level int, f func(string, ...interface{}), format string, v ...interface{}) {
	if level > l.logLevel {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.SetPrefix(levelToPrefix[level])
	f(format, v...)
}

func (l *Logger) Info(v ...interface{}) {
	l.output(LevelInfo, l.logger.Println, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.outputf(LevelInfo, l.logger.Printf, format, v...)
}

func (l *Logger) Warn(v ...interface{}) {
	l.output(LevelInfo, l.logger.Println, v...)
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	l.outputf(LevelWarn, l.logger.Printf, format, v...)
}

func (l *Logger) Error(v ...interface{}) {
	l.output(LevelError, l.logger.Println, v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.outputf(LevelError, l.logger.Printf, format, v...)
}

func (l *Logger) Fatal(v ...interface{}) {
	l.output(LevelFatal, l.logger.Fatalln, v...)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.outputf(LevelFatal, l.logger.Fatalf, format, v...)
}
