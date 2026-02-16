package internal

// Internal logging utility.

import (
	"log"
	"os"
	"runtime/debug"
	"sync"
)

type Logger struct {
	logLevel LogLevel
	logger   *log.Logger
	lock     sync.Mutex
}

type LogLevel int

const (
	// error levels that should almost always be printed
	LevelFatal LogLevel = iota // error that must stop the program (panics)
	LevelError                 // error that does not need to stop execution

	// debugging levels, okay to disable
	LevelWarn // something may be wrong, but not necessarily an error
	LevelInfo // nothing wrong, informational only

	// Production code by default only shows warnings and above.
	LogLevelDefault = LevelWarn

	// min, max levels for setting print level
	LevelMin = LevelFatal
	LevelMax = LevelInfo
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

func (l *Logger) LogLevel() LogLevel {
	return l.logLevel
}

// SetLogLevel returns the old level
func (l *Logger) SetLogLevel(level LogLevel) LogLevel {
	if level < LevelMin || level > LevelMax {
		panic("trying to set invalid log level")
	}
	old := l.logLevel
	l.logLevel = level
	return old
}

func (l *Logger) output(level LogLevel, f func(...any), v ...any) {
	if level > l.logLevel {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	l.logger.SetPrefix(levelToPrefix[level])
	f(v...)
}

func (l *Logger) outputf(level LogLevel, f func(string, ...any), format string, v ...any) {
	if level > l.logLevel {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.SetPrefix(levelToPrefix[level])
	f(format, v...)
}

func (l *Logger) Info(v ...any) {
	l.output(LevelInfo, l.logger.Println, v...)
}

func (l *Logger) Infof(format string, v ...any) {
	l.outputf(LevelInfo, l.logger.Printf, format, v...)
}

func (l *Logger) Warn(v ...any) {
	l.output(LevelWarn, l.logger.Println, v...)
}

func (l *Logger) Warnf(format string, v ...any) {
	l.outputf(LevelWarn, l.logger.Printf, format, v...)
}

func (l *Logger) Error(v ...any) {
	l.output(LevelError, l.logger.Println, v...)
}

func (l *Logger) Errorf(format string, v ...any) {
	l.outputf(LevelError, l.logger.Printf, format, v...)
}

func (l *Logger) Fatal(v ...any) {
	log.Print(string(debug.Stack()))
	l.output(LevelFatal, l.logger.Fatalln, v...)
}

func (l *Logger) Fatalf(format string, v ...any) {
	log.Print(string(debug.Stack()))
	l.outputf(LevelFatal, l.logger.Fatalf, format, v...)
}
