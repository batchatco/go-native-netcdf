package internal

// Internal logging utility.

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
)

type Logger struct {
	logLevel LogLevel
	logger   *log.Logger
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
	return &Logger{logLevel: LogLevelDefault, logger: logger}
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

func (l *Logger) output(level LogLevel, s string) {
	if level > l.logLevel {
		return
	}
	l.logger.Output(2, levelToPrefix[level]+s)
}

func (l *Logger) Info(v ...any)                 { l.output(LevelInfo, fmt.Sprintln(v...)) }
func (l *Logger) Infof(format string, v ...any) { l.output(LevelInfo, fmt.Sprintf(format, v...)) }

func (l *Logger) Warn(v ...any)                 { l.output(LevelWarn, fmt.Sprintln(v...)) }
func (l *Logger) Warnf(format string, v ...any) { l.output(LevelWarn, fmt.Sprintf(format, v...)) }

func (l *Logger) Error(v ...any)                 { l.output(LevelError, fmt.Sprintln(v...)) }
func (l *Logger) Errorf(format string, v ...any) { l.output(LevelError, fmt.Sprintf(format, v...)) }

func (l *Logger) Fatal(v ...any) {
	log.Print(string(debug.Stack()))
	l.output(LevelFatal, fmt.Sprintln(v...))
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, v ...any) {
	log.Print(string(debug.Stack()))
	l.output(LevelFatal, fmt.Sprintf(format, v...))
	os.Exit(1)
}
