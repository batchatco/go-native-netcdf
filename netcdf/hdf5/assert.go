package hdf5

import "github.com/batchatco/go-thrower"

// Various kinds of assertions

// Panics if condition isn't met
func assert(condition bool, msg string) {
	if condition {
		return
	}
	fail(msg)
}

// Warns if condition isn't met
func warnAssert(condition bool, msg string) {
	if condition {
		return
	}
	logger.Warn(msg)
}

// Infos if condition isn't met
func infoAssert(condition bool, msg string) {
	if condition {
		return
	}
	logger.Info(msg)
}

// Asserts with given error and message
func assertError(condition bool, err error, msg string) {
	if condition {
		return
	}
	failError(err, msg)
}

// Panics always with message
func fail(msg string) {
	failError(ErrInternal, msg)
}

// Panics with specified error and message
func failError(err error, msg string) {
	logger.Error(msg)
	thrower.Throw(err)
	panic("never gets here")
}
