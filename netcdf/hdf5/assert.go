package hdf5

// Various kinds of assertions

import (
	"fmt"
	"io"

	"github.com/batchatco/go-thrower"
)

// for padBytesCheck()
const (
	dontRound = false // the number is the number of pad bytes to check for.
	round     = true  // the number is the byte-boundary to check up to (1, 3 or 7).
)

var (
	logFunc   = logger.Fatal // logging function for padBytesCheck()
	maybeFail = fail         // fail function, can be disabled for testing
)

// Non-standard things don't always pad the bytes correctly, so we
// want to be more forgiving in those cases.
func setNonStandard(non bool) bool {
	old := allowNonStandard
	allowNonStandard = non
	if allowNonStandard {
		logFunc = logger.Info
		maybeFail = warn
	} else {
		logFunc = logger.Fatal
		maybeFail = fail
	}
	return old
}

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

// Warns with message
func warn(msg string) {
	logger.Warn(msg)
}

// Panics with specified error and message
func failError(err error, msg string) {
	logger.Error(msg)
	thrower.Throw(err)
	panic("never gets here")
}

// Check that pad bytes are zero up to byte boundary (pad32 = 1,3 or 7)
func padBytes(bf io.Reader, pad32 int) {
	padBytesCheck(bf, pad32, round, logFunc)
}

// Check that a run of len pad bytes are zero
func checkZeroes(bf io.Reader, len int) {
	padBytesCheck(bf, len, dontRound, logFunc)
}

// Check that pad bytes are zero.
func padBytesCheck(obf io.Reader, pad32 int, round bool,
	logFunc func(v ...interface{})) bool {
	cbf := obf.(remReader)
	success := true
	var extra int
	if round {
		pad64 := int64(pad32)
		rounded := (cbf.Count() + pad64) & ^pad64
		extra = int(rounded) - int(cbf.Count())
	} else {
		extra = pad32
	}
	if extra > 0 {
		logger.Info(cbf.Count(), "prepad", extra, "bytes")
		b := make([]byte, extra)
		read(cbf, b)
		for i := 0; i < int(extra); i++ {
			if b[i] != 0 {
				success = false
			}
		}
		if !success {
			logFunc(fmt.Sprintf("Reserved not zero len=%d 0x%x", extra, b))
		}
	}
	return success
}
