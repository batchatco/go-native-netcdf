package thrower

import (
	"errors"
	"testing"
)

var (
	myError    = errors.New("my error")
	notMyError = errors.New("not my error")
)

func helper(t *testing.T, e error, notError error) (err error) {
	t.Helper()
	defer RecoverError(&err)
	ThrowIfError(e)
	return notError
}

func TestThrow(t *testing.T) {
	err := helper(t, myError, notMyError)
	if err != myError {
		t.Error("Didn't catch my error", err)
	}
}

func TestThrowIfNil(t *testing.T) {
	myError := errors.New("my error")
	err := helper(t, nil, myError)
	if err != myError {
		t.Error("Was not supposed to throw an error")
	}
}

func TestThrowIfError(t *testing.T) {
	err := helper(t, myError, notMyError)
	if err != myError {
		t.Error("Didn't catch my error", err)
	}
}

func TestDisabled(t *testing.T) {
	DisableCatching()
	_ = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				return
			}
			t.Error("panic should not have been caught")
		}()

		defer RecoverError(&err)
		Throw(myError) // should cause a panic that doesn't get caught.
		return nil
	}()
	ReEnableCatching()
	_ = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Error("panic should have been caught")
			}
		}()

		defer RecoverError(&err)
		Throw(myError) // should cause a panic that gets caught.
		return nil
	}()
}

func TestOtherPanic(t *testing.T) {
	_ = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				return
			}
			t.Error("panic should not have been caught")
		}()

		defer RecoverError(&err)
		panic(errors.New("something"))
	}()
}

func TestString(t *testing.T) {
	th := newThrown(myError)
	s := th.Error()
	if s != "my error" {
		t.Error("wrong string", s)
	}
}
