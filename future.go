// Package future provides a Future type that correlates async work with unique IDs (integer or string).
// A zero Future is valid; use NewFuture only if you want a preallocated map.
// Promise registers a pending result, Complete delivers it, and the returned await function blocks until then.
// IDs come from github.com/google/uuid: string form, the first 8 random bytes for 64-bit integer types,
// UUID.ID() for 32-bit integer types, and int follows the width of the platform.
package future

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"
)

var (
	ErrPendingNotFound   = errors.New("future: pending not found")
	ErrCompleted         = errors.New("future: pending already completed")
	ErrUnsupportedIDType = errors.New("future: unsupported ID type")
)

type FutureID interface {
	int32 | int64 | uint32 | uint64 | int | string
}

// pending holds the completion state for a single async operation.
type pending[T any] struct {
	once  sync.Once
	done  chan struct{}
	value T
	err   error
}

// Future correlates in-flight async operations by unique IDs.
// The zero value is ready to use; the task map is allocated on the first Promise.
type Future[I FutureID, T any] struct {
	tasks map[I]*pending[T]
	mutex sync.RWMutex
}

// NewFuture returns an empty Future with its task map preallocated.
// Callers may use a zero Future[I, T] instead; NewFuture is optional.
func NewFuture[I FutureID, T any]() *Future[I, T] {
	return &Future[I, T]{tasks: make(map[I]*pending[T])}
}

type PromiseOptions struct {
	timeout time.Duration
}

type PromiseOption func(opts *PromiseOptions)

// WithTimeout sets the timeout for the promise.
func WithTimeout(timeout time.Duration) PromiseOption {
	return func(opts *PromiseOptions) { opts.timeout = timeout }
}

// Promise starts one async operation: it generates a unique ID, registers a pending entry,
// invokes fn(id) so the caller can start work (e.g. fire a request), and returns await.
// await blocks until the async operation is completed or ctx is canceled; if completion and
// cancellation happen together, the completed value is returned. Call await at least once so
// the registration is removed from the Future (otherwise the entry is leaked).
func (f *Future[I, T]) Promise(ctx context.Context, fn func(ID I), opts ...PromiseOption) (await func() (T, error)) {
	var options PromiseOptions
	for _, opt := range opts {
		opt(&options)
	}
	// Option: timeout
	if options.timeout < 0 {
		options.timeout = 0
	}

	// Generate a unique ID.
	ID, err := f.generateID()
	if err != nil {
		await = func() (T, error) {
			var zero T
			return zero, err
		}
		return await
	}
	pd := &pending[T]{
		done: make(chan struct{}),
	}

	f.mutex.Lock()
	if f.tasks == nil {
		f.tasks = make(map[I]*pending[T])
	}
	f.tasks[ID] = pd
	f.mutex.Unlock()

	// Invoke the function with the unique ID.
	fn(ID)

	// Timeout context.
	var cancel = context.CancelFunc(func() {})
	if options.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, options.timeout)
	}

	await = func() (T, error) {
		defer func() {
			cancel()
			f.mutex.Lock()
			delete(f.tasks, ID)
			f.mutex.Unlock()
		}()
		select {
		// Context is done, return the context error if the context is done.
		case <-ctx.Done():
			select {
			case <-pd.done:
				return pd.value, pd.err
			default:
				var zero T
				return zero, ctx.Err()
			}
		// Completion is ready, return the completed value and error.
		case <-pd.done:
			return pd.value, pd.err
		}
	}
	return await
}

// Complete resolves the pending operation for ID with a value or error,
// unblocking the corresponding await.
func (f *Future[I, T]) Complete(ID I, value T, err error) error {
	f.mutex.RLock()
	pd, ok := f.tasks[ID]
	f.mutex.RUnlock()
	if !ok {
		return ErrPendingNotFound
	}
	completed := false
	pd.once.Do(func() {
		pd.value = value
		pd.err = err
		close(pd.done)
		completed = true
	})
	if !completed {
		return ErrCompleted
	}
	return nil
}

// generateID generates a unique ID for the future.
// It returns the ID and an error if the ID type is unsupported.
func (f *Future[I, T]) generateID() (ID I, err error) {
	u := uuid.New()
	switch any(ID).(type) {
	case int32:
		ID = any(int32(u.ID())).(I)
	case int64:
		ID = any(int64(binary.BigEndian.Uint64(u[0:8]))).(I)
	case uint32:
		ID = any(u.ID()).(I)
	case uint64:
		ID = any(binary.BigEndian.Uint64(u[0:8])).(I)
	case int:
		if unsafe.Sizeof(int(0)) == 8 {
			ID = any(int(binary.BigEndian.Uint64(u[0:8]))).(I)
		} else {
			ID = any(int(u.ID())).(I)
		}
	case string:
		ID = any(u.String()).(I)
	default:
		return ID, ErrUnsupportedIDType
	}
	return ID, nil
}
