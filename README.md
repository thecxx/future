# future

Correlate in-flight async work with a unique ID: `Promise` registers a pending operation, `Complete` delivers the result, and the returned `await` blocks on the caller side until completion or context cancellation.

## Install

```bash
go get github.com/thecxx/future
```

## Core types

- **`Future[I, T]`**: `I` is the ID type (see below); `T` is the result type returned when the operation finishes.
- **`FutureID`**: supported `I` values are `int32 | int64 | uint32 | uint64 | int | string`.

## Construction

- **Zero value is valid**: `var f Future[string, Result]` or an embedded `Future` field in a struct; the internal map is allocated lazily on the **first** `Promise`.
- **`NewFuture[I, T]()`** (optional): pre-`make`s the task map—useful when you expect many concurrent registrations and want to avoid that first allocation.

## API summary

| API | Role |
|-----|------|
| `Promise(ctx, fn, opts...) (await func() (T, error))` | Generates an ID, registers a pending entry, calls `fn(id)` to start async-side work, returns `await`. |
| `Complete(id, value, err) error` | Completes the pending entry for `id` and unblocks the matching `await`. |
| `WithTimeout(d)` | Optional `Promise` option: when `d > 0`, wraps `ctx` with `context.WithTimeout`; `d < 0` is treated as `0` (no extra timeout). |

## Typical flow

1. Call `Promise`: in `fn(id)`, pass `id` to the async path (e.g. attach it to a request or store it for a callback).
2. When work finishes elsewhere, call `Complete(id, value, err)` (`err` can represent a business failure; `await` still receives both `value` and `err`).
3. Call the returned `await()` at least once to wait for the outcome and **remove** the ID from the internal map; skipping `await` leaks the registration.

## ID generation

`Promise` uses `github.com/google/uuid` and derives the ID from the concrete type `I` (same rules as `generateID`):

| `I` | Rule |
|-----|------|
| `string` | `uuid.String()` |
| `uint32` | `uuid.ID()` (32-bit field from the UUID) |
| `int32` | `int32(uuid.ID())` |
| `int64` / `uint64` | Big-endian `uint64` from the first 8 bytes of the UUID, converted to the target type |
| `int` | 64-bit platforms: first 8 bytes, big-endian, as `int`; 32-bit platforms: `int(uuid.ID())` |

If `I` is ever outside this set, `Promise` still returns an `await`, but invoking it yields the zero value of `T` and `ErrUnsupportedIDType` (nothing is registered in the map).

## Context and timeouts

- If `ctx` is canceled or times out **before** completion, `await` returns the zero value of `T` and `ctx.Err()`.
- With `WithTimeout`, `Promise` applies an additional deadline via a child context.
- **Races**: if cancellation and completion happen close together, whichever case the outer `select` takes wins; if `ctx.Done()` is chosen first, the implementation checks again—if completion has already happened, it returns the `value, err` from `Complete` instead of `ctx.Err()`.

## Errors

| Variable | Meaning |
|----------|---------|
| `ErrPendingNotFound` | `Complete` called with an ID that is not in the map. |
| `ErrCompleted` | That ID was already completed once; duplicate `Complete`. |
| `ErrUnsupportedIDType` | The type parameter `I` is not supported by ID generation (see table above). |

## Examples

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thecxx/future"
)

func main() {
	var f future.Future[string, string] // zero value is fine

	ctx := context.Background()
	await := f.Promise(ctx, func(id string) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = f.Complete(id, "hello", nil)
		}()
	})

	v, err := await()
	if err != nil {
		panic(err)
	}
	fmt.Println(v) // hello
}
```

With timeout and a pointer `Future`:

```go
f := future.NewFuture[int, []byte]()
await := f.Promise(context.Background(), func(id int) {
	// hand id to the async layer…
}, future.WithTimeout(2*time.Second))
b, err := await()
_ = b
_ = err
```

## Concurrency

- The task map is guarded by `sync.RWMutex`; each pending completion uses `sync.Once` so the done channel is closed only once.
- Multiple goroutines may call `Promise` / `Complete` on the same `Future` concurrently; each `Promise` gets its own ID, and each `await` removes its entry when run.
