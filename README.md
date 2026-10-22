# charm-store

Observable state for Bubble Tea TUIs: a typed store with subscriptions, bounded undo/redo and pluggable snapshots.

[Português](README.pt-br.md)

## Install

```bash
go get github.com/junhinhow/charm-store
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/junhinhow/charm-store"
)

func main() {
	counter := store.New(0)
	unsubscribe := counter.Subscribe(func(v int) {
		fmt.Println("counter:", v)
	})
	defer unsubscribe()

	counter.Set(1)
	counter.Set(1) // equal value: no notification and no history
	counter.Update(func(v int) int { return v + 1 })

	counter.Undo()
	fmt.Println("after undo:", counter.Get())
}
```

### Bubble Tea integration

The store is the source of truth. `Subscribe` pushes a signal into a channel and a `tea.Cmd` turns that signal into a message. Sending is non-blocking, so a slow UI never blocks `Set`.

```go
package main

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
	"github.com/junhinhow/charm-store"
)

type storeChanged struct{}

type model struct {
	store  *store.Store[int]
	count  int
	events <-chan struct{}
}

func newModel() model {
	s := store.New(0)
	events := make(chan struct{}, 1)
	s.Subscribe(func(int) {
		select {
		case events <- struct{}{}:
		default: // a signal is already pending; coalesce
		}
	})
	return model{store: s, events: events}
}

func (m model) Init() tea.Cmd { return waitForStore(m.events) }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "+":
			m.store.Update(func(v int) int { return v + 1 })
		case "-":
			m.store.Update(func(v int) int { return v - 1 })
		case "u":
			m.store.Undo()
		case "r":
			m.store.Redo()
		}
	case storeChanged:
		m.count = m.store.Get()
		return m, waitForStore(m.events)
	}
	return m, nil
}

func (m model) View() tea.View {
	return tea.NewView(fmt.Sprintf("count: %d\n\n+/- change, u undo, r redo, q quit\n", m.count))
}

func waitForStore(events <-chan struct{}) tea.Cmd {
	return func() tea.Msg {
		<-events
		return storeChanged{}
	}
}

func main() {
	if _, err := tea.NewProgram(newModel()).Run(); err != nil {
		panic(err)
	}
}
```

Because the message only wakes the UI, the model rereads `m.store.Get()` instead of trusting the message payload. If several changes happen while no command is waiting, the buffered signal coalesces and the view still shows the latest value. Call `Notify()` when you need to push the current value without changing it.

## API

| Symbol | Description |
| --- | --- |
| `New[T](initial T) *Store[T]` | Creates a store with the default 100-state undo limit. |
| `(*Store[T]).Get` / `Set` / `Update` | Reads and writes state; `Set` and `Update` only notify on real changes. |
| `(*Store[T]).Subscribe(fn) func()` | Registers an observer and returns an idempotent cancel function. |
| `(*Store[T]).Notify()` | Calls every observer with the current value, even if unchanged. |
| `(*Store[T]).Undo` / `Redo` | Moves through history and reports whether it changed anything. |
| `(*Store[T]).CanUndo` / `CanRedo` | Reports whether undo/redo is available. |
| `(*Store[T]).History()` | Number of states currently available for undo (redo is not counted). |
| `(*Store[T]).SetHistoryLimit(n)` | Sets the undo/redo depth; values below 1 are clamped to 1. |
| `Codec` / `JSONCodec` | Serialization interface and its JSON implementation. |
| `(*Store[T]).Snapshot(c)` / `Restore(c, data)` | Serializes the current value and restores a snapshot. |
| `Select[T, V](s, fn)` | Derived store that only notifies when the mapped value changes. |

## Design notes

- **Concurrency:** every method is safe for concurrent use. Observers are copied under the lock and called after it is released, in registration order, so a callback may safely call back into the store. An observer removed during a notification still receives that round; it is skipped afterwards.
- **Change detection:** values are compared with `reflect.DeepEqual`. A no-op `Set` or `Update` neither notifies nor grows history. As a consequence, `NaN` always counts as different.
- **History:** `Set` after an undo clears redo. `Undo` and `Redo` notify with the value they restore. A successful `Restore` clears both stacks and always notifies, even when the restored value is equal.
- **Update semantics:** `fn` runs outside the lock, so concurrent updates are last-write-wins rather than atomic read-modify-write.
- **Derived stores:** `Select` returns a normal observable store with history disabled: `Undo`, `Redo`, `CanUndo` and `CanRedo` are documented no-ops and `History` is always 0. Its subscription lives as long as the source; there is no `Close`.
- **Shallow values:** `Get`, `Snapshot` and history store the value as-is. Treat maps, slices and pointers inside `T` as immutable, or synchronize their mutation yourself.
- **Codec errors:** errors are wrapped with context (`store: ...: %w`) and leave the store untouched. `Restore` unmarshals before taking the lock, so a bad payload cannot corrupt state.

## License

MIT
