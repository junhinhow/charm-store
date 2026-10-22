# charm-store

Estado observável para TUIs Bubble Tea: um store tipado com inscrições, undo/redo limitado e snapshots plugáveis.

[English](README.md)

## Instalação

```bash
go get github.com/junhinhow/charm-store
```

## Uso

```go
package main

import (
	"fmt"

	"github.com/junhinhow/charm-store"
)

func main() {
	contador := store.New(0)
	cancelar := contador.Subscribe(func(v int) {
		fmt.Println("contador:", v)
	})
	defer cancelar()

	contador.Set(1)
	contador.Set(1) // valor igual: sem notificação e sem histórico
	contador.Update(func(v int) int { return v + 1 })

	contador.Undo()
	fmt.Println("após undo:", contador.Get())
}
```

### Integração com Bubble Tea

O store é a fonte da verdade. `Subscribe` envia um sinal para um canal e um `tea.Cmd` transforma esse sinal em mensagem. O envio não bloqueia, então uma UI lenta nunca trava o `Set`.

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
		default: // já existe um sinal pendente; coalesce
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
	return tea.NewView(fmt.Sprintf("contador: %d\n\n+/- altera, u desfaz, r refaz, q sai\n", m.count))
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

Como a mensagem só acorda a UI, o modelo relê `m.store.Get()` em vez de confiar no payload da mensagem. Se várias mudanças acontecerem enquanto nenhum comando espera, o sinal no canal com buffer é coalescido e a view continua mostrando o valor mais recente. Chame `Notify()` quando precisar reenviar o valor atual sem alterá-lo.

## API

| Símbolo | Descrição |
| --- | --- |
| `New[T](initial T) *Store[T]` | Cria um store com limite padrão de 100 estados para undo. |
| `(*Store[T]).Get` / `Set` / `Update` | Lê e escreve o estado; `Set` e `Update` só notificam em mudanças reais. |
| `(*Store[T]).Subscribe(fn) func()` | Registra um observador e devolve uma função de cancelamento idempotente. |
| `(*Store[T]).Notify()` | Chama todos os observadores com o valor atual, mesmo sem mudança. |
| `(*Store[T]).Undo` / `Redo` | Navega pelo histórico e informa se algo mudou. |
| `(*Store[T]).CanUndo` / `CanRedo` | Informa se undo/redo está disponível. |
| `(*Store[T]).History()` | Quantidade de estados disponíveis para undo (redo não é contado). |
| `(*Store[T]).SetHistoryLimit(n)` | Define a profundidade de undo/redo; valores abaixo de 1 são ajustados para 1. |
| `Codec` / `JSONCodec` | Interface de serialização e sua implementação JSON. |
| `(*Store[T]).Snapshot(c)` / `Restore(c, data)` | Serializa o valor atual e restaura um snapshot. |
| `Select[T, V](s, fn)` | Store derivado que só notifica quando o valor mapeado muda. |

## Notas de projeto

- **Concorrência:** todos os métodos são seguros para uso concorrente. Os observadores são copiados sob o lock e chamados depois de soltá-lo, na ordem de inscrição, então um callback pode chamar a API do store sem deadlock. Um observador removido durante uma notificação ainda recebe aquela rodada; depois é ignorado.
- **Detecção de mudança:** valores são comparados com `reflect.DeepEqual`. Um `Set` ou `Update` sem mudança não notifica nem cresce o histórico. Como consequência, `NaN` sempre conta como diferente.
- **Histórico:** `Set` depois de um undo limpa o redo. `Undo` e `Redo` notificam com o valor restaurado. Um `Restore` bem-sucedido limpa as duas pilhas e sempre notifica, mesmo que o valor restaurado seja igual.
- **Semântica de Update:** `fn` roda fora do lock, então atualizações concorrentes seguem a última escrita, e não um read-modify-write atômico.
- **Stores derivados:** `Select` devolve um store observável normal com histórico desativado: `Undo`, `Redo`, `CanUndo` e `CanRedo` são no-ops documentados e `History` é sempre 0. A inscrição na origem vive enquanto a origem viver; não há `Close`.
- **Valores rasos:** `Get`, `Snapshot` e o histórico guardam o valor como está. Trate mapas, slices e ponteiros dentro de `T` como imutáveis, ou sincronize a mutação por conta própria.
- **Erros de codec:** erros são envolvidos com contexto (`store: ...: %w`) e deixam o store intacto. `Restore` desserializa antes de pegar o lock, então um payload inválido não corrompe o estado.

## Licença

MIT
