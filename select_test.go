package store_test

import (
	"reflect"
	"testing"

	"github.com/junhinhow/charm-store"
)

type estadoSelecionado struct {
	Contador int
	Nome     string
	Tags     []string
}

func TestSelectPropagaApenasMudancas(t *testing.T) {
	t.Parallel()

	origem := store.New(estadoSelecionado{Contador: 1, Nome: "a"})
	contador := store.Select(origem, func(e estadoSelecionado) int { return e.Contador })
	if got := contador.Get(); got != 1 {
		t.Fatalf("valor inicial do derivado = %d, quer 1", got)
	}

	var recebidos []int
	contador.Subscribe(func(v int) { recebidos = append(recebidos, v) })

	origem.Set(estadoSelecionado{Contador: 1, Nome: "b"})
	if len(recebidos) != 0 {
		t.Fatalf("mudança sem impacto no mapeamento notificou %v", recebidos)
	}

	origem.Set(estadoSelecionado{Contador: 2, Nome: "b"})
	if quer := []int{2}; !reflect.DeepEqual(recebidos, quer) {
		t.Fatalf("notificações = %v, quer %v", recebidos, quer)
	}
	if got := contador.Get(); got != 2 {
		t.Fatalf("valor do derivado = %d, quer 2", got)
	}

	origem.Notify()
	if quer := []int{2}; !reflect.DeepEqual(recebidos, quer) {
		t.Fatalf("Notify da origem sem mudança notificou %v", recebidos)
	}

	origem.Set(estadoSelecionado{Contador: 2, Nome: "c"})
	if quer := []int{2}; !reflect.DeepEqual(recebidos, quer) {
		t.Fatalf("segunda mudança sem impacto notificou %v", recebidos)
	}
}

func TestSelectComparaComDeepEqual(t *testing.T) {
	t.Parallel()

	origem := store.New(estadoSelecionado{Tags: []string{"a", "b"}})
	tags := store.Select(origem, func(e estadoSelecionado) []string { return e.Tags })

	var recebidos [][]string
	tags.Subscribe(func(v []string) { recebidos = append(recebidos, v) })

	origem.Set(estadoSelecionado{Tags: []string{"a", "b"}})
	if len(recebidos) != 0 {
		t.Fatalf("slice com conteúdo igual notificou %v", recebidos)
	}

	origem.Set(estadoSelecionado{Tags: []string{"a", "b", "c"}})
	if len(recebidos) != 1 || !reflect.DeepEqual(recebidos[0], []string{"a", "b", "c"}) {
		t.Fatalf("notificações = %v, quer [[a b c]]", recebidos)
	}
}

func TestSelectNaoTemHistorico(t *testing.T) {
	t.Parallel()

	origem := store.New(0)
	dobro := store.Select(origem, func(v int) int { return v * 2 })

	origem.Set(1)
	origem.Set(2)

	if got := dobro.Get(); got != 4 {
		t.Fatalf("valor do derivado = %d, quer 4", got)
	}
	if dobro.History() != 0 {
		t.Fatalf("History do derivado = %d, quer 0", dobro.History())
	}
	if dobro.CanUndo() || dobro.CanRedo() {
		t.Fatalf("derivado não deveria ter undo/redo: CanUndo=%v CanRedo=%v",
			dobro.CanUndo(), dobro.CanRedo())
	}
	if dobro.Undo() {
		t.Fatal("Undo do derivado devolveu true")
	}
	if dobro.Redo() {
		t.Fatal("Redo do derivado devolveu true")
	}
}
