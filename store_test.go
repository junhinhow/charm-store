package store_test

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/junhinhow/charm-store"
)

func TestGetSetUpdate(t *testing.T) {
	t.Parallel()

	s := store.New(1)
	if got := s.Get(); got != 1 {
		t.Fatalf("Get inicial = %d, quer 1", got)
	}

	s.Set(2)
	if got := s.Get(); got != 2 {
		t.Fatalf("Get após Set = %d, quer 2", got)
	}

	s.Update(func(v int) int { return v + 3 })
	if got := s.Get(); got != 5 {
		t.Fatalf("Get após Update = %d, quer 5", got)
	}

	s.Update(nil)
	if got := s.Get(); got != 5 {
		t.Fatalf("Update(nil) alterou o valor: %d, quer 5", got)
	}
}

func TestSetIgualNaoNotificaNemEmpilha(t *testing.T) {
	t.Parallel()

	type estado struct {
		Nome  string
		Itens []string
	}

	inicial := estado{Nome: "a", Itens: []string{"x"}}
	s := store.New(inicial)

	var notificacoes int
	s.Subscribe(func(estado) { notificacoes++ })

	s.Set(estado{Nome: "a", Itens: []string{"x"}})
	if notificacoes != 0 {
		t.Fatalf("Set com valor igual notificou %d vez(es)", notificacoes)
	}
	if s.History() != 0 {
		t.Fatalf("History após Set igual = %d, quer 0", s.History())
	}

	s.Update(func(v estado) estado { return v })
	if notificacoes != 0 {
		t.Fatalf("Update sem mudança notificou %d vez(es)", notificacoes)
	}
	if s.History() != 0 {
		t.Fatalf("History após Update igual = %d, quer 0", s.History())
	}

	s.Set(estado{Nome: "b", Itens: []string{"x"}})
	if notificacoes != 1 {
		t.Fatalf("Set com mudança notificou %d vez(es), quer 1", notificacoes)
	}
	if s.History() != 1 {
		t.Fatalf("History após mudança = %d, quer 1", s.History())
	}
}

func TestMultiplosInscritosEOrdem(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	var ordem []string

	s.Subscribe(func(int) { ordem = append(ordem, "primeiro") })
	s.Subscribe(func(int) { ordem = append(ordem, "segundo") })
	s.Subscribe(func(int) { ordem = append(ordem, "terceiro") })

	s.Notify()
	quer := []string{"primeiro", "segundo", "terceiro"}
	if !reflect.DeepEqual(ordem, quer) {
		t.Fatalf("ordem = %v, quer %v", ordem, quer)
	}
}

func TestSubscribeNilEIdempotente(t *testing.T) {
	t.Parallel()

	s := store.New(0)

	cancelarNulo := s.Subscribe(nil)
	cancelarNulo()
	cancelarNulo()

	var chamadas int
	cancelar := s.Subscribe(func(int) { chamadas++ })
	cancelar()
	cancelar()
	cancelar()

	s.Notify()
	if chamadas != 0 {
		t.Fatalf("observador cancelado foi chamado %d vez(es)", chamadas)
	}
}

func TestUnsubscribeDuranteNotificacao(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	var chamadas []string
	var cancelarB func()

	s.Subscribe(func(int) {
		chamadas = append(chamadas, "a")
		cancelarB()
		cancelarB()
	})
	cancelarB = s.Subscribe(func(int) {
		chamadas = append(chamadas, "b")
	})

	s.Notify()
	if quer := []string{"a", "b"}; !reflect.DeepEqual(chamadas, quer) {
		t.Fatalf("primeira notificação = %v, quer %v", chamadas, quer)
	}

	s.Notify()
	if quer := []string{"a", "b", "a"}; !reflect.DeepEqual(chamadas, quer) {
		t.Fatalf("segunda notificação = %v, quer %v", chamadas, quer)
	}
}

func TestNotifySempreDisparaComValorAtual(t *testing.T) {
	t.Parallel()

	s := store.New(7)
	var recebidos []int
	s.Subscribe(func(v int) { recebidos = append(recebidos, v) })

	s.Notify()
	s.Notify()
	if quer := []int{7, 7}; !reflect.DeepEqual(recebidos, quer) {
		t.Fatalf("valores = %v, quer %v", recebidos, quer)
	}
}

func TestUndoRedo(t *testing.T) {
	t.Parallel()

	s := store.New("a")
	if s.Undo() {
		t.Fatal("Undo no início devolveu true")
	}
	if s.Redo() {
		t.Fatal("Redo no início devolveu true")
	}

	var recebidos []string
	s.Subscribe(func(v string) { recebidos = append(recebidos, v) })

	s.Set("b")
	s.Set("c")
	if !s.CanUndo() || s.CanRedo() {
		t.Fatalf("estado inesperado: CanUndo=%v CanRedo=%v", s.CanUndo(), s.CanRedo())
	}
	if got := s.History(); got != 2 {
		t.Fatalf("History = %d, quer 2", got)
	}

	if !s.Undo() {
		t.Fatal("Undo devolveu false com histórico disponível")
	}
	if got := s.Get(); got != "b" {
		t.Fatalf("Get após Undo = %q, quer %q", got, "b")
	}
	if s.History() != 1 || !s.CanRedo() {
		t.Fatalf("após Undo: History=%d CanRedo=%v", s.History(), s.CanRedo())
	}

	if !s.Undo() {
		t.Fatal("segundo Undo devolveu false")
	}
	if got := s.Get(); got != "a" {
		t.Fatalf("Get após segundo Undo = %q, quer %q", got, "a")
	}
	if s.Undo() {
		t.Fatal("Undo no fim da pilha devolveu true")
	}

	if !s.Redo() {
		t.Fatal("Redo devolveu false com mudança desfeita")
	}
	if got := s.Get(); got != "b" {
		t.Fatalf("Get após Redo = %q, quer %q", got, "b")
	}
	if !s.Redo() {
		t.Fatal("segundo Redo devolveu false")
	}
	if s.Redo() {
		t.Fatal("Redo no fim da pilha devolveu true")
	}
	if got := s.Get(); got != "c" {
		t.Fatalf("Get após segundo Redo = %q, quer %q", got, "c")
	}

	quer := []string{"b", "c", "b", "a", "b", "c"}
	if !reflect.DeepEqual(recebidos, quer) {
		t.Fatalf("notificações = %v, quer %v", recebidos, quer)
	}
}

func TestSetLimpaRedo(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	s.Set(1)
	s.Set(2)
	s.Undo()
	if !s.CanRedo() {
		t.Fatal("CanRedo deveria ser true após Undo")
	}

	s.Set(5)
	if s.CanRedo() {
		t.Fatal("Set após Undo deveria limpar o Redo")
	}

	s.Set(5)
	if got := s.History(); got != 2 {
		t.Fatalf("History = %d, quer 2", got)
	}
}

func TestHistoryLimitUm(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	s.SetHistoryLimit(1)
	s.Set(1)
	s.Set(2)
	s.Set(3)

	if got := s.History(); got != 1 {
		t.Fatalf("History = %d, quer 1", got)
	}

	if !s.Undo() {
		t.Fatal("Undo devolveu false")
	}
	if got := s.Get(); got != 2 {
		t.Fatalf("Get após Undo = %d, quer 2", got)
	}
	if s.Undo() {
		t.Fatal("Undo deveria respeitar o limite 1")
	}

	if !s.Redo() {
		t.Fatal("Redo devolveu false")
	}
	if got := s.Get(); got != 3 {
		t.Fatalf("Get após Redo = %d, quer 3", got)
	}
}

func TestHistoryLimitPadraoEReducao(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	for i := 1; i <= 101; i++ {
		s.Set(i)
	}
	if got := s.History(); got != 100 {
		t.Fatalf("History com limite padrão = %d, quer 100", got)
	}

	s.SetHistoryLimit(3)
	if got := s.History(); got != 3 {
		t.Fatalf("History após reduzir limite = %d, quer 3", got)
	}

	if !s.Undo() {
		t.Fatal("primeiro Undo devolveu false")
	}
	if got := s.Get(); got != 100 {
		t.Fatalf("primeiro Undo = %d, quer 100", got)
	}
	if !s.Undo() {
		t.Fatal("segundo Undo devolveu false")
	}
	if got := s.Get(); got != 99 {
		t.Fatalf("segundo Undo = %d, quer 99", got)
	}
	if !s.Undo() {
		t.Fatal("terceiro Undo devolveu false")
	}
	if got := s.Get(); got != 98 {
		t.Fatalf("terceiro Undo = %d, quer 98", got)
	}
	if s.Undo() {
		t.Fatal("quarto Undo deveria falhar: limite 3")
	}

	s.Set(200)
	s.SetHistoryLimit(0)
	if got := s.History(); got != 1 {
		t.Fatalf("History após SetHistoryLimit(0) = %d, quer 1", got)
	}
}

func TestSetHistoryLimitNaoNotifica(t *testing.T) {
	t.Parallel()

	s := store.New(0)
	var notificacoes int
	s.Subscribe(func(int) { notificacoes++ })

	s.SetHistoryLimit(10)
	if notificacoes != 0 {
		t.Fatalf("SetHistoryLimit notificou %d vez(es)", notificacoes)
	}
}

type estadoSnapshot struct {
	Nome  string   `json:"nome"`
	Itens []string `json:"itens"`
}

func TestSnapshotRestoreJSON(t *testing.T) {
	t.Parallel()

	origem := store.New(estadoSnapshot{})
	origem.Set(estadoSnapshot{Nome: "primeiro", Itens: []string{"a"}})
	origem.Set(estadoSnapshot{Nome: "segundo", Itens: []string{"b", "c"}})

	data, err := origem.Snapshot(store.JSONCodec())
	if err != nil {
		t.Fatalf("Snapshot devolveu erro: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Snapshot devolveu dados vazios")
	}

	destino := store.New(estadoSnapshot{})
	destino.Set(estadoSnapshot{Nome: "antigo"})
	if destino.History() != 1 {
		t.Fatalf("History antes de Restore = %d, quer 1", destino.History())
	}

	var notificados []estadoSnapshot
	destino.Subscribe(func(v estadoSnapshot) { notificados = append(notificados, v) })

	if err := destino.Restore(store.JSONCodec(), data); err != nil {
		t.Fatalf("Restore devolveu erro: %v", err)
	}
	if !reflect.DeepEqual(destino.Get(), origem.Get()) {
		t.Fatalf("valor restaurado = %#v, quer %#v", destino.Get(), origem.Get())
	}
	if destino.History() != 0 || destino.CanUndo() || destino.CanRedo() {
		t.Fatalf("Restore deveria limpar o histórico: History=%d CanUndo=%v CanRedo=%v",
			destino.History(), destino.CanUndo(), destino.CanRedo())
	}
	if len(notificados) != 1 {
		t.Fatalf("Restore notificou %d vez(es), quer 1", len(notificados))
	}
	if !reflect.DeepEqual(notificados[0], origem.Get()) {
		t.Fatalf("notificação = %#v, quer %#v", notificados[0], origem.Get())
	}
}

func TestRestoreNotificaMesmoComValorIgual(t *testing.T) {
	t.Parallel()

	s := store.New(estadoSnapshot{Nome: "igual"})
	var notificacoes int
	s.Subscribe(func(estadoSnapshot) { notificacoes++ })

	data, err := s.Snapshot(store.JSONCodec())
	if err != nil {
		t.Fatalf("Snapshot devolveu erro: %v", err)
	}
	if err := s.Restore(store.JSONCodec(), data); err != nil {
		t.Fatalf("Restore devolveu erro: %v", err)
	}
	if notificacoes != 1 {
		t.Fatalf("Restore com valor igual notificou %d vez(es), quer 1", notificacoes)
	}
}

type codecFalho struct {
	err error
}

func (c codecFalho) Marshal(any) ([]byte, error) {
	return nil, c.err
}

func (c codecFalho) Unmarshal([]byte, any) error {
	return c.err
}

func TestErrosDeCodec(t *testing.T) {
	t.Parallel()

	s := store.New(10)
	falha := errors.New("falha proposital")
	codec := codecFalho{err: falha}

	if _, err := s.Snapshot(codec); err == nil {
		t.Fatal("Snapshot com codec falho devolveu nil")
	} else if !errors.Is(err, falha) {
		t.Fatalf("erro de Snapshot = %v, quer envolver %v", err, falha)
	} else if !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("erro de Snapshot sem contexto: %v", err)
	}

	if err := s.Restore(codec, []byte("qualquer")); err == nil {
		t.Fatal("Restore com codec falho devolveu nil")
	} else if !errors.Is(err, falha) {
		t.Fatalf("erro de Restore = %v, quer envolver %v", err, falha)
	} else if !strings.Contains(err.Error(), "restaurar") {
		t.Fatalf("erro de Restore sem contexto: %v", err)
	}

	if _, err := s.Snapshot(nil); err == nil {
		t.Fatal("Snapshot(nil) devolveu nil")
	}
	if err := s.Restore(nil, nil); err == nil {
		t.Fatal("Restore(nil) devolveu nil")
	}
}

func TestRestoreInvalidoNaoAlteraEstado(t *testing.T) {
	t.Parallel()

	s := store.New(estadoSnapshot{Nome: "atual"})
	s.Set(estadoSnapshot{Nome: "anterior"})
	antes := s.Get()
	historyAntes := s.History()

	var notificacoes int
	s.Subscribe(func(estadoSnapshot) { notificacoes++ })

	err := s.Restore(store.JSONCodec(), []byte("{"))
	if err == nil {
		t.Fatal("Restore com JSON inválido devolveu nil")
	}
	if !strings.Contains(err.Error(), "restaurar") {
		t.Fatalf("erro sem contexto: %v", err)
	}
	if !reflect.DeepEqual(s.Get(), antes) {
		t.Fatalf("estado alterado após erro: %#v, quer %#v", s.Get(), antes)
	}
	if got := s.History(); got != historyAntes {
		t.Fatalf("History alterado após erro = %d, quer %d", got, historyAntes)
	}
	if notificacoes != 0 {
		t.Fatalf("Restore com erro notificou %d vez(es)", notificacoes)
	}
}

func TestConcorrencia(t *testing.T) {
	t.Parallel()

	const goroutines = 50
	const iteracoes = 20

	s := store.New(0)
	var chamadas atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iteracoes; j++ {
				s.Set(i*iteracoes + j)
				_ = s.Get()

				cancelar := s.Subscribe(func(int) { chamadas.Add(1) })
				s.Notify()
				cancelar()
				cancelar()

				s.Update(func(v int) int { return v + 1 })
				s.Undo()
				s.Redo()
				_ = s.CanUndo()
				_ = s.CanRedo()
				_ = s.History()
			}
		}(i)
	}

	wg.Wait()

	if s.Get() < 0 {
		t.Fatalf("valor final inesperado: %d", s.Get())
	}
	if chamadas.Load() == 0 {
		t.Fatal("nenhum observador foi chamado durante o teste concorrente")
	}
}
