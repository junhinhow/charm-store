// Package store implementa um contêiner de estado observável para TUIs.
//
// O Store guarda um valor, notifica inscritos quando ele muda e mantém um
// histórico limitado para Undo e Redo. Todos os métodos são seguros para uso
// concorrente e os callbacks nunca são chamados com os locks internos presos,
// o que permite que um observador chame de volta a API do store.
//
// Os valores não são copiados em profundidade. Use tipos imutáveis (valores,
// structs sem ponteiros compartilhados) ou sincronize por conta própria a
// mutação de mapas, slices e ponteiros guardados dentro de T.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// defaultHistoryLimit é a quantidade padrão de estados anteriores mantidos
// para Undo.
const defaultHistoryLimit = 100

// Codec serializa e desserializa o valor de um Store. JSONCodec, codecs YAML e
// encoding/gob podem ser usados desde que respeitem esta interface.
type Codec interface {
	// Marshal converte v em bytes.
	Marshal(v any) ([]byte, error)
	// Unmarshal lê data e preenche v.
	Unmarshal(data []byte, v any) error
}

// JSONCodec devolve um Codec baseado em encoding/json.
func JSONCodec() Codec {
	return jsonCodec{}
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Store é um contêiner de estado observável com histórico para desfazer e
// refazer mudanças. O valor zero de Store não é utilizável: crie-o com New.
type Store[T any] struct {
	mu sync.RWMutex

	value  T
	subs   []subscriber[T]
	nextID uint64

	undo  []T
	redo  []T
	limit int

	// derived marca stores criados por Select. Eles continuam observáveis,
	// mas não mantêm histórico próprio.
	derived bool
}

type subscriber[T any] struct {
	id uint64
	fn func(T)
}

// New cria um Store com o valor inicial informado, sem histórico e com limite
// padrão de 100 estados para Undo.
func New[T any](initial T) *Store[T] {
	return &Store[T]{
		value: initial,
		limit: defaultHistoryLimit,
	}
}

// Get devolve o valor atual.
func (s *Store[T]) Get() T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value
}

// Set substitui o valor atual. Se o novo valor for igual ao atual segundo
// reflect.DeepEqual, nada acontece: nem histórico, nem notificação. Em uma
// mudança real, o valor anterior entra no histórico de Undo, o histórico de
// Redo é descartado e os inscritos são notificados fora do lock.
func (s *Store[T]) Set(v T) {
	s.mu.Lock()
	if reflect.DeepEqual(s.value, v) {
		s.mu.Unlock()
		return
	}
	s.recordLocked(s.value)
	s.redo = nil
	s.value = v
	subs := s.subscribersLocked()
	s.mu.Unlock()

	dispatch(subs, v)
}

// Update calcula o próximo valor com fn e o aplica como Set. fn é chamada fora
// do lock e uma função nil é ignorada. Atualizações concorrentes seguem a regra
// da última escrita: cada uma lê o valor no início e a que terminar por último
// vence.
func (s *Store[T]) Update(fn func(T) T) {
	if fn == nil {
		return
	}
	s.Set(fn(s.Get()))
}

// Subscribe registra fn para receber o valor a cada mudança e a cada Notify.
// Os observadores são chamados na ordem de inscrição, fora do lock. A função
// devolvida cancela a inscrição e é idempotente; chamá-la mais de uma vez não
// causa panic. Uma função nil é ignorada e devolve um cancelamento inócuo.
//
// Se um observador cancelar outro durante uma notificação, a lista já copiada
// para aquela rodada continua sendo usada: o cancelado ainda recebe o valor
// atual, mas não participa das notificações seguintes.
func (s *Store[T]) Subscribe(fn func(T)) (unsubscribe func()) {
	if fn == nil {
		return func() {}
	}

	s.mu.Lock()
	s.nextID++
	id := s.nextID
	s.subs = append(s.subs, subscriber[T]{id: id, fn: fn})
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.removeSubscriber(id)
		})
	}
}

// Notify chama todos os inscritos com o valor atual, mesmo que ele não tenha
// mudado. É útil para reenviar o estado a um tea.Cmd ou sincronizar uma view
// depois de uma restauração externa.
func (s *Store[T]) Notify() {
	s.mu.RLock()
	value := s.value
	subs := s.subscribersLocked()
	s.mu.RUnlock()

	dispatch(subs, value)
}

// Undo volta um estado no histórico e devolve true. Sem estados anteriores
// (ou em um store derivado de Select), devolve false sem alterar nada. O valor
// atual é empilhado para Redo e os inscritos são notificados com o novo valor.
func (s *Store[T]) Undo() bool {
	s.mu.Lock()
	if s.derived || len(s.undo) == 0 {
		s.mu.Unlock()
		return false
	}

	last := len(s.undo) - 1
	previous := s.undo[last]
	s.undo = s.undo[:last]

	s.redo = append(s.redo, s.value)
	s.redo = trimFront(s.redo, s.limit)
	s.value = previous
	subs := s.subscribersLocked()
	s.mu.Unlock()

	dispatch(subs, previous)
	return true
}

// Redo refaz a última mudança desfeita e devolve true. Sem mudanças para
// refazer (ou em um store derivado de Select), devolve false sem alterar nada.
// O valor atual é empilhado para Undo e os inscritos são notificados.
func (s *Store[T]) Redo() bool {
	s.mu.Lock()
	if s.derived || len(s.redo) == 0 {
		s.mu.Unlock()
		return false
	}

	last := len(s.redo) - 1
	next := s.redo[last]
	s.redo = s.redo[:last]

	s.undo = append(s.undo, s.value)
	s.undo = trimFront(s.undo, s.limit)
	s.value = next
	subs := s.subscribersLocked()
	s.mu.Unlock()

	dispatch(subs, next)
	return true
}

// CanUndo informa se há um estado anterior disponível para Undo.
func (s *Store[T]) CanUndo() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.derived && len(s.undo) > 0
}

// CanRedo informa se há uma mudança desfeita disponível para Redo.
func (s *Store[T]) CanRedo() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.derived && len(s.redo) > 0
}

// History devolve quantos estados anteriores estão disponíveis para Undo. O
// histórico de Redo não é contado. Stores derivados de Select sempre devolvem 0.
func (s *Store[T]) History() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.derived {
		return 0
	}
	return len(s.undo)
}

// SetHistoryLimit define o número máximo de estados mantidos em Undo e Redo.
// Valores menores que 1 são ajustados para 1. Ao reduzir o limite, os estados
// mais antigos são descartados e os mais recentes são preservados.
func (s *Store[T]) SetHistoryLimit(n int) {
	if n < 1 {
		n = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = n
	s.undo = trimFront(s.undo, n)
	s.redo = trimFront(s.redo, n)
}

// Snapshot serializa o valor atual com o codec informado. O codec é chamado
// fora do lock. Erros do codec são devolvidos com contexto; um codec nil
// devolve erro sem chamar nada.
func (s *Store[T]) Snapshot(c Codec) ([]byte, error) {
	if c == nil {
		return nil, errors.New("store: codec não pode ser nil ao gerar snapshot")
	}

	s.mu.RLock()
	value := s.value
	s.mu.RUnlock()

	data, err := c.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("store: falha ao gerar snapshot: %w", err)
	}
	return data, nil
}

// Restore desserializa data no valor do store. Em caso de erro do codec, o
// estado e o histórico permanecem intactos. Em caso de sucesso, o histórico de
// Undo e Redo é limpo, nenhum estado é empilhado e todos os inscritos são
// notificados, mesmo que o valor restaurado seja igual ao atual.
//
// Restaurar um store derivado de Select é permitido, mas o valor pode ser
// sobrescrito na próxima mudança da origem.
func (s *Store[T]) Restore(c Codec, data []byte) error {
	if c == nil {
		return errors.New("store: codec não pode ser nil ao restaurar snapshot")
	}

	var value T
	if err := c.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("store: falha ao restaurar snapshot: %w", err)
	}

	s.mu.Lock()
	s.value = value
	s.undo = nil
	s.redo = nil
	subs := s.subscribersLocked()
	s.mu.Unlock()

	dispatch(subs, value)
	return nil
}

// Select cria um store derivado que mapeia o valor de s com fn. O store
// derivado começa com o valor atual mapeado e se atualiza sempre que a origem
// muda; ele só notifica seus inscritos quando o valor mapeado realmente muda,
// segundo reflect.DeepEqual.
//
// O derivado não mantém histórico próprio: Undo, Redo, CanUndo e CanRedo são
// no-op e History devolve 0. A inscrição na origem acompanha o tempo de vida da
// origem; não há Close.
//
// Select entra em panic se s ou fn forem nil, pois são erros de programação.
func Select[T, V any](s *Store[T], fn func(T) V) *Store[V] {
	if s == nil {
		panic("store: Select recebeu store de origem nil")
	}
	if fn == nil {
		panic("store: Select recebeu fn nil")
	}

	derived := New(fn(s.Get()))
	derived.mu.Lock()
	derived.derived = true
	derived.mu.Unlock()

	s.Subscribe(func(value T) {
		derived.Set(fn(value))
	})
	return derived
}

// recordLocked empilha previous no histórico de Undo, respeitando o limite.
func (s *Store[T]) recordLocked(previous T) {
	if s.derived {
		return
	}
	s.undo = append(s.undo, previous)
	s.undo = trimFront(s.undo, s.limit)
}

// subscribersLocked copia a lista de inscritos para que a notificação aconteça
// sem o lock. O chamador deve segurar s.mu.
func (s *Store[T]) subscribersLocked() []subscriber[T] {
	if len(s.subs) == 0 {
		return nil
	}
	out := make([]subscriber[T], len(s.subs))
	copy(out, s.subs)
	return out
}

// removeSubscriber remove a inscrição com o id informado. Ids inexistentes são
// ignorados, o que torna o cancelamento idempotente.
func (s *Store[T]) removeSubscriber(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.subs {
		if s.subs[i].id == id {
			s.subs = append(s.subs[:i], s.subs[i+1:]...)
			return
		}
	}
}

// dispatch chama os inscritos sem segurar nenhum lock.
func dispatch[T any](subs []subscriber[T], value T) {
	for _, sub := range subs {
		sub.fn(value)
	}
}

// trimFront mantém no máximo max elementos, descartando os mais antigos.
func trimFront[T any](items []T, max int) []T {
	if len(items) <= max {
		return items
	}

	out := make([]T, max)
	copy(out, items[len(items)-max:])
	return out
}
