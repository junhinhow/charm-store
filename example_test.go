package store_test

import (
	"fmt"

	"github.com/junhinhow/charm-store"
)

func ExampleStore() {
	s := store.New(0)
	s.Subscribe(func(v int) {
		fmt.Println("mudou:", v)
	})

	s.Set(1)
	s.Set(1) // valor igual: sem notificação e sem histórico
	s.Update(func(v int) int { return v + 1 })
	fmt.Println("valor:", s.Get())

	s.Undo()
	fmt.Println("após undo:", s.Get())

	// Output:
	// mudou: 1
	// mudou: 2
	// valor: 2
	// mudou: 1
	// após undo: 1
}

func ExampleSelect() {
	origem := store.New([]int{1, 2, 3})
	tamanho := store.Select(origem, func(v []int) int { return len(v) })

	tamanho.Subscribe(func(v int) {
		fmt.Println("tamanho:", v)
	})

	origem.Set([]int{1, 2})
	origem.Set([]int{1, 2, 3, 4})

	// Output:
	// tamanho: 2
	// tamanho: 4
}
