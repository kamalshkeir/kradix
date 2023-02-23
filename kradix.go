package kradix

import (
	"bytes"
	"sync"
)

const (
	branchingFactor = 128
)

type node[T any] struct {
	terminal bool
	value    T
	edges    [branchingFactor]*node[T]
}

type RadixTree[T any] struct {
	root *node[T]
	pool sync.Pool
}

func New[T any]() *RadixTree[T] {
	return &RadixTree[T]{
		root: &node[T]{},
		pool: sync.Pool{
			New: func() interface{} {
				return &node[T]{}
			},
		},
	}
}

func (t *RadixTree[T]) Insert(key string, value T) {
	t.root = t.insert(t.root, key, value)
}

func (t *RadixTree[T]) insert(n *node[T], key string, value T) *node[T] {
	if n == nil {
		n = t.pool.Get().(*node[T])
	}

	if len(key) == 0 {
		n.terminal = true
		n.value = value
		return n
	}

	c := key[0]
	child := n.edges[c]
	if child == nil {
		child = t.pool.Get().(*node[T])
		n.edges[c] = child
	}
	child = t.insert(child, key[1:], value)
	n.edges[c] = child

	return n
}

func (t *RadixTree[T]) Get(key string) (T, bool) {
	n := t.get(t.root, key)
	if n == nil {
		return *new(T), false
	}
	return n.value, n.terminal
}

func (t *RadixTree[T]) get(n *node[T], key string) *node[T] {
	if n == nil {
		return nil
	}

	if len(key) == 0 {
		return n
	}

	c := key[0]
	return t.get(n.edges[c], key[1:])
}

func (t *RadixTree[T]) Delete(key string) bool {
	var deleted bool
	t.root = t.delete(t.root, key, &deleted)
	return deleted
}

func (t *RadixTree[T]) delete(n *node[T], key string, deleted *bool) *node[T] {
	if n == nil {
		return nil
	}

	if len(key) == 0 {
		n.terminal = false
		n.value = *new(T)
		*deleted = true
		return t.release(n)
	}

	c := key[0]
	child := t.delete(n.edges[c], key[1:], deleted)
	n.edges[c] = child

	if !n.terminal && t.isLeaf(n) && !*deleted {
		*deleted = true
		return t.release(n)
	}

	return n
}

func (t *RadixTree[T]) release(n *node[T]) *node[T] {
	for i := range n.edges {
		if n.edges[i] != nil {
			t.release(n.edges[i])
			n.edges[i] = nil
		}
	}

	t.pool.Put(n)
	return nil
}

func (t *RadixTree[T]) isLeaf(n *node[T]) bool {
	for _, e := range n.edges {
		if e != nil {
			return false
		}
	}
	return true
}

func (t *RadixTree[T]) Traverse(f func(string, T)) {
	var wg sync.WaitGroup
	stack := make([]*node[T], 0, branchingFactor)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	wg.Add(1)
	stack = append(stack, t.root)

	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if n == nil {
			continue
		}

		if n.terminal {
			f(t.prefix(stack), n.value)
		}

		if t.hasChildren(n) {
			var numChildren int
			var children []*node[T]
			for _, e := range n.edges {
				if e != nil {
					numChildren++
					children = append(children, e)
				}
			}
			wg.Add(1)
			go func(children []*node[T], prefixLen int) {
				for _, child := range children {
					stack = append(stack, child)
				}
				wg.Done()
			}(children, len(stack))

			// To avoid creating too many goroutines, we only create a new goroutine
			// once the number of children exceeds a certain threshold.
			if numChildren > 10 {
				wg.Wait()
			}
		}
	}

	wg.Done()
	<-done
}

func (t *RadixTree[T]) hasChildren(n *node[T]) bool {
	for _, e := range n.edges {
		if e != nil {
			return true
		}
	}
	return false
}

func (t *RadixTree[T]) prefix(stack []*node[T]) string {
	var buffer bytes.Buffer
	for _, n := range stack[1:] {
		for i, e := range n.edges {
			if e == stack[len(stack)-1] {
				buffer.WriteByte(byte(i))
				break
			}
		}
	}
	return buffer.String()
}
