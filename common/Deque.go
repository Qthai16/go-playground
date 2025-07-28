package common

// Element is an element of a linked list.
type Element struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a deque d is implemented
	// as a ring, such that &l.root is both the next element of the last
	// deque element (l.Back()) and the previous element of the first deque
	// element (l.Front()).
	next, prev *Element
	// The value stored with this element.
	Value any
}

type Deque struct {
	root Element
	size int
}

func NewDeque() *Deque {
	return new(Deque).init()
}

// init initializes or clears deque d.
func (d *Deque) init() *Deque {
	d.root.next = &d.root
	d.root.prev = &d.root
	d.size = 0
	return d
}

// link links e after at, increments size.
func (d *Deque) link(e, at *Element) {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	d.size++
}

// unlink unlinks e from its deque, decrements size.
func (d *Deque) unlink(e *Element) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	d.size--
}

func (d *Deque) Size() int {
	return d.size
}

// Remove removes e from its deque, decrements size.
func (d *Deque) Remove(e *Element) {
	if e.next == nil && e.prev == nil {
		return
	}
	if d.size > 0 {
		d.unlink(e)
	}
}

// Front returns the first element of deque d or nil if the deque is empty.
func (d *Deque) Front() *Element {
	if d.size == 0 {
		return nil
	}
	return d.root.next
}

// Back returns the last element of deque d or nil if the deque is empty.
func (d *Deque) Back() *Element {
	if d.size == 0 {
		return nil
	}
	return d.root.prev
}

// PopFront removes an element e at the front of deque d and returns e.
func (d *Deque) PopFront() *Element {
	if d.size == 0 {
		return nil
	}
	e := d.root.next
	d.unlink(e)
	return e
}

// PopBack removes an element e at the back of deque d and returns e.
func (d *Deque) PopBack() *Element {
	if d.size == 0 {
		return nil
	}
	e := d.root.prev
	d.unlink(e)
	return e
}

// PushFront inserts a new element e at the front of deque d.
func (d *Deque) PushFront(e *Element) {
	d.link(e, &d.root)
}

// PushBack inserts a new element e at the back of deque d.
func (d *Deque) PushBack(e *Element) {
	if e.next != nil || e.prev != nil {
		return
	}
	d.link(e, d.root.prev)
}

// PushFrontValue inserts a new element e with value v at the front of deque d and returns e.
func (d *Deque) PushFrontValue(v any) *Element {
	e := &Element{Value: v}
	d.link(e, &d.root)
	return e
}

// PushBackValue inserts a new element e with value v at the back of deque d and returns e.
func (d *Deque) PushBackValue(v any) *Element {
	e := &Element{Value: v}
	d.link(e, d.root.prev)
	return e
}
