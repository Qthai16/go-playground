package lru

type LRUItem struct {
	Key   []byte
	Value any
}

func NewItem(key []byte, value any) *LRUItem {
	item := &LRUItem{
		Key:   make([]byte, len(key)),
		Value: value,
	}
	copy(item.Key, key)
	return item
}
