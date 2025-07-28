package lru

import (
	"fmt"
	"math/rand"

	"hash/fnv"
	"sync"

	"github.com/Qthai16/go-playground/utils/hashkit"

	"github.com/aviddiviner/go-murmur"
)

// TODO: item with random expired time [from, to)

type (
	EvictItemCb    func(item *LRUItem) error
	TraverseItemFn func(item *LRUItem)
	HashFn         func([]byte) uint32

	LRUConfig struct {
		TableSize     uint32  // max store items
		BucketTbRatio float32 // ratio of bucket size to table size, must > 1, larger value means less collision
		EvictRatio    float32 // threshold in percent that LRU will evict when full
		Hash32        HashFn
		EvictCb       EvictItemCb
	}

	LRUTable struct {
		LRUConfig
		buckets     []bucket
		lruList     linkedList
		freeList    linkedList
		mu          sync.Mutex
		bucketSize  uint32
		currentSize uint32
	}

	bucket struct {
		lru  idxLink
		col  idxLink
		item *LRUItem
	}

	idxLink struct {
		next int32
		pre  int32
	}

	linkedList struct {
		head int32
		tail int32
		tb   *LRUTable
	}
)

func JenkinHash(v []byte) uint32 {
	// jenkinsHash := hashkit.New32()
	jenkinHashHandle.Reset()
	jenkinHashHandle.Write(v)
	return jenkinHashHandle.Sum32()
}

func FNVHash(v []byte) uint32 {
	// fnvHash := fnv.New32a()
	fnvHashHandle.Reset()
	fnvHashHandle.Write(v)
	return fnvHashHandle.Sum32()
}

func Murmur32Hash(v []byte) uint32 {
	// murmurHash := murmur.New32(Murmur32Seed)
	murmur32HashHanle.Reset()
	murmur32HashHanle.Write(v)
	return murmur32HashHanle.Sum32()
}

func byteEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func dummyEvictCb(item *LRUItem) error {
	return nil
}

const (
	DefaultEvictRatio    = 0.1
	DefaultBucketTbRatio = 1.5
)

var (
	jenkinHashHandle  = hashkit.NewJenkins32()
	fnvHashHandle     = fnv.New32a()
	murmur32HashHanle = murmur.New32(rand.Uint32())
	defaultHash       = JenkinHash
)

func (i *idxLink) init() {
	i.next = -1
	i.pre = -1
}

func (idx *idxLink) empty() bool {
	return idx.next == -1 && idx.pre == -1
}

func (l *idxLink) String() string {
	return fmt.Sprintf("pre: %v, next: %v", l.pre, l.next)
}

func (l *linkedList) empty() bool {
	return (l.head == -1 && l.tail == -1)
}

// pushFront unlink buckets[idx], then push front buckets[idx] to the list
func (l *linkedList) pushFront(idx int32) {
	if l.empty() {
		l.head, l.tail = idx, idx
		l.tb.buckets[idx].lru.next = -1
		l.tb.buckets[idx].lru.pre = -1
		return
	}
	if l.head == idx {
		return
	}
	if l.tail == idx {
		l.tail = l.tb.buckets[idx].lru.pre
	}
	next := l.tb.buckets[idx].lru.next
	pre := l.tb.buckets[idx].lru.pre
	if next != -1 {
		l.tb.buckets[next].lru.pre = pre
	}
	if pre != -1 {
		l.tb.buckets[pre].lru.next = next
	}
	l.tb.buckets[l.head].lru.pre = idx
	l.tb.buckets[idx].lru.pre = -1
	l.tb.buckets[idx].lru.next = l.head
	l.head = idx
}

// pop remove the first element in list and update head
func (l *linkedList) pop() (idx int32) {
	if l.empty() {
		return -1
	}
	idx = l.head
	next := l.tb.buckets[idx].lru.next
	l.tb.buckets[idx].lru.next = -1
	if next == -1 { // pop last element
		l.head = -1
		l.tail = -1
		return idx
	}
	l.tb.buckets[next].lru.pre = -1
	l.head = next
	return idx
}

// erase unlink a bucket at idx and remove it from list
func (l *linkedList) erase(idx int32) {
	if l.empty() {
		return
	}
	next := l.tb.buckets[idx].lru.next
	pre := l.tb.buckets[idx].lru.pre
	if next != -1 {
		l.tb.buckets[next].lru.pre = pre
	}
	if pre != -1 {
		l.tb.buckets[pre].lru.next = next
	}
	if l.head == idx {
		l.head = next
	}
	if l.tail == idx {
		l.tail = pre
	}
}

func (l *linkedList) String() string {
	idx := l.head
	retStr := "["
	for idx != -1 {
		idxNext := l.tb.buckets[idx].lru.next
		if idxNext != -1 {
			retStr += fmt.Sprintf(" %v ->", idx)
		} else {
			retStr += fmt.Sprintf(" %v ]", idx)
		}
		idx = idxNext
	}
	return retStr
}

func (b *bucket) reset() {
	b.item = nil
	b.lru.init()
	b.col.init()
}

func (b *bucket) String() string {
	return fmt.Sprintf("lru: {%v}, col: {%v}, item: {%v}\n", b.lru, b.col, b.item)
}

func NewLRUTable(tableSize uint32, evictCb EvictItemCb) *LRUTable {
	bucketSize := uint32(float64(tableSize) * DefaultBucketTbRatio)
	if evictCb == nil {
		evictCb = dummyEvictCb
	}
	lru := &LRUTable{
		LRUConfig: LRUConfig{
			TableSize:     tableSize,
			BucketTbRatio: DefaultBucketTbRatio,
			EvictRatio:    DefaultEvictRatio,
			Hash32:        defaultHash,
			EvictCb:       evictCb,
		},
		buckets:     make([]bucket, bucketSize),
		lruList:     linkedList{head: -1, tail: -1, tb: nil},
		freeList:    linkedList{head: 0, tail: int32(bucketSize - 1), tb: nil},
		mu:          sync.Mutex{},
		bucketSize:  bucketSize,
		currentSize: 0,
	}
	lru.init()
	return lru
}

func NewLRUTableConf(config LRUConfig) *LRUTable {
	if config.BucketTbRatio <= 1 {
		config.BucketTbRatio = DefaultBucketTbRatio
	}
	if config.EvictRatio <= 0 || config.EvictRatio > 1 {
		config.EvictRatio = DefaultEvictRatio
	}
	bucketSize := uint32(float32(config.TableSize) * config.BucketTbRatio)
	if config.EvictCb == nil {
		config.EvictCb = dummyEvictCb
	}
	lru := &LRUTable{
		LRUConfig:   config,
		buckets:     make([]bucket, bucketSize),
		lruList:     linkedList{head: -1, tail: -1, tb: nil},
		freeList:    linkedList{head: 0, tail: int32(bucketSize - 1), tb: nil},
		mu:          sync.Mutex{},
		bucketSize:  bucketSize,
		currentSize: 0,
	}
	lru.init()
	return lru
}

func (p *LRUTable) init() {
	p.lruList.tb = p
	p.freeList.tb = p
	for i := 0; i < len(p.buckets); i++ {
		p.buckets[i].lru.pre = int32(i - 1)
		if i == len(p.buckets)-1 {
			p.buckets[i].lru.next = -1
		} else {
			p.buckets[i].lru.next = int32(i + 1)
		}
		p.buckets[i].col.init()
		p.buckets[i].item = nil
	}
}

// GetOrCreate return item if exist, otherwise create new and return it
func (p *LRUTable) GetOrCreate(key []byte) (*LRUItem, error) {
	item, err := p.Get(key)
	if item != nil {
		return item, err
	}
	return p.Set(key, nil)
}

func (p *LRUTable) Get(key []byte) (*LRUItem, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := int32(p.Hash32(key) % p.bucketSize)
	b := &p.buckets[idx]
	if b.item == nil {
		return nil, nil // not exist
	}
	for {
		if byteEqual(b.item.Key, key) {
			p.lruList.pushFront(idx) // warmup
			return b.item, nil
		}
		if idx = b.col.next; idx == -1 {
			break
		}
		b = &p.buckets[idx]
	}
	return nil, nil
}

// Set add a item to the LRU and warmup it
func (p *LRUTable) Set(key []byte, value any) (*LRUItem, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evictIfFull()
	idx := int32(p.Hash32(key) % p.bucketSize)
	b := &p.buckets[idx]
	if b.item == nil {
		b.item = NewItem(key, value)
		p.freeList.erase(idx)
		p.lruList.pushFront(idx) // warmup
		p.currentSize++
		return b.item, nil
	}
	b_idx := int32(p.Hash32(b.item.Key) % p.bucketSize)

	if b_idx != idx {
		// occupied by another item not belong to this bucket
		// move current item to new bucket and construct new item in current bucket
		freeIdx := p.freeList.pop()
		p.moveBucket(freeIdx, idx)
		p.buckets[idx].reset() // reset lru list and col list after move
		b.item = NewItem(key, value)
		p.lruList.pushFront(idx)
		p.currentSize++
		return b.item, nil
	}

	for { // hash collision, loop through collision list
		if b.col.next == -1 {
			break
		}
		idx = b.col.next
		b = &p.buckets[idx]
		if byteEqual(b.item.Key, key) { // update value
			b.item.Value = value
			p.lruList.pushFront(idx)
			return b.item, nil
		}
	}
	// not found any, create new, and link to collision list
	freeIdx := p.freeList.pop()
	newBucket := &p.buckets[freeIdx]
	newBucket.item = NewItem(key, value)
	newBucket.col.pre = idx
	newBucket.col.next = -1
	b.col.next = freeIdx
	p.lruList.pushFront(freeIdx)
	p.currentSize++
	return b.item, nil
}

// Remove remove a item from LRU
func (p *LRUTable) Remove(key []byte) (error, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := int32(p.Hash32(key) % p.bucketSize)
	b := &p.buckets[idx]
	if b.item == nil {
		return nil, false
	}
	b_idx := int32(p.Hash32(b.item.Key) % p.bucketSize)
	if b_idx != idx { // occupied by another item
		return nil, false
	}

	for {
		if byteEqual(b.item.Key, key) {
			p.eraseItem(idx)
			return nil, true
		}
		if idx = b.col.next; idx == -1 {
			break
		}
		b = &p.buckets[idx]
	}
	return nil, false
}

func (p *LRUTable) Purge() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for idx := p.lruList.head; idx != -1; idx = p.lruList.head {
		p.eraseItem(idx)
	}
}

func (p *LRUTable) Size() uint32 {
	return p.currentSize
}

// traverse from latest to oldest
func (p *LRUTable) Traverse(fn TraverseItemFn) (cnt uint32) {
	cnt = 0
	var b bucket
	idx := p.lruList.head
	for idx != -1 {
		b = p.buckets[idx]
		if b.item == nil {
			idx = b.lru.next
			continue
		}
		fn(b.item)
		cnt++
		idx = b.lru.next
	}
	return cnt
}

// traverse from oldest to lastest
func (p *LRUTable) RTraverse(fn TraverseItemFn) (cnt uint32) {
	cnt = 0
	var b bucket
	idx := p.lruList.tail
	for idx != -1 {
		b = p.buckets[idx]
		if b.item == nil {
			idx = b.lru.pre
			continue
		}
		fn(b.item)
		cnt++
		idx = b.lru.pre
	}
	return cnt
}

// moveBucket move a bucket from oldIdx to newIdx
func (p *LRUTable) moveBucket(newIdx, oldIdx int32) error {
	if newIdx == -1 || newIdx >= int32(len(p.buckets)) {
		panic(fmt.Sprintf("invalid index: %v", newIdx))
	}
	new_b := &p.buckets[newIdx]
	new_b.item = p.buckets[oldIdx].item
	new_b.lru = p.buckets[oldIdx].lru
	new_b.col = p.buckets[oldIdx].col
	if new_b.lru.pre != -1 {
		p.buckets[new_b.lru.pre].lru.next = newIdx
	}
	if new_b.lru.next != -1 {
		p.buckets[new_b.lru.next].lru.pre = newIdx
	}
	if new_b.col.pre != -1 {
		p.buckets[new_b.col.pre].col.next = newIdx
	}
	if new_b.col.next != -1 {
		p.buckets[new_b.col.next].col.pre = newIdx
	}
	if p.lruList.head == oldIdx {
		p.lruList.head = newIdx
	}
	if p.lruList.tail == oldIdx {
		p.lruList.tail = newIdx
	}
	return nil
}

// eraseNonColItem remove not collision buckets[idx] from lru list and push to free list
func (p *LRUTable) eraseNonColItem(idx int32) {
	bucket := &p.buckets[idx]
	p.EvictCb(bucket.item)
	p.currentSize--
	p.lruList.erase(idx)
	bucket.reset()
	p.freeList.pushFront(idx)
}

// eraseItem remove buckets[idx] from lru list and push it to free list
func (p *LRUTable) eraseItem(idx int32) {
	b := &p.buckets[idx]
	if b.col.empty() {
		p.eraseNonColItem(idx)
		return
	}
	col_next := b.col.next
	col_pre := b.col.pre
	if col_next != -1 && col_pre != -1 {
		p.buckets[col_next].col.pre = col_pre
		p.buckets[col_pre].col.next = col_next
		p.eraseNonColItem(idx)
		return
	}
	if col_next != -1 { // first element in collision list
		p.buckets[col_next].col.pre = -1
		p.EvictCb(b.item)
		p.currentSize--
		p.lruList.erase(idx)
		p.moveBucket(idx, col_next)
		p.buckets[col_next].reset()
		p.freeList.pushFront(col_next)
		return
	}
	// last element in collision list
	p.buckets[col_pre].col.next = -1
	p.eraseNonColItem(idx)
}

func (p *LRUTable) evictIfFull() {
	if p.currentSize+1 <= p.TableSize {
		return
	}
	evictCnt := int(min(float32(p.TableSize)*p.EvictRatio, 1000))
	cnt := 0
	// tail is updated in eraseItem
	for idx := p.lruList.tail; idx != -1 && cnt < evictCnt; idx = p.lruList.tail {
		p.eraseItem(idx)
		cnt++
	}
}

// head --> tail
func (p *LRUTable) debugTraverse() {
	colCnt := 0
	traverseFn := func(idx int32) {
		fmt.Printf("idx: [%v], value: %v", idx, &p.buckets[idx])
	}
	idx := p.lruList.head
	for idx != -1 {
		traverseFn(idx)
		if p.buckets[idx].col.next != -1 {
			colCnt++
		}
		idx = p.buckets[idx].lru.next
	}
	fmt.Printf("___________[DEBUG] collision cnt: %v\n", colCnt)
}
