package lru

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

type testData struct {
	KeyPerRoutines int
	Base           int
	wg             *sync.WaitGroup
}

func lruTestSet(t *testing.T, lru *LRUTable, td testData) {
	randIdx := rand.Perm(td.KeyPerRoutines)
	for _, v := range randIdx {
		key := fmt.Sprintf("key_%d", td.Base*td.KeyPerRoutines+v)
		action := rand.Intn(10)
		if action < 5 {
			value := fmt.Sprintf("value_%d", td.Base*td.KeyPerRoutines+v)
			_, err := lru.Set([]byte(key), value)
			if err != nil {
				t.Errorf("set failed, err: %v", err)
			}
			fmt.Printf("[SET] %v: %v\n", key, value)
		} else {
			value := td.Base*td.KeyPerRoutines + v
			_, err := lru.Set([]byte(key), value)
			if err != nil {
				t.Errorf("set failed, err: %v", err)
			}
			fmt.Printf("[SET] %v: %v\n", key, value)
		}
	}

}

func lruTestGet(t *testing.T, lru *LRUTable, td testData) {
	randIdx := rand.Perm(td.KeyPerRoutines)
	for _, v := range randIdx {
		checkKey := fmt.Sprintf("key_%d", td.Base*td.KeyPerRoutines+v)
		var expectValue interface{}
		fmt.Printf("[GET] %v\n", checkKey)
		item, _ := lru.Get([]byte(checkKey))
		if item == nil {
			fmt.Printf("key: [%v] not exist\n", checkKey)
			continue
		}
		if item.Value == nil {
			t.Error("nil value")
		}
		switch item.Value.(type) {
		case string:
			expectValue = fmt.Sprintf("value_%d", td.Base*td.KeyPerRoutines+v)
			if item.Value.(string) != expectValue.(string) {
				t.Errorf("value not match, expect: [%v], got: [%v]", expectValue.(string), item.Value)
			}
		case int:
			expectValue = td.Base*td.KeyPerRoutines + v
			if item.Value.(int) != expectValue.(int) {
				t.Errorf("value not match, expect: [%v], got: [%v]", expectValue.(int), item.Value)
			}
		}
		// fmt.Printf("get [%v], got [%v]\n", checkKey, item.Value)
	}
}

func lruTestRemove(t *testing.T, lru *LRUTable, td testData) {
	randIdx := rand.Perm(td.KeyPerRoutines)
	for _, v := range randIdx {
		removeKey := fmt.Sprintf("key_%d", td.Base*td.KeyPerRoutines+v)
		fmt.Printf("[REMOVE] %v\n", removeKey)
		_, removed := lru.Remove([]byte(removeKey))
		if !removed {
			t.Errorf("failed to remove key: [%v]", removeKey)
		}
	}
}

func lruTestSequence(t *testing.T, lru *LRUTable, td testData) {
	lruTestSet(t, lru, td)
	lruTestGet(t, lru, td)
	// lruTestRemove(t, lru, td)
	lru.debugTraverse()
}

// func lruTestTraverse(t *testing.T, lru *LRUTable, td testData) {

// }

// func lruTestPurge(t *testing.T, lru *LRUTable, td testData) {

// }

func TestLRUTable(t *testing.T) {
	evictCb := func(item *LRUItem) error {
		if item == nil {
			panic("nil item")
		}
		switch item.Value.(type) {
		case string, int:
		default:
			panic("invalid type")
		}
		return nil
	}
	const tableSize = 100
	lruConf := LRUConfig{
		TableSize:     tableSize,
		BucketTbRatio: 1.5,
		EvictRatio:    0.1,
		Hash32:        JenkinHash,
		EvictCb:       evictCb,
	}
	lruTb := NewLRUTableConf(lruConf)
	tests := make(map[string]func(*testing.T, *LRUTable, testData))
	tests["set/get/remove"] = lruTestSequence
	// tests["traverse"] = lruTestTraverse
	// tests["purge"] = lruTestPurge
	totalTestKey := 200
	maxRoutines := 5
	td := testData{
		KeyPerRoutines: int(totalTestKey / maxRoutines),
		Base:           0,
		wg:             &sync.WaitGroup{},
	}
	for name, testFn := range tests {
		t.Run(name, func(t *testing.T) {
			td.wg.Add(maxRoutines)
			for i := 0; i < maxRoutines; i++ {
				testFn(t, lruTb, td)
				td.Base++
			}
		})
	}
}
