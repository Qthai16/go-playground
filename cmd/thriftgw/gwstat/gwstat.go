package gwstat

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// todo: pool stats
// - host remote addr
// - map<destAddr:destPort, methodStat>
// 	- methodStat: map<methodName, count>
//  - error map:
//    - pool-create: count
//    - pool-get: count
//    - msg-parse: count
// todo: print stats as json, serialize to statcore api

const (
	PoolCreateErrKey = "pool_create"
	PoolGetErrKey    = "pool_get"
	MsgParseErrKey   = "msg_parse"
)

type (
	JSONAtomicI64 struct {
		atomic.Int64
	}
	MethodStat struct {
		Method string        `json:"name"`
		Count  JSONAtomicI64 `json:"count"`
	}
	MethodStatList struct {
		Data []*MethodStat `json:"methods"`
		mu   sync.Mutex
	}
	GWStats struct {
		Remotes  []string                   `json:"remotes"`
		Stats    map[string]*MethodStatList `json:"stats"` // destAddr-destPort : {method, count}
		ErrorMap map[string]*JSONAtomicI64  `json:"errors"`
		rwMu     sync.RWMutex
	}
)

func (f *JSONAtomicI64) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", f.Load())), nil
}

func NewMethodStat(api string) *MethodStat {
	return &MethodStat{Method: api, Count: JSONAtomicI64{}}
}

func (p *MethodStat) String() string {
	return fmt.Sprintf("\"%v\": %v", p.Method, p.Count.Load())
}

func NewMethodStatList() *MethodStatList {
	return &MethodStatList{Data: make([]*MethodStat, 0), mu: sync.Mutex{}}
}

func (msl *MethodStatList) String() string {
	var s string
	// msl.mu.Lock()
	// defer msl.mu.Unlock()
	s += fmt.Sprintf("_len_: %v\n", len(msl.Data))
	for _, m := range msl.Data {
		s += "    " + m.String() + "\n"
	}
	return s
}

func (msl *MethodStatList) AddNewMethod(method string) {
	// notes: lock in caller
	apiStat := NewMethodStat(method)
	apiStat.Count.Add(1)
	msl.Data = append(msl.Data, apiStat)
	sort.Slice(msl.Data, func(i, j int) bool {
		return msl.Data[i].Method < msl.Data[j].Method
	})
}

func NewGWStats() *GWStats {
	ErrorMap := map[string]*JSONAtomicI64{
		PoolCreateErrKey: {},
		PoolGetErrKey:    {},
		MsgParseErrKey:   {},
	}
	return &GWStats{
		Remotes:  make([]string, 0),
		Stats:    make(map[string]*MethodStatList),
		ErrorMap: ErrorMap,
		rwMu:     sync.RWMutex{},
	}
}

func (gws *GWStats) String() string {
	s1 := fmt.Sprintf("Remote Conns: %v\n", gws.Remotes)
	s2 := "Stats: "
	if len(gws.Stats) == 0 {
		s2 += "[]\n"
	} else {
		s2 += "\n"
		for k, v := range gws.Stats {
			s2 += fmt.Sprintf("  %v\n", k)
			s2 += fmt.Sprintf("    %v\n", v)
		}
	}
	s3 := "Errors: {"
	for k, v := range gws.ErrorMap {
		s3 += fmt.Sprintf("\"%v\": %v, ", k, v.Load())
	}
	s3 += "}\n"
	return s1 + s2 + s3
}

func (gws *GWStats) AddRemote(remote string) {
	gws.rwMu.Lock()
	defer gws.rwMu.Unlock()
	gws.Remotes = append(gws.Remotes, remote)
	sort.Slice(gws.Remotes, func(i, j int) bool {
		return gws.Remotes[i] < gws.Remotes[j]
	})
}

func (gws *GWStats) DelRemote(remote string) {
	gws.rwMu.Lock()
	defer gws.rwMu.Unlock()
	ind, found := slices.BinarySearchFunc(gws.Remotes, remote, func(r string, target string) int {
		return strings.Compare(r, target)
	})
	if ind < 0 || ind >= len(gws.Remotes) || !found {
		return
	}
	gws.Remotes = slices.Delete(gws.Remotes, ind, 1)
}

// func (gws *GWStats) AddTarget(destAddr string) {
// 	gws.mu.Lock()
// 	defer gws.mu.Unlock()
// 	if _, ok := gws.Stats[destAddr]; !ok {
// 		gws.Stats[destAddr] = NewMethodStatList()
// 	}
// }

func (gws *GWStats) AddMethodStat(destAddr, method string) {
	gws.rwMu.RLock()
	if _, ok := gws.Stats[destAddr]; ok {
		gws.AddMethod2ExistTarget(destAddr, method)
		gws.rwMu.RUnlock()
		return
	}
	gws.rwMu.RUnlock()
	gws.rwMu.Lock()
	defer gws.rwMu.Unlock()
	if _, ok := gws.Stats[destAddr]; ok {
		gws.AddMethod2ExistTarget(destAddr, method)
		return
	}
	gws.Stats[destAddr] = NewMethodStatList()
	gws.Stats[destAddr].AddNewMethod(method)
}

func (gws *GWStats) AddMethod2ExistTarget(destAddr, method string) {
	st := gws.Stats[destAddr]
	st.mu.Lock()
	defer st.mu.Unlock()
	ind, found := slices.BinarySearchFunc(st.Data, method, func(r *MethodStat, target string) int {
		return strings.Compare(r.Method, target)
	})
	if ind < 0 || ind >= len(st.Data) || !found {
		// not found, add to method list and increase count
		st.AddNewMethod(method)
	} else {
		// found, increase count
		st.Data[ind].Count.Add(1)
	}
}

func (gws *GWStats) IncErrStat(key string) {
	gws.rwMu.RLock()
	defer gws.rwMu.RUnlock()
	if _, ok := gws.ErrorMap[key]; ok {
		gws.ErrorMap[key].Add(1)
	}
}

func (gws *GWStats) IncPoolCreateErr() {
	gws.IncErrStat(PoolCreateErrKey)
}

func (gws *GWStats) IncPoolGetErr() {
	gws.IncErrStat(PoolGetErrKey)
}

func (gws *GWStats) IncMsgParseErr() {
	gws.IncErrStat(MsgParseErrKey)
}
