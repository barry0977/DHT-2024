package kademlia

import (
	"sync"
)

type List struct {
	Addr    string
	Buckets [k]string
	Visit   map[string]bool
	Size    int
}

func (list *List) Init(addr string) {
	list.Addr = addr
	list.Size = 0
	list.Visit = make(map[string]bool)
}

func (list *List) Pushback(addr string) {
	if list.Find(addr) > 0 { //已经存在
		return
	}
	if list.Size >= k {
		return
	}
	list.Buckets[list.Size] = addr
	list.Visit[addr] = false
	list.Size++
}

func (list *List) Popback() {
	delete(list.Visit, list.Buckets[list.Size-1])
	list.Size--
}

func (list *List) Find(addr string) int {
	for i := 0; i < list.Size; i++ {
		if list.Buckets[i] == addr {
			return i
		}
	}
	return -1
}

func (list *List) Insert(addr string, order int) {
	if order >= k {
		return
	}
	if list.Find(addr) > 0 {
		return
	}
	for i := list.Size; i > order; i-- {
		list.Buckets[i] = list.Buckets[i-1]
	}
	list.Buckets[order] = addr
	list.Visit[addr] = false
	list.Size++
}

func (list *List) Delete(order int) {
	if order >= list.Size {
		return
	}
	delete(list.Visit, list.Buckets[order])
	for i := order; i < list.Size-1; i++ {
		list.Buckets[i] = list.Buckets[i+1]
	}
	list.Size--
}

func (list *List) Move(order int) { //把一个结点移到链表末尾
	addr := list.Buckets[order]
	for i := order; i < list.Size-1; i++ {
		list.Buckets[i] = list.Buckets[i+1]
	}
	list.Buckets[list.Size-1] = addr
}

type Bucket struct {
	Blist List
	Block sync.RWMutex
}

func (bucket *Bucket) Init(addr string) error {
	bucket.Block.Lock()
	bucket.Blist.Init(addr)
	bucket.Block.Unlock()
	return nil
}

func (bucket *Bucket) Update(n *Node, addr string) { //检查是否能将新的结点插入k-bucket中
	if addr == bucket.Blist.Addr { //不能把节点加入自己的k-bucket中
		return
	}
	bucket.Block.RLock()
	place := bucket.Blist.Find(addr)
	bucket.Block.RUnlock()
	er := n.RemoteCall(addr, "Node.Ping", "", nil)
	if er != nil { //已经掉线
		if place != -1 {
			bucket.Block.Lock()
			bucket.Blist.Delete(place)
			bucket.Block.Unlock()
		}
		return
	}
	if place == -1 { //不在list中
		if bucket.Blist.Size < k {
			bucket.Block.Lock()
			bucket.Blist.Pushback(addr)
			bucket.Block.Unlock()
		} else { //已经满了
			bucket.Block.RLock()
			head := bucket.Blist.Buckets[0]
			bucket.Block.RUnlock()
			err := n.RemoteCall(head, "Node.Ping", "", nil)
			if err != nil { //头部结点掉线
				bucket.Block.Lock()
				bucket.Blist.Delete(0)
				bucket.Blist.Pushback(addr)
				bucket.Block.Unlock()
			} else {
				bucket.Block.Lock()
				bucket.Blist.Move(0)
				bucket.Block.Unlock()
			}
		}
	} else { //已经在list中，放到尾部
		bucket.Block.Lock()
		bucket.Blist.Move(place)
		bucket.Block.Unlock()
	}
}

type Answerlist struct {
	Anslist List
	Anslock sync.RWMutex
}

func (ans *Answerlist) Insert(n *Node, addr string) bool { //返回是否更新了答案列表
	ans.Anslock.Lock()
	defer ans.Anslock.Unlock()
	err := n.RemoteCall(addr, "Node.Ping", "", nil)
	if err != nil {
		// logrus.Info("Insert失败： ", addr)
		return false
	}
	place := ans.Anslist.Find(addr)
	if place != -1 {
		// logrus.Info("Insert失败：", addr, " 已经存在")
		return false
	}
	dis := xor(Hash(addr), Hash(ans.Anslist.Addr)) //要加入的地址和目标id的距离
	if ans.Anslist.Size < k {
		for i := 0; i < ans.Anslist.Size; i++ {
			tmp := xor(Hash(ans.Anslist.Buckets[i]), Hash(ans.Anslist.Addr)) //答案列表和自己的距离
			if dis.Cmp(tmp) < 0 {                                            //按照距离从小到大排序
				ans.Anslist.Insert(addr, i)
				return true
			}
		}
		ans.Anslist.Pushback(addr)
		return true
	} else { //答案列表已满
		for i := 0; i < ans.Anslist.Size; i++ {
			tmp := xor(Hash(ans.Anslist.Buckets[i]), Hash(ans.Anslist.Addr)) //答案列表和自己的距离
			if dis.Cmp(tmp) < 0 {
				ans.Anslist.Popback()       //把最远的删去
				ans.Anslist.Insert(addr, i) //加入该节点
				return true
			}
		}
	}
	return false
}

func (ans *Answerlist) Delete(addr string) bool {
	ans.Anslock.Lock()
	defer ans.Anslock.Unlock()
	for i := 0; i < ans.Anslist.Size; i++ {
		if ans.Anslist.Buckets[i] == addr {
			ans.Anslist.Delete(i)
			return true
		}
	}
	return false
}

func (ans *Answerlist) Visited(addr string) {
	ans.Anslock.Lock()
	ans.Anslist.Visit[addr] = true
	ans.Anslock.Unlock()
}

func (ans *Answerlist) GetAlpha() ([]string, int) { //找到a个未被访问过的节点
	var reslist []string
	length := 0
	ans.Anslock.RLock()
	for i := ans.Anslist.Size - 1; i >= 0; i-- {
		if !ans.Anslist.Visit[ans.Anslist.Buckets[i]] { //如果未被访问过
			reslist = append(reslist, ans.Anslist.Buckets[i])
			length++
		}
		if length == a {
			break
		}
	}
	ans.Anslock.RUnlock()
	return reslist, length
}

func (ans *Answerlist) GetRest() ([]string, int) { //找到其余所有未被访问过的节点
	var reslist []string
	length := 0
	ans.Anslock.RLock()
	for i := ans.Anslist.Size - 1; i >= 0; i-- {
		if !ans.Anslist.Visit[ans.Anslist.Buckets[i]] { //如果未被访问过
			reslist = append(reslist, ans.Anslist.Buckets[i])
			length++
		}
	}
	ans.Anslock.RUnlock()
	return reslist, length
}
