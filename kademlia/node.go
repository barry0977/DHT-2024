package kademlia

import (
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("kademlia-test.log")
	logrus.SetOutput(f)
	count()
}

const k = 20
const a = 3

type Node struct {
	server   *rpc.Server
	listener net.Listener

	Addr     string
	Id       *big.Int
	online   bool
	kbuckets [160]Bucket
	data     map[string]Pair
	datalock sync.RWMutex
	isquit   chan bool
}

type SRid struct {
	Sender    string //RPC请求的发送者
	Recipient string //RPC请求的接收者
	Object    string //目标Id
}

type SRdata struct {
	Sender    string
	Recepient string
	Data      Pair
}

type ValueWithNode struct {
	Data     Pair
	Finddata bool
	Nodes    Answerlist
}

type DataNodes struct {
	Data     Pair
	Finddata bool
	Nodelist []string
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
}

//初始化一个节点
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.Id = Hash(addr)
	node.isquit = make(chan bool, 1)
	for i := 0; i < 160; i++ {
		node.kbuckets[i].Init(addr)
	}
	node.datalock.Lock()
	node.data = make(map[string]Pair)
	node.datalock.Unlock()
}

func (node *Node) RunRPCServer() {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		select {
		case <-node.isquit:
			logrus.Info("节点下线: ", node.Addr)
			node.StopRPCServer()
			return
		default:
			conn, err := node.listener.Accept()
			if err != nil {
				logrus.Error("accept error: ", err)
				return
			}
			go node.server.ServeConn(conn)
		}
	}
}

func (node *Node) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	// if method != "Node.Ping" {
	// 	logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	// }
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		// logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error(method, " RemoteCall error: ", err)
		return err
	}
	return nil
}

//检查是否在线
func (node *Node) Ping(_ string, _ *struct{}) error {
	return nil
}

func (node *Node) FindNode(addr string, res *Answerlist) error {
	res.Anslist.Init(addr)
	i := belong(Hash(addr), node.Id) //找到所在的bucket
	if i != -1 {
		node.kbuckets[i].Block.RLock()
		for j := 0; j < node.kbuckets[i].Blist.Size; j++ {
			res.Insert(node, node.kbuckets[i].Blist.Buckets[j])
		}
		node.kbuckets[i].Block.RUnlock()
	} else { //addr等于node的地址
		res.Insert(node, node.Addr)
	}
	if res.Anslist.Size == k { //已经找到k个
		return nil
	}
	for k := i - 1; k >= 0; k-- {
		node.kbuckets[k].Block.RLock()
		for j := 0; j < node.kbuckets[k].Blist.Size; j++ {
			res.Insert(node, node.kbuckets[k].Blist.Buckets[j])
			if res.Anslist.Size == k {
				node.kbuckets[k].Block.RUnlock()
				return nil
			}
		}
		node.kbuckets[k].Block.RUnlock()
	}
	for k := i + 1; k < 160; k++ {
		node.kbuckets[k].Block.RLock()
		for j := 0; j < node.kbuckets[k].Blist.Size; j++ {
			res.Insert(node, node.kbuckets[k].Blist.Buckets[j])
			if res.Anslist.Size == k {
				node.kbuckets[k].Block.RUnlock()
				return nil
			}
		}
		node.kbuckets[k].Block.RUnlock()
	}
	if i != -1 {
		res.Insert(node, node.Addr)
	}
	return nil
}

func (node *Node) Flush(addr string, reply *struct{}) error { //调用RPC时更新k-bucket的节点顺序
	i := belong(Hash(addr), node.Id) //找到所在的k-bucket编号
	if i != -1 {
		node.kbuckets[i].Update(node, addr)
	}
	return nil
}

func (node *Node) RPCFindNode(sr SRid, res *[]string) error { //用于remotecall调用，可以进行k-bucket的更新
	var tmp Answerlist
	node.FindNode(sr.Object, &tmp)
	*res = make([]string, 0, k)
	for i := 0; i < tmp.Anslist.Size; i++ {
		*res = append(*res, tmp.Anslist.Buckets[i])
	}
	if sr.Sender != node.Addr { //node收到sender的通信，更新sender所在的k-bucket
		node.Flush(sr.Sender, nil)
	}
	return nil
}

//从找到的k个节点中选出a个，让这a个再去FindNode
//参数中res是目前找到的k个最近节点，todolist是选出的a个节点，target是要查找的目标地址
func (node *Node) SearchAlpha(res *Answerlist, todolist []string, length int, target string) []string {
	var tmplist []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(length)
	// logrus.Info("SearchAlpha len: ", length)
	for i := 0; i < length; i++ {
		go func(addr string) {
			defer wg.Done()
			var tmp []string
			sr := SRid{node.Addr, addr, target}
			node.Flush(addr, nil)
			err := node.RemoteCall(addr, "Node.RPCFindNode", sr, &tmp)
			if err != nil { //remotecall失败,从答案列表中删去
				// logrus.Info(node.Addr, " 删除掉线节点 ", addr)
				is := res.Delete(addr)
				if is {
					logrus.Info("删除节点成功：", addr)
				} else {
					logrus.Info("删除节点失败：", addr)
				}
				return
			}
			res.Anslock.Lock()
			res.Anslist.Visit[addr] = true //标记已经访问过
			res.Anslock.Unlock()
			lock.Lock() //一次只能有一个在写入
			for i := 0; i < len(tmp); i++ {
				tmplist = append(tmplist, tmp[i])
			}
			lock.Unlock()
		}(todolist[i])
	}
	wg.Wait()
	return tmplist
}

func (node *Node) Lookup(addr string, res *[]string) error {
	var tmplist Answerlist
	tmplist.Anslist.Init(addr)
	node.FindNode(addr, &tmplist) //从自己的bucket中找到k个最近的
	for {
		// logrus.Info("进入Lookup的循环: ", node.Addr, " for ", addr)
		todolist, len := tmplist.GetAlpha()
		newlist := node.SearchAlpha(&tmplist, todolist, len, addr) //得到返回的新一轮列表
		ismodified := false
		for _, possi := range newlist { //将获取的可能的节点列表更新答案列表
			// logrus.Info("尝试插入新节点: ", possi)
			is := tmplist.Insert(node, possi)
			if is {
				ismodified = true
			}
		}
		if !ismodified { //一轮过后没有找到更近的节点
			rest, l := tmplist.GetRest() //获取剩余全部没有访问过的节点
			newlist = node.SearchAlpha(&tmplist, rest, l, addr)
			for _, possi := range newlist {
				is := tmplist.Insert(node, possi)
				if is {
					ismodified = true
				}
			}
		}
		if !ismodified { //如果还是没有找到更近的，结束
			break
		}
	}
	// logrus.Info("结束Lookup的循环: ", node.Addr, " for ", addr, " 数量为 ", tmplist.Anslist.Size)
	if res != nil {
		*res = make([]string, 0, k)
		for i := 0; i < tmplist.Anslist.Size; i++ {
			// logrus.Info("Lookup到的第 ", i, " 个节点为 ", tmplist.Anslist.Buckets[i])
			*res = append(*res, tmplist.Anslist.Buckets[i])
		}
	}
	return nil
}

func (node *Node) Store(obj Pair, reply *struct{}) error {
	node.datalock.Lock()
	node.data[obj.Key] = obj
	node.datalock.Unlock()
	return nil
}

func (node *Node) RPCStore(obj SRdata, reply *struct{}) error {
	node.Store(obj.Data, nil)
	if obj.Sender != node.Addr {
		node.Flush(obj.Sender, nil)
	}
	return nil
}

//如果接收者含有该key，则返回value;否则等效于FindNode
func (node *Node) FindValue(key string) ValueWithNode {
	var res ValueWithNode
	node.datalock.RLock()
	value, ok := node.data[key]
	node.datalock.RUnlock()
	if ok {
		res.Finddata = true
		res.Data = value
		return res
	}
	var ans Answerlist
	node.FindNode(key, &ans)
	res.Finddata = false
	res.Nodes = ans
	return res
}

func (node *Node) RPCFindValue(sr SRid, reply *DataNodes) error {
	res := node.FindValue(sr.Object)
	(*reply).Data = res.Data
	(*reply).Finddata = res.Finddata
	(*reply).Nodelist = make([]string, 0, k)
	for i := 0; i < res.Nodes.Anslist.Size; i++ {
		(*reply).Nodelist = append((*reply).Nodelist, res.Nodes.Anslist.Buckets[i])
	}
	if sr.Sender != node.Addr { //node收到sender的通信，更新sender所在的k-bucket
		node.Flush(sr.Sender, nil)
	}
	return nil
}

//与寻找节点的SearchAlpha对应
//从找到的k个节点中选出a个，让这a个再去FindValue
//参数中res是目前找到的k个最近节点，todolist是选出的a个节点，target是要查找的目标地址
func (node *Node) SearchData(res *Answerlist, todolist []string, length int, target string) ([]string, Pair) {
	var tmplist []string
	// logrus.Info("SearchAlpha len: ", length)
	for i := 0; i < length; i++ {
		var tmp DataNodes
		sr := SRid{node.Addr, todolist[i], target}
		node.Flush(todolist[i], nil)
		err := node.RemoteCall(todolist[i], "Node.RPCFindValue", sr, &tmp)
		// logrus.Info("从 ", todolist[i], " 中找数据 ", target)
		if err != nil { //remotecall失败,从答案列表中删去
			res.Delete(todolist[i])
			continue
		}
		if tmp.Finddata {
			return []string{}, tmp.Data
		}
		res.Anslock.Lock()
		res.Anslist.Visit[todolist[i]] = true //标记已经访问过
		res.Anslock.Unlock()
		for i := 0; i < len(tmp.Nodelist); i++ {
			tmplist = append(tmplist, tmp.Nodelist[i])
		}
	}
	return tmplist, Pair{"", ""}
}

//对应Lookup
func (node *Node) LookupValue(key string, ans *Answerlist) (bool, string) {
	for {
		todolist, len := ans.GetAlpha()
		newlist, data := node.SearchData(ans, todolist, len, key) //得到返回的新一轮列表
		if data.Value != "" {
			return true, data.Value
		}
		ismodified := false
		for _, possi := range newlist { //将获取的可能的节点列表更新答案列表
			is := ans.Insert(node, possi)
			if is {
				ismodified = true
			}
		}
		if !ismodified { //一轮过后没有找到更近的节点
			rest, l := ans.GetRest() //获取剩余全部没有访问过的节点
			newlist, data = node.SearchData(ans, rest, l, key)
			if data.Value != "" {
				return true, data.Value
			}
			for _, possi := range newlist {
				is := ans.Insert(node, possi)
				if is {
					ismodified = true
				}
			}
		}
		if !ismodified { //如果还是没有找到更近的，结束
			break
		}
	}
	return false, ""
}

func (node *Node) RepublishAll() {
	logrus.Info("执行RepublishAll: ", node.Addr)
	var wg sync.WaitGroup
	for _, value := range node.data {
		wg.Add(1)
		go func(data Pair) {
			defer wg.Done()
			node.RepublishData(data)
		}(value)
	}
	wg.Wait()
}

func (node *Node) RepublishData(data Pair) {
	var nodelist []string
	node.Lookup(data.Key, &nodelist)
	var wg sync.WaitGroup
	wg.Add(len(nodelist))
	for i := 0; i < len(nodelist); i++ {
		go func(addr string) {
			defer wg.Done()
			if addr == node.Addr {
				node.Store(data, nil)
			} else {
				node.Flush(addr, nil)
				obj := SRdata{node.Addr, addr, data}
				node.RemoteCall(addr, "Node.RPCStore", obj, nil)
			}
		}(nodelist[i])
	}
	wg.Wait()
}

func (node *Node) Run() {
	logrus.Info("Enter Run: ", node.Addr)
	node.online = true
	go node.RunRPCServer()
}

func (node *Node) Create() {
	logrus.Infof("Create %s %d", node.Addr, *node.Id)
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s %s", addr, node.Addr)
	err := node.RemoteCall(addr, "Node.Ping", "", nil)
	if err != nil {
		logrus.Error("Join error: offline ", err)
		return false
	}
	i := belong(Hash(addr), node.Id)
	node.kbuckets[i].Update(node, addr) //把已经在网络中的节点加入新节点的k-bucket中
	node.Lookup(node.Addr, nil)
	return true
}

func (node *Node) Quit() {
	if !node.online {
		logrus.Error(node.Addr, " already quit")
		return
	}
	node.RepublishAll()
	node.isquit <- true
	node.listener.Close()
	node.online = false
	logrus.Info(node.Addr, " 执行quit")
}

func (node *Node) ForceQuit() {
	if !node.online {
		logrus.Error(node.Addr, " already quit")
		return
	}
	node.isquit <- true
	node.listener.Close()
	node.online = false
	logrus.Info(node.Addr, " 执行forcequit")
}

// Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
func (node *Node) Put(key string, value string) bool {
	logrus.Info("Enter Put: ", key, " from ", node.Addr)
	var nodelist []string
	node.Lookup(key, &nodelist) //获取最近的k个节点
	var wg sync.WaitGroup
	wg.Add(len(nodelist))
	flag := false
	for i := 0; i < len(nodelist); i++ {
		go func(addr string) {
			defer wg.Done()
			if addr == node.Addr {
				node.Store(Pair{key, value}, nil)
			} else {
				srdata := SRdata{node.Addr, addr, Pair{key, value}}
				err := node.RemoteCall(addr, "Node.RPCStore", srdata, nil)
				if err != nil {
					logrus.Error("Put Failed, key: ", key, " from ", addr, " ", err)
					return
				}
				node.Flush(addr, nil)
			}
			flag = true
		}(nodelist[i])
	}
	wg.Wait()
	return flag
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *Node) Get(key string) (bool, string) {
	tmp := node.FindValue(key)
	if tmp.Finddata {
		return true, tmp.Data.Value
	} else {
		ok, value := node.LookupValue(key, &tmp.Nodes)
		if ok {
			return true, value
		}
	}
	logrus.Info("Get Failed ", key, " from ", node.Addr)
	return false, ""
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *Node) Delete(key string) bool {
	return true
}
