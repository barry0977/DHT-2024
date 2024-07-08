package chord

import (
	"errors"
	"fmt"
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
	f, _ := os.Create("chord-test.log")
	logrus.SetOutput(f)
	count()
}

type Nodeinf struct {
	Addr string
	Id   *big.Int
}

type Node struct {
	server   *rpc.Server
	listener net.Listener

	Addr             string
	Id               *big.Int
	nodeinf          Nodeinf
	online           bool
	finger_node      [161]string
	finger_nodelock  sync.RWMutex
	finger_start     [161]*big.Int
	finger_startlock sync.RWMutex
	predecessor      string
	predecessorlock  sync.RWMutex
	successor        [10]string
	successorlock    sync.RWMutex
	data             map[string]Pair
	datalock         sync.RWMutex
	backup           map[string]Pair
	backuplock       sync.RWMutex
	isquit           chan bool
	cur_i            int
	stabilizelock    sync.RWMutex
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
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
		logrus.Error("dialing: ", err)
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

//更新后继列表
func (node *Node) CheckSuccessor() error {
	var suclist, newsuc [10]string
	node.successorlock.RLock()
	for i := 0; i < 10; i++ {
		suclist[i] = node.successor[i]
	}
	node.successorlock.RUnlock()
	for j := 0; j < 10; j++ {
		err := node.RemoteCall(suclist[j], "Node.Ping", "", nil)
		if err == nil { //在线
			err1 := node.RemoteCall(suclist[j], "Node.GetSuccessorlist", "", &newsuc)
			if err1 != nil {
				continue
			}
			node.successorlock.Lock()
			node.successor[0] = suclist[j]
			for k := 1; k < 10; k++ {
				node.successor[k] = newsuc[k-1]
			}
			node.successorlock.Unlock()
			return nil
		}
	}
	var suc string
	node.FindSuccessor(node.Id, &suc)
	node.successorlock.Lock()
	err := node.RemoteCall(suc, "Node.Ping", "", nil)
	if err != nil {
		node.successor[0] = node.Addr
	} else {
		node.successor[0] = suc
	}
	node.successorlock.Unlock()
	return nil
}

//获得后继
func (node *Node) GetSuccessor(_ string, reply *string) error {
	node.CheckSuccessor()
	node.successorlock.RLock()
	defer node.successorlock.RUnlock()
	for i := 0; i < 10; i++ {
		err := node.RemoteCall(node.successor[i], "Node.Ping", "", nil)
		if err == nil {
			*reply = node.successor[i]
			return nil
		}
	}
	return errors.New("cannot find successor")
}

//获得后继列表
func (node *Node) GetSuccessorlist(_ string, reply *[10]string) error {
	node.successorlock.RLock()
	for i := 0; i < 10; i++ {
		reply[i] = node.successor[i]
	}
	node.successorlock.RUnlock()
	return nil
}

//获得前驱
func (node *Node) GetPredecessor(_ string, reply *string) error {
	node.predecessorlock.RLock()
	*reply = node.predecessor
	node.predecessorlock.RUnlock()
	return nil
}

//通过Node n找到id的后继
func (n *Node) FindSuccessor(id *big.Int, reply *string) error {
	var res, suc string
	n.FindPredecessor(id, &res)
	n.RemoteCall(res, "Node.GetSuccessor", "", &suc)
	*reply = suc
	return nil
}

//通过Node n找到id的前驱
func (n *Node) FindPredecessor(id *big.Int, reply *string) error {
	var tmp, tmp_successor string
	tmp = n.Addr
	n.successorlock.RLock()
	tmp_successor = n.successor[0]
	n.successorlock.RUnlock()
	for !belong(Hash(tmp), Hash(tmp_successor), id, true, false) {
		n.RemoteCall(tmp, "Node.ClosestPrecedingFinger", id, &tmp)
		n.RemoteCall(tmp, "Node.GetSuccessor", "", &tmp_successor)
	}
	*reply = tmp
	return nil
}

//返回Node n在id前的最近的finger
func (n *Node) ClosestPrecedingFinger(id *big.Int, reply *string) error {
	n.finger_nodelock.Lock()
	defer n.finger_nodelock.Unlock()
	for i := 160; i > 1; i-- { //finger_node[1]就是successor[0],因为FixFinger的时候没有维护finger_node[1],所以这里用successor[0]来判断
		err := n.RemoteCall(n.finger_node[i], "Node.Ping", "", nil)
		if err != nil { //finger_node[i]不在线
			logrus.Error(n.Addr, " 的finger ", n.finger_node[i], " 已经下线")
			if i == 160 {
				n.finger_node[i] = n.Addr
			} else {
				n.finger_node[i] = n.finger_node[i+1]
			}
		}
		if belong(n.Id, id, Hash(n.finger_node[i]), true, true) {
			*reply = n.finger_node[i]
			return nil
		}
	}
	err := n.RemoteCall(n.successor[0], "Node.Ping", "", nil)
	if err != nil {
		n.successor[0] = n.Addr
	} else {
		if belong(n.Id, id, Hash(n.successor[0]), true, true) {
			*reply = n.successor[0]
			return nil
		}
	}
	*reply = n.Addr
	return nil
}

//修改n的前驱为n_
func (n *Node) ChangePredecessor(n_ string, reply *struct{}) error {
	n.predecessorlock.Lock()
	n.predecessor = n_
	n.predecessorlock.Unlock()
	return nil
}

//修改n的后继
func (n *Node) ChangeSuccessor(n_ [10]string, reply *struct{}) error {
	n.successorlock.Lock()
	for i := 0; i < 10; i++ {
		n.successor[i] = n_[i]
	}
	n.successorlock.Unlock()
	return nil
}

//周期性验证n的后继，并通知n的后继自己的存在
func (n *Node) Stabilize() {
	var suc, x string
	n.GetSuccessor("", &suc)                                                 //获得当前结点的后继
	n.RemoteCall(suc, "Node.GetPredecessor", "", &x)                         //获得后继的前驱
	if x != "" && x != suc && belong(n.Id, Hash(suc), Hash(x), true, true) { //一开始没有设置前驱，可能为空
		suc = x //发现更好的后继，修改
		var successorlist [10]string
		n.RemoteCall(suc, "Node.GetSuccessorlist", "", &successorlist)
		n.successorlock.Lock()
		for i := 9; i > 0; i-- {
			//n.successor[i] = successorlist[i-1]
			n.successor[i] = n.successor[i-1]
		}
		n.successor[0] = suc
		n.successorlock.Unlock()
		n.finger_nodelock.Lock()
		n.finger_node[1] = suc
		n.finger_nodelock.Unlock()
	}
	err := n.RemoteCall(suc, "Node.Notify", n.Addr, nil)
	if err != nil {
		logrus.Error(suc, "'s Notify Failed, to ", n.Addr)
	}
}

func (node *Node) CheckPredecessor() {
	//如果前驱是空，说明还没被notify，没问题
	if node.predecessor != "" {
		err := node.RemoteCall(node.predecessor, "Node.Ping", "", nil)
		if err != nil { //前驱掉线,则置为空，等待notify
			logrus.Info(node.Addr, " 的前驱 ", node.predecessor, " 掉线，恢复备份")
			node.predecessorlock.Lock()
			node.predecessor = ""
			node.predecessorlock.Unlock()

			node.datalock.Lock()
			node.backuplock.Lock()
			for key, value := range node.backup {
				node.data[key] = value //把备份转移到主数据
			}
			node.datalock.Unlock()
			node.backuplock.Unlock()

			var successor string
			node.GetSuccessor("", &successor)
			node.backuplock.RLock()
			node.RemoteCall(successor, "Node.TransferBackup", node.backup, nil) //把node的备份转移到后继中去
			node.backuplock.RUnlock()
			node.backuplock.Lock()
			node.backup = make(map[string]Pair) //清空node的备份
			node.backuplock.Unlock()
		}
	}
}

//n_为n可能的前驱
func (n *Node) Notify(n_ string, reply *struct{}) error {
	if n.predecessor == "" || belong(Hash(n.predecessor), n.Id, Hash(n_), true, true) {
		n.predecessorlock.Lock()
		n.predecessor = n_
		n.predecessorlock.Unlock()
		if n_ != n.Addr {
			//把属于n_的数据转移给n_
			err := n.RemoteCall(n_, "Node.Filter", n.Addr, nil)
			if err != nil {
				logrus.Info("转移数据失败: 从 ", n.Addr, " 到 ", n_, " ", err)
			}
			//把n的备份给n_
			n.RemoteCall(n_, "Node.BackupForward", n.Addr, nil)
			//把n_的数据备份到n中
			var predata map[string]Pair = make(map[string]Pair)
			n.RemoteCall(n_, "Node.GetMap", "", &predata)
			n.backuplock.Lock()
			n.backup = make(map[string]Pair)
			for key, value := range predata {
				n.backup[key] = value
			}
			n.backuplock.Unlock()
		}
	}
	return nil
}

//周期性更新finger_table
func (n *Node) FixFingers() {
	var successor string
	n.FindSuccessor(n.finger_start[n.cur_i], &successor)
	n.finger_nodelock.Lock()
	n.finger_node[n.cur_i] = successor
	n.finger_nodelock.Unlock()
	n.cur_i = (n.cur_i-1)%159 + 2
}

func (n *Node) Maintain() {
	go func() {
		for n.online {
			n.stabilizelock.Lock()
			n.Stabilize()
			n.stabilizelock.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for n.online {
			n.FixFingers()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for n.online {
			n.CheckPredecessor()
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

//初始化一个结点
func (node *Node) Init(addr string) {
	logrus.Info("Enter Init: ", addr, node.Addr)
	node.Addr = addr
	node.Id = Hash(addr)
	node.nodeinf.Addr, node.nodeinf.Id = node.Addr, node.Id
	node.isquit = make(chan bool)
	node.finger_startlock.Lock()
	node.finger_nodelock.Lock()
	for i := 1; i <= 160; i++ {
		node.finger_start[i] = new(big.Int)
		node.finger_start[i].Add(node.Id, pow[i-1]).Mod(node.finger_start[i], pow[160])
		node.finger_node[i] = addr
	}
	node.finger_nodelock.Unlock()
	node.finger_startlock.Unlock()
	node.datalock.Lock()
	node.data = make(map[string]Pair)
	node.datalock.Unlock()
	node.backuplock.Lock()
	node.backup = make(map[string]Pair)
	node.backuplock.Unlock()
	node.cur_i = 2
}

func (node *Node) Run() {
	logrus.Info("Enter Run: ", node.Addr)
	node.online = true
	go node.RunRPCServer()
}

func (node *Node) Create() {
	logrus.Infof("Create %s %d", node.Addr, *node.Id)
	node.finger_nodelock.Lock()
	node.finger_node[1] = node.Addr
	node.finger_nodelock.Unlock()
	node.successorlock.Lock()
	node.successor[0] = node.Addr
	node.successorlock.Unlock()
	node.Maintain()
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s %s", addr, node.Addr)
	err := node.RemoteCall(addr, "Node.Ping", "", nil)
	if err != nil {
		logrus.Error("Join error: offline ", err)
		return false
	}
	node.predecessorlock.Lock()
	node.predecessor = ""
	node.predecessorlock.Unlock()
	var successor string
	err = node.RemoteCall(addr, "Node.FindSuccessor", node.Id, &successor) //找到后继结点
	if err != nil {
		logrus.Error("Join error: no successor ", err)
		return false
	}
	var successorlist [10]string
	err = node.RemoteCall(successor, "Node.GetSuccessorlist", "", &successorlist)
	if err != nil {
		logrus.Error("Join error: no successorlist ", err)
		return false
	}
	node.successorlock.Lock()
	node.successor[0] = successor
	for i := 1; i < 10; i++ {
		node.successor[i] = successorlist[i-1]
	}
	node.successorlock.Unlock()
	node.Maintain()
	return true
}

func (node *Node) Quit() {
	if !node.online {
		logrus.Error(node.Addr, " already quit")
		return
	}
	node.isquit <- true
	node.listener.Close()
	node.online = false
	node.stabilizelock.Lock() //阻塞stabilize
	logrus.Info(node.Addr, "执行quit")
	node.predecessorlock.RLock()
	predecessor := node.predecessor
	node.predecessorlock.RUnlock()
	node.successorlock.RLock()
	successor := node.successor
	node.successorlock.RUnlock()
	node.RemoteCall(predecessor, "Node.ChangeSuccessor", successor, nil)
	node.RemoteCall(successor[0], "Node.ChangePredecessor", predecessor, nil)
	node.RemoteCall(successor[0], "Node.Transfer", node.data, nil)         //把数据转移到后继
	node.RemoteCall(successor[0], "Node.TransferBackup", node.backup, nil) //把备份转移到后继
	var suc_suc string                                                     //得到后继的后继
	node.RemoteCall(successor[0], "Node.GetSuccessor", "", &suc_suc)
	node.RemoteCall(suc_suc, "Node.BackupForward", successor[0], nil) //把后继的备份转移到后继的后继
	node.stabilizelock.Unlock()
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

func (node *Node) AddData(obj Pair, reply *struct{}) error {
	node.datalock.Lock()
	node.data[obj.Key] = obj
	node.datalock.Unlock()
	return nil
}

func (node *Node) AddBackup(obj Pair, reply *struct{}) error {
	node.backuplock.Lock()
	node.backup[obj.Key] = obj
	node.backuplock.Unlock()
	return nil
}

func (node *Node) GetData(key string, reply *string) error {
	node.datalock.RLock()
	value, ok := node.data[key]
	node.datalock.RUnlock()
	if ok {
		*reply = value.Value
		return nil
	} else {
		return errors.New("key not exist")
	}
}

func (node *Node) RemoveData(key string, reply *struct{}) error {
	node.datalock.RLock()
	_, ok := node.data[key]
	node.datalock.RUnlock()
	if ok {
		node.datalock.Lock()
		delete(node.data, key)
		node.datalock.Unlock()
		return nil
	} else {
		return errors.New("key not exist")
	}
}

func (node *Node) RemoveBackup(key string, reply *struct{}) error {
	node.backuplock.RLock()
	_, ok := node.backup[key]
	node.backuplock.RUnlock()
	if ok {
		node.backuplock.Lock()
		delete(node.backup, key)
		node.backuplock.Unlock()
		return nil
	} else {
		return errors.New("backup key not exist")
	}
}

//把_map合并到node的data中
func (node *Node) Transfer(_map map[string]Pair, reply *struct{}) error {
	node.datalock.Lock()
	for key, value := range _map {
		node.data[key] = value
	}
	node.datalock.Unlock()
	return nil
}

func (node *Node) GetMap(_ string, reply *map[string]Pair) error {
	node.datalock.RLock()
	for key, value := range node.data {
		(*reply)[key] = value
	}
	node.datalock.RUnlock()
	return nil
}

func (node *Node) GetBackup(_ string, reply *map[string]Pair) error {
	node.backuplock.RLock()
	for key, value := range node.backup {
		(*reply)[key] = value
	}
	node.backuplock.RUnlock()
	return nil
}

//从n_中获得应该在node中的数据
func (node *Node) Filter(n_ string, reply *struct{}) error {
	var _map map[string]Pair
	node.RemoteCall(n_, "Node.GetMap", "", &_map)
	node.datalock.Lock()
	for key, value := range _map {
		keyid := Hash(key)
		if belong(Hash(n_), node.Id, keyid, true, false) { //如果key应该属于node
			node.data[key] = value
		}
	}
	node.datalock.Unlock()
	return nil
}

//把数据转移到node的backup来
func (node *Node) TransferBackup(_map map[string]Pair, reply *struct{}) error {
	node.backuplock.Lock()
	for key, value := range _map {
		node.backup[key] = value
	}
	node.backuplock.Unlock()
	return nil
}

//把n_的backup转移到node中
func (node *Node) BackupForward(n_ string, reply *struct{}) error {
	var _map map[string]Pair
	node.RemoteCall(n_, "Node.GetBackup", "", &_map)
	node.backuplock.Lock()
	for key, value := range _map {
		node.backup[key] = value
	}
	node.backuplock.Unlock()
	return nil
}

// Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
func (node *Node) Put(key string, value string) bool {
	keyid := Hash(key)
	var successor, suc_suc string
	err := node.FindSuccessor(keyid, &successor)
	if err != nil {
		logrus.Error("Put Failed: FindSuccessor ", key, " ", value, " ", err)
		return false
	}
	err = node.RemoteCall(successor, "Node.GetSuccessor", "", &suc_suc)
	if err != nil {
		logrus.Error("Put Failed: FindSuccessor'successor ", key, " ", value, " ", err)
		return false
	}
	obj := Pair{key, value}
	if successor == node.Addr {
		err = node.AddData(obj, nil)
	} else {
		err = node.RemoteCall(successor, "Node.AddData", obj, nil)
	}
	if err != nil {
		logrus.Error("Put Failed: AddData ", key, " ", value, " ", err)
		return false
	}
	if suc_suc == node.Addr {
		err = node.AddBackup(obj, nil)
	} else {
		err = node.RemoteCall(suc_suc, "Node.AddBackup", obj, nil)
	}
	if err != nil {
		logrus.Error("Put Failed: AddBackup ", key, " ", value, " ", err)
		return false
	}
	return true
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *Node) Get(key string) (bool, string) {
	keyid := Hash(key)
	var successor string
	err := node.FindSuccessor(keyid, &successor)
	if err != nil {
		logrus.Error("Get Failed: FindSuccessor ", key, err)
		return false, ""
	}
	var res string
	err = node.RemoteCall(successor, "Node.GetData", key, &res)
	if err != nil {
		logrus.Error("Get Failed: GetData ", key, err)
		return false, ""
	}
	return true, res
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *Node) Delete(key string) bool {
	var successor, suc_suc string
	err := node.FindSuccessor(Hash(key), &successor)
	if err != nil {
		logrus.Error("Delete Failed: FindSuccessor ", key, err)
		return false
	}
	err = node.RemoteCall(successor, "Node.GetSuccessor", "", &suc_suc)
	if err != nil {
		logrus.Error("Delete Failed: FindSuccessor'successor", key, err)
		return false
	}

	if successor == node.Addr {
		err = node.RemoveData(key, nil)
	} else {
		err = node.RemoteCall(successor, "Node.RemoveData", key, nil)
	}
	if err != nil {
		logrus.Error("Delete Failed: RemoveData ", key, err)
		return false
	}

	if suc_suc == node.Addr {
		err = node.RemoveBackup(key, nil)
	} else {
		err = node.RemoteCall(suc_suc, "Node.RemoveBackup", key, nil)
	}
	if err != nil {
		logrus.Error("Delete Failed: RemoveBackup ", key, err)
		return false
	}
	return true
}

func (node *Node) Check(i int) {
	// fmt.Printf("[%d]self: %s  pre: %s  suc: [0]%s  [1]%s  [2]%s\n", i, node.Addr, node.predecessor, node.successor[0], node.successor[1], node.successor[2])
	fmt.Printf("[%d]self: %s  pre: %s  suc: [0]%s\n", i, node.Addr, node.predecessor, node.successor[0])
}
