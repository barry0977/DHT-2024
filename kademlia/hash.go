package kademlia

import (
	"crypto/sha1"
	"math/big"
)

var pow [161]*big.Int //记录2的幂次

func Hash(Addr string) *big.Int {
	hash := sha1.New()         // 创建一个新的 SHA-1 哈希对象
	hash.Write([]byte(Addr))   // 将字符串数据写入哈希对象
	hashBytes := hash.Sum(nil) // 获取哈希摘要（20 字节）
	hashInt := big.NewInt(0).SetBytes(hashBytes)
	return hashInt
}

func xor(a, b *big.Int) *big.Int {
	return new(big.Int).Xor(a, b)
}

func belong(obj, owner *big.Int) int { //找出所在的KBucket编号
	dis := xor(obj, owner)
	for i := 159; i >= 0; i-- {
		if dis.Cmp(pow[i]) >= 0 {
			return i
		}
	}
	return -1 //obj=owner
}

func count() {
	pow[0] = big.NewInt(1)
	for i := 1; i < 161; i++ {
		pow[i] = new(big.Int).Lsh(pow[i-1], 1) //每次左移一位
	}
}
