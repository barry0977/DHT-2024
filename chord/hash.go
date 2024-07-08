package chord

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

func belong(left, right, obj *big.Int, lo, ro bool) bool {
	cmpleft := obj.Cmp(left)
	cmpright := obj.Cmp(right)
	cmpl_r := left.Cmp(right)
	if cmpl_r < 0 { //左端点比右端点小
		if lo && ro { //开区间
			return (cmpleft > 0) && (cmpright < 0)
		} else if lo && (!ro) { //左开右闭
			return (cmpleft > 0) && (cmpright <= 0)
		} else if (!lo) && ro { //左闭右开
			return (cmpleft >= 0) && (cmpright < 0)
		} else { //闭区间
			return (cmpleft >= 0) && (cmpright <= 0)
		}
	} else if cmpl_r > 0 { //左端点比右端点大
		if lo && ro { //开区间
			return (cmpleft > 0) || (cmpright < 0)
		} else if lo && (!ro) { //左开右闭
			return (cmpleft > 0) || (cmpright <= 0)
		} else if (!lo) && ro { //左闭右开
			return (cmpleft >= 0) || (cmpright < 0)
		} else { //闭区间
			return (cmpleft >= 0) || (cmpright <= 0)
		}
	} else { //左右端点一样大
		if lo && ro {
			return cmpleft != 0
		} else {
			return true
		}
	}
}

func count() {
	pow[0] = big.NewInt(1)
	for i := 1; i < 161; i++ {
		pow[i] = new(big.Int).Lsh(pow[i-1], 1) //每次左移一位
	}
}
