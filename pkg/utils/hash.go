package utils

import "strconv"

func HashString(s string) string {
	var h uint64
	for i := 0; i < len(s); i++ {
		h = (h << 5) + h + uint64(s[i])
	}
	return strconv.Itoa(int(h))
}
