package utils

func HashString(s string) uint64 {
	var h uint64
	for i := 0; i < len(s); i++ {
		h = (h << 5) + h + uint64(s[i])
	}
	return h
}
