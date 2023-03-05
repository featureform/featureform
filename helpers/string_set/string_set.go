package string_set

type StringSet map[string]bool

func (a StringSet) Contains(b StringSet) bool {
	for str := range b {
		if _, ok := a[str]; !ok {
			return false
		}
	}
	return true
}
