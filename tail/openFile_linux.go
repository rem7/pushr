package tail

import "os"

func tailFileOpen(path string) (*os.File, error) {
	f, err := os.Open(path)
	return f, err
}
