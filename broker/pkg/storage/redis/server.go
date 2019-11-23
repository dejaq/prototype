package redis

import (
	"github.com/alicebob/miniredis"
)

// New Server will create in  memory new redis server
func NewServer() (*miniredis.Miniredis, error) {
	s, err := miniredis.Run()
	// close server somewhere

	//s.FlushDB()
	//s.FlushAll()
	//s.FlushDB()
	return s, err
}
