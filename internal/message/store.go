package message

import "sync"

type Store struct {
	mu  sync.RWMutex
	arr []float64
	hm  map[float64]struct{}
}

// NewStore returns a new message store.
func NewStore() *Store {
	return &Store{
		mu:  sync.RWMutex{},
		arr: make([]float64, 0),
		hm:  make(map[float64]struct{}),
	}
}

// Add adds a message to the store.
func (s *Store) Add(msg float64) {
	if s.Has(msg) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.arr = append(s.arr, msg)
	s.hm[msg] = struct{}{}
}

// Has returns true if the store contains the message.
func (s *Store) Has(msg float64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.hm[msg]
	return ok
}

// Messages returns a slice of all messages in the store.
func (s *Store) Messages() []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.arr
}
