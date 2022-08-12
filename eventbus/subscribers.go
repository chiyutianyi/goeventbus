package eventbus

import "sync"

// Subscribers is the list of subscribers
type Subscribers struct {
	sync.RWMutex
	m map[string]*Subscriber
}

func newSubscribers() *Subscribers {
	return &Subscribers{
		m: make(map[string]*Subscriber),
	}
}

func (s *Subscribers) Contains(uid string) bool {
	s.RLock()
	_, ok := s.m[uid]
	s.RUnlock()
	return ok
}

func (s *Subscribers) Add(sub *Subscriber) {
	s.Lock()
	defer s.Unlock()
	s.m[sub.UID()] = sub
}

func (s *Subscribers) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}

func (s *Subscribers) Each(f func(sub *Subscriber)) {
	s.RLock()
	defer s.RUnlock()
	for _, sub := range s.m {
		f(sub)
	}
}
