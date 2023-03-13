package broadcast

import (
	"time"
)

type HandleFunc func(dst string, message float64) error

type request struct {
	dst     string
	message float64
}

type result struct {
	req        *request
	err        error
	queueAfter time.Time
}

type Queue struct {
	handler  HandleFunc
	reqQueue chan *request
	resQueue chan *result
}

// NewQueue returns a new broadcast queue.
func NewQueue(handler HandleFunc) *Queue {
	return &Queue{
		handler:  handler,
		reqQueue: make(chan *request, 100),
		resQueue: make(chan *result, 1000),
	}
}

// Enqueue adds a message to the broadcast queue.
func (q *Queue) Enqueue(dst string, message float64) {
	q.reqQueue <- &request{
		dst:     dst,
		message: message,
	}
}

// Run starts the processing loop for the broadcast queue.
func (q *Queue) Run(workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case req := <-q.reqQueue:
					q.resQueue <- &result{
						req: &request{
							dst:     req.dst,
							message: req.message,
						},
						err:        q.handler(req.dst, req.message),
						queueAfter: time.Now().Add(3 * time.Second),
					}
				case res := <-q.resQueue:
					if res.err != nil {
						if res.queueAfter.After(time.Now()) {
							q.resQueue <- res
							continue
						}

						q.reqQueue <- res.req
					}
				}

				<-ticker.C
			}
		}()
	}
}
