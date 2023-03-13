package broadcast

type HandleFunc func(dst string, message float64) error

type request struct {
	dst     string
	message float64
}

type result struct {
	req *request
	err error
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
		resQueue: make(chan *result, 100),
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
			for {
				select {
				case req := <-q.reqQueue:
					q.resQueue <- &result{
						req: req,
						err: q.handler(req.dst, req.message),
					}
				case res := <-q.resQueue:
					if res.err != nil {
						q.reqQueue <- res.req
					}
				}
			}
		}()
	}
}
