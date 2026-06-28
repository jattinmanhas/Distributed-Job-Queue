package worker

import (
	"context"
	"log"
	"sync"

	"github.com/jattin/distributed-job-queue/internal/models"
)

// Pool runs a fixed number of worker goroutines that consume jobs from a shared
// channel and hand each one to the Processor.
type Pool struct {
	numWorkers int
	jobsChan   chan models.Job
	processor  *Processor
	wg         sync.WaitGroup
}

// NewPool creates a Pool with a buffered jobs channel sized to numWorkers * 2.
func NewPool(numWorkers int, processor *Processor) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		jobsChan:   make(chan models.Job, numWorkers*2),
		processor:  processor,
	}
}

// Start launches numWorkers goroutines. Each drains the jobs channel until it is
// closed or the context is cancelled. A panic in any single worker is recovered
// so it can never bring down the process.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
}

func (p *Pool) runWorker(ctx context.Context, index int) {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			// Processor.Process already recovers per job; this is a last-resort
			// guard so the goroutine exits cleanly instead of crashing the program.
			log.Printf("worker %d recovered from panic: %v", index, r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d stopping: context cancelled", index)
			return
		case job, ok := <-p.jobsChan:
			if !ok {
				log.Printf("worker %d stopping: jobs channel closed", index)
				return
			}
			if err := p.processor.Process(ctx, job); err != nil {
				log.Printf("worker %d finished job_id=%s with error: %v", index, job.JobID, err)
			}
		}
	}
}

// JobsChan returns the write-only channel used to feed jobs into the pool.
func (p *Pool) JobsChan() chan<- models.Job {
	return p.jobsChan
}

// Wait blocks until every worker goroutine has exited.
func (p *Pool) Wait() {
	p.wg.Wait()
}
