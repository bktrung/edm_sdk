package lifecycle

import (
	"sync"
	"testing"
)

func TestAccountingCountsEveryDisposition(t *testing.T) {
	var accounting Accounting
	dispositions := []Disposition{Handled, Requeued, Retried, DeadLettered}
	const workers = 8
	const each = 1000
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(offset int) {
			defer group.Done()
			for i := 0; i < each; i++ {
				accounting.Record(dispositions[(offset+i)%len(dispositions)])
			}
		}(worker)
	}
	group.Wait()
	counts := accounting.Snapshot()
	if counts.Total() != workers*each {
		t.Fatalf("accounting total = %d, want %d", counts.Total(), workers*each)
	}
}

func TestAccountingNilAndUnknownDisposition(t *testing.T) {
	var accounting *Accounting
	accounting.Record(Handled)
	if got := accounting.Snapshot(); got.Total() != 0 {
		t.Fatalf("nil accounting snapshot = %+v", got)
	}
	var value Accounting
	value.Record(Disposition(99))
	if got := value.Snapshot(); got.Total() != 0 {
		t.Fatalf("unknown disposition snapshot = %+v", got)
	}
}
