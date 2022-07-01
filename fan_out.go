package fan_out

import (
	"context"
	"sync"
)

type readerInfo struct {
	taken    bool
	position int
}

// FanOut is a shared growing buffer with multiple independent readers.
// Thread safe.
type FanOut struct {
	mu sync.RWMutex

	data []interface{}

	minPosition   int
	nrMinPosition int
	maxPosition   int

	readers   []readerInfo // Positions of all the readers
	nrReaders int

	writerChan chan struct{} // Block writer
	readerChan chan struct{} // Block readers until new data available

	maxBestReaderDistance  int // Maximum distance between best reader and writer (should be >= 1)
	maxWorstReaderDistance int // Maximum distance between worse reader and writer (should be >= 1)

	closedChan chan struct{}
}

// NewFanOut constructs a new FanOut instance with provided parameters.
func NewFanOut(initialSize int, maxBestReaderDistance int, maxWorstReaderDistance int) *FanOut {
	closedChan := make(chan struct{})
	close(closedChan)

	return &FanOut{
		data: make([]interface{}, 0, initialSize),

		minPosition:   0,
		nrMinPosition: 0,
		maxPosition:   0,

		readers:   nil,
		nrReaders: 0,

		writerChan: make(chan struct{}),
		readerChan: make(chan struct{}),

		maxBestReaderDistance:  maxBestReaderDistance,
		maxWorstReaderDistance: maxWorstReaderDistance,

		closedChan: closedChan,
	}
}

type Reader struct {
	buf      *FanOut
	readerId int
}

func (r Reader) Receive() <-chan struct{} {
	return r.buf.notifyChan(r.readerId)
}

func (r Reader) TryRead() (interface{}, bool) {
	return r.buf.read(r.readerId)
}

func (r Reader) Read(ctx context.Context) interface{} {
	var val interface{}

loop:
	for ok := false; !ok; val, ok = r.TryRead() {
		select {
		case <-r.Receive():
		case <-ctx.Done():
			break loop
		}
	}

	return val
}

func (r Reader) close() {
	r.buf.dropreaderInfo(r.readerId)
}

type CloseFn func()

func (r *FanOut) NewReader() (Reader, CloseFn) {
	newreaderInfo := readerInfo{
		position: len(r.data) - 1 + 1, // A new reader starts without any messages
		taken:    true,
	}

	foundSlot := false
	var readerId int

	r.mu.Lock()
	for readerId = range r.readers {
		if r.readers[readerId].taken {
			continue
		}

		foundSlot = true
		r.readers[readerId] = newreaderInfo
	}
	if !foundSlot {
		r.readers = append(r.readers, newreaderInfo)
		readerId = len(r.readers) - 1
	}
	r.nrReaders++
	r.mu.Unlock()

	reader := Reader{
		buf:      r,
		readerId: readerId,
	}

	return reader, reader.close
}

func (r *FanOut) lockedUpdateMin(oldPosition int) bool {
	if r.nrMinPosition == 0 {
		for _, reader := range r.readers {
			if !reader.taken || reader.position != r.minPosition {
				continue
			}
			r.nrMinPosition++
		}
	} else {
		r.nrMinPosition--
	}

	if r.nrReaders > 0 && r.nrMinPosition == 0 {
		r.minPosition++

		for i := 0; i < r.minPosition-1; i++ {
			r.data[i] = nil
		}

		return true
	}

	return false
}

func (r *FanOut) dropreaderInfo(readerId int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.readers[readerId].taken {
		panic("cancel was already called once")
	}

	r.readers[readerId].taken = false
	r.nrReaders--
	position := r.readers[readerId].position
	if position == r.minPosition {
		r.lockedUpdateMin(position)
	}
}

func (r *FanOut) notifyChan(readerId int) <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	position := r.readers[readerId].position
	if position < len(r.data) {
		return r.closedChan
	}

	return r.readerChan
}

func (r *FanOut) read(readerId int) (data interface{}, ok bool) {
	shouldNotify := false
	defer func() {
		// notify writer that the best reader has been updated or
		// that the worst reader has been updated
		if shouldNotify {
			select {
			case r.writerChan <- struct{}{}:
			default:
			}
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	position := r.readers[readerId].position
	if position >= len(r.data) {
		return nil, false
	}

	r.readers[readerId].position++
	if position+1 > r.maxPosition {
		r.maxPosition = position + 1
		shouldNotify = true
	}
	if position == r.minPosition {
		didUpdate := r.lockedUpdateMin(position)
		shouldNotify = shouldNotify || didUpdate
	}

	return r.data[position], true
}

func (r *FanOut) Send() <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if (r.nrReaders > 0) &&
		(len(r.data)-r.maxBestReaderDistance < r.maxPosition) &&
		(len(r.data)-r.minPosition < r.maxWorstReaderDistance) {
		return r.closedChan
	}

	return r.writerChan
}

func (r *FanOut) TryWrite(data interface{}) bool {
	r.mu.Lock()

	shouldNotify := false
	defer func() {
		// notify readers that reached the end of the buffer
		if shouldNotify {
			channel := make(chan struct{})
			channel, r.readerChan = r.readerChan, channel
			r.mu.Unlock()
			close(channel)
		} else {
			r.mu.Unlock()
		}
	}()

	if (r.nrReaders == 0) ||
		(len(r.data)-r.maxBestReaderDistance >= r.maxPosition) ||
		(len(r.data)-r.minPosition >= r.maxWorstReaderDistance) {
		return false
	}

	// if no space left and new data fits in existing buffer, move buffer to start to make more space
	isNoSpaceLeft := len(r.data) == cap(r.data)
	canFitInBuffer := r.minPosition > 0
	if isNoSpaceLeft && canFitInBuffer {
		startLen := len(r.data)
		copy(r.data[:cap(r.data)], r.data[r.minPosition:])
		r.data = r.data[:startLen-r.minPosition]

		for id := range r.readers {
			r.readers[id].position -= r.minPosition
		}
		r.maxPosition -= r.minPosition
		r.minPosition = 0
	}

	if r.maxPosition == len(r.data) {
		shouldNotify = true
	}

	r.data = append(r.data, data)

	return true
}

func (r *FanOut) Write(ctx context.Context, data interface{}) {
loop:
	for ok := false; !ok; ok = r.TryWrite(data) {
		select {
		case <-r.Send():
		case <-ctx.Done():
			break loop
		}
	}
}

func (r *FanOut) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.data) - r.minPosition
}
