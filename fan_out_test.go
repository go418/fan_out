package fan_out_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	fan_out "github.com/go418/fan_out"
	"golang.org/x/sync/errgroup"
)

func TestBufferExample1(t *testing.T) {
	ctx := context.Background()
	buf := fan_out.NewFanOut(0, 1, 100)

	if ok := buf.TryWrite("should not read"); ok {
		t.Fatal("expected nothing to send yet")
	}

	reader1, cancel1 := buf.NewReader()
	if _, ok := reader1.TryRead(); ok {
		t.Fatal("expected nothing to read yet")
	}

	reader2, cancel2 := buf.NewReader()
	if _, ok := reader2.TryRead(); ok {
		t.Fatal("expected nothing to read yet")
	}

	const val1 = "first read result"
	buf.Write(ctx, val1)

	if buf.Len() != 1 {
		t.Fatal("expected 1 message")
	}

	cancel1()

	if buf.Len() != 1 {
		t.Fatal("expected 1 message")
	}

	if val, ok := reader2.TryRead(); !ok {
		t.Fatal("expected a result")
	} else if val != val1 {
		t.Fatalf("expected %s got %s instead", val1, val)
	}

	if buf.Len() != 0 {
		t.Fatal("expected 0 messages")
	}
	cancel2()
}

func TestBufferExample2(t *testing.T) {
	testExample(t, 5, 100)
}

func FuzzBufferExample2(f *testing.F) {
	f.Add(5, 100)
	f.Add(1, 1)
	f.Add(0, 0)
	f.Add(0, 100)
	f.Add(100, 0)

	f.Fuzz(func(t *testing.T, a int, b int) {
		testExample(t, a, b)
	})
}

func testExample(t testing.TB, maxBestReaderDistance, maxWorstReaderDistance int) {
	buf := fan_out.NewFanOut(0, maxBestReaderDistance, maxWorstReaderDistance)

	// Expect writing to fail since no readers are added yet
	select {
	case <-buf.Send():
		t.Fatal("expected write to fail")
	default:
	}

	if ok := buf.TryWrite("test"); ok {
		t.Fatal("expected write to fail")
	}

	reader1, cancel1 := buf.NewReader()
	reader2, cancel2 := buf.NewReader()
	defer cancel1()
	defer cancel2()

	// Reader should fail since nothing is available yet
	select {
	case <-reader1.Receive():
		t.Fatal("expected reading to be not possible")
	default:
	}

	if _, ok := reader1.TryRead(); ok {
		t.Fatal("expected reading to fail")
	}

	for k := 0; k < 5; k++ {
		minMaxDistance := maxBestReaderDistance
		if maxWorstReaderDistance < minMaxDistance {
			minMaxDistance = maxWorstReaderDistance
		}

		// Writing should be allowed until first limit is reached
		for i := 0; i < minMaxDistance; i++ {
			select {
			case <-buf.Send():
			default:
				t.Fatal("expected writing")
			}

			if ok := buf.TryWrite("test"); !ok {
				t.Fatal("expected writing")
			}
		}

		select {
		case <-buf.Send():
			t.Fatal("expected write to fail")
		default:
		}

		if ok := buf.TryWrite("test"); ok {
			t.Fatal("expected write to fail")
		}

		// Make best reader catch up
		for i := 0; i < minMaxDistance; i++ {
			select {
			case <-reader1.Receive():
			default:
				t.Fatal("expected reading to be possible")
			}

			if _, ok := reader1.TryRead(); !ok {
				t.Fatal("expected reading")
			}
		}

		select {
		case <-reader1.Receive():
			t.Fatal("expected reading to be not possible")
		default:
		}

		if _, ok := reader1.TryRead(); ok {
			t.Fatal("expected reading to fail")
		}

		newLimit := maxBestReaderDistance
		if maxWorstReaderDistance-minMaxDistance < newLimit {
			newLimit = maxWorstReaderDistance - minMaxDistance
		}

		// Further write to fan out until maxBestReaderDistance is reached
		for i := 0; i < newLimit; i++ {
			select {
			case <-buf.Send():
			default:
				t.Fatal("expected writing")
			}

			if ok := buf.TryWrite("test"); !ok {
				t.Fatal("expected writing")
			}
		}

		select {
		case <-buf.Send():
			t.Fatal("expected write to fail")
		default:
		}

		if ok := buf.TryWrite("test"); ok {
			t.Fatal("expected write to fail")
		}

		nrMessages := minMaxDistance
		if minMaxDistance+newLimit > nrMessages {
			nrMessages = minMaxDistance + newLimit
		}

		// Let worst reader read all additional messages
		for i := 0; i < nrMessages; i++ {
			select {
			case <-reader2.Receive():
			default:
				t.Fatal("expected reading to be possible")
			}

			if _, ok := reader2.TryRead(); !ok {
				t.Fatal("expected reading")
			}
		}

		select {
		case <-reader2.Receive():
			t.Fatal("expected reading to be not possible")
		default:
		}

		if _, ok := reader2.TryRead(); ok {
			t.Fatal("expected reading to fail")
		}

		// Let new worst reader read all additional messages
		for i := 0; i < newLimit; i++ {
			select {
			case <-reader1.Receive():
			default:
				t.Fatal("expected reading to be possible")
			}

			if _, ok := reader1.TryRead(); !ok {
				t.Fatal("expected reading")
			}
		}

		select {
		case <-reader1.Receive():
			t.Fatal("expected reading to be not possible")
		default:
		}

		if _, ok := reader1.TryRead(); ok {
			t.Fatal("expected reading to fail")
		}
	}
}

type Bomb struct{}

func TestBufferLightLoad(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)

	ctx := context.Background()

	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		reader, cancel := buf.NewReader()

		group.Go(func() error {
			counter := 0

		loop:
			for {
				val := reader.Read(gctx)

				select {
				case <-gctx.Done():
					break loop
				default:
				}

				if _, ok := val.(Bomb); ok {
					break loop
				} else if nr, ok := val.(int); ok {
					if nr == counter {
						counter++
					} else {
						return fmt.Errorf("expected %d got %v instead", counter, nr)
					}
				} else {
					return fmt.Errorf("received unexpected value %v", val)
				}
			}

			cancel()

			return nil
		})
	}

	for i := 0; i < 10000; i++ {
		buf.Write(ctx, i)
	}

	buf.Write(ctx, Bomb(struct{}{}))

	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != 0 {
		t.Fatal("expected 0 messages")
	}
}

func TestBufferHeavyLoad(t *testing.T) {
	if os.Getenv("LONG") == "" {
		t.Skip("Skipping this test since it can take couple of seconds, enable by setting LONG env variable")
	}

	buf := fan_out.NewFanOut(0, 1, 100)

	ctx := context.Background()

	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < 10000; i++ {
		reader, cancel := buf.NewReader()

		group.Go(func() error {
			counter := 0

		loop:
			for {
				val := reader.Read(gctx)

				select {
				case <-gctx.Done():
					break loop
				default:
				}

				if _, ok := val.(Bomb); ok {
					break loop
				} else if nr, ok := val.(int); ok {
					if nr == counter {
						counter++
					} else {
						return fmt.Errorf("expected %d got %v instead", counter, nr)
					}
				} else {
					return fmt.Errorf("received unexpected value %v", val)
				}
			}

			cancel()

			return nil
		})
	}

	for i := 0; i < 1000; i++ {
		buf.Write(ctx, i)
	}

	buf.Write(ctx, Bomb(struct{}{}))

	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != 0 {
		t.Fatal("expected 0 messages")
	}
}

func TestBufferReadBlock(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)

	ctx := context.Background()

	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < 1000; i++ {
		reader, cancel := buf.NewReader()

		group.Go(func() error {
			_ = reader.Read(gctx)

			cancel()

			return nil
		})
	}

	time.Sleep(200 * time.Millisecond)

	buf.Write(ctx, "done")

	if err := group.Wait(); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestBufferWriteBlock(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)
	reader, cancel := buf.NewReader()

	ctx := context.Background()

	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < 1000; i++ {
		group.Go(func() error {
			buf.Write(gctx, "value")

			return nil
		})
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 1000; i++ {
		_ = reader.Read(ctx)
	}
	cancel()

	if err := group.Wait(); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestContextCancel(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)

	parentCtx := context.Background()

	{
		ctx, cancel := context.WithCancel(parentCtx)

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			buf.Write(ctx, "value")
		}()

		time.Sleep(200 * time.Millisecond)

		cancel()

		<-doneCh
	}

	{
		ctx, cancel := context.WithCancel(parentCtx)
		reader, cancelReader := buf.NewReader()

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			_ = reader.Read(ctx)
		}()

		time.Sleep(200 * time.Millisecond)

		cancel()

		<-doneCh

		cancelReader()
	}
}

func TestSlotReuse(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)

	_, cancel1 := buf.NewReader()
	_, cancel2 := buf.NewReader()
	_, cancel3 := buf.NewReader()
	cancel1()
	cancel2()
	cancel3()
	_, cancel4 := buf.NewReader()
	cancel4()
}

func TestDoubleCancelPanic(t *testing.T) {
	buf := fan_out.NewFanOut(0, 1, 100)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	_, cancel1 := buf.NewReader()
	cancel1()
	cancel1()
}

func BenchmarkFanOut1000messages(b *testing.B) {
	ctx := context.Background()

	buf := fan_out.NewFanOut(0, 1, 100)
	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < b.N; i++ {
		reader, cancel := buf.NewReader()

		group.Go(func() error {
			counter := 0

		loop:
			for {
				val := reader.Read(gctx)

				select {
				case <-gctx.Done():
					break loop
				default:
				}

				if _, ok := val.(Bomb); ok {
					break loop
				} else if nr, ok := val.(int); ok {
					if nr == counter {
						counter++
					} else {
						return fmt.Errorf("expected %d got %v instead", counter, nr)
					}
				} else {
					return fmt.Errorf("received unexpected value %v", val)
				}
			}

			cancel()

			return nil
		})
	}

	for i := 0; i < 1000; i++ {
		buf.Write(ctx, i)
	}

	buf.Write(ctx, Bomb(struct{}{}))

	if err := group.Wait(); err != nil {
		b.Fatal(err)
	}

	if buf.Len() != 0 {
		b.Fatal("expected 0 messages")
	}
}

func BenchmarkFanOut1000readers(b *testing.B) {
	ctx := context.Background()

	buf := fan_out.NewFanOut(0, 1, 100)
	group, gctx := errgroup.WithContext(ctx)
	for i := 0; i < 1000; i++ {
		reader, cancel := buf.NewReader()

		group.Go(func() error {
			counter := 0

		loop:
			for {
				val := reader.Read(gctx)

				select {
				case <-gctx.Done():
					break loop
				default:
				}

				if _, ok := val.(Bomb); ok {
					break loop
				} else if nr, ok := val.(int); ok {
					if nr == counter {
						counter++
					} else {
						return fmt.Errorf("expected %d got %v instead", counter, nr)
					}
				} else {
					return fmt.Errorf("received unexpected value %v", val)
				}
			}

			cancel()

			return nil
		})
	}

	for i := 0; i < b.N; i++ {
		buf.Write(ctx, i)
	}

	buf.Write(ctx, Bomb(struct{}{}))

	if err := group.Wait(); err != nil {
		b.Fatal(err)
	}

	if buf.Len() != 0 {
		b.Fatal("expected 0 messages")
	}
}
