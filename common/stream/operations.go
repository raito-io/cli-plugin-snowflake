package stream

import "context"

func ArrayToChannel[T any](ctx context.Context, data []T) <-chan T {
	ch := make(chan T)

	go func() {
		defer close(ch)

		for _, item := range data {
			select {
			case <-ctx.Done():
				return
			case ch <- item:
			}
		}
	}()

	return ch
}
