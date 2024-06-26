package stream

type MaybeError[T any] struct {
	obj T
	err error
}

func NewMaybeErrorValue[T any](obj T) MaybeError[T] {
	return MaybeError[T]{
		obj: obj,
		err: nil,
	}
}

func NewMaybeErrorError[T any](err error) MaybeError[T] {
	return MaybeError[T]{
		err: err,
	}
}

func (o *MaybeError[T]) HasError() bool {
	return o.err != nil
}

func (o *MaybeError[T]) Error() error {
	return o.err
}

func (o *MaybeError[T]) Value() T {
	return o.obj
}

func (o *MaybeError[T]) ValueIfNoError() *T {
	if o.HasError() {
		return nil
	}

	return &o.obj
}
