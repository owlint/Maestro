package errors

type ValidationError struct {
	Origin error
}

func (e ValidationError) Error() string {
	return e.Origin.Error()
}
