package smartremote

type nullWriter struct{}

func (nw nullWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
