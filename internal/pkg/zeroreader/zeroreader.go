package zeroreader

import "io"

// zeroReader is an implementation of the io.Reader interface that fills buffers with returns zero bytes.
type zeroReader struct{}

func New() io.Reader {
	return &zeroReader{}
}

func (r *zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
