package s2

import (
    "io"
)

type ReadSeekCloser interface {
    io.Reader
    io.Seeker
    io.Closer
}
