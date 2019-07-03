package models

import (
    "net/http"
    "sync"
    "time"

    "github.com/pachyderm/s3server"
)

var (
    GlobalUser = s3server.User{
        ID:          "s3server-demo",
        DisplayName: "s3server demo",
    }

    Epoch = time.Unix(0, 0)

    StorageClass = "STANDARD"
)

type Storage struct {
    Lock    *sync.RWMutex
    Buckets map[string]Bucket
}

func NewStorage() Storage {
    return Storage{
        Lock:    &sync.RWMutex{},
        Buckets: map[string]Bucket{},
    }
}

func (s Storage) Bucket(r *http.Request, name string) (Bucket, *s3server.Error) {
    bucket, ok := s.Buckets[name]
    if !ok {
        return NewBucket(), s3server.NoSuchBucketError(r)
    }
    return bucket, nil
}

type Bucket struct {
    Objects map[string][]byte
}

func NewBucket() Bucket {
    return Bucket{
        Objects: map[string][]byte{},
    }
}

func (b Bucket) Object(r *http.Request, key string) ([]byte, *s3server.Error) {
    bytes, ok := b.Objects[key]
    if !ok {
        return nil, s3server.NoSuchKeyError(r)
    }
    return bytes, nil
}
