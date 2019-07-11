package models

import (
    "net/http"
    "sync"
    "time"

    "github.com/pachyderm/s2"
)

var (
    GlobalUser = s2.User{
        ID:          "s2-demo",
        DisplayName: "s2 demo",
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

func (s Storage) Bucket(r *http.Request, name string) (Bucket, error) {
    bucket, ok := s.Buckets[name]
    if !ok {
        return NewBucket(), s2.NoSuchBucketError(r)
    }
    return bucket, nil
}

type Bucket struct {
    Objects    map[string][]byte
    Multiparts map[MultipartKey]map[int][]byte
}

func NewBucket() Bucket {
    return Bucket{
        Objects:    map[string][]byte{},
        Multiparts: map[MultipartKey]map[int][]byte{},
    }
}

func (b Bucket) Object(r *http.Request, key string) ([]byte, error) {
    bytes, ok := b.Objects[key]
    if !ok {
        return nil, s2.NoSuchKeyError(r)
    }
    return bytes, nil
}

func (b Bucket) Multipart(r *http.Request, key, uploadID string) (map[int][]byte, error) {
    multipart, ok := b.Multiparts[NewMultipartKey(key, uploadID)]
    if !ok {
        return nil, s2.NoSuchUploadError(r)
    }
    return multipart, nil
}

type MultipartKey struct {
    Key      string
    UploadID string
}

func NewMultipartKey(key, uploadID string) MultipartKey {
    return MultipartKey{
        Key:      key,
        UploadID: uploadID,
    }
}
