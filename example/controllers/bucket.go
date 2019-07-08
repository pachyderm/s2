package controllers

import (
	"crypto/md5"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strings"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

type BucketController struct {
	DB models.Storage
}

func (c BucketController) GetLocation(r *http.Request, name string, result *s2.LocationConstraint) *s2.Error {
	result.Location = "pachydermia"
	return nil
}

// Lists bucket contents. Note that this doesn't support common prefixes or
// delimiters.
func (c BucketController) List(r *http.Request, name string, result *s2.ListBucketResult) *s2.Error {
	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	if result.Delimiter != "" {
		return s2.NotImplementedError(r)
	}

	bucket, s3Err := c.DB.Bucket(r, name)
	if s3Err != nil {
		return s3Err
	}

	keys := []string{}
	for key := range bucket.Objects {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		if key <= result.Marker {
			continue
		}
		if !strings.HasPrefix(key, result.Prefix) {
			break
		}

		if result.IsFull() {
			if result.MaxKeys > 0 {
				result.IsTruncated = true
			}
			break
		}

		contents := bucket.Objects[key]
		hash := md5.Sum(contents)

		result.Contents = append(result.Contents, s2.Contents{
			Key:          key,
			LastModified: models.Epoch,
			ETag:         fmt.Sprintf("%x", hash),
			Size:         uint64(len(contents)),
			StorageClass: models.StorageClass,
			Owner:        models.GlobalUser,
		})
	}

	if result.IsTruncated && len(result.Contents) > 0 {
		result.NextMarker = result.Contents[len(result.Contents)-1].Key
	}

	return nil
}

func (c BucketController) Create(r *http.Request, name string) *s2.Error {
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	_, ok := c.DB.Buckets[name]
	if ok {
		return s2.BucketAlreadyOwnedByYouError(r)
	}

	c.DB.Buckets[name] = models.NewBucket()
	return nil
}

func (c BucketController) Delete(r *http.Request, name string) *s2.Error {
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	_, s3Err := c.DB.Bucket(r, name)
	if s3Err != nil {
		return s3Err
	}
	delete(c.DB.Buckets, name)
	return nil
}
