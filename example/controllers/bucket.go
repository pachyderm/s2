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

func (c Controller) GetLocation(r *http.Request, name string) (location string, err error) {
	c.logger.Tracef("GetLocation: %+v", name)
	return models.Location, nil
}

// Lists bucket contents. Note that this doesn't support common prefixes or
// delimiters.
func (c Controller) ListObjects(r *http.Request, name, prefix, marker, delimiter string, maxKeys int) (contents []s2.Contents, commonPrefixes []s2.CommonPrefixes, isTruncated bool, err error) {
	c.logger.Tracef("ListObjects: name=%+v, prefix=%+v, marker=%+v, delimiter=%+v, maxKeys=%+v", name, prefix, marker, delimiter, maxKeys)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	if delimiter != "" {
		err = s2.NotImplementedError(r)
		return
	}

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
	}

	keys := []string{}
	for key := range bucket.Objects {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		if key <= marker {
			continue
		}
		if !strings.HasPrefix(key, prefix) {
			break
		}

		if len(contents)+len(commonPrefixes) >= maxKeys {
			if maxKeys > 0 {
				isTruncated = true
			}
			break
		}

		bytes := bucket.Objects[key]
		hash := md5.Sum(bytes)

		contents = append(contents, s2.Contents{
			Key:          key,
			LastModified: models.Epoch,
			ETag:         fmt.Sprintf("%x", hash),
			Size:         uint64(len(bytes)),
			StorageClass: models.StorageClass,
			Owner:        models.GlobalUser,
		})
	}

	return
}

func (c Controller) CreateBucket(r *http.Request, name string) error {
	c.logger.Tracef("CreateBucket: %+v", name)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	_, ok := c.DB.Buckets[name]
	if ok {
		return s2.BucketAlreadyOwnedByYouError(r)
	}

	c.DB.Buckets[name] = models.NewBucket()
	return nil
}

func (c Controller) DeleteBucket(r *http.Request, name string) error {
	c.logger.Tracef("DeleteBucket: %+v", name)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	_, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}
	delete(c.DB.Buckets, name)
	return nil
}
