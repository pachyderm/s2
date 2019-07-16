package controllers

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) GetObject(r *http.Request, name, key string) (etag string, modTime time.Time, content io.ReadSeeker, err error) {
	c.logger.Tracef("GetObject: name=%+v, key=%+v", name, key)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
	}

	object, err := bucket.Object(r, key)
	if err != nil {
		return
	}

	hash := md5.Sum(object)

	etag = fmt.Sprintf("%x", hash)
	modTime = models.Epoch
	content = bytes.NewReader(object)
	return
}

func (c Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (etag string, err error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		err = s2.InternalError(r, err)
		return
	}

	bucket.Objects[key] = bytes

	etag = fmt.Sprintf("%x", md5.Sum(bytes))
	return
}

func (c Controller) DeleteObject(r *http.Request, name, key string) error {
	c.logger.Tracef("DeleteObject: name=%+v, key=%+v", name, key)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	_, err = bucket.Object(r, key)
	if err != nil {
		return err
	}

	delete(bucket.Objects, key)
	return nil
}
