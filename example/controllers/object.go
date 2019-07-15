package controllers

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) GetObject(r *http.Request, name, key string, result *s2.GetObjectResult) error {
	c.logger.Tracef("GetObject: name=%+v, key=%+v, result=%+v", name, key, result)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	object, err := bucket.Object(r, key)
	if err != nil {
		return err
	}

	hash := md5.Sum(object)

	result.Name = key
	result.ETag = fmt.Sprintf("%x", hash)
	result.ModTime = models.Epoch
	result.Content = bytes.NewReader(object)
	return nil
}

func (c Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (string, error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return "", err
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", s2.InternalError(r, err)
	}

	hash := md5.Sum(bytes)
	bucket.Objects[key] = bytes
	return fmt.Sprintf("%x", hash), nil
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
