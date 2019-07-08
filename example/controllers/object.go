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

type ObjectController struct {
	DB models.Storage
}

func (c ObjectController) Get(r *http.Request, name, key string, result *s2.GetObjectResult) error {
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

func (c ObjectController) Put(r *http.Request, name, key string, reader io.Reader) error {
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return s2.InternalError(r, err)
	}

	bucket.Objects[key] = bytes
	return nil
}

func (c ObjectController) Del(r *http.Request, name, key string) error {
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
