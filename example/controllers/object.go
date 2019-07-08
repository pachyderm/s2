package controllers

import (
	"bytes"
	"crypto/md5"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

type ObjectController struct {
	DB models.Storage
}

func (c ObjectController) Get(r *http.Request, name, key string, result *s2.GetObjectResult) *s2.Error {
	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, s3Err := c.DB.Bucket(r, name)
	if s3Err != nil {
		return s3Err
	}

	object, s3Err := bucket.Object(r, key)
	if s3Err != nil {
		return s3Err
	}

	hash := md5.Sum(object)

	result.Name = key
	result.Hash = hash[:]
	result.ModTime = models.Epoch
	result.Content = bytes.NewReader(object)
	return nil
}

func (c ObjectController) Put(r *http.Request, name, key string, reader io.Reader) *s2.Error {
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, s3Err := c.DB.Bucket(r, name)
	if s3Err != nil {
		return s3Err
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return s2.InternalError(r, err)
	}

	bucket.Objects[key] = bytes
	return nil
}

func (c ObjectController) Del(r *http.Request, name, key string) *s2.Error {
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, s3Err := c.DB.Bucket(r, name)
	if s3Err != nil {
		return s3Err
	}

	_, s3Err = bucket.Object(r, key)
	if s3Err != nil {
		return s3Err
	}

	delete(bucket.Objects, key)
	return nil
}
