package controllers

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) GetObject(r *http.Request, name, key, version string) (etag string, fetchedVersion string, modTime time.Time, content io.ReadSeeker, err error) {
	c.logger.Tracef("GetObject: name=%+v, key=%+v, version=%+v", name, key, version)
	vars := mux.Vars(r)
	tx := vars["tx"]

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	object, err := models.GetObject(tx, bucket.ID, key)
	if err != nil {
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchKeyError(r)
		}
		return
	}

	etag = object.ETag
	modTime = models.Epoch
	content = bytes.NewReader(object.Content)
	return
}

func (c Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (etag, createdVersion string, err error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)
	vars := mux.Vars(r)
	tx := vars["tx"]

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}

	_, err = models.UpsertObject(tx, bucket.ID, key, bytes)
	return
}

func (c Controller) DeleteObject(r *http.Request, name, key, version string) (removedVersion string, err error) {
	c.logger.Tracef("DeleteObject: name=%+v, key=%+v, version=%+v", name, key, version)
	vars := mux.Vars(r)
	tx := vars["tx"]

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	err = models.DeleteObject(tx, bucket.ID, key)
	return
}
