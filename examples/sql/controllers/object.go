package controllers

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
)

func (c Controller) GetObject(r *http.Request, name, key, version string) (etag string, fetchedVersion string, modTime time.Time, content io.ReadSeeker, err error) {
	c.logger.Tracef("GetObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.db.Begin()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		tx.Rollback()
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	object, err := models.GetObject(tx, bucket.ID, key)
	if err != nil {
		tx.Rollback()
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchKeyError(r)
		}
		return
	}

	etag = object.ETag
	modTime = models.Epoch
	content = bytes.NewReader(object.Content)
	c.commit(tx)
	return
}

func (c Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (etag, createdVersion string, err error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)
	tx := c.db.Begin()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		tx.Rollback()
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		tx.Rollback()
		return
	}

	var object models.Object
	object, err = models.UpsertObject(tx, bucket.ID, key, bytes)
	if err != nil {
		tx.Rollback()
		return
	}

	etag = object.ETag
	c.commit(tx)
	return
}

func (c Controller) DeleteObject(r *http.Request, name, key, version string) (removedVersion string, err error) {
	c.logger.Tracef("DeleteObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.db.Begin()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		tx.Rollback()
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	err = models.DeleteObject(tx, bucket.ID, key)
	if err != nil {
		tx.Rollback()
		return
	}

	c.commit(tx)
	return
}
