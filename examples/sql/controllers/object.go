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

func (c *Controller) GetObject(r *http.Request, name, key, version string) (etag string, fetchedVersion string, modTime time.Time, content io.ReadSeeker, err error) {
	c.logger.Tracef("GetObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.trans()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	var object models.Object
	if version != "" {
		object, err = models.GetObjectVersion(tx, bucket.ID, key, version)
	} else {
		object, err = models.GetObject(tx, bucket.ID, key)
	}
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchKeyError(r)
		}
		return
	}

	etag = object.ETag
	modTime = models.Epoch
	content = bytes.NewReader(object.Content)
	if bucket.Versioning == s2.VersioningEnabled {
		fetchedVersion = object.Version
	}
	c.commit(tx)
	return
}

func (c *Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (etag, createdVersion string, err error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}

	tx := c.trans()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	var object models.Object
	object, err = models.UpsertObject(tx, bucket.ID, key, bytes)
	if err != nil {
		c.rollback(tx)
		return
	}

	etag = object.ETag
	if bucket.Versioning == s2.VersioningEnabled {
		createdVersion = object.Version
	}
	c.commit(tx)
	return
}

func (c *Controller) DeleteObject(r *http.Request, name, key, version string) (removedVersion string, err error) {
	c.logger.Tracef("DeleteObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.trans()

	var bucket models.Bucket
	bucket, err = models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	var object models.Object
	if version != "" {
		object, err = models.DeleteObjectVersion(tx, bucket.ID, key, version)
	} else {
		object, err = models.DeleteObject(tx, bucket.ID, key)
	}
	if err != nil {
		c.rollback(tx)
		return
	}

	if bucket.Versioning == s2.VersioningEnabled {
		removedVersion = object.Version
	}
	c.commit(tx)
	return
}
