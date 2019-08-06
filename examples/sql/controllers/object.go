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
	"github.com/pachyderm/s2/examples/sql/util"
)

func (c *Controller) GetObject(r *http.Request, name, key, version string) (etag string, fetchedVersion string, deleteMarker bool, modTime time.Time, content io.ReadSeeker, err error) {
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
		object, err = models.GetObject(tx, bucket.ID, key, version)
	} else {
		object, err = models.GetCurrentObject(tx, bucket.ID, key)
	}
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchKeyError(r)
		}
		return
	}

	if object.DeletedAt != nil {
		deleteMarker = true
		err = s2.NoSuchKeyError(r)
	} else {
		etag = object.ETag
		modTime = models.Epoch
		content = bytes.NewReader(object.Content)
		if bucket.Versioning == s2.VersioningEnabled {
			fetchedVersion = object.Version
		}
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

	version := ""
	if bucket.Versioning == s2.VersioningEnabled {
		version = util.RandomString(10)
	}

	var object models.Object
	object, err = models.UpsertObject(tx, bucket.ID, key, version, bytes)
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

func (c *Controller) DeleteObject(r *http.Request, name, key, version string) (removedVersion string, deleteMarker bool, err error) {
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
		object, err = models.GetObject(tx, bucket.ID, key, version)
	} else {
		object, err = models.GetCurrentObject(tx, bucket.ID, key)
	}
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchKeyError(r)
		}
		return
	}

	moveHead := version != "" && object.Current && bucket.Versioning == s2.VersioningEnabled

	if moveHead {
		object.Current = false
		if err = tx.Save(&object).Error; err != nil {
			c.rollback(tx)
			return
		}
	}

	if object.DeletedAt != nil {
		if err = tx.Unscoped().Delete(&object).Error; err != nil {
			c.rollback(tx)
			return
		}

		deleteMarker = true
	} else {
		if err = tx.Delete(&object).Error; err != nil {
			c.rollback(tx)
			return
		}
	}

	if moveHead {
		var latestObject models.Object
		latestObject, err = models.GetLatestLivingObject(tx, bucket.ID, key)
		if err != nil {
			if !gorm.IsRecordNotFoundError(err) {
				c.rollback(tx)
				return
			}
		} else {
			latestObject.Current = true
			if err = tx.Save(&latestObject).Error; err != nil {
				c.rollback(tx)
				return
			}
		}
	}

	if bucket.Versioning == s2.VersioningEnabled {
		removedVersion = object.Version
	}
	c.commit(tx)
	return
}
