package controllers

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/jinzhu/gorm"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
	"github.com/pachyderm/s2/examples/sql/util"
)

func (c *Controller) GetObject(r *http.Request, name, key, version string) (*s2.GetObjectResult, error) {
	c.logger.Tracef("GetObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	var object models.Object
	if version != "" {
		object, err = models.GetObject(tx, bucket.ID, key, version)
	} else {
		object, err = models.GetLatestLivingObject(tx, bucket.ID, key)
		if gorm.IsRecordNotFoundError(err) {
			object, err = models.GetLatestObject(tx, bucket.ID, key)
		}
	}
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchKeyError(r)
		}
		return nil, err
	}

	if object.DeletedAt != nil {
		result := s2.GetObjectResult{
			DeleteMarker: true,
		}

		return &result, nil
	}

	result := s2.GetObjectResult{
		ETag:    object.ETag,
		ModTime: models.Epoch,
		Content: bytes.NewReader(object.Content),
	}
	if bucket.Versioning == s2.VersioningEnabled {
		result.Version = object.Version
	}

	c.commit(tx)
	return &result, nil
}

func (c *Controller) PutObject(r *http.Request, name, key string, reader io.Reader) (*s2.PutObjectResult, error) {
	c.logger.Tracef("PutObject: name=%+v, key=%+v", name, key)

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	version := ""
	if bucket.Versioning == s2.VersioningEnabled {
		version = util.RandomString(10)
	}

	object, err := models.UpsertObject(tx, bucket.ID, key, version, bytes)
	if err != nil {
		c.rollback(tx)
		return nil, err
	}

	result := s2.PutObjectResult{
		ETag: object.ETag,
	}
	if bucket.Versioning == s2.VersioningEnabled {
		result.Version = object.Version
	}

	c.commit(tx)
	return &result, nil
}

func (c *Controller) DeleteObject(r *http.Request, name, key, version string) (*s2.DeleteObjectResult, error) {
	c.logger.Tracef("DeleteObject: name=%+v, key=%+v, version=%+v", name, key, version)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	var object models.Object
	if version != "" {
		object, err = models.GetObject(tx, bucket.ID, key, version)
	} else {
		object, err = models.GetLatestObject(tx, bucket.ID, key)
	}
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchKeyError(r)
		}
		return nil, err
	}

	result := s2.DeleteObjectResult{}
	if bucket.Versioning == s2.VersioningEnabled {
		result.Version = object.Version
	}

	if object.DeletedAt != nil {
		if err = tx.Unscoped().Delete(&object).Error; err != nil {
			c.rollback(tx)
			return nil, err
		}

		result.DeleteMarker = true
	} else {
		if err = tx.Delete(&object).Error; err != nil {
			c.rollback(tx)
			return nil, err
		}
	}

	c.commit(tx)
	return &result, nil
}
