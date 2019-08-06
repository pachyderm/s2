package controllers

import (
	"io"
	"io/ioutil"
	"net/http"

	"github.com/jinzhu/gorm"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
	"github.com/pachyderm/s2/examples/sql/util"
)

func (c *Controller) ListMultipart(r *http.Request, name, keyMarker, uploadIDMarker string, maxUploads int) (isTruncated bool, uploads []s2.Upload, err error) {
	c.logger.Tracef("ListMultipart: name=%+v, keyMarker=%+v, uploadIDMarker=%+v, maxUploads=%+v", name, keyMarker, uploadIDMarker, maxUploads)
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

	dbUploads, err := models.ListUploads(tx, bucket.ID, keyMarker, uploadIDMarker, maxUploads+1)
	if err != nil {
		c.rollback(tx)
		return
	}

	for _, dbUpload := range dbUploads {
		if len(uploads) >= maxUploads {
			if maxUploads > 0 {
				isTruncated = true
			}
			break
		}

		uploads = append(uploads, s2.Upload{
			Key:          dbUpload.Key,
			UploadID:     dbUpload.ID,
			Initiator:    models.GlobalUser,
			StorageClass: models.StorageClass,
			Initiated:    models.Epoch,
		})
	}

	c.commit(tx)
	return
}

func (c *Controller) InitMultipart(r *http.Request, name, key string) (uploadID string, err error) {
	c.logger.Tracef("InitMultipart: name=%+v, key=%+v", name, key)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	upload, err := models.CreateUpload(tx, bucket.ID, key)
	if err != nil {
		c.rollback(tx)
		return
	}

	uploadID = upload.ID
	c.commit(tx)
	return
}

func (c *Controller) AbortMultipart(r *http.Request, name, key, uploadID string) error {
	c.logger.Tracef("AbortMultipart: name=%+v, key=%+v, uploadID=%+v", name, key, uploadID)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return s2.NoSuchBucketError(r)
		}
		return err
	}

	_, err = models.GetUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return s2.NoSuchUploadError(r)
		}
		return err
	}

	err = models.DeleteUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		return err
	}

	c.commit(tx)
	return nil
}

func (c *Controller) CompleteMultipart(r *http.Request, name, key, uploadID string, parts []s2.Part) (location, etag, createdVersion string, err error) {
	c.logger.Tracef("CompleteMultipart: name=%+v, key=%+v, uploadID=%+v, parts=%+v", name, key, uploadID, parts)
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

	_, err = models.GetUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchUploadError(r)
			return
		}
		return
	}

	content := []byte{}

	for i, part := range parts {
		var uploadPart models.UploadPart
		uploadPart, err = models.GetUploadPart(tx, uploadID, part.PartNumber)
		if err != nil {
			c.rollback(tx)
			if gorm.IsRecordNotFoundError(err) {
				err = s2.InvalidPartError(r)
			}
			return
		}
		if uploadPart.ETag != part.ETag {
			c.rollback(tx)
			err = s2.InvalidPartError(r)
			return
		}
		// each part, except for the last, is expected to be at least 5mb in
		// s3
		if i < len(parts)-1 && len(uploadPart.Content) < 5*1024*1024 {
			c.rollback(tx)
			err = s2.EntityTooSmallError(r)
			return
		}

		content = append(content, uploadPart.Content...)
	}

	version := ""
	if bucket.Versioning == s2.VersioningEnabled {
		version = util.RandomString(10)
	}

	var obj models.Object
	obj, err = models.UpsertObject(tx, bucket.ID, key, version, content)
	if err != nil {
		c.rollback(tx)
		return
	}
	if err = models.DeleteUpload(tx, bucket.ID, key, uploadID); err != nil {
		c.rollback(tx)
		return
	}

	location = models.Location
	etag = obj.ETag
	if bucket.Versioning == s2.VersioningEnabled {
		createdVersion = obj.Version
	}
	c.commit(tx)
	return
}

func (c *Controller) ListMultipartChunks(r *http.Request, name, key, uploadID string, partNumberMarker, maxParts int) (initiator, owner *s2.User, storageClass string, isTruncated bool, parts []s2.Part, err error) {
	c.logger.Tracef("ListMultipartChunks: name=%+v, key=%+v, uploadID=%+v, partNumberMarker=%+v, maxParts=%+v", name, key, uploadID, partNumberMarker, maxParts)
	tx := c.trans()

	_, err = models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	uploadParts, err := models.ListUploadParts(tx, uploadID, partNumberMarker, maxParts+1)
	if err != nil {
		c.rollback(tx)
		return
	}

	for _, uploadPart := range uploadParts {
		if len(parts) >= maxParts {
			if maxParts > 0 {
				isTruncated = true
			}
			break
		}

		parts = append(parts, s2.Part{
			PartNumber: uploadPart.Number,
			ETag:       uploadPart.ETag,
		})
	}

	initiator = &models.GlobalUser
	owner = &models.GlobalUser
	storageClass = models.StorageClass
	c.commit(tx)
	return
}

func (c *Controller) UploadMultipartChunk(r *http.Request, name, key, uploadID string, partNumber int, reader io.Reader) (etag string, err error) {
	c.logger.Tracef("UploadMultipartChunk: name=%+v, key=%+v, uploadID=%+v partNumber=%+v", name, key, uploadID, partNumber)

	content, err := ioutil.ReadAll(reader)
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

	_, err = models.GetUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchUploadError(r)
		}
		return
	}

	uploadPart, err := models.UpsertUploadPart(tx, uploadID, partNumber, content)
	if err != nil {
		c.rollback(tx)
		return
	}

	etag = uploadPart.ETag
	c.commit(tx)
	return
}
