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

func (c *Controller) ListMultipart(r *http.Request, name, keyMarker, uploadIDMarker string, maxUploads int) (*s2.ListMultipartResult, error) {
	c.logger.Tracef("ListMultipart: name=%+v, keyMarker=%+v, uploadIDMarker=%+v, maxUploads=%+v", name, keyMarker, uploadIDMarker, maxUploads)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	uploads, err := models.ListUploads(tx, bucket.ID, keyMarker, uploadIDMarker, maxUploads+1)
	if err != nil {
		c.rollback(tx)
		return nil, err
	}

	result := s2.ListMultipartResult{
		Uploads: []s2.Upload{},
	}

	for _, upload := range uploads {
		if len(result.Uploads) >= maxUploads {
			if maxUploads > 0 {
				result.IsTruncated = true
			}
			break
		}

		result.Uploads = append(result.Uploads, s2.Upload{
			Key:          upload.Key,
			UploadID:     upload.ID,
			Initiator:    models.GlobalUser,
			StorageClass: models.StorageClass,
			Initiated:    models.Epoch,
		})
	}

	c.commit(tx)
	return &result, nil
}

func (c *Controller) InitMultipart(r *http.Request, name, key string) (string, error) {
	c.logger.Tracef("InitMultipart: name=%+v, key=%+v", name, key)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return "", s2.NoSuchBucketError(r)
		}
		return "", err
	}

	upload, err := models.CreateUpload(tx, bucket.ID, key)
	if err != nil {
		c.rollback(tx)
		return "", err
	}

	c.commit(tx)
	return upload.ID, nil
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

func (c *Controller) CompleteMultipart(r *http.Request, name, key, uploadID string, parts []s2.Part) (*s2.CompleteMultipartResult, error) {
	c.logger.Tracef("CompleteMultipart: name=%+v, key=%+v, uploadID=%+v, parts=%+v", name, key, uploadID, parts)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	_, err = models.GetUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchUploadError(r)
		}
		return nil, err
	}

	content := []byte{}

	for i, part := range parts {
		uploadPart, err := models.GetUploadPart(tx, uploadID, part.PartNumber)
		if err != nil {
			c.rollback(tx)
			if gorm.IsRecordNotFoundError(err) {
				return nil, s2.InvalidPartError(r)
			}
			return nil, err
		}
		if uploadPart.ETag != part.ETag {
			c.rollback(tx)
			return nil, s2.InvalidPartError(r)
		}
		// each part, except for the last, is expected to be at least 5mb in
		// s3
		if i < len(parts)-1 && len(uploadPart.Content) < 5*1024*1024 {
			c.rollback(tx)
			return nil, s2.EntityTooSmallError(r)
		}

		content = append(content, uploadPart.Content...)
	}

	version := ""
	if bucket.Versioning == s2.VersioningEnabled {
		version = util.RandomString(10)
	}

	obj, err := models.UpsertObject(tx, bucket.ID, key, version, content)
	if err != nil {
		c.rollback(tx)
		return nil, err
	}
	if err := models.DeleteUpload(tx, bucket.ID, key, uploadID); err != nil {
		c.rollback(tx)
		return nil, err
	}

	result := s2.CompleteMultipartResult{
		Location: models.Location,
		ETag:     obj.ETag,
	}
	if bucket.Versioning == s2.VersioningEnabled {
		result.Version = obj.Version
	}

	c.commit(tx)
	return &result, nil
}

func (c *Controller) ListMultipartChunks(r *http.Request, name, key, uploadID string, partNumberMarker, maxParts int) (*s2.ListMultipartChunksResult, error) {
	c.logger.Tracef("ListMultipartChunks: name=%+v, key=%+v, uploadID=%+v, partNumberMarker=%+v, maxParts=%+v", name, key, uploadID, partNumberMarker, maxParts)
	tx := c.trans()

	_, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return nil, s2.NoSuchBucketError(r)
		}
		return nil, err
	}

	uploadParts, err := models.ListUploadParts(tx, uploadID, partNumberMarker, maxParts+1)
	if err != nil {
		c.rollback(tx)
		return nil, err
	}

	result := s2.ListMultipartChunksResult{
		Initiator:    &models.GlobalUser,
		Owner:        &models.GlobalUser,
		StorageClass: models.StorageClass,
		Parts:        []s2.Part{},
	}

	for _, uploadPart := range uploadParts {
		if len(result.Parts) >= maxParts {
			if maxParts > 0 {
				result.IsTruncated = true
			}
			break
		}

		result.Parts = append(result.Parts, s2.Part{
			PartNumber: uploadPart.Number,
			ETag:       uploadPart.ETag,
		})
	}

	c.commit(tx)
	return &result, nil
}

func (c *Controller) UploadMultipartChunk(r *http.Request, name, key, uploadID string, partNumber int, reader io.Reader) (string, error) {
	c.logger.Tracef("UploadMultipartChunk: name=%+v, key=%+v, uploadID=%+v partNumber=%+v", name, key, uploadID, partNumber)

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}

	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return "", s2.NoSuchBucketError(r)
		}
		return "", err
	}

	_, err = models.GetUpload(tx, bucket.ID, key, uploadID)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return "", s2.NoSuchUploadError(r)
		}
		return "", err
	}

	uploadPart, err := models.UpsertUploadPart(tx, uploadID, partNumber, content)
	if err != nil {
		c.rollback(tx)
		return "", err
	}

	c.commit(tx)
	return uploadPart.ETag, nil
}
