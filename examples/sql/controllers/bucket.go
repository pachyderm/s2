package controllers

import (
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/jinzhu/gorm"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
)

func (c *Controller) GetLocation(r *http.Request, name string) (location string, err error) {
	c.logger.Tracef("GetLocation: %+v", name)
	return models.Location, nil
}

// Lists bucket contents. Note that this doesn't support common prefixes or
// delimiters.
func (c *Controller) ListObjects(r *http.Request, name, prefix, marker, delimiter string, maxKeys int) (contents []s2.Contents, commonPrefixes []s2.CommonPrefixes, isTruncated bool, err error) {
	c.logger.Tracef("ListObjects: name=%+v, prefix=%+v, marker=%+v, delimiter=%+v, maxKeys=%+v", name, prefix, marker, delimiter, maxKeys)
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

	var objects []models.Object
	objects, err = models.ListObjects(tx, bucket.ID, marker, maxKeys+1)
	if err != nil {
		c.rollback(tx)
		return
	}

	for _, object := range objects {
		if !strings.HasPrefix(object.Key, prefix) || isDelimiterFiltered(object.Key, prefix, delimiter) {
			continue
		}

		if delimiter != "" {
			parts := strings.SplitN(object.Key[len(prefix):], delimiter, 2)
			if len(parts) == 2 && len(parts[1]) > 0 {
				continue
			}
		}

		if len(contents)+len(commonPrefixes) >= maxKeys {
			if maxKeys > 0 {
				isTruncated = true
			}
			break
		}

		contents = append(contents, s2.Contents{
			Key:          object.Key,
			LastModified: models.Epoch,
			ETag:         object.ETag,
			Size:         uint64(len(object.Content)),
			StorageClass: models.StorageClass,
			Owner:        models.GlobalUser,
		})
	}

	c.commit(tx)
	return
}

func (c *Controller) ListVersionedObjects(r *http.Request, name, prefix, keyMarker, versionMarker string, delimiter string, maxKeys int) (versions []s2.Version, deleteMarkers []s2.DeleteMarker, isTruncated bool, err error) {
	c.logger.Tracef("ListVersionedObjects: name=%+v, prefix=%+v, keyMarker=%+v, versionMarker=%+v, delimiter=%+v, maxKeys=%+v", name, prefix, keyMarker, versionMarker, delimiter, maxKeys)
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

	// if bucket.Versioning != s2.VersioningEnabled {
	// 	c.rollback(tx)
	// 	err = s2.NotImplementedError(r)
	// 	return
	// }

	var objects []models.Object
	objects, err = models.ListObjectVersions(tx, bucket.ID, keyMarker, versionMarker, maxKeys+1)
	if err != nil {
		c.rollback(tx)
		return
	}

	for _, object := range objects {
		if !strings.HasPrefix(object.Key, prefix) || isDelimiterFiltered(object.Key, prefix, delimiter) {
			continue
		}

		if len(versions)+len(deleteMarkers) >= maxKeys {
			if maxKeys > 0 {
				isTruncated = true
			}
			break
		}

		if object.DeletedAt == nil {
			versions = append(versions, s2.Version{
				Key:          object.Key,
				Version:      object.Version,
				IsLatest:     object.Current,
				LastModified: models.Epoch,
				ETag:         object.ETag,
				Size:         uint64(len(object.Content)),
				StorageClass: models.StorageClass,
				Owner:        models.GlobalUser,
			})
		} else {
			deleteMarkers = append(deleteMarkers, s2.DeleteMarker{
				Key:          object.Key,
				Version:      object.Version,
				IsLatest:     object.Current,
				LastModified: models.Epoch,
				Owner:        models.GlobalUser,
			})
		}
	}

	c.commit(tx)
	return
}

func (c *Controller) CreateBucket(r *http.Request, name string) (err error) {
	c.logger.Tracef("CreateBucket: %+v", name)
	tx := c.trans()

	_, err = models.GetBucket(tx, name)
	if err == nil {
		c.rollback(tx)
		err = s2.BucketAlreadyOwnedByYouError(r)
		return
	} else if !gorm.IsRecordNotFoundError(err) {
		c.rollback(tx)
		return
	}

	_, err = models.CreateBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		return
	}

	c.commit(tx)
	return
}

func (c *Controller) DeleteBucket(r *http.Request, name string) (err error) {
	c.logger.Tracef("DeleteBucket: %+v", name)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			err = s2.NoSuchBucketError(r)
		}
		return
	}

	err = tx.Delete(bucket).Error
	if err != nil {
		c.rollback(tx)
		return
	}

	c.commit(tx)
	return
}

func (c *Controller) GetBucketVersioning(r *http.Request, name string) (status string, err error) {
	c.logger.Tracef("GetBucketVersioning: %+v", name)
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

	c.commit(tx)
	return bucket.Versioning, nil
}

func (c *Controller) SetBucketVersioning(r *http.Request, name, status string) error {
	c.logger.Tracef("SetBucketVersioning: name=%+v, status=%+v", name, status)
	tx := c.trans()

	bucket, err := models.GetBucket(tx, name)
	if err != nil {
		c.rollback(tx)
		if gorm.IsRecordNotFoundError(err) {
			return s2.NoSuchBucketError(r)
		}
		return err
	}

	if status == s2.VersioningDisabled && bucket.Versioning != "" {
		c.rollback(tx)
		return s2.IllegalVersioningConfigurationError(r)
	}

	bucket.Versioning = status

	if err = tx.Save(&bucket).Error; err != nil {
		c.rollback(tx)
		return err
	}

	c.commit(tx)
	return nil
}

func isDelimiterFiltered(key, prefix, delimiter string) bool {
	if delimiter == "" {
		return false
	}
	parts := strings.SplitN(key[len(prefix):], delimiter, 2)
	return len(parts) == 2 && len(parts[1]) > 0
}