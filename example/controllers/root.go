package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) ListBuckets(r *http.Request, result *s2.ListAllMyBucketsResult) error {
	c.logger.Tracef("ListBuckets: %+v", result)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()
	result.Owner = models.GlobalUser

	for bucket := range c.DB.Buckets {
		result.Buckets = append(result.Buckets, s2.Bucket{
			Name:         bucket,
			CreationDate: models.Epoch,
		})
	}

	return nil
}
