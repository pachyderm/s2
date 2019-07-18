package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) ListBuckets(r *http.Request) (owner *s2.User, buckets []s2.Bucket, err error) {
	c.logger.Tracef("ListBuckets")

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	for bucket := range c.DB.Buckets {
		buckets = append(buckets, s2.Bucket{
			Name:         bucket,
			CreationDate: models.Epoch,
		})
	}

	owner = &models.GlobalUser
	return
}
