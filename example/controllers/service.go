package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

func (c Controller) ListBuckets(r *http.Request) (owner *s2.User, buckets []s2.Bucket, err error) {
	c.logger.Tracef("ListBuckets")

	var dbBuckets []models.Bucket
	if err := db.Find(&dbBuckets).Err; err != nil {
		return
	}

	for bucket := range dbBuckets {
		buckets = append(buckets, s2.Bucket{
			Name:         bucket.Name,
			CreationDate: models.Epoch,
		})
	}

	owner = &models.GlobalUser
	return
}
