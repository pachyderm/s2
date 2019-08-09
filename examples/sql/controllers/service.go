package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
)

func (c *Controller) ListBuckets(r *http.Request) (owner *s2.User, buckets []s2.Bucket, err error) {
	c.logger.Tracef("ListBuckets")
	tx := c.trans()

	var dbBuckets []*models.Bucket
	if err = tx.Find(&dbBuckets).Error; err != nil {
		c.rollback(tx)
		return
	}

	for _, bucket := range dbBuckets {
		buckets = append(buckets, s2.Bucket{
			Name:         bucket.Name,
			CreationDate: models.Epoch,
		})
	}

	owner = &models.GlobalUser
	c.commit(tx)
	return
}
