package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/examples/sql/models"
)

func (c *Controller) ListBuckets(r *http.Request) (*s2.ListBucketsResult, error) {
	c.logger.Tracef("ListBuckets")
	tx := c.trans()

	var buckets []*models.Bucket
	if err := tx.Find(&buckets).Error; err != nil {
		c.rollback(tx)
		return nil, err
	}

	result := s2.ListBucketsResult{
		Owner:   &models.GlobalUser,
		Buckets: []s2.Bucket{},
	}

	for _, bucket := range buckets {
		result.Buckets = append(result.Buckets, s2.Bucket{
			Name:         bucket.Name,
			CreationDate: models.Epoch,
		})
	}

	result.Owner = &models.GlobalUser
	c.commit(tx)
	return &result, nil
}
