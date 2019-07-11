package controllers

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

const randomStringOptions = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = randomStringOptions[rand.Intn(len(randomStringOptions))]
	}
	return string(b)
}

func (c Controller) ListMultipart(r *http.Request, name string, result *s2.ListMultipartUploadsResult) error {
	c.logger.Tracef("ListMultipart: name=%+v, result=%+v", name, result)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	keys := []models.MultipartKey{}
	for key := range bucket.Multiparts {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Key < keys[j].Key {
			return true
		}
		if keys[i].UploadID < keys[j].UploadID {
			return true
		}
		return false
	})

	for _, key := range keys {
		if key.Key < result.KeyMarker {
			continue
		}
		if key.UploadID < result.UploadIDMarker {
			continue
		}

		if result.IsFull() {
			if result.MaxUploads > 0 {
				result.IsTruncated = true
			}
			break
		}

		result.Uploads = append(result.Uploads, s2.Upload{
			Key:          key.Key,
			UploadID:     key.UploadID,
			Initiator:    models.GlobalUser,
			StorageClass: models.StorageClass,
			Initiated:    models.Epoch,
		})
	}

	return nil
}

func (c Controller) InitMultipart(r *http.Request, name, key string) (string, error) {
	c.logger.Tracef("InitMultipart: name=%+v, key=%+v", name, key)
	uploadID := randomString(10)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return "", err
	}

	multipartKey := models.NewMultipartKey(key, uploadID)
	bucket.Multiparts[multipartKey] = map[int][]byte{}
	return uploadID, nil
}

func (c Controller) AbortMultipart(r *http.Request, name, key, uploadID string) error {
	c.logger.Tracef("AbortMultipart: name=%+v, key=%+v, uploadID=%+v", name, key, uploadID)
	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	if _, err = bucket.Multipart(r, key, uploadID); err != nil {
		return err
	}

	multipartKey := models.NewMultipartKey(key, uploadID)
	delete(bucket.Multiparts, multipartKey)
	return nil
}

func (c Controller) CompleteMultipart(r *http.Request, name, key, uploadID string, parts []s2.Part, result *s2.CompleteMultipartUploadResult) error {
	c.logger.Tracef("CompleteMultipart: name=%+v, key=%+v, uploadID=%+v, parts=%+v, result=%+v", name, key, uploadID, parts, result)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return err
	}

	bytes := []byte{}

	for i, part := range parts {
		chunk, ok := multipart[part.PartNumber]
		if !ok || fmt.Sprintf("%x", md5.Sum(chunk)) != part.ETag {
			return s2.InvalidPartError(r)
		}

		if i < len(parts)-1 && len(chunk) < 5*1024*1024 {
			// each part, except for the last, is expected to be at least 5mb
			// in s3
			return s2.EntityTooSmallError(r)
		}

		bytes = append(bytes, chunk...)
	}

	bucket.Objects[key] = bytes
	multipartKey := models.NewMultipartKey(key, uploadID)
	delete(bucket.Multiparts, multipartKey)
	return nil
}

func (c Controller) ListMultipartChunks(r *http.Request, name, key, uploadID string, result *s2.ListPartsResult) error {
	c.logger.Tracef("ListMultipartChunks: name=%+v, key=%+v, uploadID=%+v, result=%+v", name, key, uploadID, result)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return err
	}

	keys := []int{}
	for key := range multipart {
		keys = append(keys, key)
	}

	sort.Ints(keys)

	result.Initiator = models.GlobalUser
	result.Owner = models.GlobalUser
	result.StorageClass = models.StorageClass

	for _, key := range keys {
		if key < result.PartNumberMarker {
			continue
		}

		if result.IsFull() {
			if result.MaxParts > 0 {
				result.IsTruncated = true
			}
			break
		}

		result.Parts = append(result.Parts, s2.Part{
			PartNumber: key,
			ETag:       fmt.Sprintf("%x", md5.Sum(multipart[key])),
		})
	}

	return nil
}

func (c Controller) UploadMultipartChunk(r *http.Request, name, key, uploadID string, partNumber int, reader io.Reader) error {
	c.logger.Tracef("UploadMultipartChunk: name=%+v, key=%+v, uploadID=%+v partNumber=%+v", name, key, uploadID, partNumber)

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return s2.InternalError(r, err)
	}

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return err
	}

	multipart[partNumber] = bytes
	return nil
}

func (c Controller) DeleteMultipartChunk(r *http.Request, name, key, uploadID string, partNumber int) error {
	c.logger.Tracef("DeleteMultipartChunk: name=%+v, key=%+v, uploadID=%+v partNumber=%+v", name, key, uploadID, partNumber)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return err
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return err
	}

	if _, ok := multipart[partNumber]; !ok {
		return s2.InvalidPartError(r)
	}

	delete(multipart, partNumber)
	return nil
}
