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

func (c Controller) ListMultipart(r *http.Request, name, keyMarker, uploadIDMarker string, maxUploads int) (isTruncated bool, uploads []s2.Upload, err error) {
	c.logger.Tracef("ListMultipart: name=%+v, keyMarker=%+v, uploadIDMarker=%+v, maxUploads=%+v", name, keyMarker, uploadIDMarker, maxUploads)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
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
		if key.Key <= keyMarker {
			continue
		}
		if key.UploadID <= uploadIDMarker {
			continue
		}

		if len(uploads) >= maxUploads {
			if maxUploads > 0 {
				isTruncated = true
			}
			break
		}

		uploads = append(uploads, s2.Upload{
			Key:          key.Key,
			UploadID:     key.UploadID,
			Initiator:    models.GlobalUser,
			StorageClass: models.StorageClass,
			Initiated:    models.Epoch,
		})
	}

	return
}

func (c Controller) InitMultipart(r *http.Request, name, key string) (uploadID string, err error) {
	c.logger.Tracef("InitMultipart: name=%+v, key=%+v", name, key)
	uploadID = randomString(10)

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

func (c Controller) CompleteMultipart(r *http.Request, name, key, uploadID string, parts []s2.Part) (location, etag string, err error) {
	c.logger.Tracef("CompleteMultipart: name=%+v, key=%+v, uploadID=%+v, parts=%+v", name, key, uploadID, parts)

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return
	}

	bytes := []byte{}

	for i, part := range parts {
		chunk, ok := multipart[part.PartNumber]
		if !ok || fmt.Sprintf("%x", md5.Sum(chunk)) != part.ETag {
			err = s2.InvalidPartError(r)
			return
		}

		if i < len(parts)-1 && len(chunk) < 5*1024*1024 {
			// each part, except for the last, is expected to be at least 5mb
			// in s3
			err = s2.EntityTooSmallError(r)
			return
		}

		bytes = append(bytes, chunk...)
	}

	bucket.Objects[key] = bytes
	multipartKey := models.NewMultipartKey(key, uploadID)
	delete(bucket.Multiparts, multipartKey)

	location = models.Location
	etag = fmt.Sprintf("%x", md5.Sum(bytes))
	return
}

func (c Controller) ListMultipartChunks(r *http.Request, name, key, uploadID string, partNumberMarker, maxParts int) (initiator, owner *s2.User, storageClass string, isTruncated bool, parts []s2.Part, err error) {
	c.logger.Tracef("ListMultipartChunks: name=%+v, key=%+v, uploadID=%+v, partNumberMarker=%+v, maxParts=%+v", name, key, uploadID, partNumberMarker, maxParts)

	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return
	}

	keys := []int{}
	for key := range multipart {
		keys = append(keys, key)
	}

	sort.Ints(keys)

	for _, key := range keys {
		if key <= partNumberMarker {
			continue
		}

		if len(parts) >= maxParts {
			if maxParts > 0 {
				isTruncated = true
			}
			break
		}

		parts = append(parts, s2.Part{
			PartNumber: key,
			ETag:       fmt.Sprintf("%x", md5.Sum(multipart[key])),
		})
	}

	initiator = &models.GlobalUser
	owner = &models.GlobalUser
	storageClass = models.StorageClass
	return
}

func (c Controller) UploadMultipartChunk(r *http.Request, name, key, uploadID string, partNumber int, reader io.Reader) (etag string, err error) {
	c.logger.Tracef("UploadMultipartChunk: name=%+v, key=%+v, uploadID=%+v partNumber=%+v", name, key, uploadID, partNumber)

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", s2.InternalError(r, err)
	}

	c.DB.Lock.Lock()
	defer c.DB.Lock.Unlock()

	bucket, err := c.DB.Bucket(r, name)
	if err != nil {
		return "", err
	}

	multipart, err := bucket.Multipart(r, key, uploadID)
	if err != nil {
		return "", err
	}

	hash := md5.Sum(bytes)
	multipart[partNumber] = bytes
	return fmt.Sprintf("%x", hash), nil
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
