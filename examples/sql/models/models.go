package models

// TODO:
// - handle cascading deletes
// - composite primary keys

import (
    "crypto/md5"
    "fmt"
    "time"

    "github.com/jinzhu/gorm"
    "github.com/pachyderm/s2"
    "github.com/pachyderm/s2/examples/sql/util"
)

var (
    GlobalUser = s2.User{
        ID:          "s2-demo",
        DisplayName: "s2 demo",
    }

    Epoch = time.Unix(0, 0)

    StorageClass = "STANDARD"

    Location = "pachydermia"

    AccessKey = "homer"
    SecretKey = "donuts"
)

func Init(db *gorm.DB) error {
    return db.AutoMigrate(&Bucket{}, &Object{}, &Upload{}, &UploadPart{}).Error
}

type Bucket struct {
    ID         uint   `gorm:"primary_key"`
    Name       string `gorm:"not null,unique_index"`
    Versioning string `gorm:"not null"`
}

func CreateBucket(db *gorm.DB, name string) (*Bucket, error) {
    bucket := &Bucket{Name: name}
    err := db.Save(bucket).Error
    return bucket, err
}

func GetBucket(db *gorm.DB, name string) (Bucket, error) {
    var bucket Bucket
    err := db.Where("name = ?", name).First(&bucket).Error
    return bucket, err
}

type Object struct {
    ID        uint       `gorm:"primary_key"`
    DeletedAt *time.Time `gorm:"index" jsonapi:"attr,deleted_at"`

    BucketID uint   `gorm:"not null"`
    Key      string `gorm:"not null,index:idx_object_key"`
    Version  string `gorm:"index:idx_object_version"`
    Current  bool   `gorm:"not null,index:idx_object_current"`

    ETag    string `gorm:"not null"`
    Content []byte `gorm:"not null"`
}

func GetObject(db *gorm.DB, bucketID uint, key string) (Object, error) {
    var object Object
    err := db.Where("bucket_id = ? AND key = ? AND current = 1 AND deleted_at IS NULL", bucketID, key).First(&object).Error
    if object.Content == nil {
        object.Content = []byte{}
    }
    return object, err
}

func GetObjectVersion(db *gorm.DB, bucketID uint, key, version string) (Object, error) {
    var object Object
    err := db.Where("bucket_id = ? AND key = ? AND version = ?", bucketID, key, version).First(&object).Error
    if object.Content == nil {
        object.Content = []byte{}
    }
    return object, err
}

func ListObjects(db *gorm.DB, bucketID uint, marker string, limit int) ([]Object, error) {
    var objects []Object
    err := db.Limit(limit).Order("bucket_id, key").Where("bucket_id = ? AND key > ? AND current = 1 AND deleted_at IS NULL", bucketID, marker).Find(&objects).Error
    for _, object := range objects {
        if object.Content == nil {
            object.Content = []byte{}
        }
    }
    return objects, err
}

func ListObjectVersions(db *gorm.DB, bucketID uint, keyMarker, versionMarker string, limit int) ([]Object, error) {
    var objects []Object
    err := db.Limit(limit).Order("bucket_id ASC, key ASC, version ASC").Where("bucket_id = ? AND key >= ? AND version > ?", bucketID, keyMarker, versionMarker).Find(&objects).Error
    for _, object := range objects {
        if object.Content == nil {
            object.Content = []byte{}
        }
    }
    return objects, err
}

func UpsertObject(db *gorm.DB, bucketID uint, key string, content []byte) (Object, error) {
    objToCreate := Object{
        BucketID: bucketID,
        ETag:     fmt.Sprintf("%x", md5.Sum(content)),
        Key:      key,
        Content:  content,
        Version:  util.RandomString(10),
        Current:  true,
    }

    existingObj, err := GetObject(db, bucketID, key)
    if err != nil {
        if !gorm.IsRecordNotFoundError(err) {
            return objToCreate, err
        }
    } else {
        existingObj.Current = false
        err = db.Save(&existingObj).Error
        if err != nil {
            return objToCreate, err
        }
    }

    err = db.Create(&objToCreate).Error
    return objToCreate, err
}

func DeleteObject(db *gorm.DB, bucketID uint, key string) (Object, error) {
    var object Object
    err := db.Delete(&object, Object{
        BucketID: bucketID,
        Key:      key,
        Current:  true,
    }).Error
    return object, err
}

func DeleteObjectVersion(db *gorm.DB, bucketID uint, key, version string) (Object, error) {
    var object Object
    err := db.Delete(&object, Object{
        BucketID: bucketID,
        Key:      key,
        Version:  version,
    }).Error
    return object, err
}

type Upload struct {
    ID       string `gorm:"primary_key"`
    BucketID uint   `gorm:"not null"`
    Key      string `gorm:"not null,index:idx_upload_key"`
}

func CreateUpload(db *gorm.DB, bucketID uint, key string) (Upload, error) {
    upload := Upload{
        ID:       util.RandomString(10),
        BucketID: bucketID,
        Key:      key,
    }
    err := db.Create(&upload).Error
    return upload, err
}

func GetUpload(db *gorm.DB, bucketID uint, key, id string) (Upload, error) {
    var upload Upload
    err := db.Where("bucket_id = ? AND key = ? AND id = ?", bucketID, key, id).First(&upload).Error
    return upload, err
}

func ListUploads(db *gorm.DB, bucketID uint, keyMarker string, idMarker string, limit int) ([]Upload, error) {
    var parts []Upload
    err := db.Limit(limit).Order("bucket_id, key, id").Where("bucket_id = ? AND key >= ? AND id > ?", bucketID, keyMarker, idMarker).Find(&parts).Error
    return parts, err
}

type UploadPart struct {
    UploadID string `gorm:"not null,primary_key"`
    Number   int    `gorm:"not null,primary_key"`
    ETag     string `gorm:"not null"`
    Content  []byte `gorm:"not null"`
}

func UpsertUploadPart(db *gorm.DB, uploadID string, number int, content []byte) (UploadPart, error) {
    partToCreate := UploadPart{
        UploadID: uploadID,
        Number:   number,
        ETag:     fmt.Sprintf("%x", md5.Sum(content)),
        Content:  content,
    }

    existingPart, err := GetUploadPart(db, uploadID, number)
    if err != nil {
        if !gorm.IsRecordNotFoundError(err) {
            return partToCreate, err
        }
    } else {
        existingPart.ETag = partToCreate.ETag
        existingPart.Content = partToCreate.Content
        err = db.Save(&existingPart).Error
        if err != nil {
            return existingPart, err
        }
    }

    err = db.Create(&partToCreate).Error
    return partToCreate, err
}

func GetUploadPart(db *gorm.DB, uploadID string, number int) (UploadPart, error) {
    var part UploadPart
    err := db.Where("upload_id = ? AND number = ?", uploadID, number).First(&part).Error
    return part, err
}

func ListUploadParts(db *gorm.DB, uploadID string, partNumberMarker, limit int) ([]UploadPart, error) {
    var parts []UploadPart
    err := db.Limit(limit).Order("upload_id, number").Where("upload_id = ? AND number > ?", uploadID, partNumberMarker).Find(&parts).Error
    return parts, err
}

func DeleteUpload(db *gorm.DB, bucketID uint, key, uploadID string) error {
    err := db.Delete(Upload{
        ID:       uploadID,
        BucketID: bucketID,
        Key:      key,
    }).Error
    if err != nil {
        return err
    }
    return db.Delete(UploadPart{
        UploadID: uploadID,
    }).Error
}
