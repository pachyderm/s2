package models

// TODO:
// - handle cascading deletes

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

// Gorm has a bug where it deserializes empty byte arrays as nil, which then
// triggers downstream serialization of the object that don't expect nil. This
// works around the issue.
func fixContent(object *Object) {
    if object.Content == nil {
        object.Content = []byte{}
    }
}

type Object struct {
    ID        uint       `gorm:"primary_key"`
    UpdatedAt time.Time  `gorm:"index"`
    DeletedAt *time.Time `gorm:"index" jsonapi:"attr,deleted_at"`

    BucketID uint   `gorm:"not null"`
    Key      string `gorm:"not null,index:idx_object_key"`
    Version  string `gorm:"index:idx_object_version"`
    Current  bool   `gorm:"not null,index:idx_object_current"`

    ETag    string `gorm:"not null"`
    Content []byte `gorm:"not null"`
}

func GetObject(db *gorm.DB, bucketID uint, key, version string) (Object, error) {
    var object Object
    err := db.Unscoped().Where("bucket_id = ? AND key = ? AND version = ?", bucketID, key, version).First(&object).Error
    fixContent(&object)
    return object, err
}

func GetCurrentObject(db *gorm.DB, bucketID uint, key string) (Object, error) {
    var object Object
    err := db.Unscoped().Where("bucket_id = ? AND key = ? AND current = 1", bucketID, key).First(&object).Error
    fixContent(&object)
    return object, err
}

func ListCurrentObjects(db *gorm.DB, bucketID uint, marker string, limit int) ([]Object, error) {
    var objects []Object
    err := db.Limit(limit).Order("bucket_id, key").Where("bucket_id = ? AND key > ? AND current = 1 AND deleted_at IS NULL", bucketID, marker).Find(&objects).Error
    for _, object := range objects {
        fixContent(&object)
    }
    return objects, err
}

func ListObjects(db *gorm.DB, bucketID uint, keyMarker, versionMarker string, limit int) ([]Object, error) {
    var objects []Object
    q := db.Unscoped().Limit(limit).Order("bucket_id, key, version")

    if keyMarker == "" && versionMarker == "" {
        q = q.Where("bucket_id = ?", bucketID).Find(&objects)
    } else if versionMarker == "" {
        q = q.Where("bucket_id = ? AND key > ?", bucketID, keyMarker).Find(&objects)
    } else {
        q = q.Where("bucket_id = ? AND key >= ? AND version > ?", bucketID, keyMarker, versionMarker).Find(&objects)
    }

    return objects, q.Error
}

func UpsertObject(db *gorm.DB, bucketID uint, key, version string, content []byte) (Object, error) {
    etag := fmt.Sprintf("%x", md5.Sum(content))

    object, err := GetCurrentObject(db, bucketID, key)
    if err != nil {
        if !gorm.IsRecordNotFoundError(err) {
            return Object{}, err
        }
    } else {
        if object.Version == version && object.DeletedAt == nil {
            object.ETag = etag
            object.Content = content
            err = db.Save(&object).Error
            return object, err
        }

        object.Current = false
        err = db.Unscoped().Save(&object).Error
        if err != nil {
            return Object{}, err
        }
    }

    newObject := Object{
        BucketID: bucketID,
        ETag:     etag,
        Key:      key,
        Content:  content,
        Version:  version,
        Current:  true,
    }
    err = db.Create(&newObject).Error
    return newObject, err
}

func GetLatestLivingObject(db *gorm.DB, bucketID uint, key string) (Object, error) {
    var object Object
    err := db.Order("updated_at DESC").Where("bucket_id = ? AND key = ? AND deleted_at IS NULL", bucketID, key).First(&object).Error
    fixContent(&object)
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
    q := db.Limit(limit).Order("bucket_id, key, id")

    if keyMarker == "" && idMarker == "" {
        q = q.Where("bucket_id = ?", bucketID).Find(&parts)
    } else if idMarker == "" {
        q = q.Where("bucket_id = ? AND key > ?", bucketID, keyMarker).Find(&parts)
    } else {
        q = q.Where("bucket_id = ? AND key >= ? AND id > ?", bucketID, keyMarker, idMarker).Find(&parts)
    }

    return parts, q.Error
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
