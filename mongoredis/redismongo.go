package mongoredis

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/daqiancode/cachelayer"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RedisMongo[T cachelayer.Table[I], I string] struct {
	*cachelayer.CacheBase[T, I]
	db         *mongo.Client
	red        *cachelayer.RedisJson[T]
	redId      *cachelayer.RedisJson[string]
	redIds     *cachelayer.RedisJson[[]string]
	database   string
	collection string
	c          *mongo.Collection
}

func NewRedisMongo[T cachelayer.Table[I], I string](prefix, database, table, idField string, db *mongo.Client, red *redis.Client, ttl time.Duration) *RedisMongo[T, I] {
	return &RedisMongo[T, I]{
		CacheBase:  cachelayer.NewCacheBase[T, I](prefix, table, idField, context.Background()),
		db:         db,
		red:        cachelayer.NewRedisJson[T](red, ttl),
		redId:      cachelayer.NewRedisJson[string](red, ttl),
		redIds:     cachelayer.NewRedisJson[[]string](red, ttl),
		database:   database,
		collection: table,
		c:          db.Database(database).Collection(table),
	}
}

func (s *RedisMongo[T, I]) Close() error {
	return s.db.Disconnect(s.GetCtx())
}
func (s *RedisMongo[T, I]) ClearCache(id I, indexes cachelayer.Indexes) error {

	var keys []string
	if !cachelayer.IsNullID(id) {
		keys = append(keys, s.MakeCacheKey(cachelayer.NewIndex(s.GetIdField(), id)))
	}
	for _, v := range indexes {
		keys = append(keys, s.MakeCacheKey(v))
	}
	keys = cachelayer.UniqueStrings(keys)
	return s.red.Del(s.GetCtx(), keys...).Err()
}

func (s *RedisMongo[T, I]) Get(id I) (T, bool, error) {
	var t T

	r := s.c.FindOne(s.GetCtx(), bson.M{"_id": id})
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}

func (s *RedisMongo[T, I]) List(ids ...I) ([]T, error) {
	var t []T
	var err error
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(cachelayer.Stringify(v, ""))
	// 	if err != nil {
	// 		return t, err
	// 	}
	// }
	query := bson.M{"_id": bson.M{"$in": ids}}
	r, err := s.c.Find(s.GetCtx(), query)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.GetCtx(), &t)
	return t, err
}

func (s *RedisMongo[T, I]) Create(t *T) error {
	if t == nil {
		return nil
	}
	if cachelayer.IsNullID((*t).GetID()) {
		reflect.ValueOf(t).Elem().FieldByName(s.GetIdField()).SetString(primitive.NewObjectID().Hex())
	}
	_, err := s.c.InsertOne(s.GetCtx(), *t)
	if err != nil {
		return err
	}
	s.ClearCache((*t).GetID(), (*t).ListIndexes())
	return nil
}
func (s *RedisMongo[T, I]) Save(t *T) error {
	if t == nil {
		return nil
	}
	id := (*t).GetID()
	if cachelayer.IsNullID(id) {
		return s.Create(t)
	}
	_, exist, err := s.Get(id)
	if err != nil {
		return nil
	}
	if !exist {
		return s.Create(t)
	}
	query := bson.M{"_id": id}
	err = s.c.FindOneAndReplace(s.GetCtx(), query, *t).Err()
	return err
}

func (s *RedisMongo[T, I]) Delete(ids ...I) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	objs, err := s.List(ids...)
	if err != nil {
		return 0, err
	}
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(cachelayer.Stringify(v, ""))
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	query := bson.M{"_id": bson.M{"$in": ids}}
	rs, err := s.c.DeleteMany(s.GetCtx(), query)
	if err != nil {
		return 0, err
	}
	for _, v := range objs {
		s.ClearCache(v.GetID(), v.ListIndexes())
	}
	return rs.DeletedCount, err
}

//Update values type: map[string]interface{} , eg:map[string]interface{}{"addr.country": "uae", "tags.0.name": "gg"}
func (s *RedisMongo[T, I]) Update(id I, values interface{}) (int64, error) {
	if cachelayer.IsNullID(id) {
		return 0, nil
	}
	// objectId, err := primitive.ObjectIDFromHex(cachelayer.Stringify(id, ""))
	// if err != nil {
	// 	return err
	// }
	old, _, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	var setD bson.D
	if m, ok := values.(map[string]interface{}); ok {
		for k, v := range m {
			setD = append(setD, bson.E{Key: k, Value: v})
		}
	} else {
		return 0, errors.New("RedisMongo.Update not support this type of update values, only support map[string]interface{}")
	}
	setValues := bson.D{{Key: "$set", Value: setD}}
	rs, err := s.c.UpdateOne(s.GetCtx(), bson.M{"_id": id}, setValues)

	if err != nil {
		return 0, err
	}
	newObj, _, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	err = s.ClearCache(old.GetID(), old.ListIndexes().Merge(newObj.ListIndexes()))

	return rs.MatchedCount, nil
}

func (s *RedisMongo[T, I]) GetBy(index cachelayer.Index) (T, bool, error) {
	var t T
	r := s.c.FindOne(s.GetCtx(), index)
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}

func (s *RedisMongo[T, I]) ListBy(index cachelayer.Index, orderBys cachelayer.OrderBys) ([]T, error) {
	var t []T
	var err error
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(cachelayer.Stringify(v, ""))
	// 	if err != nil {
	// 		return t, err
	// 	}
	// }
	var opts *options.FindOptions
	if len(orderBys) > 0 {
		ds := make([]bson.E, len(orderBys))
		for i, v := range orderBys {
			ds[i] = bson.E{Key: v.Field, Value: v.Asc}
		}
		opts = options.Find().SetSort(ds)
	}

	r, err := s.c.Find(s.GetCtx(), index, opts)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.GetCtx(), &t)
	return t, err
}
