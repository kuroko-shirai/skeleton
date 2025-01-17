package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

type Multi struct {
	mainConn *Redis
	conn     []*Redis
}

func NewMultiple(cfg []Config, metrics metrics) (*Multi, error) {

	if len(cfg) == 0 {
		return nil, fmt.Errorf("list config is empty")
	}

	rm := new(Multi)

	for _, config := range cfg {
		r, err := New(&config, metrics)
		if err != nil {
			return nil, err
		}
		r.connectionName = config.Name
		rm.conn = append(rm.conn, r)
	}
	rm.mainConn = rm.conn[0]

	return rm, nil
}

func HasError(ds []rueidis.RedisResult) error {
	for idx, result := range ds {
		if result.Error() != nil && !rueidis.IsRedisNil(result.Error()) {
			if strings.HasPrefix(result.Error().Error(), "MOVED") {
				slog.Error("Ошибка записи в редис", "error", result.Error(), "idx", idx)
				continue
			}
			return result.Error()
		}
	}
	return nil
}

func (r *Multi) DoMulti(ctx context.Context, multi rueidis.Commands) (result []rueidis.RedisResult) {
	return r.mainConn.DoMulti(ctx, multi...)
}

func (r *Multi) HDelCompleted(ctx context.Context, key string, fields ...string) rueidis.Completed {
	return r.mainConn.conn.B().Hdel().Key(key).Field(fields...).Build()
}

func (r *Multi) HDel(ctx context.Context, key string, fields ...string) (result int64, err error) {
	for _, conn := range r.conn {
		var cnt int64
		if cnt, err = conn.HDel(ctx, key, fields...); err != nil {
			return 0, err
		}
		result += cnt
	}

	return result, nil
}

func (r *Multi) DoMultiExec(ctx context.Context, multi rueidis.Commands) error {
	for idx := len(r.conn) - 1; idx >= 0; idx-- {
		conn := r.conn[idx]
		var result []rueidis.RedisResult
		if idx == 0 {
			result = conn.DoMulti(ctx, multi...)
		} else {
			cmd := make([]rueidis.Completed, 0, len(multi))
			for _, commands := range multi {
				tmp := commands.Commands()
				cmd = append(cmd, conn.conn.B().Arbitrary(tmp[:2]...).Args(tmp[2:]...).Build())
			}
			result = conn.DoMulti(ctx, cmd...)
		}
		if err := HasError(result); err != nil {
			return err
		}
	}
	return nil
}

func (r *Multi) HGetAll(ctx context.Context, key string) (result map[string]rueidis.RedisMessage, err error) {
	for _, conn := range r.conn {
		if result, err = conn.HGetAll(ctx, key); err == nil {
			return result, nil
		}
	}
	return nil, err
}

func (r *Multi) HGetCompleted(key, field string) rueidis.Completed {
	return r.mainConn.conn.B().Hget().Key(key).Field(field).Build()
}

func (r *Multi) HMSet(ctx context.Context, key string, kvs map[string]string) (err error) {
	for _, conn := range r.conn {
		if err = conn.HMSet(ctx, key, kvs); err != nil {
			return err
		}
	}
	return nil
}

func (r *Multi) HMSetComplete(key string, kvs map[string]string) (result rueidis.Completed) {
	kvObj := r.mainConn.conn.B().Hmset().Key(key).FieldValue()
	for k, v := range kvs {
		kvObj.FieldValue(k, v)
	}
	result = kvObj.Build()
	return result
}

func (r *Multi) ExpireAtCompleted(key string, at time.Time) rueidis.Completed {
	return r.mainConn.conn.B().Expireat().Key(key).Timestamp(at.Unix()).Build()
}

func (r *Multi) HMGet(ctx context.Context, key string, fields ...string) (result []rueidis.RedisMessage, err error) {
	for _, conn := range r.conn {
		if result, err = conn.HMGet(ctx, key, fields...); err == nil {
			return result, err
		}
	}
	return nil, err
}

func (r *Multi) DelMulti(ctx context.Context, keys ...string) (result int64, err error) {
	for _, conn := range r.conn {
		var cnt int64
		if cnt, err = conn.DelMulti(ctx, keys...); err != nil {
			return 0, err
		}
		result += cnt
	}
	return result, nil
}

func (r *Multi) SAdd(ctx context.Context, key string, members ...string) (result int64, err error) {
	for _, conn := range r.conn {
		if result, err = conn.SAdd(ctx, key, members...); err != nil {
			return 0, err
		}
	}
	return result, nil
}

func (r *Multi) SAddCompleted(key string, members ...string) rueidis.Completed {
	return r.mainConn.conn.B().Sadd().Key(key).Member(members...).Build()
}

func (r *Multi) SRem(ctx context.Context, key string, members ...string) (result int64, err error) {
	for _, conn := range r.conn {
		if result, err = conn.SRem(ctx, key, members...); err != nil {
			return 0, err
		}
	}
	return result, nil
}

func (r *Multi) SMembers(ctx context.Context, key string) (result []rueidis.RedisMessage, err error) {
	for _, conn := range r.conn {
		if result, err = conn.SMembers(ctx, key); err == nil {
			return result, err
		}
	}
	return nil, err
}

func (r *Multi) Scan(ctx context.Context, cursor uint64, match string, count int64) (uint64, []string, error) {
	cursor, elements, err := r.mainConn.Scan(ctx, cursor, match, count)
	if err != nil {
		for i := 1; i < len(r.conn); i++ {
			cursor, elements, err = r.conn[i].Scan(ctx, cursor, match, count)
			if err == nil {
				break
			}
		}

	}
	return cursor, elements, err
}

func (r *Multi) ScanAllKeys(ctx context.Context, match string, count int64) (result map[string]struct{}, err error) {
	result, err = r.mainConn.ScanAllKeys(ctx, match, count)
	if err != nil {
		for i := 1; i < len(r.conn); i++ {
			result, err = r.conn[i].ScanAllKeys(ctx, match, count)
			if err == nil {
				break
			}
		}
	}
	return result, err
}

func (r *Multi) ScanAllFields(ctx context.Context, key string, fieldMatch string, count int64) (result []string, err error) {
	result, err = r.mainConn.ScanAllFields(ctx, key, fieldMatch, count)
	if err != nil {
		for i := 1; i < len(r.conn); i++ {
			result, err = r.conn[i].ScanAllFields(ctx, key, fieldMatch, count)
			if err == nil {
				break
			}
		}
	}
	return result, err
}

func (r *Multi) HSet(ctx context.Context, key, field, value string) (result int64, err error) {
	for _, conn := range r.conn {
		var cnt int64
		if cnt, err = conn.HSet(ctx, key, field, value); err != nil {
			return 0, err
		}
		result += cnt
	}

	return result, nil
}

func (r *Multi) Del(ctx context.Context, key string) (result int64, err error) {
	for _, conn := range r.conn {
		var cnt int64
		if cnt, err = conn.Del(ctx, key); err != nil {
			return 0, err
		}
		result += cnt
	}

	return result, nil
}

func (r *Multi) DelComplete(key string) rueidis.Completed {
	return r.mainConn.DelCompleted(key)
}

func (r *Multi) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	for _, conn := range r.conn {
		if err := conn.Set(ctx, key, value, ttl); err != nil {
			return err
		}
	}

	return nil
}

func (r *Multi) SetCompleted(ctx context.Context, key, value string, ttl time.Duration) rueidis.Completed {
	return r.mainConn.SetCompleted(ctx, key, value, ttl)
}

func (r *Multi) Get(ctx context.Context, key string) (result string, err error) {
	for _, conn := range r.conn {
		result, err = conn.Get(ctx, key)
		if err == nil {
			return result, nil
		}
	}

	return "", err
}

func (r *Multi) Expire(ctx context.Context, key string, ttl time.Duration) (err error) {
	for _, conn := range r.conn {
		if err = conn.Expire(ctx, key, ttl); err != nil {
			return err
		}
	}

	return nil
}
