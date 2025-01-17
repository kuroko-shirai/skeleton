package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"skeleton/pkg/hostname"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

type metrics interface {
	WriteTimingAndCounter(startTime time.Time, query string, success bool)
}

type Config struct {
	Name                 string        `env:"NAME" yaml:"name"`
	Hosts                []string      `env:"REDIS_HOST" yaml:"host"`
	Username             string        `env:"REDIS_USERNAME" yaml:"username"`
	Password             string        `env:"REDIS_PASSWORD" yaml:"password"`
	ClientName           string        `env:"REDIS_APP" yaml:"app"`
	TTL                  time.Duration `env:"REDIS_TTL" yaml:"ttl"`
	AlwaysPipelining     bool          `env:"REDIS_ALWAYS_PIPELINING" yaml:"always_pipelining" default:"true" env-default:"true"`
	PipelineMultiplex    int           `env:"REDIS_PIPELINE_MULTIPLEX" yaml:"pipeline_multiplex" default:"1" env-default:"1"`
	RedisSentinelPrimary string        `env:"REDIS_SENTINEL_PRIMARY" yaml:"sentinel_primary" default:"" env-default:""`
	DisableCache         bool          `env:"REDIS_DISABLE_CACHE" yaml:"disable_cache"`
	ReplicaOnly          bool          `env:"REDIS_REPLICA_ONLY" yaml:"replica_only" default:"false" env-default:"false"`
	DialTimeout          time.Duration `env:"REDIS_DIAL_TIMEOUT" yaml:"dial_timeout" default:"10s" env-default:"10s"`
	ReadTimeout          time.Duration `env:"REDIS_READ_TIMEOUT" yaml:"read_timeout" default:"1s" env-default:"1s"`
	DisableRetry         bool          `env:"REDIS_DISABLE_RETRY" yaml:"disable_retry" default:"false" env-default:"false"`
	ForceSingleClient    bool          `env:"REDIS_FORCE_SINGLE_CLIENT" yaml:"force_single_client" default:"false" env-default:"false"`
	MaxFlushDelay        time.Duration `env:"REDIS_MAX_FLASH_DELAY" yaml:"max_flush_delay" env-default:"10ms"`
}

type Redis struct {
	connectionName string
	conn           rueidis.Client
	ttl            time.Duration
	metrics        metrics
}

func New(cfg *Config, metrics metrics) (*Redis, error) {
	var conn rueidis.Client
	var err error

	if cfg.ClientName == "" {
		cfg.ClientName = hostname.GetHostName()
	}

	if cfg.RedisSentinelPrimary != "" {
		conn, err = rueidis.NewClient(rueidis.ClientOption{
			Password:          cfg.Password,
			ClientName:        cfg.ClientName,
			AlwaysPipelining:  cfg.AlwaysPipelining,
			PipelineMultiplex: cfg.PipelineMultiplex,
			InitAddress:       cfg.Hosts,
			DisableCache:      cfg.DisableCache,
			DisableRetry:      cfg.DisableRetry,
			ReplicaOnly:       cfg.ReplicaOnly,
			MaxFlushDelay:     cfg.MaxFlushDelay,
			Dialer: net.Dialer{
				Timeout: cfg.DialTimeout,
			},
			Sentinel: rueidis.SentinelOption{
				MasterSet:  cfg.RedisSentinelPrimary,
				Password:   cfg.Password,
				ClientName: cfg.ClientName,
			},
		})
	} else {
		conn, err = rueidis.NewClient(rueidis.ClientOption{
			Username:          cfg.Username,
			Password:          cfg.Password,
			ClientName:        cfg.ClientName,
			InitAddress:       cfg.Hosts,
			AlwaysPipelining:  cfg.AlwaysPipelining,
			PipelineMultiplex: cfg.PipelineMultiplex,
			DisableCache:      cfg.DisableCache,
			ReplicaOnly:       cfg.ReplicaOnly,
			ForceSingleClient: cfg.ForceSingleClient,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("Ошибка при инициализации клиента rueidis %w", err)
	}

	return &Redis{
		conn:    conn,
		metrics: metrics,
		ttl:     cfg.TTL,
	}, nil
}

func (r *Redis) writeTimingAndCounter(startTime time.Time, query string, success bool) {
	if r.metrics != nil {
		r.metrics.WriteTimingAndCounter(startTime, query, success)
	}
}

func (r *Redis) Exists(ctx context.Context, key ...string) (bool, error) {
	start := time.Now()
	asBool, err := r.conn.Do(ctx, r.conn.B().Exists().Key(key...).Build()).AsBool()
	r.writeTimingAndCounter(start, "redis_exist", err == nil)
	return asBool, err
}

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	start := time.Now()
	value, err := r.conn.Do(ctx, r.conn.B().Get().Key(key).Build()).ToString()
	r.writeTimingAndCounter(start, "redis_get", err == nil)

	return value, err
}

func (r *Redis) GetMulti(ctx context.Context, keys ...string) ([]rueidis.RedisMessage, error) {
	start := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Mget().Key(keys...).Build()).ToArray()
	r.writeTimingAndCounter(start, "redis_get_multi", err == nil)

	return result, err
}

func (r *Redis) SetCompleted(ctx context.Context, key, value string, ttl time.Duration) rueidis.Completed {
	if ttl > 0 {
		return r.conn.B().Set().Key(key).Value(value).Ex(ttl).Build()
	} else {
		return r.conn.B().Set().Key(key).Value(value).Build()
	}
}

func (r *Redis) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	start := time.Now()
	b := r.conn.B().Set().Key(key).Value(value)
	if ttl > 0 {
		b.Ex(ttl)
	}
	err := r.conn.Do(ctx, b.Build()).Error()
	r.writeTimingAndCounter(start, "redis_set", err == nil)

	return err
}

func (r *Redis) Del(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	cnt, err := r.conn.Do(ctx, r.conn.B().Del().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_del", err == nil)

	return cnt, err
}

func (r *Redis) DelCompleted(key string) rueidis.Completed {
	return r.conn.B().Del().Key(key).Build()
}

func (r *Redis) DelMulti(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	cnt, err := r.conn.Do(ctx, r.conn.B().Del().Key(keys...).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_del_multi", err == nil)

	return cnt, err
}

func (r *Redis) Expire(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	_, err := r.conn.Do(ctx, r.conn.B().Expire().Key(key).Seconds(int64(ttl/time.Second)).Build()).ToAny()
	r.writeTimingAndCounter(start, "redis_expire", err == nil)

	return err
}

func (r *Redis) ExpireAt(ctx context.Context, key string, at time.Time) error {
	start := time.Now()
	err := r.conn.Do(ctx, r.conn.B().Expireat().Key(key).Timestamp(at.Unix()).Build()).Error()
	r.writeTimingAndCounter(start, "redis_expire_at", err == nil)

	return err
}

func (r *Redis) ExpireAtCompleted(key string, at time.Time) rueidis.Completed {
	return r.conn.B().Expireat().Key(key).Timestamp(at.Unix()).Build()
}

func (r *Redis) TTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	value, err := r.conn.Do(ctx, r.conn.B().Ttl().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_ttl", err == nil)

	return value, err
}

func (r *Redis) PTTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	value, err := r.conn.Do(ctx, r.conn.B().Pttl().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_pttl", err == nil)

	return value, err
}

func (r *Redis) Incr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	value, err := r.conn.Do(ctx, r.conn.B().Incr().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_incr", err == nil)

	return value, err
}

func (r *Redis) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	start := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Incrby().Key(key).Increment(value).Build()).ToInt64()
	r.writeTimingAndCounter(start, "redis_incr_by", err == nil)

	return result, err
}

func (r *Redis) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	startTime := time.Now()
	value, err := r.conn.Do(ctx, r.conn.B().Getrange().Key(key).Start(start).End(end).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_get_range", err == nil)

	return value, err
}

func (r *Redis) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Setrange().Key(key).Offset(offset).Value(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_set_range", err == nil)

	return result, err
}

func (r *Redis) StrLen(ctx context.Context, key string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Strlen().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_strlen", err == nil)

	return result, err
}

func (r *Redis) MGet(ctx context.Context, keys ...string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Mget().Key(keys...).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_mget", err == nil)

	return result, err
}

func (r *Redis) MSet(ctx context.Context, kvs map[string]string) error {
	startTime := time.Now()
	kvObj := r.conn.B().Mset().KeyValue()
	for k, v := range kvs {
		kvObj.KeyValue(k, v)
	}
	_, err := r.conn.Do(ctx, kvObj.Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_mset", err == nil)

	return err
}

func (r *Redis) HGetCompleted(key, field string) rueidis.Completed {
	return r.conn.B().Hget().Key(key).Field(field).Build()
}

func (r *Redis) HGet(ctx context.Context, key, field string) (string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hget().Key(key).Field(field).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_hget", err == nil)

	return result, err
}

func (r *Redis) HSet(ctx context.Context, key, field, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_hset", err == nil)

	return result, err
}

func (r *Redis) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hdel().Key(key).Field(fields...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_hdel", err == nil)

	return result, err
}

func (r *Redis) HDelCompleted(ctx context.Context, key string, fields ...string) rueidis.Completed {
	return r.conn.B().Hdel().Key(key).Field(fields...).Build()
}

func (r *Redis) HGetAll(ctx context.Context, key string) (map[string]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hgetall().Key(key).Build()).ToMap()
	r.writeTimingAndCounter(startTime, "redis_hgetall", err == nil)

	return result, err
}

func (r *Redis) HIncrBy(ctx context.Context, key, field string, value int64) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hincrby().Key(key).Field(field).Increment(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_hincrby", err == nil)

	return result, err
}

func (r *Redis) HKeys(ctx context.Context, key string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hkeys().Key(key).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_hkeys", err == nil)

	return result, err
}

func (r *Redis) HLen(ctx context.Context, key string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hlen().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_hlen", err == nil)

	return result, err
}

func (r *Redis) HMGet(ctx context.Context, key string, fields ...string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hmget().Key(key).Field(fields...).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_hmget", err == nil)

	return result, err
}

func (r *Redis) HMSet(ctx context.Context, key string, kvs map[string]string) error {
	startTime := time.Now()
	kvObj := r.conn.B().Hmset().Key(key).FieldValue()
	for k, v := range kvs {
		kvObj.FieldValue(k, v)
	}
	err := r.conn.Do(ctx, kvObj.Build()).Error()
	r.writeTimingAndCounter(startTime, "redis_hmset", err == nil)

	return err
}

func (r *Redis) HMSetComplete(key string, kvs map[string]string) rueidis.Completed {
	kvObj := r.conn.B().Hmset().Key(key).FieldValue()
	for k, v := range kvs {
		kvObj.FieldValue(k, v)
	}
	return kvObj.Build()
}

func (r *Redis) HSetNX(ctx context.Context, key, field, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hsetnx().Key(key).Field(field).Value(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_hsetnx", err == nil)

	return result, err
}

func (r *Redis) HVals(ctx context.Context, key string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hvals().Key(key).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_hvals", err == nil)

	return result, err
}

func (r *Redis) LIndex(ctx context.Context, key string, index int64) (string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lindex().Key(key).Index(index).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_lindex", err == nil)

	return result, err
}

func (r *Redis) LInsert(ctx context.Context, key, pivot, value string, before bool) (int64, error) {
	startTime := time.Now()
	var result int64
	var err error
	if before {
		result, err = r.conn.Do(ctx, r.conn.B().Linsert().Key(key).Before().Pivot(pivot).Element(value).Build()).ToInt64()
	} else {
		result, err = r.conn.Do(ctx, r.conn.B().Linsert().Key(key).After().Pivot(pivot).Element(value).Build()).ToInt64()
	}
	r.writeTimingAndCounter(startTime, "redis_linsert", err == nil)

	return result, err
}

func (r *Redis) LLen(ctx context.Context, key string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Llen().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_llen", err == nil)

	return result, err
}

func (r *Redis) LPop(ctx context.Context, key string) (string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lpop().Key(key).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_lpop", err == nil)

	return result, err
}

func (r *Redis) LPush(ctx context.Context, key string, values ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lpush().Key(key).Element(values...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_lpush", err == nil)

	return result, err
}

func (r *Redis) LPushX(ctx context.Context, key, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lpushx().Key(key).Element(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_lpushx", err == nil)

	return result, err
}

func (r *Redis) LRange(ctx context.Context, key string, start, stop int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lrange().Key(key).Start(start).Stop(stop).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_lrange", err == nil)

	return result, err
}

func (r *Redis) LRem(ctx context.Context, key string, count int64, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Lrem().Key(key).Count(count).Element(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_lrem", err == nil)

	return result, err
}

func (r *Redis) LSet(ctx context.Context, key string, index int64, value string) error {
	startTime := time.Now()
	err := r.conn.Do(ctx, r.conn.B().Lset().Key(key).Index(index).Element(value).Build()).Error()
	r.writeTimingAndCounter(startTime, "redis_lset", err == nil)

	return err
}

func (r *Redis) LTrim(ctx context.Context, key string, start, stop int64) error {
	startTime := time.Now()
	err := r.conn.Do(ctx, r.conn.B().Ltrim().Key(key).Start(start).Stop(stop).Build()).Error()
	r.writeTimingAndCounter(startTime, "redis_ltrim", err == nil)

	return err
}

func (r *Redis) RPop(ctx context.Context, key string) (string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Rpop().Key(key).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_rpop", err == nil)

	return result, err
}

func (r *Redis) RPush(ctx context.Context, key string, values ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Rpush().Key(key).Element(values...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_rpush", err == nil)

	return result, err
}

func (r *Redis) RPushX(ctx context.Context, key, value string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Rpushx().Key(key).Element(value).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_rpushx", err == nil)

	return result, err
}

func (r *Redis) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sadd().Key(key).Member(members...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_sadd", err == nil)

	return result, err
}

func (r *Redis) SAddCompleted(key string, members ...string) rueidis.Completed {
	return r.conn.B().Sadd().Key(key).Member(members...).Build()
}

func (r *Redis) SCard(ctx context.Context, key string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Scard().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_scard", err == nil)

	return result, err
}

func (r *Redis) SDiff(ctx context.Context, keys ...string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sdiff().Key(keys...).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_sdiff", err == nil)

	return result, err
}

func (r *Redis) SInter(ctx context.Context, keys ...string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sinter().Key(keys...).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_sinter", err == nil)

	return result, err
}

func (r *Redis) SIsMember(ctx context.Context, key, member string) (bool, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sismember().Key(key).Member(member).Build()).ToBool()
	r.writeTimingAndCounter(startTime, "redis_sismember", err == nil)

	return result, err
}

func (r *Redis) SMembers(ctx context.Context, key string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Smembers().Key(key).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_smembers", err == nil)

	return result, err
}

func (r *Redis) SMembersCompleted(key string) rueidis.Completed {
	result := r.conn.B().Smembers().Key(key).Build()
	return result
}

func (r *Redis) SMove(ctx context.Context, source, destination, member string) (bool, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Smove().Source(source).Destination(destination).Member(member).Build()).ToBool()
	r.writeTimingAndCounter(startTime, "redis_smove", err == nil)

	return result, err
}

func (r *Redis) SPop(ctx context.Context, key string) (string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Spop().Key(key).Build()).ToString()
	r.writeTimingAndCounter(startTime, "redis_spop", err == nil)

	return result, err
}

func (r *Redis) SRandMember(ctx context.Context, key string, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Srandmember().Key(key).Count(count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_srandmember", err == nil)

	return result, err
}

func (r *Redis) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Srem().Key(key).Member(members...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_srem", err == nil)

	return result, err
}

func (r *Redis) SUnion(ctx context.Context, keys ...string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sunion().Key(keys...).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_sunion", err == nil)

	return result, err
}

func (r *Redis) ZAddXX(ctx context.Context, key string, score float64, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zadd().Key(key).Xx().ScoreMember().ScoreMember(score, member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zadd_xx", err == nil)

	return result, err
}

func (r *Redis) ZAddNX(ctx context.Context, key string, score float64, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zadd().Key(key).Nx().ScoreMember().ScoreMember(score, member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zadd_nx", err == nil)

	return result, err
}

func (r *Redis) ZAddCh(ctx context.Context, key string, score float64, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zadd().Key(key).Ch().ScoreMember().ScoreMember(score, member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zadd_ch", err == nil)

	return result, err
}

func (r *Redis) ZAdd(ctx context.Context, key string, score float64, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zadd().Key(key).ScoreMember().ScoreMember(score, member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zadd", err == nil)

	return result, err
}

func (r *Redis) ZCard(ctx context.Context, key string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zcard().Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zcard", err == nil)

	return result, err
}

func (r *Redis) ZCount(ctx context.Context, key, min, max string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zcount().Key(key).Min(min).Max(max).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zcount", err == nil)

	return result, err
}

func (r *Redis) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zincrby().Key(key).Increment(increment).Member(member).Build()).ToFloat64()
	r.writeTimingAndCounter(startTime, "redis_zincrby", err == nil)

	return result, err
}

func (r *Redis) ZInterStore(ctx context.Context, destination, key string, numkeys int64) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zinterstore().Destination(destination).Numkeys(numkeys).Key(key).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zinterstore", err == nil)

	return result, err
}

func (r *Redis) ZLexCount(ctx context.Context, key, min, max string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zlexcount().Key(key).Min(min).Max(max).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zlexcount", err == nil)

	return result, err
}

func (r *Redis) ZPopMax(ctx context.Context, key string, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zpopmax().Key(key).Count(count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zpopmax", err == nil)

	return result, err
}

func (r *Redis) ZPopMin(ctx context.Context, key string, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zpopmin().Key(key).Count(count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zpopmin", err == nil)

	return result, err
}

func (r *Redis) ZRange(ctx context.Context, key, start, stop string) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrange().Key(key).Min(start).Max(stop).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrange", err == nil)

	return result, err
}

func (r *Redis) ZRangeByLex(ctx context.Context, key, min, max string, offset, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrangebylex().Key(key).Min(min).Max(max).Limit(offset, count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrange_by_lex", err == nil)

	return result, err
}

func (r *Redis) ZRangeByScore(ctx context.Context, key, min, max string, offset, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrangebyscore().Key(key).Min(min).Max(max).Limit(offset, count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrange_by_score", err == nil)

	return result, err
}

func (r *Redis) ZRank(ctx context.Context, key, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrank().Key(key).Member(member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrank", err == nil)

	return result, err
}

func (r *Redis) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrem().Key(key).Member(members...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrem", err == nil)

	return result, err
}

func (r *Redis) ZRemRangeByLex(ctx context.Context, key, min, max string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zremrangebylex().Key(key).Min(min).Max(max).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrem_range_by_lex", err == nil)

	return result, err
}

func (r *Redis) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zremrangebyrank().Key(key).Start(start).Stop(stop).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrem_range_by_rank", err == nil)

	return result, err
}

func (r *Redis) ZRemRangeByScore(ctx context.Context, key, min, max string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zremrangebyscore().Key(key).Min(min).Max(max).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrem_range_by_score", err == nil)

	return result, err
}

func (r *Redis) ZRevRange(ctx context.Context, key string, start, stop int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrevrange().Key(key).Start(start).Stop(stop).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrevrange", err == nil)

	return result, err
}

func (r *Redis) ZRevRangeByLex(ctx context.Context, key, min, max string, offset, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrevrangebylex().Key(key).Max(max).Min(min).Limit(offset, count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrevrange_by_lex", err == nil)

	return result, err
}

func (r *Redis) ZRevRangeByScore(ctx context.Context, key, min, max string, offset, count int64) ([]rueidis.RedisMessage, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrevrangebyscore().Key(key).Max(max).Min(min).Limit(offset, count).Build()).ToArray()
	r.writeTimingAndCounter(startTime, "redis_zrevrange_by_score", err == nil)

	return result, err
}

func (r *Redis) ZRevRank(ctx context.Context, key, member string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zrevrank().Key(key).Member(member).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zrevrank", err == nil)

	return result, err
}

func (r *Redis) ZScore(ctx context.Context, key, member string) (float64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zscore().Key(key).Member(member).Build()).ToFloat64()
	r.writeTimingAndCounter(startTime, "redis_zscore", err == nil)

	return result, err
}

func (r *Redis) ZUnionStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zunionstore().Destination(destination).Numkeys(int64(len(keys))).Key(keys...).Build()).ToInt64()
	r.writeTimingAndCounter(startTime, "redis_zunionstore", err == nil)

	return result, err
}

func (r *Redis) Close() {
	r.conn.Close()
}

func (r *Redis) Keys(ctx context.Context, pattern string) ([]string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	r.metrics.WriteTimingAndCounter(startTime, "redis_keys", err == nil)

	return result, err
}

func (r *Redis) Scan(ctx context.Context, cursor uint64, match string, count int64) (uint64, []string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Scan().Cursor(cursor).Match(match).Count(count).Build()).AsScanEntry()
	r.writeTimingAndCounter(startTime, "redis_scan", err == nil)

	return result.Cursor, result.Elements, err
}

func (r *Redis) ScanAllKeys(ctx context.Context, match string, count int64) (map[string]struct{}, error) {
	startTime := time.Now()
	sumElements := make(map[string]struct{})
	var errAll error
	wg := sync.WaitGroup{}
	wg.Add(len(r.conn.Nodes()))
	mu := sync.Mutex{}
	for _, node := range r.conn.Nodes() {
		go func(node rueidis.Client, wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			var cursor uint64 = 0
			for {
				result, err := node.Do(ctx, r.conn.B().Scan().Cursor(cursor).Match(match).Count(count).Build()).AsScanEntry()
				if err != nil {
					errAll = errors.Join(errAll, err)
					break
				}
				cursor = result.Cursor
				mu.Lock()
				for _, item := range result.Elements {
					sumElements[item] = struct{}{}
				}
				mu.Unlock()
				if cursor == 0 {
					break
				}
			}
		}(node, &wg, &mu)
	}
	wg.Wait()
	r.writeTimingAndCounter(startTime, "redis_scan_all", errAll == nil)
	return sumElements, errAll
}

func (r *Redis) ScanAllFields(ctx context.Context, key string, fieldMatch string, count int64) ([]string, error) {
	startTime := time.Now()
	sumElements := make([]string, 0, count)
	var errAll error
	var cursor uint64 = 0
	for {
		result, err := r.conn.Do(ctx, r.conn.B().Hscan().Key(key).Cursor(cursor).Match(fieldMatch).Count(count).Build()).AsScanEntry()
		if err != nil {
			errAll = errors.Join(errAll, err)
			break
		}
		cursor = result.Cursor
		sumElements = append(sumElements, result.Elements...)
		if cursor == 0 {
			break
		}
	}
	r.writeTimingAndCounter(startTime, "redis_scan_all_fields", errAll == nil)
	return sumElements, errAll
}

func (r *Redis) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) (uint64, []string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Sscan().Key(key).Cursor(cursor).Match(match).Count(count).Build()).AsScanEntry()
	r.writeTimingAndCounter(startTime, "redis_sscan", err == nil)

	return result.Cursor, result.Elements, err
}

func (r *Redis) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) (uint64, []string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Hscan().Key(key).Cursor(cursor).Match(match).Count(count).Build()).AsScanEntry()
	r.writeTimingAndCounter(startTime, "redis_hscan", err == nil)

	return result.Cursor, result.Elements, err
}

func (r *Redis) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) (uint64, []string, error) {
	startTime := time.Now()
	result, err := r.conn.Do(ctx, r.conn.B().Zscan().Key(key).Cursor(cursor).Match(match).Count(count).Build()).AsScanEntry()
	r.writeTimingAndCounter(startTime, "redis_zscan", err == nil)

	return result.Cursor, result.Elements, err
}

func (r *Redis) GetClient() rueidis.Client {
	return r.conn
}

func (r *Redis) CTHmget(key string, fields ...string) rueidis.CacheableTTL {
	return rueidis.CT(r.conn.B().Hmget().Key(key).Field(fields...).Cache(), r.ttl)
}

func (r *Redis) CTGet(key string) rueidis.CacheableTTL {
	return rueidis.CT(r.conn.B().Get().Key(key).Cache(), r.ttl)
}

func (r *Redis) DoMultiCache(ctx context.Context, commands ...rueidis.CacheableTTL) []rueidis.RedisResult {
	startTime := time.Now()
	result := r.conn.DoMultiCache(ctx, commands...)
	r.writeTimingAndCounter(startTime, "redis_domulticache", true)
	return result
}

func (r *Redis) DoCache(ctx context.Context, cmd rueidis.CacheableTTL) rueidis.RedisResult {
	startTime := time.Now()
	result := r.conn.DoCache(ctx, cmd.Cmd, cmd.TTL)
	r.writeTimingAndCounter(startTime, "redis_docache", true)
	return result
}

func (r *Redis) GetCompleted(key string) rueidis.Completed {
	return r.conn.B().Get().Key(key).Build()
}

func (r *Redis) HMGetCompleted(ctx context.Context, key string, fields ...string) rueidis.Completed {
	result := r.conn.B().Hmget().Key(key).Field(fields...).Build()
	return result
}

func (r *Redis) DoMulti(ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	resp := r.conn.DoMulti(ctx, multi...)
	return resp
}

func (r *Redis) ScanEntryFields(ctx context.Context, key string, fieldMatch string, cursor uint64, count int64) (*rueidis.ScanEntry, error) {
	result, err := r.conn.Do(ctx, r.conn.B().Hscan().Key(key).Cursor(cursor).Match(fieldMatch).Count(count).Build()).AsScanEntry()
	if err != nil {
		return &rueidis.ScanEntry{}, err
	}
	return &result, nil
}

func (r *Redis) HGetAllCompleted(ctx context.Context, key string) rueidis.Completed {
	return r.conn.B().Hgetall().Key(key).Build()
}

func (r *Redis) DoMultiExec(ctx context.Context, multi rueidis.Commands) error {
	result := r.DoMulti(ctx, multi...)
	if err := HasError(result); err != nil {
		return err
	}
	return nil
}
