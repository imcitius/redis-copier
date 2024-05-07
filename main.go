package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	srcAddr = flag.String("src", "localhost:6379", "source Redis server address")
	dstAddr = flag.String("dst", "localhost:6380", "destination Redis server address")
)

func main() {
	flag.Parse()

	srcClient := redis.NewClient(&redis.Options{
		Addr:         *srcAddr,
		DB:           0,
		PoolSize:     50,               // Set pool size to 50 connections
		MinIdleConns: 10,               // Maintain at least 10 idle connections
		MaxConnAge:   0,                // No age limit, depends on the use case
		PoolTimeout:  30 * time.Second, // Wait up to 30 seconds for a connection
		IdleTimeout:  5 * time.Minute,  // Close idle connections after 5 minutes
	})

	destClient := redis.NewClient(&redis.Options{
		Addr:         *dstAddr,
		DB:           0,
		PoolSize:     50,
		MinIdleConns: 10,
		MaxConnAge:   0,
		PoolTimeout:  30 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})

	ctx := context.Background()
	copyKeys(ctx, srcClient, destClient)
}

func copyKeys(ctx context.Context, srcClient, destClient *redis.Client) {
	var cursor uint64
	const batchSize = 1000
	var wg sync.WaitGroup
	var grCount int

	for {
		keys, newCursor, err := srcClient.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			log.Printf("Error scanning keys: %v", err)
			break
		}

		wg.Add(1)
		grCount++
		go func(keys []string) {
			defer wg.Done()
			copyBatch(ctx, keys, srcClient, destClient)
		}(keys)

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	wg.Wait()
	fmt.Println("Copying complete.")
}

func copyBatch(ctx context.Context, keys []string, srcClient, destClient *redis.Client) {
	pipe := destClient.Pipeline()
	processedKeys := 0
	successfulOps := 0
	errorsByType := make(map[string]int)

	for _, key := range keys {
		keyType, err := srcClient.Type(ctx, key).Result()
		if err != nil {
			log.Printf("Error checking type of key %s: %v", key, err)
			errorsByType["type_check"]++
			continue
		}
		processedKeys++

		switch keyType {
		case "string":
			val, err := srcClient.Get(ctx, key).Result()
			if err != nil {
				log.Printf("Error getting string key %s: %v", key, err)
				errorsByType["string"]++
				continue
			}
			pipe.Set(ctx, key, val, 0)
			successfulOps++
		case "list":
			vals, err := srcClient.LRange(ctx, key, 0, -1).Result()
			if err != nil {
				log.Printf("Error getting list key %s: %v", key, err)
				errorsByType["list"]++
				continue
			}
			for _, val := range vals {
				pipe.RPush(ctx, key, val)
			}
			successfulOps++
		case "hash":
			hashValues, err := srcClient.HGetAll(ctx, key).Result()
			if err != nil {
				log.Printf("Error getting hash key %s: %v", key, err)
				errorsByType["hash"]++
				continue
			}
			for field, value := range hashValues {
				pipe.HSet(ctx, key, field, value)
			}
			successfulOps++
		case "zset":
			zVals, err := srcClient.ZRangeWithScores(ctx, key, 0, -1).Result()
			if err != nil {
				log.Printf("Error getting zset key %s: %v", key, err)
				errorsByType["zset"]++
				continue
			}
			for _, zVal := range zVals {
				pipe.ZAdd(ctx, key, &redis.Z{
					Score:  zVal.Score,
					Member: zVal.Member,
				})
			}
			successfulOps++
        case "stream":
            entries, err := srcClient.XRange(ctx, key, "-", "+").Result()
            if err != nil {
                log.Printf("Error reading stream key %s: %v", key, err)
                errorsByType["stream"]++
                continue
            }
            for _, entry := range entries {
                // Create a map of values for XAdd
                values := make(map[string]interface{})
                for k, v := range entry.Values {
                    values[k] = v
                }
                _, err := destClient.XAdd(ctx, &redis.XAddArgs{
                    Stream: key,
                    ID:     "*", // Let Redis generate the ID
                    Values: values,
                }).Result()
                if err != nil {
                    log.Printf("Error writing to stream key %s: %v", key, err)
                } else {
                    successfulOps++
                }
            }

		default:
			log.Printf("Unsupported key type %s for key %s", keyType, key)
			errorsByType["unsupported"]++
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error executing pipeline: %v", err)
		errorsByType["pipeline"]++
	}
	pipe.Close()

	// Log the statistics after processing the batch
	log.Printf("Batch completed. Processed Keys: %d, Successful Operations: %d, Errors: %v", processedKeys, successfulOps, errorsByType)
}
