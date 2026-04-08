package main

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func CheckedClose(closer io.Closer) {
	if err := closer.Close(); err != nil {
		panic(err)
	}
}

// from flow/shared/crypto.go
func DecodePKCS8PrivateKey(rawKey []byte, password *string) (*rsa.PrivateKey, error) {
	PEMBlock, _ := pem.Decode(rawKey)
	if PEMBlock == nil {
		return nil, fmt.Errorf("failed to decode private key PEM block")
	}

	var privateKey *rsa.PrivateKey
	var err error
	if password != nil {
		privateKey, err = pkcs8.ParsePKCS8PrivateKeyRSA(PEMBlock.Bytes, []byte(*password))
	} else {
		privateKey, err = pkcs8.ParsePKCS8PrivateKeyRSA(PEMBlock.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key PEM block as PKCS8: %w", err)
	}

	return privateKey, nil
}

func ParseJsonKeyVal[T any](path string) (T, error) {
	var result T
	f, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}
	defer CheckedClose(f)

	jsonContent, err := io.ReadAll(f)
	if err != nil {
		return result, fmt.Errorf("failed to read file: %w", err)
	}

	err = json.Unmarshal(jsonContent, &result)
	return result, err
}

func handleIteratorError(err error) bool {
	if err != nil {
		if err == iterator.Done {
			return true
		}
		panic(err)
	}
	return false
}

func CleanupBQ(ctx context.Context) {
	config, err := ParseJsonKeyVal[map[string]string](os.Getenv("TEST_BQ_CREDS"))
	if err != nil {
		panic(err)
	}

	config["type"] = config["auth_type"]
	delete(config, "auth_type")
	config_json, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}

	client, err := bigquery.NewClient(
		ctx,
		config["project_id"],
		option.WithCredentialsJSON(config_json),
	)
	if err != nil {
		panic(err)
	}
	defer CheckedClose(client)

	datasets := client.Datasets(ctx)
	datasetPrefix := config["dataset_id"]
	for {
		ds, err := datasets.Next()
		if handleIteratorError(err) {
			break
		}

		if strings.HasPrefix(ds.DatasetID, datasetPrefix) {
			meta, err := ds.Metadata(ctx)
			if err != nil {
				panic(err)
			}
			if meta.CreationTime.Before(time.Now().AddDate(0, 0, -1)) {
				fmt.Printf("Deleting %s\n", ds.DatasetID)
				if err := ds.DeleteWithContents(ctx); err != nil {
					panic(err)
				}
			}
		}
	}

	// now pubsub too, lack metadata to avoid deleting currently running tests
	psclient, err := pubsub.NewClient(ctx, config["project_id"], option.WithCredentialsJSON(config_json))
	if err != nil {
		panic(err)
	}
	defer CheckedClose(psclient)

	topics := psclient.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + psclient.Project()})
	for {
		topic, err := topics.Next()
		if handleIteratorError(err) {
			break
		}
		if strings.Contains(topic.Name, "/topics/e2e") {
			if err := psclient.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic.Name}); err != nil {
				panic(err)
			}
		}
	}

	subscriptions := psclient.SubscriptionAdminClient.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/" + psclient.Project()})
	for {
		subscription, err := subscriptions.Next()
		if handleIteratorError(err) {
			break
		}
		if strings.Contains(subscription.Name, "/subscriptions/e2e") {
			if err := psclient.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{
				Subscription: subscription.Name,
			}); err != nil {
				panic(err)
			}
		}
	}
}

func CleanupSF(ctx context.Context) {
	config, err := ParseJsonKeyVal[struct {
		AccountId  string  `json:"account_id"`
		Username   string  `json:"username"`
		Database   string  `json:"database"`
		Warehouse  string  `json:"warehouse"`
		Role       string  `json:"role"`
		Password   *string `json:"password"`
		PrivateKey string  `json:"private_key"`
	}](os.Getenv("TEST_SF_CREDS"))
	if err != nil {
		panic(err)
	}

	privateKey, err := DecodePKCS8PrivateKey([]byte(config.PrivateKey), config.Password)
	if err != nil {
		panic(err)
	}

	snowflakeConfig := gosnowflake.Config{
		Account:          config.AccountId,
		User:             config.Username,
		Authenticator:    gosnowflake.AuthTypeJwt,
		PrivateKey:       privateKey,
		Database:         config.Database,
		Warehouse:        config.Warehouse,
		Role:             config.Role,
		RequestTimeout:   time.Minute,
		DisableTelemetry: true,
	}

	snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		panic(err)
	}

	database, err := sql.Open("snowflake", snowflakeConfigDSN)
	if err != nil {
		panic(err)
	}
	defer CheckedClose(database)
	_, err = database.ExecContext(ctx, `DECLARE c CURSOR FOR
SELECT database_name FROM INFORMATION_SCHEMA.DATABASES
WHERE database_name ILIKE 'E2E_TEST_%' AND created < timeadd('hour', -2, CURRENT_DATE);
BEGIN
  FOR record IN c DO
	EXECUTE IMMEDIATE 'DROP DATABASE ' || record.database_name;
  END FOR;
END;`)
	if err != nil {
		panic(err)
	}
}

func Handle() {
	if r := recover(); r != nil {
		fmt.Printf("ERROR %v %s\n", r, string(debug.Stack()))
	}
}

func Run(ctx context.Context, wg *sync.WaitGroup, f func(context.Context)) {
	defer wg.Done()
	defer Handle()
	f(ctx)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go Run(ctx, &wg, CleanupBQ)
	go Run(ctx, &wg, CleanupSF)
	wg.Wait()
}
