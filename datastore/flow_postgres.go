package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/OpenSlides/openslides-go/datastore/dskey"
	"github.com/OpenSlides/openslides-go/environment"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:generate  sh -c "go run genfields/main.go > field_def.go"

var (
	envPostgresHost         = environment.NewVariable("DATABASE_HOST", "localhost", "Postgres Host.")
	envPostgresPort         = environment.NewVariable("DATABASE_PORT", "5432", "Postgres Post.")
	envPostgresDatabase     = environment.NewVariable("DATABASE_NAME", "openslides", "Postgres User.")
	envPostgresUser         = environment.NewVariable("DATABASE_USER", "openslides", "Postgres Database.")
	envPostgresPasswordFile = environment.NewVariable("DATABASE_PASSWORD_FILE", "/run/secrets/postgres_password", "Postgres Password.")
)

// FlowPostgres uses postgres to get the connections.
type FlowPostgres struct {
	pool *pgxpool.Pool

	fields map[string][]string // TODO: Generate this list at compiletime.
}

// encodePostgresConfig encodes a string to be used in the postgres key value style.
//
// See: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
func encodePostgresConfig(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

// NewFlowPostgres initializes a SourcePostgres.
//
// TODO: This should be unexported, but there is an import cycle in the tests.
func NewFlowPostgres(lookup environment.Environmenter) (*FlowPostgres, error) {
	password, err := environment.ReadSecret(lookup, envPostgresPasswordFile)
	if err != nil {
		return nil, fmt.Errorf("reading postgres password: %w", err)
	}

	addr := fmt.Sprintf(
		`user='%s' password='%s' host='%s' port='%s' dbname='%s'`,
		encodePostgresConfig(envPostgresUser.Value(lookup)),
		encodePostgresConfig(password),
		encodePostgresConfig(envPostgresHost.Value(lookup)),
		encodePostgresConfig(envPostgresPort.Value(lookup)),
		encodePostgresConfig(envPostgresDatabase.Value(lookup)),
	)

	config, err := pgxpool.ParseConfig(addr)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	flow := FlowPostgres{pool: pool}

	return &flow, nil
}

// Get fetches the keys from postgres.
func (p *FlowPostgres) Get(ctx context.Context, keys ...dskey.Key) (map[dskey.Key][]byte, error) {
	collectionIDs := make(map[string][]int)
	for _, key := range keys {
		if slices.Contains(collectionIDs[key.Collection()], key.ID()) {
			continue
		}

		collectionIDs[key.Collection()] = append(collectionIDs[key.Collection()], key.ID())
	}

	keyValues := make(map[dskey.Key][]byte, len(keys))
	for collection, ids := range collectionIDs {
		// TODO: Fix me after this is fixed: https://github.com/OpenSlides/openslides-meta/issues/243
		collectionTableName := collection
		if collectionTableName == "user" || collectionTableName == "group" {
			collectionTableName += "_"
		}

		// TODO: maybe only fetch id and requested keys
		sql := fmt.Sprintf(`SELECT * FROM %s WHERE id = ANY ($1) `, collectionTableName)

		rows, err := p.pool.Query(ctx, sql, ids)
		if err != nil {
			return nil, fmt.Errorf("sending query for %s: %w", collection, err)
		}
		defer rows.Close()

		err = forEachRow(rows, func(row pgx.CollectableRow) error {
			// TODO: Instead of using row.Values, it would be nicer, if I could
			// use RawValues or skip .Values all together.
			values, err := row.Values()
			if err != nil {
				return fmt.Errorf("get values: %w", err)
			}

			// TODO: This expects the first field to be the id.
			id32, ok := values[0].(int32)
			if !ok {
				return fmt.Errorf("invalid id: %v, %T", values[0], values[0])
			}

			id := int(id32)

			for i, value := range values {
				field := row.FieldDescriptions()[i].Name
				key, err := dskey.FromParts(collection, id, field)
				if err != nil {
					return fmt.Errorf("invalid key on field %d: %w", i, err)
				}

				keyValues[key] = nil
				if value != nil {
					bytes, err := json.Marshal(value)
					if err != nil {
						return fmt.Errorf("converting field %d to json: %w", i, err)
					}

					keyValues[key] = bytes
				}
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("parse collection %s: %w", collection, err)
		}
	}

	// The current implementation of cache expects to gets exactly the keys,
	// that where requested. If a key does not exist, nil has to be used.
	result := make(map[dskey.Key][]byte, len(keys))
	for _, key := range keys {
		result[key] = keyValues[key]
	}

	return result, nil
}

// Update listens on pg notify to fetch updates.
func (p *FlowPostgres) Update(ctx context.Context, updateFn func(map[dskey.Key][]byte, error)) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		updateFn(nil, fmt.Errorf("acquire connection: %w", err))
		return
	}
	defer conn.Release()

	// TODO: Only listen to one channel after this is fixed:
	// https://github.com/OpenSlides/openslides-meta/issues/245
	_, err = conn.Exec(ctx, "LISTEN insert")
	if err != nil {
		updateFn(nil, fmt.Errorf("listen on channel insert: %w", err))
		return
	}

	_, err = conn.Exec(ctx, "LISTEN update")
	if err != nil {
		updateFn(nil, fmt.Errorf("listen on channel update: %w", err))
		return
	}

	_, err = conn.Exec(ctx, "LISTEN delete")
	if err != nil {
		updateFn(nil, fmt.Errorf("listen on channel delete: %w", err))
		return
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			updateFn(nil, fmt.Errorf("wait for notification: %w", err))
			return
		}

		collectionName, id, err := getCollectionNameAndID(notification.Payload)
		if err != nil {
			updateFn(nil, fmt.Errorf("split fqid from %s: %w", notification.Payload, err))
			continue
		}

		keys, err := createKeyList(collectionName, id)
		if err != nil {
			updateFn(nil, fmt.Errorf("creating key list from notification: %w", err))
			continue
		}

		updateFn(p.Get(ctx, keys...))
	}
}

func createKeyList(collection string, id int) ([]dskey.Key, error) {
	fields := collectionFields[collection]
	keys := make([]dskey.Key, len(fields))
	var err error
	for i, field := range fields {
		keys[i], err = dskey.FromParts(collection, id, field)
		if err != nil {
			return nil, fmt.Errorf("creating key from parts: %w", err)
		}
	}
	return keys, nil
}

// forEachRow is like pgx.ForEachRow but uses CollectableRow instead of scan.
func forEachRow(rows pgx.Rows, fn func(row pgx.CollectableRow) error) error {
	defer rows.Close()

	for rows.Next() {

		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// getCollectionNameAndID removes the suffix from the collection name.
func getCollectionNameAndID(keyStr string) (string, int, error) {
	idx1 := strings.IndexByte(keyStr, '/')

	if idx1 == -1 {
		return "", 0, fmt.Errorf("invalid key `%s`: missing slash", keyStr)
	}

	id, err := strconv.Atoi(keyStr[idx1+1:])
	if err != nil {
		return "", 0, fmt.Errorf("invalid key `%s`: id is not an integer", keyStr)
	}

	// Can be removed when this is merged:
	// https://github.com/OpenSlides/openslides-meta/pull/240
	return strings.TrimSuffix(keyStr[:idx1], "_t"), id, nil
}
