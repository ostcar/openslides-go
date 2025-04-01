package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/OpenSlides/openslides-go/datastore/dskey"
	"github.com/OpenSlides/openslides-go/environment"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

	result := make(map[dskey.Key][]byte, len(keys))
	for collection, ids := range collectionIDs {
		collectionTableName := collection
		if collectionTableName == "user" || collectionTableName == "group" {
			collectionTableName += "_"
		}

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

				bytes, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("converting field %d to json: %w", i, err)
				}

				result[key] = bytes
			}

			return nil

		})
		if err != nil {
			return nil, fmt.Errorf("parse collection %s: %w", collection, err)
		}
	}

	return result, nil
}

// HistoryInformation fetches the history information for one fqid.
// TODO: Fix me
func (p *FlowPostgres) HistoryInformation(ctx context.Context, fqid string, w io.Writer) error {
	sql := `select distinct on (position) position, timestamp, user_id, information from positions natural join events
	where fqid = $1 and information::text<>'null'::text order by position asc`

	rows, err := p.pool.Query(ctx, sql, fqid)
	if err != nil {
		return fmt.Errorf("sending query: %w", err)
	}
	defer rows.Close()

	type historyInformation struct {
		Position    int             `json:"position"`
		Timestamp   int             `json:"timestamp"`
		UserID      int             `json:"user_id"`
		Information json.RawMessage `json:"information"`
	}

	output := make(map[string][]historyInformation, 1)

	for rows.Next() {
		var hi historyInformation
		var timestamp time.Time

		if err = rows.Scan(&hi.Position, &timestamp, &hi.UserID, &hi.Information); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		hi.Timestamp = int(timestamp.Unix())
		output[fqid] = append(output[fqid], hi)
	}

	if err := json.NewEncoder(w).Encode(output); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

// Update listens on pg notify to fetch updates.
func (p *FlowPostgres) Update(ctx context.Context, updateFn func(map[dskey.Key][]byte, error)) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		updateFn(nil, fmt.Errorf("acquire connection: %w", err))
		return
	}
	defer conn.Release()

	// TODO: On which channel should we listen?
	_, err = conn.Exec(ctx, "LISTEN insert")
	if err != nil {
		updateFn(nil, fmt.Errorf("listen on channel: %w", err))
		return
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			updateFn(nil, fmt.Errorf("listen on channel: %w", err))
			return
		}

		// TODO: How to bundle many keys in one update?
		key, err := dskey.FromString(notification.Payload)
		if err != nil {
			updateFn(nil, fmt.Errorf("got invalid key `%s`from pg_notify: %w", notification.Payload, err))
			continue
		}

		updateFn(p.Get(ctx, key))
	}
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
