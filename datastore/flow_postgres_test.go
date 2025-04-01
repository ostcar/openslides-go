package datastore_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/OpenSlides/openslides-go/datastore"
	"github.com/OpenSlides/openslides-go/datastore/dskey"
	"github.com/OpenSlides/openslides-go/environment"
	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
)

func TestFlowPostgres(t *testing.T) {
	ctx := t.Context()

	t.Parallel()
	if testing.Short() {
		t.Skip("Postgres Test")
	}

	tp, err := newTestPostgres(ctx)
	if err != nil {
		t.Fatalf("starting postgres: %v", err)
	}
	defer tp.Close()

	for _, tt := range []struct {
		name   string // Name of the test
		insert string
		expect map[string][]byte // expected data. Uses a get request on all keys of the expect map
	}{
		{
			"Same fqid",
			`
			INSERT INTO theme_t (name, accent_500, primary_500, warn_500) values ('standard theme', '#123456', '#123456', '#123456')
			`,
			map[string][]byte{
				"theme/1/name": []byte(`"standard theme"`),
			},
		},
		{
			"different fqid",
			`
			INSERT INTO theme_t (name, accent_500, primary_500, warn_500) values ('standard theme', '#123456', '#123456', '#123456');
			INSERT INTO user_ (id, username, first_name) values (42,'hugo', 'Hugo');
			`,
			map[string][]byte{
				"user/42/username":   []byte(`"hugo"`),
				"user/42/first_name": []byte(`"Hugo"`),
				"theme/1/name":       []byte(`"standard theme"`),
			},
		},
		{
			"Empty Data",
			`
			INSERT INTO user_ (id, username) values (55,'hugo');
			`,
			map[string][]byte{
				"user/55/username": []byte(`"hugo"`),
				"motion/2/title":   nil,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// if err := tp.addExampleData(t.Context()); err != nil {
			// 	t.Fatalf("adding example data: %v", err)
			// }

			flow, err := datastore.NewFlowPostgres(environment.ForTests(tp.Env))
			if err != nil {
				t.Fatalf("NewFlowPostgres(): %v", err)
			}

			conn, err := tp.conn(ctx)
			if err != nil {
				t.Fatalf("creating connection: %v", err)
			}

			if _, err := conn.Exec(ctx, tt.insert); err != nil {
				t.Fatalf("adding example data: %v", err)
			}

			keys := make([]dskey.Key, 0, len(tt.expect))
			for k := range tt.expect {
				keys = append(keys, dskey.MustKey(k))
			}

			got, err := flow.Get(ctx, keys...)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			for k, v := range tt.expect {
				if string(got[dskey.MustKey(k)]) != string(v) {
					t.Errorf("got %v, want %v", got[dskey.MustKey(k)], v)
				}
			}

		})
	}
}

// func TestBigQuery(t *testing.T) {
// 	t.Parallel()

// 	if testing.Short() {
// 		t.Skip("Postgres Test")
// 	}

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	tp, err := newTestPostgres(ctx)
// 	if err != nil {
// 		t.Fatalf("starting postgres: %v", err)
// 	}
// 	defer tp.Close()

// 	source, err := datastore.NewFlowPostgres(environment.ForTests(tp.Env), nil)
// 	if err != nil {
// 		t.Fatalf("NewSource(): %v", err)
// 	}

// 	count := 2_000

// 	keys := make([]dskey.Key, count)
// 	for i := 0; i < count; i++ {
// 		keys[i], _ = dskey.FromParts("user", 1, fmt.Sprintf("f%d", i))
// 	}

// 	testData := make(map[dskey.Key][]byte)
// 	for _, key := range keys {
// 		testData[key] = []byte(fmt.Sprintf(`"%s"`, key.String()))
// 	}

// 	if err := tp.addTestData(ctx, testData); err != nil {
// 		t.Fatalf("Writing test data: %v", err)
// 	}

// 	got, err := source.Get(ctx, keys...)
// 	if err != nil {
// 		t.Errorf("Sending request with %d fields returns: %v", count, err)
// 	}

// 	if !reflect.DeepEqual(got, testData) {
// 		t.Errorf("testdata is diffrent then the result: for key %s got('%s') expect ('%s')", keys[1600], got[keys[1600]], testData[keys[1600]])
// 	}
// }

type testPostgres struct {
	dockerPool     *dockertest.Pool
	dockerResource *dockertest.Resource

	Env map[string]string

	pgxConfig *pgx.ConnConfig
}

func newTestPostgres(ctx context.Context) (tp *testPostgres, err error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("connect to docker: %w", err)
	}

	runOpts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "13",
		Env: []string{
			"POSTGRES_USER=postgres",
			"POSTGRES_PASSWORD=openslides",
			"POSTGRES_DB=database",
		},
	}

	resource, err := pool.RunWithOptions(&runOpts)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}

	port := resource.GetPort("5432/tcp")
	addr := fmt.Sprintf(`user=postgres password='openslides' host=localhost port=%s dbname=database`, port)
	config, err := pgx.ParseConfig(addr)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	tp = &testPostgres{
		dockerPool:     pool,
		dockerResource: resource,
		pgxConfig:      config,

		Env: map[string]string{
			"DATABASE_HOST": "localhost",
			"DATABASE_PORT": port,
			"DATABASE_NAME": "database",
			"DATABASE_USER": "postgres",
		},
	}

	defer func() {
		if err != nil {
			if err := tp.Close(); err != nil {
				log.Println("Closing postgres: %w", err)
			}
		}
	}()

	if err := tp.addSchema(ctx); err != nil {
		return nil, fmt.Errorf("add schema: %w", err)
	}

	return tp, nil
}

func (tp *testPostgres) Close() error {
	if err := tp.dockerPool.Purge(tp.dockerResource); err != nil {
		return fmt.Errorf("purge postgres container: %w", err)
	}
	return nil
}

func (tp *testPostgres) conn(ctx context.Context) (*pgx.Conn, error) {
	var conn *pgx.Conn

	for {
		var err error
		conn, err = pgx.ConnectConfig(ctx, tp.pgxConfig)
		if err == nil {
			return conn, nil
		}

		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (tp *testPostgres) addSchema(ctx context.Context) error {
	schema, err := os.ReadFile("../meta/dev/sql/schema_relational.sql")
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}

	conn, err := tp.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}

	if _, err := conn.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("adding schema: %w", err)
	}
	return nil
}

func (tp *testPostgres) addExampleData(ctx context.Context) error {
	exampleData, err := os.ReadFile("../meta/dev/sql/example_transactional.sql")
	if err != nil {
		return fmt.Errorf("reading example file: %w", err)
	}

	conn, err := tp.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}

	if _, err := conn.Exec(ctx, string(exampleData)); err != nil {
		return fmt.Errorf("adding example data: %w", err)
	}

	return nil
}

func (tp *testPostgres) dropData(ctx context.Context) error {
	conn, err := tp.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}

	// TODO:
	// - Get all tables from the database
	// - For each table, truncate it

	sql := `TRUNCATE models;`
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("executing psql `%s`: %w", sql, err)
	}

	return nil
}
