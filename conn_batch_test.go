package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchContextCancel(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		conn, err   = Open(&Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			MaxOpenConns: 1,
		})
	)
	if err != nil {
		t.Skipf("ClickHouse is not running: %v", err)
		return
	}
	defer func() {
		conn.Close()
	}()

	if err := conn.Ping(ctx); err != nil {
		t.Skip("ClickHouse is not running")
	}

	err = conn.Exec(ctx, "DROP TABLE IF EXISTS test_batch_cancel")
	require.NoError(t, err)
	err = conn.Exec(ctx, "CREATE TABLE test_batch_cancel (col1 UInt64) ENGINE = MergeTree() ORDER BY col1")
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_batch_cancel")
	require.NoError(t, err)

	err = batch.Append(uint64(1))
	require.NoError(t, err)

	cancel()

	err = batch.Send()
	assert.ErrorIs(t, err, context.Canceled)

	// Verify that the connection pool is not exhausted and we can ping/query
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()
	err = conn.Ping(ctx2)
	assert.NoError(t, err)
}