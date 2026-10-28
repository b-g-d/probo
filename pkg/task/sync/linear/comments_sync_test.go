// Copyright (c) 2026 Probo Inc <hello@probo.com>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package linear

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/internal/test"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	linear "go.probo.inc/probo/pkg/task/sync/linear/client"
)

func TestRollbackLinkedComments_DeletesSavedCommentLinks(t *testing.T) {
	t.Parallel()

	t.Run(
		"removed linear comment",
		func(t *testing.T) {
			t.Parallel()

			fixture := newRollbackCommentFixture(t)
			client := linearDeleteServer(t, http.StatusOK, `{"data":{"commentDelete":{"success":true}}}`)

			(&Service{pg: fixture.pg}).rollbackLinkedComments(
				t.Context(),
				fixture.scope,
				client,
				nil,
				[]string{"linear-comment-removed"},
			)

			assert.Zero(t, countCommentLinks(t, fixture.pg, fixture.taskID, "linear-comment-removed"))
			assert.Equal(t, 1, countCommentLinks(t, fixture.pg, fixture.taskID, "linear-comment-kept"))
			assert.Equal(t, 1, countTaskComments(t, fixture.pg, fixture.commentID))
			assert.Equal(t, 1, countTaskComments(t, fixture.pg, fixture.keptCommentID))
		},
	)

	t.Run(
		"linear comment already gone",
		func(t *testing.T) {
			t.Parallel()

			fixture := newRollbackCommentFixture(t)
			client := linearDeleteServer(
				t,
				http.StatusOK,
				`{"errors":[{"message":"Entity not found"}]}`,
			)

			(&Service{pg: fixture.pg}).rollbackLinkedComments(
				t.Context(),
				fixture.scope,
				client,
				nil,
				[]string{"linear-comment-removed"},
			)

			assert.Zero(t, countCommentLinks(t, fixture.pg, fixture.taskID, "linear-comment-removed"))
			assert.Equal(t, 1, countCommentLinks(t, fixture.pg, fixture.taskID, "linear-comment-kept"))
			assert.Equal(t, 1, countTaskComments(t, fixture.pg, fixture.commentID))
		},
	)

	t.Run(
		"linear delete failed",
		func(t *testing.T) {
			t.Parallel()

			fixture := newRollbackCommentFixture(t)
			client := linearDeleteServer(t, http.StatusInternalServerError, `{}`)

			(&Service{pg: fixture.pg}).rollbackLinkedComments(
				t.Context(),
				fixture.scope,
				client,
				nil,
				[]string{"linear-comment-removed"},
			)

			assert.Equal(t, 1, countCommentLinks(t, fixture.pg, fixture.taskID, "linear-comment-removed"))
			assert.Equal(t, 1, countTaskComments(t, fixture.pg, fixture.commentID))
		},
	)
}

type rollbackCommentFixture struct {
	pg            *pg.Client
	scope         coredata.Scoper
	taskID        gid.GID
	commentID     gid.GID
	keptCommentID gid.GID
}

func newRollbackCommentFixture(t *testing.T) rollbackCommentFixture {
	t.Helper()

	ctx := t.Context()
	client := test.PGClient(t)
	tenantID := gid.NewTenantID()
	scope := coredata.NewScope(tenantID, gid.Nil)
	organizationID := gid.New(tenantID, coredata.OrganizationEntityType)
	taskID := gid.New(tenantID, coredata.TaskEntityType)
	connectorID := gid.New(tenantID, coredata.ConnectorEntityType)
	commentID := gid.New(tenantID, coredata.TaskCommentEntityType)
	keptCommentID := gid.New(tenantID, coredata.TaskCommentEntityType)
	now := time.Now().UTC().Truncate(time.Microsecond)

	require.NoError(t, client.WithTx(ctx, func(ctx context.Context, tx pg.Tx) error {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO organizations (id, tenant_id, name, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $4)`,
			organizationID,
			tenantID,
			"linear-rollback-"+organizationID.String(),
			now,
		); err != nil {
			return err
		}

		if _, err := tx.Exec(
			ctx,
			`INSERT INTO tasks (
			     id, tenant_id, organization_id, name, content, reference_id,
			     state, priority, rank, created_at, updated_at
			 )
			 VALUES (
			     $1, $2, $3, 'links', '{"type":"doc","content":[]}'::jsonb, $4,
			     'TODO', 'MEDIUM', 1, $5, $5
			 )`,
			taskID,
			tenantID,
			organizationID,
			taskID.String(),
			now,
		); err != nil {
			return err
		}

		if _, err := tx.Exec(
			ctx,
			`INSERT INTO connectors (
			     id, tenant_id, organization_id, provider, protocol, name,
			     settings, encrypted_connection, created_at, updated_at
			 )
			 VALUES (
			     $1, $2, $3, 'LINEAR_SYNC', 'OAUTH2', 'Linear sync',
			     '{}', '\x', $4, $4
			 )`,
			connectorID,
			tenantID,
			organizationID,
			now,
		); err != nil {
			return err
		}

		if err := insertRollbackComment(
			ctx,
			tx,
			tenantID,
			organizationID,
			taskID,
			connectorID,
			commentID,
			"linear-comment-removed",
			now,
		); err != nil {
			return err
		}

		return insertRollbackComment(
			ctx,
			tx,
			tenantID,
			organizationID,
			taskID,
			connectorID,
			keptCommentID,
			"linear-comment-kept",
			now,
		)
	}))

	t.Cleanup(func() {
		_ = client.WithTx(context.Background(), func(ctx context.Context, tx pg.Tx) error {
			if _, err := tx.Exec(
				ctx,
				`DELETE FROM task_comment_external_links WHERE task_id = $1`,
				taskID,
			); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, `DELETE FROM task_comments WHERE task_id = $1`, taskID); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, `DELETE FROM tasks WHERE id = $1`, taskID); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, `DELETE FROM connectors WHERE id = $1`, connectorID); err != nil {
				return err
			}

			_, err := tx.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, organizationID)

			return err
		})
	})

	return rollbackCommentFixture{
		pg:            client,
		scope:         scope,
		taskID:        taskID,
		commentID:     commentID,
		keptCommentID: keptCommentID,
	}
}

func insertRollbackComment(
	ctx context.Context,
	tx pg.Tx,
	tenantID gid.TenantID,
	organizationID gid.GID,
	taskID gid.GID,
	connectorID gid.GID,
	commentID gid.GID,
	externalID string,
	now time.Time,
) error {
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO task_comments (
		     id, tenant_id, organization_id, task_id, content, created_at, updated_at
		 )
		 VALUES (
		     $1, $2, $3, $4, '{"type":"doc","content":[]}'::jsonb, $5, $5
		 )`,
		commentID,
		tenantID,
		organizationID,
		taskID,
		now,
	); err != nil {
		return err
	}

	_, err := tx.Exec(
		ctx,
		`INSERT INTO task_comment_external_links (
		     task_comment_id, tenant_id, organization_id, task_id, connector_id, provider,
		     external_id, content_hash, created_at, updated_at
		 )
		 VALUES (
		     $1, $2, $3, $4, $5, 'LINEAR_SYNC', $6, 'hash', $7, $7
		 )`,
		commentID,
		tenantID,
		organizationID,
		taskID,
		connectorID,
		externalID,
		now,
	)

	return err
}

func linearDeleteServer(t *testing.T, status int, body string) *linear.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	return linear.NewClient(srv.Client(), srv.URL)
}

func countCommentLinks(t *testing.T, client *pg.Client, taskID gid.GID, externalID string) int {
	t.Helper()

	var count int

	require.NoError(t, client.WithConn(t.Context(), func(ctx context.Context, conn pg.Querier) error {
		return conn.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM task_comment_external_links
			 WHERE task_id = $1 AND external_id = $2`,
			taskID,
			externalID,
		).Scan(&count)
	}))

	return count
}

func countTaskComments(t *testing.T, client *pg.Client, commentID gid.GID) int {
	t.Helper()

	var count int

	require.NoError(t, client.WithConn(t.Context(), func(ctx context.Context, conn pg.Querier) error {
		return conn.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM task_comments WHERE id = $1`,
			commentID,
		).Scan(&count)
	}))

	return count
}
