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

package tasksync

import (
	"context"
	"fmt"

	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	"go.probo.inc/probo/pkg/page"
	"go.probo.inc/probo/pkg/task/sync/mentionnotify"
)

func (s *Service) notifySyncedCommentMentions(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	taskID gid.GID,
	oldContent string,
	newContent string,
	actorProfileID *gid.GID,
) error {
	return mentionnotify.Comment(
		ctx,
		tx,
		scope,
		taskID,
		oldContent,
		newContent,
		actorProfileID,
	)
}

func loadTaskComments(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	taskID gid.GID,
) ([]*coredata.TaskComment, error) {
	loaded, err := page.LoadAll(
		ctx,
		page.OrderBy[coredata.TaskCommentOrderField]{
			Field:     coredata.TaskCommentOrderFieldCreatedAt,
			Direction: page.OrderDirectionAsc,
		},
		func(ctx context.Context, cursor *page.Cursor[coredata.TaskCommentOrderField]) ([]*coredata.TaskComment, error) {
			var batch coredata.TaskComments
			if err := batch.LoadByTaskID(ctx, conn, scope, taskID, cursor); err != nil {
				return nil, err
			}

			return batch, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot list task comments: %w", err)
	}

	return loaded, nil
}

func (s *Service) notifySyncedMentions(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	organizationID gid.GID,
	taskID gid.GID,
	taskName string,
	oldContent string,
	newContent string,
	actorProfileID *gid.GID,
) error {
	return mentionnotify.Profiles(
		ctx,
		tx,
		scope,
		organizationID,
		taskID,
		taskName,
		oldContent,
		newContent,
		actorProfileID,
	)
}
