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

package asana

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/pkg/connectorprotocol"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/crypto/cipher"
	"go.probo.inc/probo/pkg/gid"
	"go.probo.inc/probo/pkg/task/sync/attach"
	"go.probo.inc/probo/pkg/task/sync/commenthash"
	"go.probo.inc/probo/pkg/task/sync/dates"
	"go.probo.inc/probo/pkg/task/sync/fault"
	"go.probo.inc/probo/pkg/task/sync/job"
	"go.probo.inc/probo/pkg/task/sync/lease"
)

type (
	SyncAction = job.SyncAction
	JobPayload = job.JobPayload

	Attachment       = attach.Attachment
	AttachmentReader = attach.Reader
	StoredAttachment = attach.Stored
	AttachmentWriter = attach.Writer

	RecordUpdateActivitiesFunc func(
		ctx context.Context,
		tx pg.Tx,
		scope coredata.Scoper,
		oldTask *coredata.Task,
		task *coredata.Task,
		actorID *gid.GID,
		now time.Time,
	) error

	RecordImportedTaskFunc func(
		ctx context.Context,
		tx pg.Tx,
		scope coredata.Scoper,
		task *coredata.Task,
		actorID *gid.GID,
		now time.Time,
	) error

	// Service runs Asana task sync. The HTTP client lives in the client
	// package; this type owns the database, jobs, and mention handling.
	Service struct {
		pg                     *pg.Client
		encryptionKey          cipher.EncryptionKey
		connectorRegistry      *connectorprotocol.Registry
		baseURL                string
		asanaAPIBaseURL        string
		logger                 *log.Logger
		recordUpdateActivities RecordUpdateActivitiesFunc
		recordImportedTask     RecordImportedTaskFunc
		attachments            AttachmentReader
		stores                 AttachmentWriter

		oauthHTTP       func(context.Context, *connectorprotocol.OAuth2Connection, coredata.ConnectorProvider) (*http.Client, error)
		enqueueOutbound func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskExternalLink, SyncAction) error
		notifyMentions  func(context.Context, pg.Tx, coredata.Scoper, gid.GID, gid.GID, string, string, string, *gid.GID) error
		commentLinks    func(context.Context, coredata.Scoper, gid.GID) (map[string]*coredata.TaskCommentExternalLink, error)
		updateComment   func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink, string, string, *time.Time, *gid.GID) error
		authorName      func(context.Context, coredata.Scoper, *gid.GID) (string, error)
		deleteLinks     func(context.Context, pg.Querier, coredata.Scoper, gid.GID) error
		deleteComment   func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink) error
		commentOwner    func(context.Context, pg.Querier, coredata.Scoper, *coredata.TaskExternalLink, string) (*gid.GID, error)
		notifyComment   func(context.Context, pg.Tx, coredata.Scoper, gid.GID, string, string, *gid.GID) error
	}

	// Config builds a Service. The funcs are the shared sync operations that
	// stay in pkg/task/sync.
	Config struct {
		PG                     *pg.Client
		EncryptionKey          cipher.EncryptionKey
		ConnectorRegistry      *connectorprotocol.Registry
		BaseURL                string
		APIBaseURL             string
		Logger                 *log.Logger
		RecordUpdateActivities RecordUpdateActivitiesFunc
		RecordImportedTask     RecordImportedTaskFunc
		Attachments            AttachmentReader
		Stores                 AttachmentWriter
		OAuthHTTP              func(context.Context, *connectorprotocol.OAuth2Connection, coredata.ConnectorProvider) (*http.Client, error)
		EnqueueOutbound        func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskExternalLink, SyncAction) error
		NotifyMentions         func(context.Context, pg.Tx, coredata.Scoper, gid.GID, gid.GID, string, string, string, *gid.GID) error
		CommentLinks           func(context.Context, coredata.Scoper, gid.GID) (map[string]*coredata.TaskCommentExternalLink, error)
		UpdateImportedComment  func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink, string, string, *time.Time, *gid.GID) error
		AuthorName             func(context.Context, coredata.Scoper, *gid.GID) (string, error)
		DeleteCommentLinks     func(context.Context, pg.Querier, coredata.Scoper, gid.GID) error
		DeleteSyncedComment    func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink) error
		ResolveCommentOwner    func(context.Context, pg.Querier, coredata.Scoper, *coredata.TaskExternalLink, string) (*gid.GID, error)
		NotifyCommentMentions  func(context.Context, pg.Tx, coredata.Scoper, gid.GID, string, string, *gid.GID) error
	}

	commentAuthor struct {
		Name string
	}

	outboundHandler struct {
		svc    *Service
		pg     *pg.Client
		newer  func(context.Context, *coredata.TaskSyncJob, gid.GID, coredata.Scoper) (bool, error)
		upsert func(context.Context, *coredata.TaskSyncJob, JobPayload, coredata.Scoper, bool) (*coredata.TaskComment, *coredata.TaskCommentExternalLink, bool, error)
		link   func(context.Context, pg.Querier, coredata.Scoper, gid.GID) (*coredata.TaskExternalLink, error)
		done   func(context.Context, *coredata.TaskSyncJob) error
	}

	// OutboundHooks are the shared worker operations an Asana job needs.
	OutboundHooks struct {
		Newer  func(context.Context, *coredata.TaskSyncJob, gid.GID, coredata.Scoper) (bool, error)
		Upsert func(context.Context, *coredata.TaskSyncJob, JobPayload, coredata.Scoper, bool) (*coredata.TaskComment, *coredata.TaskCommentExternalLink, bool, error)
		Link   func(context.Context, pg.Querier, coredata.Scoper, gid.GID) (*coredata.TaskExternalLink, error)
		Done   func(context.Context, *coredata.TaskSyncJob) error
	}
)

const (
	SyncActionUpdate        = job.SyncActionUpdate
	SyncActionCancel        = job.SyncActionCancel
	SyncActionCommentUpsert = job.SyncActionCommentUpsert
	SyncActionCommentDelete = job.SyncActionCommentDelete

	webhookHeartbeatInterval = 30 * time.Second
)

var ErrTaskAlreadyLinked = fault.ErrTaskAlreadyLinked

func New(cfg Config) *Service {
	return &Service{
		pg:                     cfg.PG,
		encryptionKey:          cfg.EncryptionKey,
		connectorRegistry:      cfg.ConnectorRegistry,
		baseURL:                cfg.BaseURL,
		asanaAPIBaseURL:        cfg.APIBaseURL,
		logger:                 cfg.Logger,
		recordUpdateActivities: cfg.RecordUpdateActivities,
		recordImportedTask:     cfg.RecordImportedTask,
		attachments:            cfg.Attachments,
		stores:                 cfg.Stores,
		oauthHTTP:              cfg.OAuthHTTP,
		enqueueOutbound:        cfg.EnqueueOutbound,
		notifyMentions:         cfg.NotifyMentions,
		commentLinks:           cfg.CommentLinks,
		updateComment:          cfg.UpdateImportedComment,
		authorName:             cfg.AuthorName,
		deleteLinks:            cfg.DeleteCommentLinks,
		deleteComment:          cfg.DeleteSyncedComment,
		commentOwner:           cfg.ResolveCommentOwner,
		notifyComment:          cfg.NotifyCommentMentions,
	}
}

func (s *Service) oauthHTTPClient(
	ctx context.Context,
	conn *connectorprotocol.OAuth2Connection,
	provider coredata.ConnectorProvider,
) (*http.Client, error) {
	return s.oauthHTTP(ctx, conn, provider)
}

func (s *Service) enqueueOutboundTx(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	action SyncAction,
) error {
	return s.enqueueOutbound(ctx, tx, scope, link, action)
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
	return s.notifyMentions(
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

func (s *Service) commentLinksByExternalID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[string]*coredata.TaskCommentExternalLink, error) {
	return s.commentLinks(ctx, scope, taskID)
}

func (s *Service) updateImportedComment(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	mapping *coredata.TaskCommentExternalLink,
	content string,
	hash string,
	remoteUpdatedAt *time.Time,
	actorProfileID *gid.GID,
) error {
	return s.updateComment(
		ctx,
		tx,
		scope,
		mapping,
		content,
		hash,
		remoteUpdatedAt,
		actorProfileID,
	)
}

func (s *Service) commentAuthor(
	ctx context.Context,
	scope coredata.Scoper,
	ownerID *gid.GID,
) (commentAuthor, error) {
	name, err := s.authorName(ctx, scope, ownerID)
	if err != nil {
		return commentAuthor{}, err
	}

	return commentAuthor{Name: name}, nil
}

func (s *Service) deleteCommentLinks(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	taskID gid.GID,
) error {
	return s.deleteLinks(ctx, conn, scope, taskID)
}

func (s *Service) deleteSyncedComment(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	mapping *coredata.TaskCommentExternalLink,
) error {
	return s.deleteComment(ctx, tx, scope, mapping)
}

func (s *Service) resolveCommentOwnerTx(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	email string,
) (*gid.GID, error) {
	return s.commentOwner(ctx, conn, scope, link, email)
}

func (s *Service) notifySyncedCommentMentions(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	taskID gid.GID,
	oldContent string,
	newContent string,
	actorProfileID *gid.GID,
) error {
	return s.notifyComment(ctx, tx, scope, taskID, oldContent, newContent, actorProfileID)
}

func DeadlineToDate(deadline *time.Time) *string {
	return dates.DeadlineToDate(deadline)
}

func DateToDeadline(value string) (*time.Time, error) {
	deadline, err := dates.DateToDeadline(value)
	if err != nil {
		return nil, fmt.Errorf("cannot parse deadline: %w", err)
	}

	return deadline, nil
}

func CommentContentHash(markdown string) string {
	return commenthash.Sum(markdown)
}

func projectChoice(explicitSet bool, explicit *string, defaultProjectID string) (string, bool) {
	if !explicitSet {
		if defaultProjectID == "" {
			return "", false
		}

		return defaultProjectID, true
	}

	if explicit == nil || strings.TrimSpace(*explicit) == "" {
		return "", false
	}

	return strings.TrimSpace(*explicit), true
}

func HandleOutbound(
	svc *Service,
	hooks OutboundHooks,
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
) error {
	handler := &outboundHandler{
		svc:    svc,
		pg:     svc.pg,
		newer:  hooks.Newer,
		upsert: hooks.Upsert,
		link:   hooks.Link,
		done:   hooks.Done,
	}

	return handler.handleAsana(ctx, item, payload)
}

func (h *outboundHandler) hasNewerOutboundJob(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	taskID gid.GID,
	scope coredata.Scoper,
) (bool, error) {
	return h.newer(ctx, item, taskID, scope)
}

func (h *outboundHandler) succeed(ctx context.Context, item *coredata.TaskSyncJob) error {
	return h.done(ctx, item)
}

func (h *outboundHandler) loadCommentUpsert(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
	scope coredata.Scoper,
	sent bool,
) (*coredata.TaskComment, *coredata.TaskCommentExternalLink, bool, error) {
	return h.upsert(ctx, item, payload, scope, sent)
}

func (h *outboundHandler) loadCurrentOutboundLink(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	taskID gid.GID,
) (*coredata.TaskExternalLink, error) {
	return h.link(ctx, conn, scope, taskID)
}

func outboundLeaseHeartbeatInterval(staleAfter, interval time.Duration) time.Duration {
	return lease.OutboundLeaseHeartbeatInterval(staleAfter, interval)
}

func processingLeaseLost(ctx context.Context, err error) bool {
	return lease.ProcessingLeaseLost(ctx, err)
}
