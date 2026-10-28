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
	"time"

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/pkg/connectorprotocol"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/crypto/cipher"
	"go.probo.inc/probo/pkg/gid"
	"go.probo.inc/probo/pkg/task/sync/attach"
	"go.probo.inc/probo/pkg/task/sync/commenthash"
	"go.probo.inc/probo/pkg/task/sync/commentstore"
	"go.probo.inc/probo/pkg/task/sync/lease"
	linear "go.probo.inc/probo/pkg/task/sync/linear/client"
	"go.probo.inc/probo/pkg/task/sync/mentionnotify"
)

type (
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

	// Service runs Linear task sync. The HTTP client lives in the client package.
	Service struct {
		pg                     *pg.Client
		encryptionKey          cipher.EncryptionKey
		connectorRegistry      *connectorprotocol.Registry
		baseURL                string
		linearAPIBaseURL       string
		logger                 *log.Logger
		recordUpdateActivities RecordUpdateActivitiesFunc
		recordImportedTask     RecordImportedTaskFunc
		attachments            AttachmentReader
		stores                 AttachmentWriter
		linearAssets           linearAssetCache
		inboundFiles           inboundFileCache

		oauthHTTP       func(context.Context, *connectorprotocol.OAuth2Connection, coredata.ConnectorProvider) (*http.Client, error)
		notifyMentions  func(context.Context, pg.Tx, coredata.Scoper, gid.GID, gid.GID, string, string, string, *gid.GID) error
		deleteLinks     func(context.Context, pg.Querier, coredata.Scoper, gid.GID) error
		actorProfile    func(context.Context, pg.Querier, coredata.Scoper, *gid.GID, gid.GID) (*gid.GID, error)
		listComments    func(context.Context, coredata.Scoper, gid.GID) ([]*coredata.TaskComment, error)
		linksByComment  func(context.Context, coredata.Scoper, gid.GID) (map[gid.GID]*coredata.TaskCommentExternalLink, error)
		linksByRemote   func(context.Context, coredata.Scoper, gid.GID) (map[string]*coredata.TaskCommentExternalLink, error)
		authorOf        func(context.Context, coredata.Scoper, *gid.GID) (linear.CommentAuthor, error)
		saveLink        func(context.Context, coredata.Scoper, *coredata.TaskExternalLink, gid.GID, *linear.Comment, string) error
		ownerByEmail    func(context.Context, coredata.Scoper, *coredata.TaskExternalLink, string) (*gid.GID, error)
		updateComment   func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink, string, string, *time.Time, *gid.GID) error
		enqueueOutbound func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskExternalLink, SyncAction) error
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
		NotifyMentions         func(context.Context, pg.Tx, coredata.Scoper, gid.GID, gid.GID, string, string, string, *gid.GID) error
		DeleteCommentLinks     func(context.Context, pg.Querier, coredata.Scoper, gid.GID) error
		ActorProfile           func(context.Context, pg.Querier, coredata.Scoper, *gid.GID, gid.GID) (*gid.GID, error)
		ListComments           func(context.Context, coredata.Scoper, gid.GID) ([]*coredata.TaskComment, error)
		LinksByComment         func(context.Context, coredata.Scoper, gid.GID) (map[gid.GID]*coredata.TaskCommentExternalLink, error)
		LinksByRemote          func(context.Context, coredata.Scoper, gid.GID) (map[string]*coredata.TaskCommentExternalLink, error)
		Author                 func(context.Context, coredata.Scoper, *gid.GID) (linear.CommentAuthor, error)
		SaveCommentLink        func(context.Context, coredata.Scoper, *coredata.TaskExternalLink, gid.GID, *linear.Comment, string) error
		OwnerByEmail           func(context.Context, coredata.Scoper, *coredata.TaskExternalLink, string) (*gid.GID, error)
		UpdateImportedComment  func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskCommentExternalLink, string, string, *time.Time, *gid.GID) error
		EnqueueOutbound        func(context.Context, pg.Tx, coredata.Scoper, *coredata.TaskExternalLink, SyncAction) error
	}
)

func New(cfg Config) *Service {
	return &Service{
		pg:                     cfg.PG,
		encryptionKey:          cfg.EncryptionKey,
		connectorRegistry:      cfg.ConnectorRegistry,
		baseURL:                cfg.BaseURL,
		linearAPIBaseURL:       cfg.APIBaseURL,
		logger:                 cfg.Logger,
		recordUpdateActivities: cfg.RecordUpdateActivities,
		recordImportedTask:     cfg.RecordImportedTask,
		attachments:            cfg.Attachments,
		stores:                 cfg.Stores,
		linearAssets:           linearAssetCache{urls: map[string]string{}},
		oauthHTTP:              cfg.OAuthHTTP,
		notifyMentions:         cfg.NotifyMentions,
		deleteLinks:            cfg.DeleteCommentLinks,
		actorProfile:           cfg.ActorProfile,
		listComments:           cfg.ListComments,
		linksByComment:         cfg.LinksByComment,
		linksByRemote:          cfg.LinksByRemote,
		authorOf:               cfg.Author,
		saveLink:               cfg.SaveCommentLink,
		ownerByEmail:           cfg.OwnerByEmail,
		updateComment:          cfg.UpdateImportedComment,
		enqueueOutbound:        cfg.EnqueueOutbound,
	}
}

func (s *Service) oauthHTTPClient(
	ctx context.Context,
	conn *connectorprotocol.OAuth2Connection,
	provider coredata.ConnectorProvider,
) (*http.Client, error) {
	return s.oauthHTTP(ctx, conn, provider)
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

func (s *Service) deleteCommentLinks(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	taskID gid.GID,
) error {
	return s.deleteLinks(ctx, conn, scope, taskID)
}

func deleteSyncedComment(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	mapping *coredata.TaskCommentExternalLink,
) error {
	return commentstore.Delete(ctx, tx, scope, mapping)
}

func CommentContentHash(markdown string) string {
	return commenthash.Sum(markdown)
}

func (s *Service) listTaskComments(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) ([]*coredata.TaskComment, error) {
	return s.listComments(ctx, scope, taskID)
}

func (s *Service) commentLinksByCommentID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[gid.GID]*coredata.TaskCommentExternalLink, error) {
	return s.linksByComment(ctx, scope, taskID)
}

func (s *Service) commentLinksByExternalID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[string]*coredata.TaskCommentExternalLink, error) {
	return s.linksByRemote(ctx, scope, taskID)
}

func (s *Service) commentAuthor(
	ctx context.Context,
	scope coredata.Scoper,
	ownerID *gid.GID,
) (linear.CommentAuthor, error) {
	return s.authorOf(ctx, scope, ownerID)
}

func (s *Service) saveCommentLink(
	ctx context.Context,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	commentID gid.GID,
	remote *linear.Comment,
	hash string,
) error {
	return s.saveLink(ctx, scope, link, commentID, remote, hash)
}

func (s *Service) resolveCommentOwner(
	ctx context.Context,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	email string,
) (*gid.GID, error) {
	return s.ownerByEmail(ctx, scope, link, email)
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

func (s *Service) enqueueOutboundTx(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	action SyncAction,
) error {
	return s.enqueueOutbound(ctx, tx, scope, link, action)
}

func outboundLeaseHeartbeatInterval(staleAfter, interval time.Duration) time.Duration {
	return lease.OutboundLeaseHeartbeatInterval(staleAfter, interval)
}

func processingLeaseLost(ctx context.Context, err error) bool {
	return lease.ProcessingLeaseLost(ctx, err)
}
