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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/pkg/connectorprotocol"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/crypto/cipher"
	"go.probo.inc/probo/pkg/gid"
	asanasvc "go.probo.inc/probo/pkg/task/sync/asana"
	linearsvc "go.probo.inc/probo/pkg/task/sync/linear"
	linear "go.probo.inc/probo/pkg/task/sync/linear/client"
)

type (
	// RecordUpdateActivitiesFunc writes task activity rows for an inbound
	// Linear apply. Injected from pkg/task to avoid an import cycle.
	RecordUpdateActivitiesFunc func(
		ctx context.Context,
		tx pg.Tx,
		scope coredata.Scoper,
		oldTask *coredata.Task,
		task *coredata.Task,
		actorID *gid.GID,
		now time.Time,
	) error

	// RecordImportedTaskFunc writes the created activity and webhook for a
	// task imported from a new Linear issue.
	RecordImportedTaskFunc func(
		ctx context.Context,
		tx pg.Tx,
		scope coredata.Scoper,
		task *coredata.Task,
		actorID *gid.GID,
		now time.Time,
	) error

	Service struct {
		pg                *pg.Client
		encryptionKey     cipher.EncryptionKey
		connectorRegistry *connectorprotocol.Registry
		baseURL           string
		// linearAPIBaseURL is the LINEAR_SYNC provider registration's
		// Endpoints.APIBase, threaded in by probod so a deployment that
		// repoints the Linear Sync connector moves these calls too. See
		// linear.NewClient.
		linearAPIBaseURL       string
		asanaAPIBaseURL        string
		logger                 *log.Logger
		recordUpdateActivities RecordUpdateActivitiesFunc
		recordImportedTask     RecordImportedTaskFunc
		attachments            AttachmentReader
		stores                 AttachmentWriter
		asana                  *asanasvc.Service
		linear                 *linearsvc.Service
	}
)

func NewService(
	pgClient *pg.Client,
	encryptionKey cipher.EncryptionKey,
	connectorRegistry *connectorprotocol.Registry,
	baseURL string,
	linearAPIBaseURL string,
	asanaAPIBaseURL string,
	logger *log.Logger,
	recordUpdateActivities RecordUpdateActivitiesFunc,
	recordImportedTask RecordImportedTaskFunc,
	attachments AttachmentReader,
) *Service {
	service := &Service{
		pg:                     pgClient,
		encryptionKey:          encryptionKey,
		connectorRegistry:      connectorRegistry,
		baseURL:                baseURL,
		linearAPIBaseURL:       linearAPIBaseURL,
		asanaAPIBaseURL:        asanaAPIBaseURL,
		logger:                 logger,
		recordUpdateActivities: recordUpdateActivities,
		recordImportedTask:     recordImportedTask,
		attachments:            attachments,
	}
	if writer, ok := attachments.(AttachmentWriter); ok {
		service.stores = writer
	}

	service.asana = asanasvc.New(asanasvc.Config{
		PG:                pgClient,
		EncryptionKey:     encryptionKey,
		ConnectorRegistry: connectorRegistry,
		BaseURL:           baseURL,
		APIBaseURL:        asanaAPIBaseURL,
		Logger:            logger,
		RecordUpdateActivities: asanasvc.RecordUpdateActivitiesFunc(
			recordUpdateActivities,
		),
		RecordImportedTask:    asanasvc.RecordImportedTaskFunc(recordImportedTask),
		Attachments:           service.attachments,
		Stores:                service.stores,
		OAuthHTTP:             service.oauthHTTPClient,
		EnqueueOutbound:       service.enqueueOutboundTx,
		NotifyMentions:        service.notifySyncedMentions,
		CommentLinks:          service.commentLinksByExternalID,
		UpdateImportedComment: service.updateImportedComment,
		AuthorName: func(
			ctx context.Context,
			scope coredata.Scoper,
			ownerID *gid.GID,
		) (string, error) {
			author, err := service.commentAuthor(ctx, scope, ownerID)
			if err != nil {
				return "", err
			}

			return author.Name, nil
		},
		DeleteCommentLinks:    deleteCommentLinks,
		DeleteSyncedComment:   deleteSyncedComment,
		ResolveCommentOwner:   resolveCommentOwnerTx,
		NotifyCommentMentions: service.notifySyncedCommentMentions,
	})

	service.linear = linearsvc.New(linearsvc.Config{
		PG:                pgClient,
		EncryptionKey:     encryptionKey,
		ConnectorRegistry: connectorRegistry,
		BaseURL:           baseURL,
		APIBaseURL:        linearAPIBaseURL,
		Logger:            logger,
		RecordUpdateActivities: linearsvc.RecordUpdateActivitiesFunc(
			recordUpdateActivities,
		),
		RecordImportedTask:    linearsvc.RecordImportedTaskFunc(recordImportedTask),
		Attachments:           service.attachments,
		Stores:                service.stores,
		OAuthHTTP:             service.oauthHTTPClient,
		NotifyMentions:        service.notifySyncedMentions,
		DeleteCommentLinks:    deleteCommentLinks,
		ActorProfile:          linkActivityActorID,
		ListComments:          service.listTaskComments,
		LinksByComment:        service.commentLinksByCommentID,
		LinksByRemote:         service.commentLinksByExternalID,
		Author:                service.commentAuthor,
		SaveCommentLink:       service.saveCommentLink,
		OwnerByEmail:          service.resolveCommentOwner,
		UpdateImportedComment: service.updateImportedComment,
		EnqueueOutbound:       service.enqueueOutboundTx,
	})

	return service
}

func (s *Service) GetLinkByTaskID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (*coredata.TaskExternalLink, error) {
	link := &coredata.TaskExternalLink{}

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			if err := link.LoadByTaskID(ctx, conn, scope, taskID); err != nil {
				return fmt.Errorf("cannot load task external link: %w", err)
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot get task external link: %w", err)
	}

	return link, nil
}

func (s *Service) GetLinksByTaskIDs(
	ctx context.Context,
	scope coredata.Scoper,
	taskIDs []gid.GID,
) (map[gid.GID]*coredata.TaskExternalLink, error) {
	links := coredata.TaskExternalLinks{}

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			if err := links.LoadByTaskIDs(ctx, conn, scope, taskIDs); err != nil {
				return fmt.Errorf("cannot load task external links: %w", err)
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot get task external links: %w", err)
	}

	byTaskID := make(map[gid.GID]*coredata.TaskExternalLink, len(links))
	for _, link := range links {
		byTaskID[link.TaskID] = link
	}

	return byTaskID, nil
}

func linkActivityActorID(
	ctx context.Context,
	tx pg.Querier,
	scope coredata.Scoper,
	identityID *gid.GID,
	organizationID gid.GID,
) (*gid.GID, error) {
	if identityID == nil {
		return nil, nil
	}

	profile := &coredata.MembershipProfile{}
	if err := profile.LoadByIdentityIDAndOrganizationID(
		ctx,
		tx,
		scope,
		*identityID,
		organizationID,
	); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("cannot load actor profile: %w", err)
	}

	return &profile.ID, nil
}

func (s *Service) Unlink(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) error {
	return s.pg.WithTx(
		ctx,
		func(ctx context.Context, tx pg.Tx) error {
			link := &coredata.TaskExternalLink{}
			if err := link.LoadByTaskID(ctx, tx, scope, taskID); err != nil {
				if errors.Is(err, coredata.ErrResourceNotFound) {
					return ErrTaskNotLinked
				}

				return fmt.Errorf("cannot load task external link: %w", err)
			}

			if err := link.Delete(ctx, tx, scope); err != nil {
				return fmt.Errorf("cannot delete task external link: %w", err)
			}

			if err := deleteCommentLinks(ctx, tx, scope, taskID); err != nil {
				return err
			}

			return nil
		},
	)
}

func (s *Service) EnqueueOutbound(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	taskID gid.GID,
	action SyncAction,
) error {
	link := &coredata.TaskExternalLink{}
	if err := link.LoadByTaskID(ctx, tx, scope, taskID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil
		}

		return fmt.Errorf("cannot load task external link: %w", err)
	}

	if err := s.enqueueOutboundTx(ctx, tx, scope, link, action); err != nil {
		return fmt.Errorf("cannot enqueue outbound task sync: %w", err)
	}

	return nil
}

func (s *Service) enqueueOutboundTx(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	action SyncAction,
) error {
	teamID, err := link.DestinationTeamID()
	if err != nil {
		return fmt.Errorf("cannot read Linear destination team: %w", err)
	}

	payload, err := json.Marshal(
		JobPayload{
			Action:             action,
			Provider:           link.Provider,
			TaskID:             link.TaskID,
			ExternalID:         link.ExternalID,
			ExternalIdentifier: link.ExternalIdentifier,
			TeamID:             teamID,
			ConnectorID:        link.ConnectorID,
		},
	)
	if err != nil {
		return fmt.Errorf("cannot marshal task sync payload: %w", err)
	}

	now := time.Now()
	if err := coredata.CompletePendingOutboundJobsForTask(
		ctx,
		tx,
		scope,
		link.TaskID,
		commentSyncActions(),
		now,
	); err != nil {
		return fmt.Errorf("cannot complete pending outbound task sync jobs: %w", err)
	}

	job := &coredata.TaskSyncJob{
		ID:             gid.New(scope.GetTenantID(), coredata.TaskSyncJobEntityType),
		OrganizationID: link.OrganizationID,
		Direction:      coredata.TaskSyncJobDirectionOutbound,
		Status:         coredata.TaskSyncJobStatusPending,
		Payload:        payload,
		CreatedAt:      now,
		UpdatedAt:      now,
		AttemptCount:   0,
	}

	if err := job.Insert(ctx, tx, scope); err != nil {
		return fmt.Errorf("cannot insert task sync job: %w", err)
	}

	return nil
}

func (s *Service) linearClientForConnector(
	ctx context.Context,
	scope coredata.Scoper,
	dbConnector *coredata.Connector,
) (*linear.Client, *coredata.Connector, error) {
	return s.linear.ClientForConnector(ctx, scope, dbConnector)
}

func (s *Service) oauthHTTPClient(
	ctx context.Context,
	conn *connectorprotocol.OAuth2Connection,
	provider coredata.ConnectorProvider,
) (*http.Client, error) {
	refreshCfg := s.connectorRegistry.GetOAuth2RefreshConfig(string(provider))
	if refreshCfg != nil {
		return conn.RefreshableClient(ctx, *refreshCfg)
	}

	return conn.Client(ctx)
}
