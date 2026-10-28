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
	"strings"
	"time"

	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/pkg/baseurl"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	"go.probo.inc/probo/pkg/mail"
	"go.probo.inc/probo/pkg/page"
	"go.probo.inc/probo/pkg/prosemirror"
	"go.probo.inc/probo/pkg/task/sync/commenthash"
	"go.probo.inc/probo/pkg/task/sync/commentstore"
	linear "go.probo.inc/probo/pkg/task/sync/linear/client"
	"go.probo.inc/probo/pkg/webhook"
	webhooktypes "go.probo.inc/probo/pkg/webhook/types"
)

func CommentContentHash(markdown string) string {
	return commenthash.Sum(markdown)
}

func deleteSyncedComment(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	mapping *coredata.TaskCommentExternalLink,
) error {
	return commentstore.Delete(ctx, tx, scope, mapping)
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
	taskComment := &coredata.TaskComment{}
	if err := taskComment.LoadByID(ctx, tx, scope, mapping.TaskCommentID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil
		}

		return fmt.Errorf("cannot load task comment: %w", err)
	}

	previous := *taskComment
	now := time.Now()
	taskComment.Content = content
	taskComment.UpdatedAt = now

	if err := taskComment.Update(ctx, tx, scope); err != nil {
		return fmt.Errorf("cannot update task comment: %w", err)
	}

	mapping.ContentHash = &hash
	mapping.RemoteUpdatedAt = remoteUpdatedAt
	mapping.UpdatedAt = now

	if err := mapping.Update(ctx, tx, scope); err != nil {
		return fmt.Errorf("cannot update task comment external link: %w", err)
	}

	if err := webhook.InsertUpdateData(
		ctx,
		tx,
		scope,
		taskComment.OrganizationID,
		coredata.WebhookEventTypeTaskCommentUpdated,
		webhooktypes.NewTaskComment(taskComment),
		webhooktypes.NewTaskComment(&previous),
	); err != nil {
		return fmt.Errorf("cannot emit task comment updated webhook: %w", err)
	}

	if err := s.notifySyncedCommentMentions(
		ctx,
		tx,
		scope,
		taskComment.TaskID,
		previous.Content,
		taskComment.Content,
		actorProfileID,
	); err != nil {
		return fmt.Errorf("cannot notify synced comment mentions: %w", err)
	}

	return nil
}

func (s *Service) EnqueueCommentOutbound(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	taskID gid.GID,
	commentID gid.GID,
) error {
	link := &coredata.TaskExternalLink{}
	if err := link.LoadByTaskID(ctx, tx, scope, taskID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil
		}

		return fmt.Errorf("cannot load task external link: %w", err)
	}

	if !isPublishComplete(link) {
		return nil
	}

	teamID, err := link.DestinationTeamID()
	if err != nil {
		return fmt.Errorf("cannot read Linear destination team: %w", err)
	}

	payload, err := json.Marshal(
		JobPayload{
			Action:             SyncActionCommentUpsert,
			Provider:           link.Provider,
			TaskID:             link.TaskID,
			ExternalID:         link.ExternalID,
			ExternalIdentifier: link.ExternalIdentifier,
			TeamID:             teamID,
			ConnectorID:        link.ConnectorID,
			CommentID:          commentID,
		},
	)
	if err != nil {
		return fmt.Errorf("cannot marshal task comment sync payload: %w", err)
	}

	now := time.Now()
	if err := coredata.CompletePendingOutboundCommentJobs(
		ctx,
		tx,
		scope,
		link.TaskID,
		commentID,
		[]string{string(SyncActionCommentUpsert)},
		now,
	); err != nil {
		return fmt.Errorf("cannot complete pending comment sync jobs: %w", err)
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
		return fmt.Errorf("cannot insert task comment sync job: %w", err)
	}

	return nil
}

func (s *Service) EnqueueCommentDelete(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	taskID gid.GID,
	commentID gid.GID,
) error {
	mapping := &coredata.TaskCommentExternalLink{}
	if err := mapping.LoadByCommentID(ctx, tx, scope, commentID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil
		}

		return fmt.Errorf("cannot load task comment external link: %w", err)
	}

	link := &coredata.TaskExternalLink{}
	if err := link.LoadByTaskID(ctx, tx, scope, taskID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return nil
		}

		return fmt.Errorf("cannot load task external link: %w", err)
	}

	if !isPublishComplete(link) {
		return nil
	}

	teamID, err := link.DestinationTeamID()
	if err != nil {
		return fmt.Errorf("cannot read Linear destination team: %w", err)
	}

	payload, err := json.Marshal(
		JobPayload{
			Action:             SyncActionCommentDelete,
			Provider:           link.Provider,
			TaskID:             link.TaskID,
			ExternalID:         link.ExternalID,
			ExternalIdentifier: link.ExternalIdentifier,
			TeamID:             teamID,
			ConnectorID:        link.ConnectorID,
			CommentID:          commentID,
			ExternalCommentID:  mapping.ExternalID,
		},
	)
	if err != nil {
		return fmt.Errorf("cannot marshal task comment delete payload: %w", err)
	}

	now := time.Now()
	if err := coredata.CompletePendingOutboundCommentJobs(
		ctx,
		tx,
		scope,
		link.TaskID,
		commentID,
		commentSyncActions(),
		now,
	); err != nil {
		return fmt.Errorf("cannot complete pending comment sync jobs: %w", err)
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
		return fmt.Errorf("cannot insert task comment delete job: %w", err)
	}

	return nil
}

func (h *outboundHandler) processCommentDelete(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
	client *linear.Client,
) error {
	if payload.ExternalCommentID == "" {
		return h.succeed(ctx, item)
	}

	if err := client.DeleteComment(ctx, payload.ExternalCommentID); err != nil {
		if errors.Is(err, linear.ErrCommentNotFound) {
			return h.succeed(ctx, item)
		}

		return fmt.Errorf("cannot delete Linear comment: %w", err)
	}

	return h.succeed(ctx, item)
}

func (h *outboundHandler) processCommentUpsert(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
	scope coredata.Scoper,
	client *linear.Client,
) error {
	sent := false

	for range maxOutboundRevisionAttempts {
		comment, mapping, skip, err := h.loadCommentUpsert(ctx, item, payload, scope, sent)
		if err != nil {
			return err
		}

		if skip {
			return h.succeed(ctx, item)
		}

		canonical, err := ContentToMarkdown(comment.Content)
		if err != nil {
			return fmt.Errorf("cannot convert task comment: %w", err)
		}

		if strings.TrimSpace(canonical) == "" {
			return h.succeed(ctx, item)
		}

		hash := CommentContentHash(canonical)
		loaded := false

		var known map[string]string

		if mapping != nil && mapping.ContentHash != nil && *mapping.ContentHash == hash {
			ids, err := prosemirror.FileIDs(comment.Content)
			if err != nil {
				return fmt.Errorf("cannot read comment attachment file ids: %w", err)
			}

			if len(ids) == 0 {
				return h.succeed(ctx, item)
			}

			known, err = h.svc.linearAttachments(ctx, scope, payload.TaskID)
			if err != nil {
				return fmt.Errorf("cannot load Linear attachments: %w", err)
			}

			loaded = true

			missing, err := linearFilesMissing(known, comment.Content)
			if err != nil {
				return fmt.Errorf("cannot read comment attachment file ids: %w", err)
			}

			if !missing {
				return h.succeed(ctx, item)
			}
		}

		markdown, err := h.svc.linearCommentMarkdown(
			ctx,
			scope,
			client,
			comment.OrganizationID,
			comment.Content,
		)
		if err != nil {
			return fmt.Errorf("cannot convert task comment mentions: %w", err)
		}

		var description string

		if loaded {
			description, _, _, err = h.svc.embedKnownLinearDescription(
				ctx,
				client,
				scope,
				payload.ConnectorID,
				payload.ExternalID,
				comment.OrganizationID,
				payload.TaskID,
				markdown,
				comment.Content,
				known,
			)
		} else {
			description, _, _, err = h.svc.embedLinearDescription(
				ctx,
				client,
				scope,
				payload.ConnectorID,
				payload.ExternalID,
				comment.OrganizationID,
				payload.TaskID,
				markdown,
				comment.Content,
			)
		}

		if err != nil {
			return err
		}

		created := mapping == nil

		var remote *linear.Comment

		if created {
			author, err := h.svc.commentAuthor(ctx, scope, comment.OwnerID)
			if err != nil {
				return fmt.Errorf("cannot load Linear comment author: %w", err)
			}

			remote, err = client.CreateComment(ctx, payload.ExternalID, description, author)
			if err != nil {
				return fmt.Errorf("cannot create Linear comment: %w", err)
			}
		} else {
			remote, err = client.UpdateComment(ctx, mapping.ExternalID, description)
			if err != nil {
				return fmt.Errorf("cannot update Linear comment: %w", err)
			}
		}

		sent = true

		stable, discardID, err := h.finishCommentUpsert(
			ctx,
			payload,
			scope,
			comment.ID,
			remote,
			hash,
			created,
		)
		if err != nil {
			return err
		}

		if discardID != "" {
			if err := discardLinearComment(ctx, client, discardID); err != nil {
				return err
			}
		}

		if stable {
			return h.succeed(ctx, item)
		}
	}

	return fmt.Errorf("task comment changed during Linear update")
}

func (h *outboundHandler) loadCommentUpsert(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
	scope coredata.Scoper,
	sent bool,
) (*coredata.TaskComment, *coredata.TaskCommentExternalLink, bool, error) {
	var (
		comment *coredata.TaskComment
		mapping *coredata.TaskCommentExternalLink
		skip    bool
	)

	err := h.pg.WithTx(
		ctx,
		func(ctx context.Context, tx pg.Tx) error {
			comment = &coredata.TaskComment{}
			if err := comment.LoadByID(ctx, tx, scope, payload.CommentID); err != nil {
				if errors.Is(err, coredata.ErrResourceNotFound) {
					skip = true

					return nil
				}

				return fmt.Errorf("cannot load task comment: %w", err)
			}

			link, err := loadCurrentOutboundLink(ctx, tx, scope, payload.TaskID)
			if err != nil {
				return err
			}

			if !outboundLinkMatchesJob(link, payload) {
				skip = true

				return nil
			}

			if !sent {
				newer, err := item.HasNewerOutboundCommentJob(
					ctx,
					tx,
					scope,
					payload.TaskID,
					payload.CommentID,
					commentSyncActions(),
				)
				if err != nil {
					return err
				}

				if newer {
					skip = true

					return nil
				}
			}

			existing := &coredata.TaskCommentExternalLink{}

			err = existing.LoadByCommentID(ctx, tx, scope, comment.ID)
			if errors.Is(err, coredata.ErrResourceNotFound) {
				return nil
			}

			if err != nil {
				return fmt.Errorf("cannot load task comment external link: %w", err)
			}

			mapping = existing

			return nil
		},
	)
	if err != nil {
		return nil, nil, false, err
	}

	return comment, mapping, skip, nil
}

func (h *outboundHandler) finishCommentUpsert(
	ctx context.Context,
	payload JobPayload,
	scope coredata.Scoper,
	commentID gid.GID,
	remote *linear.Comment,
	sentHash string,
	created bool,
) (bool, string, error) {
	var (
		stable    bool
		discardID string
	)

	err := h.pg.WithTx(
		ctx,
		func(ctx context.Context, tx pg.Tx) error {
			comment := &coredata.TaskComment{}
			if err := comment.LoadByIDForUpdate(ctx, tx, scope, commentID); err != nil {
				if errors.Is(err, coredata.ErrResourceNotFound) {
					stable = true

					if created {
						discardID = remote.ID
					}

					return nil
				}

				return fmt.Errorf("cannot load task comment: %w", err)
			}

			rendered, err := ContentToMarkdown(comment.Content)
			if err != nil {
				return fmt.Errorf("cannot convert task comment: %w", err)
			}

			if strings.TrimSpace(rendered) == "" {
				stable = true

				if created {
					discardID = remote.ID
				}

				return nil
			}

			if CommentContentHash(rendered) != sentHash {
				if created {
					discardID = remote.ID
				}

				return nil
			}

			link, err := loadCurrentOutboundLink(ctx, tx, scope, payload.TaskID)
			if err != nil {
				return err
			}

			if !outboundLinkMatchesJob(link, payload) {
				stable = true

				if created {
					discardID = remote.ID
				}

				return nil
			}

			existing := &coredata.TaskCommentExternalLink{}

			err = existing.LoadByCommentID(ctx, tx, scope, commentID)
			if errors.Is(err, coredata.ErrResourceNotFound) {
				if !created {
					stable = true

					return nil
				}

				createdLink := newCommentLink(link, commentID, remote, sentHash, time.Now())
				if err := createdLink.Insert(ctx, tx, scope); err != nil {
					if errors.Is(err, coredata.ErrResourceAlreadyExists) {
						discardID = remote.ID

						return nil
					}

					return fmt.Errorf("cannot insert task comment external link: %w", err)
				}

				stable = true

				return nil
			}

			if err != nil {
				return fmt.Errorf("cannot load task comment external link: %w", err)
			}

			if created {
				discardID = remote.ID
				stable = existing.ContentHash != nil && *existing.ContentHash == sentHash

				return nil
			}

			if err := saveCommentLinkTx(ctx, tx, scope, link, commentID, remote, sentHash); err != nil {
				return err
			}

			stable = true

			return nil
		},
	)
	if err != nil {
		return false, "", err
	}

	return stable, discardID, nil
}

func discardLinearComment(ctx context.Context, client *linear.Client, commentID string) error {
	if err := client.DeleteComment(ctx, commentID); err != nil && !errors.Is(err, linear.ErrCommentNotFound) {
		return fmt.Errorf("cannot delete stale Linear comment: %w", err)
	}

	return nil
}

func (s *Service) listTaskComments(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) ([]*coredata.TaskComment, error) {
	var comments []*coredata.TaskComment

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			loaded, err := loadTaskComments(ctx, conn, scope, taskID)
			if err != nil {
				return err
			}

			comments = loaded

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return comments, nil
}

func (s *Service) commentLinksByCommentID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[gid.GID]*coredata.TaskCommentExternalLink, error) {
	links, err := s.loadCommentLinks(ctx, scope, taskID)
	if err != nil {
		return nil, err
	}

	byID := make(map[gid.GID]*coredata.TaskCommentExternalLink, len(links))
	for _, link := range links {
		byID[link.TaskCommentID] = link
	}

	return byID, nil
}

func (s *Service) commentLinksByExternalID(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[string]*coredata.TaskCommentExternalLink, error) {
	links, err := s.loadCommentLinks(ctx, scope, taskID)
	if err != nil {
		return nil, err
	}

	byID := make(map[string]*coredata.TaskCommentExternalLink, len(links))
	for _, link := range links {
		byID[link.ExternalID] = link
	}

	return byID, nil
}

func (s *Service) loadCommentLinks(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (coredata.TaskCommentExternalLinks, error) {
	var links coredata.TaskCommentExternalLinks

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			loaded, err := page.LoadAll(
				ctx,
				page.OrderBy[coredata.TaskCommentExternalLinkOrderField]{
					Field:     coredata.TaskCommentExternalLinkOrderFieldTaskCommentID,
					Direction: page.OrderDirectionAsc,
				},
				func(
					ctx context.Context,
					cursor *page.Cursor[coredata.TaskCommentExternalLinkOrderField],
				) ([]*coredata.TaskCommentExternalLink, error) {
					var batch coredata.TaskCommentExternalLinks
					if err := batch.LoadByTaskID(ctx, conn, scope, taskID, cursor); err != nil {
						return nil, err
					}

					return batch, nil
				},
			)
			if err != nil {
				return fmt.Errorf("cannot load task comment external links: %w", err)
			}

			links = loaded

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return links, nil
}

func (s *Service) saveCommentLink(
	ctx context.Context,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	commentID gid.GID,
	remote *linear.Comment,
	hash string,
) error {
	return s.pg.WithTx(
		ctx,
		func(ctx context.Context, tx pg.Tx) error {
			return saveCommentLinkTx(ctx, tx, scope, link, commentID, remote, hash)
		},
	)
}

func saveCommentLinkTx(
	ctx context.Context,
	tx pg.Tx,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	commentID gid.GID,
	remote *linear.Comment,
	hash string,
) error {
	now := time.Now()
	existing := &coredata.TaskCommentExternalLink{}

	err := existing.LoadByCommentID(ctx, tx, scope, commentID)
	if errors.Is(err, coredata.ErrResourceNotFound) {
		created := newCommentLink(link, commentID, remote, hash, now)
		if err := created.Insert(ctx, tx, scope); err != nil && !errors.Is(err, coredata.ErrResourceAlreadyExists) {
			return fmt.Errorf("cannot insert task comment external link: %w", err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("cannot load task comment external link: %w", err)
	}

	existing.ExternalID = remote.ID

	existing.ContentHash = &hash
	if !remote.UpdatedAt.IsZero() {
		existing.RemoteUpdatedAt = &remote.UpdatedAt
	}

	existing.UpdatedAt = now

	if err := existing.Update(ctx, tx, scope); err != nil {
		return fmt.Errorf("cannot update task comment external link: %w", err)
	}

	return nil
}

func newCommentLink(
	link *coredata.TaskExternalLink,
	commentID gid.GID,
	remote *linear.Comment,
	hash string,
	now time.Time,
) *coredata.TaskCommentExternalLink {
	commentLink := &coredata.TaskCommentExternalLink{
		TaskCommentID:  commentID,
		OrganizationID: link.OrganizationID,
		TaskID:         link.TaskID,
		ConnectorID:    link.ConnectorID,
		Provider:       coredata.ConnectorProviderLinearSync,
		ExternalID:     remote.ID,
		ContentHash:    &hash,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if !remote.UpdatedAt.IsZero() {
		commentLink.RemoteUpdatedAt = &remote.UpdatedAt
	}

	return commentLink
}

func (s *Service) commentAuthor(
	ctx context.Context,
	scope coredata.Scoper,
	ownerID *gid.GID,
) (linear.CommentAuthor, error) {
	if ownerID == nil {
		return linear.CommentAuthor{}, nil
	}

	var author linear.CommentAuthor

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			resolved, err := loadCommentAuthor(ctx, conn, scope, s.baseURL, *ownerID)
			if err != nil {
				return err
			}

			author = resolved

			return nil
		},
	)
	if err != nil {
		return linear.CommentAuthor{}, fmt.Errorf("cannot load comment author: %w", err)
	}

	return author, nil
}

func loadCommentAuthor(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	baseURL string,
	ownerID gid.GID,
) (linear.CommentAuthor, error) {
	profile := &coredata.MembershipProfile{}
	if err := profile.LoadByID(ctx, conn, scope, ownerID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return linear.CommentAuthor{}, nil
		}

		return linear.CommentAuthor{}, fmt.Errorf("cannot load comment author profile: %w", err)
	}

	name := strings.TrimSpace(profile.FullName)
	if name == "" {
		return linear.CommentAuthor{}, nil
	}

	author := linear.CommentAuthor{Name: name}

	identity := &coredata.Identity{}
	if err := identity.LoadByID(ctx, conn, profile.IdentityID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return author, nil
		}

		return linear.CommentAuthor{}, fmt.Errorf("cannot load comment author identity: %w", err)
	}

	if identity.AvatarFileID == nil {
		return author, nil
	}

	file := &coredata.File{}
	if err := file.LoadPublicByID(ctx, conn, *identity.AvatarFileID); err != nil {
		if errors.Is(err, coredata.ErrResourceNotFound) {
			return author, nil
		}

		return linear.CommentAuthor{}, fmt.Errorf("cannot load comment author avatar: %w", err)
	}

	avatarURL, err := publicAvatarURL(baseURL, file.ID)
	if err != nil {
		return linear.CommentAuthor{}, fmt.Errorf("cannot resolve comment author avatar URL: %w", err)
	}

	author.AvatarURL = avatarURL

	return author, nil
}

func publicAvatarURL(baseURL string, fileID gid.GID) (string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return "", nil
	}

	base, err := baseurl.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("cannot parse comment author avatar base URL: %w", err)
	}

	avatarURL, err := base.WithPath("/api/files/v1/public/" + fileID.String()).String()
	if err != nil {
		return "", fmt.Errorf("cannot build comment author avatar URL: %w", err)
	}

	return avatarURL, nil
}

func (s *Service) resolveCommentOwner(
	ctx context.Context,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	email string,
) (*gid.GID, error) {
	var ownerID *gid.GID

	err := s.pg.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			id, err := resolveCommentOwnerTx(ctx, conn, scope, link, email)
			if err != nil {
				return err
			}

			ownerID = id

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return ownerID, nil
}

func resolveCommentOwnerTx(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	link *coredata.TaskExternalLink,
	email string,
) (*gid.GID, error) {
	email = strings.TrimSpace(email)
	if email != "" {
		addr, err := mail.ParseAddr(email)
		if err == nil {
			profile := &coredata.MembershipProfile{}

			err = profile.LoadByOrganizationIDAndEmail(ctx, conn, scope, link.OrganizationID, addr)
			if err == nil {
				return &profile.ID, nil
			}

			if !errors.Is(err, coredata.ErrResourceNotFound) {
				return nil, fmt.Errorf("cannot load comment owner: %w", err)
			}
		}
	}

	return nil, nil
}

func deleteCommentLinks(
	ctx context.Context,
	conn pg.Querier,
	scope coredata.Scoper,
	taskID gid.GID,
) error {
	var links coredata.TaskCommentExternalLinks
	if err := links.DeleteByTaskID(ctx, conn, scope, taskID); err != nil {
		return fmt.Errorf("cannot delete task comment external links: %w", err)
	}

	return nil
}
