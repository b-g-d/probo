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

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/worker"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	asanasvc "go.probo.inc/probo/pkg/task/sync/asana"
	asana "go.probo.inc/probo/pkg/task/sync/asana/client"
)

type (
	AsanaSettings       = asanasvc.AsanaSettings
	AsanaSettingsUpdate = asanasvc.AsanaSettingsUpdate
	AsanaMappingUpdate  = asanasvc.AsanaMappingUpdate
	AsanaProjectMapping = asanasvc.AsanaProjectMapping
	AsanaStateInput     = asanasvc.AsanaStateInput
	AsanaPriorityInput  = asanasvc.AsanaPriorityInput
	AsanaPage           = asanasvc.AsanaPage
)

var (
	ErrAsanaNotConnected           = asanasvc.ErrAsanaNotConnected
	ErrAsanaProjectIDRequired      = asanasvc.ErrAsanaProjectIDRequired
	ErrAsanaProjectNotFound        = asanasvc.ErrAsanaProjectNotFound
	ErrAsanaTaskIDRequired         = asanasvc.ErrAsanaTaskIDRequired
	ErrAsanaTaskNotFound           = asanasvc.ErrAsanaTaskNotFound
	ErrAsanaDefaultProjectRequired = asanasvc.ErrAsanaDefaultProjectRequired
	ErrAsanaSettingsRequired       = asanasvc.ErrAsanaSettingsRequired
)

func (s *Service) ListAsanaTeams(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	query string,
	limit int,
	offset string,
) (AsanaPage, error) {
	return s.asana.ListAsanaTeams(ctx, scope, organizationID, query, limit, offset)
}

func (s *Service) ListAsanaProjects(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	teamGID string,
	query string,
	limit int,
	offset string,
) (AsanaPage, error) {
	return s.asana.ListAsanaProjects(
		ctx,
		scope,
		organizationID,
		teamGID,
		query,
		limit,
		offset,
	)
}

func (s *Service) ListAsanaTasks(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	projectGID string,
	query string,
	limit int,
	offset string,
) (AsanaPage, error) {
	return s.asana.ListAsanaTasks(
		ctx,
		scope,
		organizationID,
		projectGID,
		query,
		limit,
		offset,
	)
}

func (s *Service) ProjectForNewTask(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	explicitSet bool,
	explicitProjectID *string,
) (string, bool, error) {
	return s.asana.ProjectForNewTask(
		ctx,
		scope,
		organizationID,
		explicitSet,
		explicitProjectID,
	)
}

func (s *Service) PublishToAsana(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
	projectGID string,
) (*coredata.TaskExternalLink, error) {
	return s.asana.PublishToAsana(ctx, scope, taskID, projectGID)
}

func (s *Service) LinkToAsana(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
	projectGID string,
	remoteTaskGID string,
) (*coredata.TaskExternalLink, error) {
	return s.asana.LinkToAsana(ctx, scope, taskID, projectGID, remoteTaskGID)
}

func (s *Service) AsanaSettings(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
) (AsanaSettings, error) {
	return s.asana.AsanaSettings(ctx, scope, organizationID)
}

func (s *Service) UpdateAsanaSettings(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	update AsanaSettingsUpdate,
) error {
	return s.asana.UpdateAsanaSettings(ctx, scope, organizationID, update)
}

func (s *Service) UpdateAsanaProjectMapping(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	update AsanaMappingUpdate,
) error {
	return s.asana.UpdateAsanaProjectMapping(ctx, scope, organizationID, update)
}

func (s *Service) AsanaProjectSetup(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	projectGID string,
) (asana.ProjectSetup, *AsanaProjectMapping, error) {
	return s.asana.AsanaProjectSetup(ctx, scope, organizationID, projectGID)
}

func (s *Service) ApplyAsanaWebhook(ctx context.Context, envelope []byte) error {
	return s.asana.ApplyAsanaWebhook(ctx, envelope)
}

func (s *Service) StoreAsanaHookSecret(
	ctx context.Context,
	connectorID gid.GID,
	nonce string,
	secret string,
) error {
	return s.asana.StoreAsanaHookSecret(ctx, connectorID, nonce, secret)
}

func (s *Service) AsanaHookSecret(
	ctx context.Context,
	connectorID gid.GID,
	nonce string,
) (string, error) {
	return s.asana.AsanaHookSecret(ctx, connectorID, nonce)
}

func (s *Service) EnqueueAsanaWebhook(
	ctx context.Context,
	connectorID gid.GID,
	body []byte,
) (bool, error) {
	return s.asana.EnqueueAsanaWebhook(ctx, connectorID, body)
}

func NewAsanaWebhookWorker(
	svc *Service,
	logger *log.Logger,
	opts ...worker.Option,
) *worker.Worker[coredata.AsanaWebhookEvent] {
	return asanasvc.NewAsanaWebhookWorker(svc.asana, logger, opts...)
}

func (h *outboundHandler) handleAsana(
	ctx context.Context,
	item *coredata.TaskSyncJob,
	payload JobPayload,
) error {
	return asanasvc.HandleOutbound(
		h.svc.asana,
		asanasvc.OutboundHooks{
			Newer:  h.hasNewerOutboundJob,
			Upsert: h.loadCommentUpsert,
			Link:   loadCurrentOutboundLink,
			Done:   h.succeed,
		},
		ctx,
		item,
		payload,
	)
}
