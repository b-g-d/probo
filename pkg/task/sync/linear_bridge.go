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
	"time"

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/worker"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	"go.probo.inc/probo/pkg/task/sync/dates"
	linearsvc "go.probo.inc/probo/pkg/task/sync/linear"
	linear "go.probo.inc/probo/pkg/task/sync/linear/client"
)

type (
	LinearTeam                   = linearsvc.LinearTeam
	LinearSettings               = linearsvc.LinearSettings
	LinearSyncSettingsUpdate     = linearsvc.LinearSyncSettingsUpdate
	LinearIssue                  = linearsvc.LinearIssue
	LinearIssuePage              = linearsvc.LinearIssuePage
	LinearTeamPage               = linearsvc.LinearTeamPage
	LinearReconnectRequiredError = linearsvc.LinearReconnectRequiredError
)

var (
	ErrLinearNotConnected        = linearsvc.ErrLinearNotConnected
	ErrLinearReconnectRequired   = linearsvc.ErrLinearReconnectRequired
	ErrLinearTeamNotFound        = linearsvc.ErrLinearTeamNotFound
	ErrLinearTeamIDRequired      = linearsvc.ErrLinearTeamIDRequired
	ErrLinearIssueIDRequired     = linearsvc.ErrLinearIssueIDRequired
	ErrLinearIssueNotFound       = linearsvc.ErrLinearIssueNotFound
	ErrLinearDefaultTeamRequired = linearsvc.ErrLinearDefaultTeamRequired
	ErrLinearSettingsRequired    = linearsvc.ErrLinearSettingsRequired
)

func NewLinearReconnectRequiredError(scopes []string) error {
	return linearsvc.NewLinearReconnectRequiredError(scopes)
}

func LinearPublishClientMessage(err error) (string, bool) {
	return linearsvc.LinearPublishClientMessage(err)
}

func MarkdownToContent(markdown string) (string, error) {
	return linearsvc.MarkdownToContent(markdown)
}

func ContentToMarkdown(content string) (string, error) {
	return linearsvc.ContentToMarkdown(content)
}

func LinearTypeToTaskState(stateType string) coredata.TaskState {
	return linearsvc.LinearTypeToTaskState(stateType)
}

func LinearPriorityToTask(priority int) (coredata.TaskPriority, bool) {
	return linearsvc.LinearPriorityToTask(priority)
}

func ContentHash(
	name string,
	markdown string,
	state coredata.TaskState,
	priority coredata.TaskPriority,
	deadline *time.Time,
	assignedToID *gid.GID,
) string {
	return linearsvc.ContentHash(
		name,
		markdown,
		state,
		priority,
		deadline,
		assignedToID,
	)
}

func PickWorkflowStateID(
	states []linear.WorkflowState,
	taskState coredata.TaskState,
) (string, error) {
	return linearsvc.PickWorkflowStateID(states, taskState)
}

func DeadlineToDate(deadline *time.Time) *string {
	return dates.DeadlineToDate(deadline)
}

func isPublishComplete(link *coredata.TaskExternalLink) bool {
	return linearsvc.PublishComplete(link)
}

func outboundLinearPriority(
	taskPriority coredata.TaskPriority,
	destination json.RawMessage,
) (*int, error) {
	return linearsvc.OutboundLinearPriority(taskPriority, destination)
}

func isPartialLinearEmbed(err error) bool {
	return linearsvc.PartialLinearEmbed(err)
}

func linearFilesMissing(known map[string]string, content string) (bool, error) {
	return linearsvc.FilesMissing(known, content)
}

func clearLinearNoneFromDestination(destination json.RawMessage) (json.RawMessage, error) {
	return linearsvc.ClearLinearNone(destination)
}

func (s *Service) ListLinearTeams(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
) ([]LinearTeam, error) {
	return s.linear.ListLinearTeams(ctx, scope, organizationID)
}

func (s *Service) PublishToLinear(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
	teamID string,
) (*coredata.TaskExternalLink, error) {
	return s.linear.PublishToLinear(ctx, scope, taskID, teamID)
}

func (s *Service) LinkToLinear(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
	teamID string,
	issueID string,
	identityID *gid.GID,
) (*coredata.TaskExternalLink, error) {
	return s.linear.LinkToLinear(ctx, scope, taskID, teamID, issueID, identityID)
}

func (s *Service) SearchLinearTeams(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	query string,
	first int,
	after *string,
) (LinearTeamPage, error) {
	return s.linear.SearchLinearTeams(ctx, scope, organizationID, query, first, after)
}

func (s *Service) SearchLinearIssues(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	teamID string,
	query string,
	first int,
	after *string,
) (LinearIssuePage, error) {
	return s.linear.SearchLinearIssues(
		ctx,
		scope,
		organizationID,
		teamID,
		query,
		first,
		after,
	)
}

func (s *Service) LinearSettings(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
) (LinearSettings, error) {
	return s.linear.LinearSettings(ctx, scope, organizationID)
}

func (s *Service) UpdateLinearSettings(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	update LinearSyncSettingsUpdate,
) error {
	return s.linear.UpdateLinearSettings(ctx, scope, organizationID, update)
}

func (s *Service) TeamForNewTask(
	ctx context.Context,
	scope coredata.Scoper,
	organizationID gid.GID,
	explicitSet bool,
	explicitTeamID *string,
) (string, bool, error) {
	return s.linear.TeamForNewTask(ctx, scope, organizationID, explicitSet, explicitTeamID)
}

func (s *Service) EnqueueWebhook(
	ctx context.Context,
	deliveryID string,
	envelope []byte,
) (bool, error) {
	return s.linear.EnqueueWebhook(ctx, deliveryID, envelope)
}

func NewWebhookWorker(
	svc *Service,
	logger *log.Logger,
	opts ...worker.Option,
) *worker.Worker[coredata.LinearWebhookEvent] {
	return linearsvc.NewWebhookWorker(svc.linear, logger, opts...)
}

func (s *Service) linearIssueDescriptionMarkdown(
	ctx context.Context,
	scope coredata.Scoper,
	client *linear.Client,
	organizationID gid.GID,
	content string,
) (string, error) {
	return s.linear.IssueDescriptionMarkdown(ctx, scope, client, organizationID, content)
}

func (s *Service) linearCommentMarkdown(
	ctx context.Context,
	scope coredata.Scoper,
	client *linear.Client,
	organizationID gid.GID,
	content string,
) (string, error) {
	return s.linear.CommentMarkdown(ctx, scope, client, organizationID, content)
}

func (s *Service) embedLinearDescription(
	ctx context.Context,
	client *linear.Client,
	scope coredata.Scoper,
	connectorID gid.GID,
	externalID string,
	organizationID gid.GID,
	taskID gid.GID,
	markdown string,
	content string,
) (string, map[string]string, bool, error) {
	return s.linear.EmbedDescription(
		ctx,
		client,
		scope,
		connectorID,
		externalID,
		organizationID,
		taskID,
		markdown,
		content,
	)
}

func (s *Service) embedKnownLinearDescription(
	ctx context.Context,
	client *linear.Client,
	scope coredata.Scoper,
	connectorID gid.GID,
	externalID string,
	organizationID gid.GID,
	taskID gid.GID,
	markdown string,
	content string,
	known map[string]string,
) (string, map[string]string, bool, error) {
	return s.linear.EmbedKnownDescription(
		ctx,
		client,
		scope,
		connectorID,
		externalID,
		organizationID,
		taskID,
		markdown,
		content,
		known,
	)
}

func (s *Service) linearAttachments(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[string]string, error) {
	return s.linear.Attachments(ctx, scope, taskID)
}

func (s *Service) linearAssigneeID(
	ctx context.Context,
	scope coredata.Scoper,
	client *linear.Client,
	assignedToID *gid.GID,
) *string {
	return s.linear.AssigneeID(ctx, scope, client, assignedToID)
}
