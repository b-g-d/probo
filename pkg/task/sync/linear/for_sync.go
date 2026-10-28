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
	"encoding/json"

	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
	client "go.probo.inc/probo/pkg/task/sync/linear/client"
)

func PublishComplete(link *coredata.TaskExternalLink) bool {
	return isPublishComplete(link)
}

func OutboundLinearPriority(
	taskPriority coredata.TaskPriority,
	destination json.RawMessage,
) (*int, error) {
	return outboundLinearPriority(taskPriority, destination)
}

func PartialLinearEmbed(err error) bool {
	return isPartialLinearEmbed(err)
}

func FilesMissing(known map[string]string, content string) (bool, error) {
	return linearFilesMissing(known, content)
}

func ClearLinearNone(destination json.RawMessage) (json.RawMessage, error) {
	return clearLinearNoneFromDestination(destination)
}

func (s *Service) IssueDescriptionMarkdown(
	ctx context.Context,
	scope coredata.Scoper,
	api *client.Client,
	organizationID gid.GID,
	content string,
) (string, error) {
	return s.linearIssueDescriptionMarkdown(ctx, scope, api, organizationID, content)
}

func (s *Service) CommentMarkdown(
	ctx context.Context,
	scope coredata.Scoper,
	api *client.Client,
	organizationID gid.GID,
	content string,
) (string, error) {
	return s.linearCommentMarkdown(ctx, scope, api, organizationID, content)
}

func (s *Service) EmbedDescription(
	ctx context.Context,
	api *client.Client,
	scope coredata.Scoper,
	connectorID gid.GID,
	externalID string,
	organizationID gid.GID,
	taskID gid.GID,
	markdown string,
	content string,
) (string, map[string]string, bool, error) {
	return s.embedLinearDescription(
		ctx,
		api,
		scope,
		connectorID,
		externalID,
		organizationID,
		taskID,
		markdown,
		content,
	)
}

func (s *Service) EmbedKnownDescription(
	ctx context.Context,
	api *client.Client,
	scope coredata.Scoper,
	connectorID gid.GID,
	externalID string,
	organizationID gid.GID,
	taskID gid.GID,
	markdown string,
	content string,
	known map[string]string,
) (string, map[string]string, bool, error) {
	return s.embedKnownLinearDescription(
		ctx,
		api,
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

func (s *Service) Attachments(
	ctx context.Context,
	scope coredata.Scoper,
	taskID gid.GID,
) (map[string]string, error) {
	return s.linearAttachments(ctx, scope, taskID)
}

func (s *Service) AssigneeID(
	ctx context.Context,
	scope coredata.Scoper,
	api *client.Client,
	assignedToID *gid.GID,
) *string {
	return s.linearAssigneeID(ctx, scope, api, assignedToID)
}
