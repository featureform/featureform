// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package integrations

import (
	"fmt"
	"time"

	"github.com/slack-go/slack"
)

type SlackClient interface {
	PostSimpleMessage(channelID, message string) (string, string, error)
	PostStatusChangeMessage(
		channelID,
		resourceType,
		resourceName,
		resourceVariant,
		status,
		errorMessage,
		dashboardUrl string,
	) (string, string, error)
}

type SlackClientImpl struct {
	client *slack.Client
}

// PostStatusChangeMessage returns channelID, timestamp, error
func (s *SlackClientImpl) PostStatusChangeMessage(
	channelID,
	resourceType,
	resourceName,
	resourceVariant,
	status,
	errorMessage,
	dashboardUrl string,
) (string, string, error) {
	// Make Resource Type string better. i.e. TRAINING_SET_VARIANT change to TRAINING SET
	blockSet := []slack.Block{
		CreateSectionFromFields("Type", resourceType),
		CreateSectionFromFields("Resource", fmt.Sprintf("%s (%s)", resourceName, resourceVariant)),
		CreateSectionFromFields("Status", status),
	}
	if errorMessage != "" {
		errorSection := CreateSectionFromFields("Error Message", errorMessage)
		blockSet = append(blockSet, errorSection)
	}
	blockSet = append(blockSet, CreateUrlButton("View Dashboard", dashboardUrl))

	attachment := slack.Attachment{
		Color: GetColorForStatus(status),
		Blocks: slack.Blocks{
			BlockSet: blockSet,
		},
	}

	msgOptions := []slack.MsgOption{
		slack.MsgOptionAttachments(attachment),
	}

	if _, _, err := s.client.PostMessage(channelID, msgOptions...); err != nil {
		return "", "", err
	}
	return channelID, time.Now().String(), nil
}

// PostSimpleMessage returns channelID, timestamp, error
func (s *SlackClientImpl) PostSimpleMessage(channelID, message string) (string, string, error) {
	return s.client.PostMessage(channelID, slack.MsgOptionText(message, false))
}

func NewSlackClient(token string) SlackClient {
	return &SlackClientImpl{
		client: slack.New(token),
	}
}

type MockSlackClient struct {
	Messages []string
}

func (m *MockSlackClient) PostSimpleMessage(channelID, message string) (string, string, error) {
	m.Messages = append(m.Messages, message)
	nowTimestamp := time.Now()
	return channelID, nowTimestamp.String(), nil
}

func (m *MockSlackClient) PostStatusChangeMessage(
	channelID,
	resourceType,
	resourceName,
	resourceVariant,
	status,
	errorMessage,
	dashboardUrl string,
) (string, string, error) {
	// create a string with all parameters
	msg := fmt.Sprintf(
		"Resource Type: %s\n"+
			"Resource: %s (%s)\n"+
			"Status: %s\n"+
			"Error Message: %s\n"+
			"DashboardUrl: %s",
		resourceType, resourceName, resourceVariant, status, errorMessage, dashboardUrl,
	)
	m.Messages = append(m.Messages, msg)
	nowTimestamp := time.Now()
	return channelID, nowTimestamp.String(), nil
}
