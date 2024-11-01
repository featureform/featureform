// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package notifications

import (
	"fmt"

	help "github.com/featureform/helpers"
	"github.com/featureform/integrations"
	"github.com/featureform/logging"
)

type Notifier interface {
	ChangeNotification(resourceType, resourceName, resourceVariant, status, errorMessage string) error
	ErrorNotification(resource, error string) error
}

type SlackNotifier struct {
	channelID   string // this might eventually need to be a parameter to the methods
	slackClient integrations.SlackClient
	logger      logging.Logger
}

func (sn *SlackNotifier) ChangeNotification(resourceType, resourceName, resourceVariant, status, errorMessage string) error {
	if sn.slackClient == nil {
		sn.logger.Debug("The slack client is nil, returning nil")
		return nil
	}

	dashboardUrl, err := help.BuildDashboardUrl(
		help.GetEnv("FEATUREFORM_HOST", "localhost"),
		resourceType,
		resourceName,
		resourceVariant,
	)
	if err != nil {
		sn.logger.Errorw("error building dashboard url", "error", err)
		return err
	}

	_, _, err = sn.slackClient.PostStatusChangeMessage(
		sn.channelID,
		resourceType,
		resourceName,
		resourceVariant,
		status,
		errorMessage,
		dashboardUrl,
	)
	if err != nil {
		sn.logger.Errorw("Error posting message to Slack", "error", err)
		return err
	}

	sn.logger.Infof("Successfully posted notification to slack for: %s-%s", resourceName, resourceType)

	return nil
}

func (sn *SlackNotifier) ErrorNotification(resource, error string) error {
	if sn.slackClient == nil {
		return nil
	}
	msg := fmt.Sprintf("Resource (%s) has encountered an error: %s", resource, error)
	if _, _, err := sn.slackClient.PostSimpleMessage(sn.channelID, msg); err != nil {
		sn.logger.Errorw("Error posting message to Slack", "error", err)
		return err
	}
	return nil
}

func NewSlackNotifier(channelID string, logger logging.Logger) *SlackNotifier {
	slackToken := help.GetEnv("SLACK_API_TOKEN", "")
	var slackClient integrations.SlackClient
	if slackToken == "" {
		logger.Infow("SLACK_TOKEN not set, Slack notifications will not be sent")
	} else {
		slackClient = integrations.NewSlackClient(slackToken)
	}
	return &SlackNotifier{
		channelID:   channelID,
		logger:      logger,
		slackClient: slackClient,
	}
}
