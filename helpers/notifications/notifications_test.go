// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package notifications

import (
	"fmt"
	"os"
	"testing"

	"github.com/featureform/integrations"
	"github.com/featureform/logging"
	"go.uber.org/zap"
)

func TestSlackNotifier_ErrorNotification(t *testing.T) {
	mockSlackClient := &integrations.MockSlackClient{}
	sn := &SlackNotifier{
		logger:      logging.WrapZapLogger(zap.NewExample().Sugar()),
		slackClient: mockSlackClient,
	}
	if err := sn.ErrorNotification("test.default", "test error"); err != nil {
		t.Errorf("SlackNotifier.ErrorNotification() error = %v", err)
	}
	if mockSlackClient.Messages[0] != "Resource (test.default) has encountered an error: test error" {
		t.Errorf(
			"SlackNotifier.ErrorNotification() message got = %s; want = %s",
			mockSlackClient.Messages[0], "Resource test.default has encountered an error: test error",
		)
	}
}

func TestSlackNotifier_ChangeNotification(t *testing.T) {
	channelId := "CHANNEL_FUEGO"
	slackClient := &integrations.MockSlackClient{}
	logger := logging.WrapZapLogger(zap.NewExample().Sugar())
	// set the FEATUREFORM Host env var to localhost
	if err := os.Setenv("FEATUREFORM_HOST", "localhost"); err != nil {
		t.Errorf("SetEnv() error = %v", err)
	}
	type args struct {
		resourceType    string
		resourceName    string
		resourceVariant string
		status          string
		errorMessage    string
	}

	tests := []struct {
		name                 string
		args                 args
		expectedDashboardUrl string
		want                 string
	}{
		{
			name: "Label Variant",
			args: args{
				resourceType:    "LABEL_VARIANT",
				resourceName:    "label",
				resourceVariant: "variant",
				status:          "PENDING",
				errorMessage:    "",
			},
			expectedDashboardUrl: "http://localhost/labels/label?variant=variant",
		},
		{
			name: "Training Set Variant",
			args: args{
				resourceType:    "TRAINING_SET_VARIANT",
				resourceName:    "trainingset",
				resourceVariant: "variant",
				status:          "PENDING",
				errorMessage:    "",
			},
			expectedDashboardUrl: "http://localhost/training-sets/trainingset?variant=variant",
		},
		{
			name: "Source Variant",
			args: args{
				resourceType:    "SOURCE_VARIANT",
				resourceName:    "source",
				resourceVariant: "variant",
				status:          "READY",
				errorMessage:    "",
			},
			expectedDashboardUrl: "http://localhost/sources/source?variant=variant",
		},
		{
			name: "Feature Variant",
			args: args{
				resourceType:    "FEATURE_VARIANT",
				resourceName:    "feature",
				resourceVariant: "variant",
				status:          "FAILED",
				errorMessage:    "test error",
			},
			expectedDashboardUrl: "http://localhost/features/feature?variant=variant",
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				sn := &SlackNotifier{
					channelID:   channelId,
					slackClient: slackClient,
					logger:      logger,
				}
				if err := sn.ChangeNotification(
					tt.args.resourceType,
					tt.args.resourceName,
					tt.args.resourceVariant,
					tt.args.status,
					tt.args.errorMessage,
				); err != nil {
					t.Errorf("ChangeNotification() error = %v", err)
				}
				expectedMessage := buildChangeNotificationExpectedMessage(
					tt.args.resourceType,
					tt.args.resourceName,
					tt.args.resourceVariant,
					tt.args.status,
					tt.args.errorMessage,
					tt.expectedDashboardUrl,
				)
				lastMessage := slackClient.Messages[len(slackClient.Messages)-1]
				if lastMessage != expectedMessage {
					t.Errorf(
						"ChangeNotification() message got = %s; want = %s",
						slackClient.Messages[0],
						expectedMessage,
					)
				}
			},
		)
	}
}

func buildChangeNotificationExpectedMessage(
	resourceType,
	resourceName,
	resourceVariant,
	status,
	errorMessage,
	dashboardUrl string,
) string {
	return fmt.Sprintf(
		"Resource Type: %s\n"+
			"Resource: %s (%s)\n"+
			"Status: %s\n"+
			"Error Message: %s\n"+
			"DashboardUrl: %s",
		resourceType, resourceName, resourceVariant, status, errorMessage, dashboardUrl,
	)
}
