// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package integrations

import (
	"fmt"

	"github.com/slack-go/slack"
)

var statusColorMap = map[string]string{
	"READY":   "#33AE7E", // green
	"PENDING": "#d3963f", // yellow
	"FAILED":  "#96110F", // red
}

/**
Refer to https://api.slack.com/block-kit for more information on Slack blocks.
*/

// CreateUrlButton creates a Slack button block element.
func CreateUrlButton(text, url string) slack.Block {
	textObject := slack.NewTextBlockObject("plain_text", text, false, false)
	button := slack.NewButtonBlockElement("", text, textObject)
	button.URL = url
	return slack.NewActionBlock("", button)
}

// CreateSectionFromFields creates a Slack section block from a title and value.
func CreateSectionFromFields(title, value string) slack.Block {
	text := slack.NewTextBlockObject("mrkdwn", fmt.Sprintf("*%s*\n%s", title, value), false, false)
	section := slack.NewSectionBlock(text, nil, nil)
	return section
}

func GetColorForStatus(status string) string {
	if color, ok := statusColorMap[status]; ok {
		return color
	}
	return "#000000"
}
