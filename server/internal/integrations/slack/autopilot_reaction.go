package slack

import (
	"context"
	"errors"

	slackapi "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type AutopilotReactionEvent struct {
	AppID     string `json:"app_id"`
	TeamID    string `json:"team_id"`
	ChannelID string `json:"channel_id"`
	UserID    string `json:"user_id"`
	MessageTS string `json:"message_ts"`
	Reaction  string `json:"reaction"`
	EventTS   string `json:"event_ts"`
}

type AutopilotReactionHandler interface {
	HandleAutopilotReaction(context.Context, AutopilotReactionEvent) (bool, error)
}

type AutopilotReactionTarget struct {
	ThreadTS string
	Human    bool
	Exists   bool
}

type autopilotReactionClient interface {
	GetUserInfoContext(context.Context, string) (*slackapi.User, error)
	GetReactionsContext(context.Context, slackapi.ItemRef, slackapi.GetReactionsParameters) (slackapi.ReactedItem, error)
}

func IsCompletionReaction(reaction string) bool {
	return reaction == "white_check_mark" || reaction == "heavy_check_mark" || reaction == "checkmark"
}

func autopilotReactionFrom(e slackevents.EventsAPIEvent, botUserID string) (AutopilotReactionEvent, bool) {
	reaction, ok := e.InnerEvent.Data.(*slackevents.ReactionAddedEvent)
	if !ok || reaction == nil || !IsCompletionReaction(reaction.Reaction) || reaction.Item.Type != "message" ||
		!autopilotTeamID.MatchString(e.TeamID) || !autopilotChannelID.MatchString(reaction.Item.Channel) ||
		!autopilotTimestamp.MatchString(reaction.Item.Timestamp) || !autopilotTimestamp.MatchString(reaction.EventTimestamp) ||
		e.APIAppID == "" || reaction.User == "" || reaction.User == botUserID {
		return AutopilotReactionEvent{}, false
	}
	return AutopilotReactionEvent{AppID: e.APIAppID, TeamID: e.TeamID, ChannelID: reaction.Item.Channel,
		UserID: reaction.User, MessageTS: reaction.Item.Timestamp, Reaction: reaction.Reaction, EventTS: reaction.EventTimestamp}, true
}

// ResolveAutopilotReaction runs off the Socket Mode ACK path. The event's user
// is the reacting actor; item_user is the message author and cannot authorize it.
func ResolveAutopilotReaction(ctx context.Context, installation db.ChannelInstallation, decrypt Decrypter, event AutopilotReactionEvent) (AutopilotReactionTarget, error) {
	creds, err := decodeCredentials(installation.Config, decrypt)
	if err != nil {
		return AutopilotReactionTarget{}, err
	}
	return resolveAutopilotReaction(ctx, slackapi.New(creds.BotToken), creds.BotUserID, event)
}

func resolveAutopilotReaction(ctx context.Context, api autopilotReactionClient, botUserID string, event AutopilotReactionEvent) (AutopilotReactionTarget, error) {
	user, err := api.GetUserInfoContext(ctx, event.UserID)
	if err != nil {
		return AutopilotReactionTarget{}, err
	}
	if user == nil || user.ID != event.UserID || user.IsBot || user.IsAppUser || user.Deleted || user.ID == botUserID {
		return AutopilotReactionTarget{}, nil
	}
	item, err := api.GetReactionsContext(ctx, slackapi.ItemRef{Channel: event.ChannelID, Timestamp: event.MessageTS}, slackapi.GetReactionsParameters{})
	if err != nil {
		if err.Error() == "message_not_found" || err.Error() == "channel_not_found" {
			return AutopilotReactionTarget{}, nil
		}
		return AutopilotReactionTarget{}, err
	}
	if item.Type != "message" || item.Channel != event.ChannelID || item.Message == nil || item.Message.Timestamp != event.MessageTS {
		return AutopilotReactionTarget{}, nil
	}
	root := item.Message.ThreadTimestamp
	if root == "" {
		root = item.Message.Timestamp
	}
	if !autopilotTimestamp.MatchString(root) {
		return AutopilotReactionTarget{}, errors.New("Slack returned an invalid reaction thread timestamp")
	}
	return AutopilotReactionTarget{ThreadTS: root, Human: true, Exists: true}, nil
}
