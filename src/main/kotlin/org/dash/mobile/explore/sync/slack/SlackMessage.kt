package org.dash.mobile.explore.sync.slack

import com.google.gson.annotations.SerializedName

// https://api.slack.com/methods/chat.postMessage
data class SlackMessage(
    private val text: String,
    @SerializedName("icon_emoji")
    private val iconEmoji: String,
    @SerializedName("as_user")
    private val asUser: Boolean
)
