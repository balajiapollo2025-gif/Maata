package com.maataapp.app

import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage

/** Receives pushes from the Maata server, even when the app is closed. */
class MaataMessagingService : FirebaseMessagingService() {
    override fun onNewToken(token: String) { Notifier.saveToken(this, token) }

    override fun onMessageReceived(msg: RemoteMessage) {
        val d = msg.data
        Notifier.createChannels(this)
        when (d["type"]) {
            "call" -> {
                val name = (d["title"] ?: "Maata call").replace("📞 ", "").replace("🎥 ", "").replace(" is calling…", "")
                Notifier.showIncomingCall(this, d["callId"] ?: return, name, d["kind"] ?: "voice")
            }
            "call_end" -> if (Notifier.ringingCallId(this) == d["callId"]) Notifier.stopRinging(this)
            else -> Notifier.showMessage(this, d["title"] ?: "Maata", d["body"] ?: "", d["tag"] ?: (d["type"] ?: "maata"), d["url"] ?: "/")
        }
    }
}
