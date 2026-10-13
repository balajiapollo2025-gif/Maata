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
            // Maata Secure: the owner rings this phone from another device; it rings even on silent
            "secure_alarm" -> { SecureAlarm.start(this); Notifier.showMessage(this, d["title"] ?: "Maata Secure alarm", d["body"] ?: "Open Maata to stop the alarm.", "maata-secure", d["url"] ?: "/?secure=1") }
            "secure_stop" -> SecureAlarm.stop(this)
            "secure_found" -> { SecureAlarm.stop(this); Notifier.showMessage(this, d["title"] ?: "Maata Secure", d["body"] ?: "", "maata-secure", d["url"] ?: "/") }
            else -> Notifier.showMessage(this, d["title"] ?: "Maata", d["body"] ?: "", d["tag"] ?: (d["type"] ?: "maata"), d["url"] ?: "/")
        }
    }
}
