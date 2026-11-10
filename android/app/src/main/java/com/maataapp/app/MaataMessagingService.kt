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
                val callId = d["callId"] ?: return
                val name = (d["title"] ?: "Maata call").replace("📞 ", "").replace("🎥 ", "").replace(" is calling…", "")
                val kind = d["kind"] ?: "voice"
                if (Notifier.wasEnded(this, callId)) return // hung up / answered before this push arrived
                Notifier.ackRinging(this, callId) // the caller now sees "Ringing…"
                if (MainActivity.shownCallId == callId) return // Maata is open and already showing this call
                if (MainActivity.onScreen()) {
                    // Maata is open in front of the person: give the page 3 s to show the call; if it does not, ring here
                    android.os.Handler(android.os.Looper.getMainLooper()).postDelayed({
                        if (MainActivity.shownCallId != callId && !Notifier.wasEnded(this, callId)) Notifier.showIncomingCall(this, callId, name, kind)
                    }, 3000)
                } else Notifier.showIncomingCall(this, callId, name, kind)
            }
            "call_end" -> { d["callId"]?.let { Notifier.markEnded(this, it) }; if (Notifier.ringingCallId(this) == d["callId"]) Notifier.stopRinging(this) }
            // Maata Secure: the owner rings this phone from another device; it rings even on silent
            "secure_alarm" -> { SecureAlarm.start(this); Notifier.showMessage(this, d["title"] ?: "Maata Secure alarm", d["body"] ?: "Open Maata to stop the alarm.", "maata-secure", d["url"] ?: "/?secure=1") }
            "secure_stop" -> SecureAlarm.stop(this)
            // 🛒 / 🛵 new order or new delivery: notification + spoken alert (the open app speaks by itself, so only when Maata is not on screen)
            "order_alert" -> {
                if (MainActivity.foreground) Notifier.showOrder(this, d["title"] ?: "Maata", d["body"] ?: "", d["tag"] ?: "order", d["url"] ?: "/", silent = true) // the open app rings and speaks by itself
                // Maata closed / in the background: a short foreground service rings the bell and speaks, so Android does not cut it off
                else if (!OrderAlertService.start(this, d)) {
                    Notifier.showOrder(this, d["title"] ?: "Maata", d["body"] ?: "", d["tag"] ?: "order", d["url"] ?: "/")
                    OrderVoice.speakBlocking(this, d["say"] ?: "", d["lang"] ?: "en", d["sayEn"] ?: "")
                }
            }
            // 📍 live location: the server has not heard from this phone for a few minutes (Android paused Maata) — send the place now
            "track_wake" -> TeamTrackService.wakeNow(this)
            "secure_found" -> { SecureAlarm.stop(this); Notifier.showMessage(this, d["title"] ?: "Maata Secure", d["body"] ?: "", "maata-secure", d["url"] ?: "/") }
            else -> Notifier.showMessage(this, d["title"] ?: "Maata", d["body"] ?: "", d["tag"] ?: (d["type"] ?: "maata"), d["url"] ?: "/")
        }
    }
}
