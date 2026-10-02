package com.maataapp.app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import java.net.HttpURLConnection
import java.net.URL

/** "Answer" / "Decline" buttons on the call notification. */
class CallActionReceiver : BroadcastReceiver() {
    companion object {
        const val ACTION_DECLINE = "com.maataapp.app.DECLINE"

        fun decline(ctx: Context, callId: String) {
            Notifier.stopRinging(ctx)
            val token = Notifier.token(ctx)
            Thread {
                try {
                    val c = URL(BuildConfig.APP_URL + "/api/push/decline").openConnection() as HttpURLConnection
                    c.requestMethod = "POST"; c.doOutput = true; c.connectTimeout = 8000; c.readTimeout = 8000
                    c.setRequestProperty("Content-Type", "application/json")
                    val body = "{\"fcm\":\"" + token.replace("\"", "") + "\",\"callId\":\"" + callId.replace("\"", "") + "\"}"
                    c.outputStream.use { it.write(body.toByteArray()) }
                    c.responseCode; c.disconnect()
                } catch (_: Exception) { }
            }.start()
        }
    }

    override fun onReceive(ctx: Context, intent: Intent) {
        val callId = intent.getStringExtra("callId") ?: return
        when (intent.action) {
            ACTION_DECLINE -> decline(ctx, callId)
        }
    }
}
