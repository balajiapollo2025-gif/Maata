package com.maataapp.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.media.AudioAttributes
import android.media.RingtoneManager
import android.net.Uri
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.app.Person
import androidx.core.content.ContextCompat

/** All Maata notifications: messages, and incoming calls that ring like a phone call. */
object Notifier {
    const val CH_CALLS = "maata_calls_v1"
    const val CH_MSGS = "maata_messages_v1"
    const val CALL_NOTIF_ID = 4242
    private const val PREFS = "maata"

    fun createChannels(ctx: Context) {
        val nm = ctx.getSystemService(NotificationManager::class.java)
        val ringtone: Uri = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE)
        val calls = NotificationChannel(CH_CALLS, "Calls", NotificationManager.IMPORTANCE_HIGH).apply {
            description = "Incoming Maata voice and video calls"
            setSound(ringtone, AudioAttributes.Builder()
                .setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE)
                .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION).build())
            enableVibration(true)
            vibrationPattern = longArrayOf(0, 1000, 800, 1000, 800, 1000)
            lockscreenVisibility = Notification.VISIBILITY_PUBLIC
            setBypassDnd(false)
        }
        val msgs = NotificationChannel(CH_MSGS, "Messages", NotificationManager.IMPORTANCE_HIGH).apply {
            description = "New messages, missed calls and updates"
            enableVibration(true)
        }
        nm.createNotificationChannel(calls)
        nm.createNotificationChannel(msgs)
    }

    private fun prefs(ctx: Context) = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
    fun saveToken(ctx: Context, t: String) = prefs(ctx).edit().putString("fcm", t).apply()
    fun token(ctx: Context): String = prefs(ctx).getString("fcm", "") ?: ""

    fun openAppIntent(ctx: Context, url: String, req: Int): PendingIntent {
        val i = Intent(ctx, MainActivity::class.java).apply {
            putExtra(MainActivity.EXTRA_URL, url)
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
        }
        return PendingIntent.getActivity(ctx, req, i, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    }

    private fun canPost(ctx: Context): Boolean =
        Build.VERSION.SDK_INT < 33 || ContextCompat.checkSelfPermission(ctx, android.Manifest.permission.POST_NOTIFICATIONS) == android.content.pm.PackageManager.PERMISSION_GRANTED

    // ---------- incoming call ----------
    fun ringingCallId(ctx: Context): String? {
        val p = prefs(ctx); val id = p.getString("ringing", null) ?: return null
        return if (System.currentTimeMillis() - p.getLong("ringingAt", 0) < 60_000) id else null
    }

    fun showIncomingCall(ctx: Context, callId: String, caller: String, kind: String) {
        if (ringingCallId(ctx) == callId) return // already ringing for this call
        prefs(ctx).edit().putString("ringing", callId).putLong("ringingAt", System.currentTimeMillis())
            .putString("ringingName", caller).putString("ringingKind", kind).apply()
        if (!canPost(ctx)) return

        val full = Intent(ctx, IncomingCallActivity::class.java).apply {
            putExtra("callId", callId); putExtra("caller", caller); putExtra("kind", kind)
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_NO_USER_ACTION
        }
        val fullPi = PendingIntent.getActivity(ctx, 1, full, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        // Answer opens Maata directly (Android 12+ does not allow opening apps through a middle step)
        val answerPi = PendingIntent.getActivity(ctx, 2, Intent(ctx, MainActivity::class.java).apply {
            putExtra(MainActivity.EXTRA_URL, "/#answer=$callId"); putExtra(MainActivity.EXTRA_STOP_RING, true)
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
        }, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        val declinePi = actionIntent(ctx, CallActionReceiver.ACTION_DECLINE, callId, 3)
        val who = Person.Builder().setName(caller).setImportant(true).build()
        val video = kind == "video"

        val b = NotificationCompat.Builder(ctx, CH_CALLS)
            .setSmallIcon(R.drawable.ic_notification)
            .setColor(0xFF0F4C5C.toInt())
            .setContentTitle(caller)
            .setContentText(if (video) "Incoming Maata video call" else "Incoming Maata voice call")
            .setCategory(NotificationCompat.CATEGORY_CALL)
            .setPriority(NotificationCompat.PRIORITY_MAX)
            .setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .setOngoing(true)
            .setAutoCancel(false)
            .setTimeoutAfter(45_000)
            .setFullScreenIntent(fullPi, true)
            .setContentIntent(fullPi)
        try {
            b.setStyle(NotificationCompat.CallStyle.forIncomingCall(who, declinePi, answerPi).setIsVideo(video))
        } catch (e: Exception) {
            b.addAction(0, "Decline", declinePi).addAction(0, "Answer", answerPi)
        }
        val n = b.build()
        n.flags = n.flags or Notification.FLAG_INSISTENT // keep ringing until answered, declined or 45 s
        try { NotificationManagerCompat.from(ctx).notify(CALL_NOTIF_ID, n) } catch (_: SecurityException) {}
    }

    private fun actionIntent(ctx: Context, action: String, callId: String, req: Int): PendingIntent {
        val i = Intent(ctx, CallActionReceiver::class.java).apply { this.action = action; putExtra("callId", callId) }
        return PendingIntent.getBroadcast(ctx, req, i, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    }

    fun stopRinging(ctx: Context) {
        prefs(ctx).edit().remove("ringing").apply()
        NotificationManagerCompat.from(ctx).cancel(CALL_NOTIF_ID)
        IncomingCallActivity.current?.finishSoon()
    }

    // ---------- messages, missed calls ----------
    fun showMessage(ctx: Context, title: String, body: String, tag: String, url: String) {
        if (!canPost(ctx)) return
        val n = NotificationCompat.Builder(ctx, CH_MSGS)
            .setSmallIcon(R.drawable.ic_notification)
            .setColor(0xFF0F4C5C.toInt())
            .setContentTitle(title)
            .setContentText(body)
            .setStyle(NotificationCompat.BigTextStyle().bigText(body))
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setCategory(NotificationCompat.CATEGORY_MESSAGE)
            .setAutoCancel(true)
            .setContentIntent(openAppIntent(ctx, url, tag.hashCode()))
            .build()
        try { NotificationManagerCompat.from(ctx).notify(tag, 1, n) } catch (_: SecurityException) {}
    }
}
