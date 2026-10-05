package com.maataapp.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.media.AudioManager
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager

/**
 * Runs while a Maata call is live. Android silences the microphone of apps in the background (for example when
 * the phone locks), unless the app shows an "ongoing call" notification from a foreground service — like WhatsApp.
 * This service is that notification. It also keeps Wi-Fi and the CPU awake and turns the screen off near the ear.
 */
class CallService : Service() {
    companion object {
        private const val CH_ONGOING = "maata_ongoing_call"
        private const val NOTIF_ID = 4242
        const val EXTRA_KIND = "kind"
        const val EXTRA_NAME = "name"
        @Volatile var running = false

        fun start(ctx: Context, kind: String, name: String) {
            val i = Intent(ctx, CallService::class.java).putExtra(EXTRA_KIND, kind).putExtra(EXTRA_NAME, name)
            try { if (Build.VERSION.SDK_INT >= 26) ctx.startForegroundService(i) else ctx.startService(i) } catch (_: Exception) { }
        }
        fun stop(ctx: Context) { try { ctx.stopService(Intent(ctx, CallService::class.java)) } catch (_: Exception) { } }
    }

    private var cpuLock: PowerManager.WakeLock? = null
    private var earLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null
    private var oldAudioMode = AudioManager.MODE_NORMAL

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val kind = intent?.getStringExtra(EXTRA_KIND) ?: "voice"
        val name = intent?.getStringExtra(EXTRA_NAME) ?: "Maata"
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26 && nm.getNotificationChannel(CH_ONGOING) == null)
            nm.createNotificationChannel(NotificationChannel(CH_ONGOING, "Ongoing calls", NotificationManager.IMPORTANCE_LOW).apply {
                description = "Shown while you are on a Maata call"; setShowBadge(false)
            })
        val pf = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        val open = PendingIntent.getActivity(this, 1, Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP), pf)
        val hangup = PendingIntent.getActivity(this, 2, Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP).putExtra(MainActivity.EXTRA_HANGUP, true), pf)
        val n = Notification.Builder(this, CH_ONGOING)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle((if (kind == "video") "🎥 Video call" else "📞 Voice call") + " · " + name)
            .setContentText("Ongoing Maata call · tap to return")
            .setOngoing(true).setCategory(Notification.CATEGORY_CALL).setUsesChronometer(true).setWhen(System.currentTimeMillis())
            .setContentIntent(open)
            .addAction(Notification.Action.Builder(null, "Hang up", hangup).build())
            .build()
        // microphone (and camera for video calls) may keep working while the screen is locked
        if (Build.VERSION.SDK_INT >= 30) {
            var types = ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE
            if (kind == "video") types = types or ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA
            try { startForeground(NOTIF_ID, n, types) }
            catch (_: Exception) {
                // no camera permission (or not allowed now): try microphone only; if even that is not allowed, just stop quietly
                try { startForeground(NOTIF_ID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE) } catch (_: Exception) { stopSelf(); return START_NOT_STICKY }
            }
        } else startForeground(NOTIF_ID, n)

        if (!running) {
            running = true
            val pm = getSystemService(PowerManager::class.java)
            cpuLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "maata:call").apply { setReferenceCounted(false); acquire(4 * 60 * 60 * 1000L) }
            // voice call held to the ear: switch the screen off (like a normal phone call)
            if (kind != "video" && pm.isWakeLockLevelSupported(PowerManager.PROXIMITY_SCREEN_OFF_WAKE_LOCK))
                earLock = pm.newWakeLock(PowerManager.PROXIMITY_SCREEN_OFF_WAKE_LOCK, "maata:ear").apply { setReferenceCounted(false); acquire(4 * 60 * 60 * 1000L) }
            @Suppress("DEPRECATION")
            wifiLock = (applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager)
                .createWifiLock(if (Build.VERSION.SDK_INT >= 29) WifiManager.WIFI_MODE_FULL_LOW_LATENCY else WifiManager.WIFI_MODE_FULL_HIGH_PERF, "maata:call").apply { setReferenceCounted(false); acquire() }
            val am = getSystemService(AudioManager::class.java)
            oldAudioMode = am.mode; am.mode = AudioManager.MODE_IN_COMMUNICATION
        } else {
            if (kind == "video") { earLock?.let { if (it.isHeld) it.release() }; earLock = null }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        running = false
        try { cpuLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) { }
        try { earLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) { }
        try { wifiLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) { }
        try {
            val am = getSystemService(AudioManager::class.java)
            if (Build.VERSION.SDK_INT >= 31) am.clearCommunicationDevice() else am.isSpeakerphoneOn = false // deprecated on new Android, still right for old ones
            am.mode = oldAudioMode
        } catch (_: Exception) { }
        if (Build.VERSION.SDK_INT >= 24) stopForeground(STOP_FOREGROUND_REMOVE) else @Suppress("DEPRECATION") stopForeground(true)
        super.onDestroy()
    }

    // the person swiped Maata away from recent apps during a call: end the service too
    override fun onTaskRemoved(rootIntent: Intent?) { stopSelf(); super.onTaskRemoved(rootIntent) }
}
