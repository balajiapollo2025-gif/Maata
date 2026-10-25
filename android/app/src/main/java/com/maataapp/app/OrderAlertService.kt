package com.maataapp.app

import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioTrack
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import java.util.concurrent.Executors

/**
 * 🔔 New order / new job while Maata is CLOSED.
 * The push service starts this short foreground service (allowed for a high-priority push), so Android does not stop it
 * in the middle: the notification rings with the Maata order bell, then the phone says the order out loud, twice.
 * Orders that come together are spoken one after another. The notification stays after the voice ends.
 */
class OrderAlertService : Service() {
    companion object {
        private const val FG_ID = 4343
        private val queue = Executors.newSingleThreadExecutor()
        @Volatile private var pending = 0

        /** 🔔 the Maata order bell (ding-dong-ding), made in code, played at ring volume. Quiet on silent / vibrate. Blocks ~2.4 s. */
        fun playBell(ctx: Context) {
            try { if ((ctx.getSystemService(Context.AUDIO_SERVICE) as AudioManager).ringerMode != AudioManager.RINGER_MODE_NORMAL) return } catch (_: Exception) { }
            val sr = 44100; val n = (sr * 2.4).toInt(); val x = DoubleArray(n)
            fun tone(f: Double, t0: Double, dur: Double, amp: Double) {
                val s0 = (t0 * sr).toInt(); val len = (dur * sr).toInt()
                for (i in 0 until len) { val k = s0 + i; if (k >= n) break; val t = i.toDouble() / sr
                    val env = Math.exp(-t * 3.2) * Math.min(1.0, t / 0.004)
                    var w = 0.0; for ((m, a) in listOf(1.0 to 1.0, 2.0 to 0.45, 3.01 to 0.25, 4.2 to 0.12, 5.4 to 0.06)) w += a * Math.sin(2 * Math.PI * f * m * t)
                    x[k] += amp * w * env }
            }
            tone(1318.5, 0.0, 1.6, 0.5); tone(1046.5, 0.32, 1.6, 0.5); tone(1568.0, 0.64, 1.7, 0.55); tone(1318.5, 0.96, 1.4, 0.35)
            var peak = 0.0; for (v in x) peak = Math.max(peak, Math.abs(v))
            val pcm = ShortArray(n) { (x[it] / peak * 0.89 * 32767).toInt().toShort() }
            try {
                val tr = AudioTrack.Builder()
                    .setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION).build())
                    .setAudioFormat(AudioFormat.Builder().setEncoding(AudioFormat.ENCODING_PCM_16BIT).setSampleRate(sr).setChannelMask(AudioFormat.CHANNEL_OUT_MONO).build())
                    .setTransferMode(AudioTrack.MODE_STATIC).setBufferSizeInBytes(n * 2).build()
                tr.write(pcm, 0, n); tr.play(); Thread.sleep(2500); tr.release()
            } catch (_: Exception) { }
        }

        /** true = the service took the alert; false = Android did not allow it (the caller speaks by itself) */
        fun start(ctx: Context, d: Map<String, String>): Boolean = try {
            val i = Intent(ctx, OrderAlertService::class.java)
            for (k in listOf("title", "body", "tag", "url", "say", "sayEn", "lang")) i.putExtra(k, d[k] ?: "")
            ContextCompat.startForegroundService(ctx, i); true
        } catch (e: Exception) { false }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val title = intent?.getStringExtra("title") ?: "Maata"; val body = intent?.getStringExtra("body") ?: ""
        val tag = intent?.getStringExtra("tag") ?: "order"; val url = intent?.getStringExtra("url") ?: "/"
        val say = intent?.getStringExtra("say") ?: ""; val sayEn = intent?.getStringExtra("sayEn") ?: ""; val lang = intent?.getStringExtra("lang") ?: "en"
        Notifier.createChannels(this)
        // while speaking, a quiet "Maata is reading the order" notification; the real order notification (with the bell) is posted separately
        val fg = Notifier.orderSpeakingNotification(this, title)
        try {
            ServiceCompat.startForeground(this, FG_ID, fg, if (Build.VERSION.SDK_INT >= 29) ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PLAYBACK else 0)
        } catch (e: Exception) { }
        Notifier.showOrder(this, title, body, tag, url, silent = true) // the bell below rings instead of the normal notification sound
        synchronized(OrderAlertService) { pending++ }
        val app = applicationContext
        queue.execute {
            val wl = try { (getSystemService(Context.POWER_SERVICE) as PowerManager).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "maata:order").apply { acquire(60_000) } } catch (e: Exception) { null }
            try {
                playBell(app) // 🔔 then the voice
                if (say.isNotBlank()) OrderVoice.speakBlocking(app, say, lang, sayEn)
            } catch (_: Exception) {
            } finally {
                try { wl?.release() } catch (_: Exception) { }
                val last = synchronized(OrderAlertService) { pending--; pending <= 0 }
                if (last) { try { ServiceCompat.stopForeground(this, ServiceCompat.STOP_FOREGROUND_REMOVE) } catch (_: Exception) { }; stopSelf() }
            }
        }
        return START_NOT_STICKY
    }
}
