package com.maataapp.app

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioManager
import android.media.Ringtone
import android.media.RingtoneManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.VibrationEffect
import android.os.Vibrator
import android.os.VibratorManager

/** Maata Secure: a loud alarm on the ALARM sound, so it rings even when the phone is on silent or vibrate. */
object SecureAlarm {
    @Volatile private var tone: Ringtone? = null
    private var oldVolume = -1
    private val main = Handler(Looper.getMainLooper())
    private val autoStop = Runnable { appCtx?.let { stop(it) } }
    private var appCtx: Context? = null

    private fun vibrator(ctx: Context): Vibrator? = if (Build.VERSION.SDK_INT >= 31)
        ctx.getSystemService(VibratorManager::class.java)?.defaultVibrator
    else @Suppress("DEPRECATION") ctx.getSystemService(Context.VIBRATOR_SERVICE) as? Vibrator

    @Synchronized fun start(ctx: Context, seconds: Int = 120): Boolean {
        val c = ctx.applicationContext; appCtx = c
        stop(c)
        val am = c.getSystemService(AudioManager::class.java)
        try { // full alarm volume while it rings; the old volume comes back when it stops
            oldVolume = am.getStreamVolume(AudioManager.STREAM_ALARM)
            am.setStreamVolume(AudioManager.STREAM_ALARM, am.getStreamMaxVolume(AudioManager.STREAM_ALARM), 0)
        } catch (_: Exception) { oldVolume = -1 }
        return try {
            val uri = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_ALARM) ?: RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE)
            tone = RingtoneManager.getRingtone(c, uri)?.apply {
                audioAttributes = AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_ALARM)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION).build()
                if (Build.VERSION.SDK_INT >= 28) isLooping = true
                play()
            }
            try { vibrator(c)?.vibrate(VibrationEffect.createWaveform(longArrayOf(0, 1000, 500), 0)) } catch (_: Exception) { }
            main.removeCallbacks(autoStop); main.postDelayed(autoStop, seconds * 1000L)
            tone != null
        } catch (_: Exception) { false }
    }

    @Synchronized fun stop(ctx: Context) {
        val c = ctx.applicationContext
        main.removeCallbacks(autoStop)
        try { tone?.stop() } catch (_: Exception) { }
        tone = null
        try { vibrator(c)?.cancel() } catch (_: Exception) { }
        if (oldVolume >= 0) { try { c.getSystemService(AudioManager::class.java).setStreamVolume(AudioManager.STREAM_ALARM, oldVolume, 0) } catch (_: Exception) { } }
        oldVolume = -1
    }

    val ringing: Boolean get() = tone?.isPlaying == true
}
