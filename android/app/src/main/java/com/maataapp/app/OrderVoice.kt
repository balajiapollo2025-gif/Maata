package com.maataapp.app

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioManager
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import java.util.Locale
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * 🔊 Spoken order alerts ("New order from Kavya, 103 rupees…") for shops and delivery persons, also when Maata is closed.
 * Speaks in the person's language (Telugu / Hindi / English…); if the phone has no voice for it, speaks the English text.
 * Stays quiet when the phone is on silent or vibrate.
 */
object OrderVoice {
    private const val PREFS = "maata_voice"
    fun enabled(ctx: Context) = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getBoolean("on", true)
    fun setEnabled(ctx: Context, on: Boolean) = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putBoolean("on", on).apply()
    private fun ringerOn(ctx: Context) = try { (ctx.getSystemService(Context.AUDIO_SERVICE) as AudioManager).ringerMode == AudioManager.RINGER_MODE_NORMAL } catch (_: Exception) { true }

    /** Blocks up to ~30 s (call it from a background thread, e.g. the push service). */
    fun speakBlocking(ctx: Context, text: String, lang: String, fallbackEn: String, force: Boolean = false, times: Int = 2) {
        if (text.isBlank() || (!force && (!enabled(ctx) || !ringerOn(ctx)))) return
        val ready = CountDownLatch(1); val done = CountDownLatch(1)
        var ok = false
        var tts: TextToSpeech? = null
        tts = TextToSpeech(ctx.applicationContext) { st -> ok = st == TextToSpeech.SUCCESS; ready.countDown() }
        try {
            if (!ready.await(8, TimeUnit.SECONDS) || !ok) return
            val t = tts
            // ring volume (not media volume): shop phones often keep media low; still quiet when the phone is on silent / vibrate (checked above)
            t.setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
            val loc = Locale.forLanguageTag(if (lang.contains('-')) lang else "$lang-IN")
            val has = try { t.isLanguageAvailable(loc) >= TextToSpeech.LANG_AVAILABLE } catch (_: Exception) { false }
            val say = if (has) { t.language = loc; text } else { t.language = Locale("en", "IN"); fallbackEn.ifBlank { text } }
            t.setSpeechRate(0.95f)
            t.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                override fun onStart(id: String) {}
                override fun onDone(id: String) { if (id == "b") done.countDown() }
                @Deprecated("Deprecated in Java") override fun onError(id: String) { done.countDown() }
            })
            if (times >= 2) {
                t.speak(say, TextToSpeech.QUEUE_FLUSH, null, "a")
                t.playSilentUtterance(500, TextToSpeech.QUEUE_ADD, "s")
                t.speak(say, TextToSpeech.QUEUE_ADD, null, "b") // said twice, like a shop bell, so nobody misses it
            } else t.speak(say, TextToSpeech.QUEUE_FLUSH, null, "b") // once (v373: a sarpanch announcement — their own voice follows)
            done.await(30, TimeUnit.SECONDS)
        } catch (_: Exception) {
        } finally { try { tts?.shutdown() } catch (_: Exception) { } }
    }

    /**
     * 📢 v373: plays a recorded voice (the sarpanch's announcement) from a signed link, after the spoken line.
     * Downloads it first (up to 3 MB, so a slow network does not cut it in the middle), plays at ring volume, blocks up to ~75 s.
     * Quiet on silent / vibrate and when spoken alerts are off, like the spoken line.
     */
    fun playUrlBlocking(ctx: Context, url: String) {
        if (url.isBlank() || !url.startsWith("https://") || !enabled(ctx) || !ringerOn(ctx)) return
        val f = java.io.File(ctx.cacheDir, "gsbc-voice")
        try {
            val c = (java.net.URL(url).openConnection() as java.net.HttpURLConnection).apply { connectTimeout = 10_000; readTimeout = 15_000 }
            if (c.responseCode != 200) { c.disconnect(); return }
            var n = 0L
            c.inputStream.use { inp -> f.outputStream().use { out -> val b = ByteArray(16 * 1024); while (true) { val r = inp.read(b); if (r < 0) break; n += r; if (n > 3L * 1024 * 1024) return; out.write(b, 0, r) } } }
            c.disconnect()
            if (n < 800) return
            val done = CountDownLatch(1)
            val mp = android.media.MediaPlayer()
            try {
                mp.setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
                mp.setDataSource(f.absolutePath)
                mp.setOnCompletionListener { done.countDown() }
                mp.setOnErrorListener { _, _, _ -> done.countDown(); true }
                mp.prepare(); mp.start()
                done.await(75, TimeUnit.SECONDS)
            } finally { try { mp.release() } catch (_: Exception) { } }
        } catch (_: Exception) {
        } finally { try { f.delete() } catch (_: Exception) { } }
    }
}
