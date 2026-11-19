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

    /**
     * Blocks up to ~45 s (call it from a background thread, e.g. the push service).
     * v376: returns what happened ("ok:te", "ok:en", "off", "silent", "tts-fail", "tts-timeout", "speak-error", "timeout"…) so the phone
     * can tell Maata why an alert stayed quiet; waits longer for a cold speech engine (slow phones), and if the Telugu voice fails, says the English line.
     */
    fun speakBlocking(ctx: Context, text: String, lang: String, fallbackEn: String, force: Boolean = false, times: Int = 2): String {
        if (text.isBlank()) return "blank"
        if (!force && !enabled(ctx)) return "off"
        if (!force && !ringerOn(ctx)) return "silent"
        val ready = CountDownLatch(1)
        var ok = false
        var tts: TextToSpeech? = null
        tts = TextToSpeech(ctx.applicationContext) { st -> ok = st == TextToSpeech.SUCCESS; ready.countDown() }
        try {
            if (!ready.await(15, TimeUnit.SECONDS)) return "tts-timeout"
            if (!ok) return "tts-fail"
            val t: TextToSpeech = tts ?: return "tts-fail"
            // ring volume (not media volume): shop phones often keep media low; still quiet when the phone is on silent / vibrate (checked above)
            t.setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
            t.setSpeechRate(0.95f)
            val loc = Locale.forLanguageTag(if (lang.contains('-')) lang else "$lang-IN")
            val has = try { t.isLanguageAvailable(loc) >= TextToSpeech.LANG_AVAILABLE } catch (_: Exception) { false }
            fun say1(say: String, n: Int): String {
                val done = CountDownLatch(1); var err = false
                t.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                    override fun onStart(id: String) {}
                    override fun onDone(id: String) { if (id == "b") done.countDown() }
                    @Deprecated("Deprecated in Java") override fun onError(id: String) { err = true; done.countDown() }
                    override fun onError(id: String, code: Int) { err = true; done.countDown() }
                })
                val r = if (n >= 2) {
                    t.speak(say, TextToSpeech.QUEUE_FLUSH, null, "a")
                    t.playSilentUtterance(500, TextToSpeech.QUEUE_ADD, "s")
                    t.speak(say, TextToSpeech.QUEUE_ADD, null, "b") // said twice, like a shop bell, so nobody misses it
                } else t.speak(say, TextToSpeech.QUEUE_FLUSH, null, "b") // once (v373: a sarpanch announcement — their own voice follows)
                if (r == TextToSpeech.ERROR) return "speak-error"
                if (!done.await(40, TimeUnit.SECONDS)) return "timeout"
                return if (err) "error" else "ok"
            }
            if (has) {
                t.language = loc
                val r = say1(text, times)
                if (r == "ok") return "ok:" + lang
                if (fallbackEn.isBlank()) return r + ":" + lang
                t.language = Locale("en", "IN") // the Telugu voice failed (not downloaded / network voice offline): say the English line
                return say1(fallbackEn, 1).let { if (it == "ok") "ok:en-after-" + r else it + ":en-after-" + r }
            }
            t.language = Locale("en", "IN")
            return say1(fallbackEn.ifBlank { text }, times) + ":en-nolang"
        } catch (e: Exception) { return "exc:" + (e.javaClass.simpleName)
        } finally { try { tts?.shutdown() } catch (_: Exception) { } }
    }

    /**
     * 📢 v373: plays a recorded voice (the sarpanch's announcement) from a signed link, after the spoken line.
     * Downloads it first (up to 3 MB, so a slow network does not cut it in the middle), plays at ring volume, blocks up to ~75 s.
     * Quiet on silent / vibrate and when spoken alerts are off, like the spoken line.
     */
    fun playUrlBlocking(ctx: Context, url: String): String {
        if (url.isBlank() || !url.startsWith("https://")) return "nourl"
        if (!enabled(ctx)) return "off"
        if (!ringerOn(ctx)) return "silent"
        val f = java.io.File(ctx.cacheDir, "gsbc-voice")
        try {
            val c = (java.net.URL(url).openConnection() as java.net.HttpURLConnection).apply { connectTimeout = 10_000; readTimeout = 15_000 }
            val code = c.responseCode
            if (code != 200) { c.disconnect(); return "http-" + code }
            var n = 0L
            c.inputStream.use { inp -> f.outputStream().use { out -> val b = ByteArray(16 * 1024); while (true) { val r = inp.read(b); if (r < 0) break; n += r; if (n > 3L * 1024 * 1024) return "too-big"; out.write(b, 0, r) } } }
            c.disconnect()
            if (n < 800) return "too-small"
            val done = CountDownLatch(1); var err = ""
            val mp = android.media.MediaPlayer()
            try {
                mp.setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
                mp.setDataSource(f.absolutePath)
                mp.setOnCompletionListener { done.countDown() }
                mp.setOnErrorListener { _, w, x -> err = "play-error-$w-$x"; done.countDown(); true }
                mp.prepare(); mp.start()
                if (!done.await(75, TimeUnit.SECONDS)) return "play-timeout"
                return err.ifBlank { "ok" }
            } finally { try { mp.release() } catch (_: Exception) { } }
        } catch (e: Exception) { return "exc:" + e.javaClass.simpleName
        } finally { try { f.delete() } catch (_: Exception) { } }
    }

    /** v376: tells Maata what happened to an alert on this phone (bell / spoken line / recorded voice), so "why was it quiet?" has an answer. */
    fun report(ctx: Context, r: org.json.JSONObject) {
        val token = Notifier.token(ctx); if (token.isBlank()) return
        try {
            val c = java.net.URL(BuildConfig.APP_URL + "/api/push/alert-log").openConnection() as java.net.HttpURLConnection
            c.requestMethod = "POST"; c.doOutput = true; c.connectTimeout = 8000; c.readTimeout = 8000
            c.setRequestProperty("Content-Type", "application/json")
            val body = org.json.JSONObject().put("fcm", token).put("r", r).toString()
            c.outputStream.use { it.write(body.toByteArray()) }
            c.responseCode; c.disconnect()
        } catch (_: Exception) { }
    }

}
