package com.maataapp.app

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.app.AlertDialog
import android.app.DownloadManager
import android.app.NotificationManager
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.PackageInstaller
import android.app.PendingIntent
import android.media.AudioAttributes
import android.media.AudioDeviceInfo
import android.media.Ringtone
import android.media.RingtoneManager
import android.os.VibrationEffect
import android.os.Vibrator
import android.os.VibratorManager
import android.media.AudioManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.Settings
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.speech.RecognitionListener
import android.speech.RecognizerIntent
import android.speech.SpeechRecognizer
import java.util.Locale
import android.webkit.*
import android.widget.Toast
import com.google.firebase.messaging.FirebaseMessaging

/** Maata app: the Maata web app in a full-screen WebView, with native calls, notifications and permissions. */
class MainActivity : Activity() {
    companion object {
        const val EXTRA_URL = "maata_url"
        @Volatile var foreground = false
        const val EXTRA_STOP_RING = "maata_stop_ring"
        const val EXTRA_HANGUP = "maata_hangup"
        private const val REQ_FILE = 11
        private const val REQ_PERMS = 12
        const val ACTION_INSTALL_STATUS = "com.maataapp.app.INSTALL_STATUS"
        private val OUR_HOSTS = setOf("maataapp.com", "www.maataapp.com", "maata-siut.onrender.com")
    }
    private lateinit var web: WebView
    private fun bioReply(id: String, ok: Boolean, msg: String) = runOnUiThread {
        web.evaluateJavascript("window.maataBioResult && maataBioResult(" + org.json.JSONObject.quote(id) + "," + ok + "," + org.json.JSONObject.quote(msg) + ")", null)
    }
    private var fileCallback: ValueCallback<Array<Uri>>? = null
    private var pendingWebPermission: PermissionRequest? = null
    private var pendingGeo: Pair<String, GeolocationPermissions.Callback>? = null
    private var pageReady = false
    // 📲 automatic updates: download the new Maata in the background, install it with one tap
    // (Android 12+ installs it by itself, without asking, once Maata installed the previous update itself)
    @Volatile private var updFile: java.io.File? = null
    @Volatile private var updDownloading = false
    private fun jsUpd(kind: String, a: String) = runOnUiThread {
        if (::web.isInitialized) web.evaluateJavascript("window.maataUpd && maataUpd(" + org.json.JSONObject.quote(kind) + "," + org.json.JSONObject.quote(a) + ")", null)
    }
    private fun versionCodeOf(i: android.content.pm.PackageInfo): Long = if (Build.VERSION.SDK_INT >= 28) i.longVersionCode else @Suppress("DEPRECATION") i.versionCode.toLong()
    private fun downloadUpdate(url: String) {
        if (updDownloading) return; updDownloading = true
        Thread {
            try {
                val f = java.io.File(cacheDir, "maata-auto-update.apk"); if (f.exists()) f.delete()
                val c = java.net.URL(url).openConnection() as java.net.HttpURLConnection
                c.instanceFollowRedirects = true; c.connectTimeout = 20000; c.readTimeout = 30000
                if (c.responseCode !in 200..299) throw IllegalStateException("http " + c.responseCode)
                val total = c.contentLengthLong; var got = 0L; var last = -1
                c.inputStream.use { inp -> f.outputStream().use { out ->
                    val buf = ByteArray(64 * 1024)
                    while (true) { val n = inp.read(buf); if (n < 0) break; out.write(buf, 0, n); got += n
                        if (total > 0) { val pct = (got * 100 / total).toInt(); if (pct / 5 != last) { last = pct / 5; jsUpd("progress", pct.toString()) } } }
                } }
                val info = packageManager.getPackageArchiveInfo(f.path, 0) ?: throw IllegalStateException("bad file")
                if (info.packageName != packageName || versionCodeOf(info) <= versionCodeOf(packageManager.getPackageInfo(packageName, 0))) { f.delete(); throw IllegalStateException("not newer") }
                updFile = f; jsUpd("ready", info.versionName ?: "")
            } catch (e: Exception) { jsUpd("error", e.message ?: "download failed") } finally { updDownloading = false }
        }.start()
    }
    /** true when Android lets Maata update itself without asking (Android 12+ and Maata installed itself last time) */
    private fun canSilentUpdate(): Boolean = try { Build.VERSION.SDK_INT >= 31 && packageManager.getInstallSourceInfo(packageName).installingPackageName == packageName } catch (_: Exception) { false }
    private fun installDownloaded(): String {
        val f = updFile ?: return "NONE"
        if (Build.VERSION.SDK_INT >= 26 && !packageManager.canRequestPackageInstalls()) {
            runOnUiThread { try { startActivity(Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:$packageName"))) } catch (_: Exception) { } }
            return "PERMISSION"
        }
        val pi = packageManager.packageInstaller
        val params = PackageInstaller.SessionParams(PackageInstaller.SessionParams.MODE_FULL_INSTALL)
        params.setAppPackageName(packageName)
        if (Build.VERSION.SDK_INT >= 31) params.setRequireUserAction(PackageInstaller.SessionParams.USER_ACTION_NOT_REQUIRED)
        val id = pi.createSession(params)
        pi.openSession(id).use { s ->
            f.inputStream().use { inp -> s.openWrite("maata.apk", 0, f.length()).use { out -> inp.copyTo(out); s.fsync(out) } }
            val back = Intent(ACTION_INSTALL_STATUS).setPackage(packageName)
            val p = PendingIntent.getBroadcast(this, 77, back, PendingIntent.FLAG_UPDATE_CURRENT or (if (Build.VERSION.SDK_INT >= 31) PendingIntent.FLAG_MUTABLE else 0))
            s.commit(p.intentSender)
        }
        return "OK"
    }
    private val installStatus = object : android.content.BroadcastReceiver() {
        override fun onReceive(c: android.content.Context, i: Intent) {
            when (i.getIntExtra(PackageInstaller.EXTRA_STATUS, -99)) {
                PackageInstaller.STATUS_PENDING_USER_ACTION -> {
                    val ask: Intent? = if (Build.VERSION.SDK_INT >= 33) i.getParcelableExtra(Intent.EXTRA_INTENT, Intent::class.java) else @Suppress("DEPRECATION") i.getParcelableExtra(Intent.EXTRA_INTENT)
                    ask?.let { try { startActivity(it.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) } catch (_: Exception) { } }
                    jsUpd("confirm", "")
                }
                PackageInstaller.STATUS_SUCCESS -> jsUpd("done", "")
                else -> jsUpd("error", i.getStringExtra(PackageInstaller.EXTRA_STATUS_MESSAGE) ?: "install failed")
            }
        }
    }
    // 🔊 navigation voice: the phone's own text-to-speech (WebView has no speech of its own)
    private var tts: TextToSpeech? = null
    @Volatile private var ttsState = 0 // 0 starting, 1 ready, -1 failed
    private var ttsPending: Triple<String, String, String>? = null
    // speech progress → the page (so Chilaka's beak moves with each spoken word)
    private fun ttsEvent(id: String, kind: String, a: Int, b: Int) = runOnUiThread {
        if (::web.isInitialized) web.evaluateJavascript("window.maataTts && maataTts(" + org.json.JSONObject.quote(id) + "," + org.json.JSONObject.quote(kind) + "," + a + "," + b + ")", null)
    }
    private fun startTts() {
        if (tts != null) return
        tts = TextToSpeech(applicationContext) { st ->
            ttsState = if (st == TextToSpeech.SUCCESS) 1 else -1
            if (ttsState == 1) {
                tts?.setAudioAttributes(AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_ASSISTANCE_NAVIGATION_GUIDANCE).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
                tts?.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                    override fun onStart(id: String) { ttsEvent(id, "start", 0, 0) }
                    override fun onDone(id: String) { ttsEvent(id, "done", 0, 0) }
                    @Deprecated("Deprecated in Java") override fun onError(id: String) { ttsEvent(id, "done", 0, 0) }
                    override fun onError(id: String, code: Int) { ttsEvent(id, "done", 0, 0) }
                    override fun onStop(id: String, interrupted: Boolean) { ttsEvent(id, "done", 0, 0) }
                    override fun onRangeStart(id: String, start: Int, end: Int, frame: Int) { ttsEvent(id, "word", start, end) }
                })
                ttsPending?.let { speakNow(it.first, it.second, it.third) }
            }
            ttsPending = null
        }
    }
    // 🎤 speech to text for the voice password and voice search (WebView has no speech recognition of its own)
    private var recognizer: SpeechRecognizer? = null
    private var pendingListen: Pair<String, String>? = null
    private fun listenReply(id: String, ok: Boolean, data: String) = runOnUiThread {
        web.evaluateJavascript("window.maataListenResult && maataListenResult(" + org.json.JSONObject.quote(id) + "," + ok + "," + org.json.JSONObject.quote(data) + ")", null)
    }
    private fun startListening(id: String, lang: String) {
        if (checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) { pendingListen = id to lang; requestPermissions(arrayOf(Manifest.permission.RECORD_AUDIO), REQ_PERMS); return }
        if (!SpeechRecognizer.isRecognitionAvailable(this)) { listenReply(id, false, "unsupported"); return }
        try { recognizer?.destroy() } catch (_: Exception) { }
        val r = SpeechRecognizer.createSpeechRecognizer(this); recognizer = r
        val done = { try { r.destroy() } catch (_: Exception) { }; if (recognizer === r) recognizer = null }
        r.setRecognitionListener(object : RecognitionListener {
            override fun onReadyForSpeech(p: Bundle?) { runOnUiThread { web.evaluateJavascript("window.maataListenReady && maataListenReady(" + org.json.JSONObject.quote(id) + ")", null) } }
            override fun onResults(b: Bundle?) { val list = b?.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION) ?: arrayListOf<String>(); listenReply(id, list.isNotEmpty(), if (list.isEmpty()) "nohear" else org.json.JSONArray(list).toString()); done() }
            override fun onError(e: Int) {
                listenReply(id, false, when (e) { SpeechRecognizer.ERROR_NO_MATCH, SpeechRecognizer.ERROR_SPEECH_TIMEOUT -> "nohear"; SpeechRecognizer.ERROR_INSUFFICIENT_PERMISSIONS -> "perm"; SpeechRecognizer.ERROR_NETWORK, SpeechRecognizer.ERROR_NETWORK_TIMEOUT -> "network"; SpeechRecognizer.ERROR_RECOGNIZER_BUSY -> "busy"; else -> "error$e" }); done()
            }
            override fun onBeginningOfSpeech() {}
            override fun onRmsChanged(v: Float) {}
            override fun onBufferReceived(b: ByteArray?) {}
            override fun onEndOfSpeech() {}
            override fun onPartialResults(b: Bundle?) {}
            override fun onEvent(t: Int, b: Bundle?) {}
        })
        r.startListening(Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH)
            .putExtra(RecognizerIntent.EXTRA_LANGUAGE_MODEL, RecognizerIntent.LANGUAGE_MODEL_FREE_FORM)
            .putExtra(RecognizerIntent.EXTRA_LANGUAGE, lang).putExtra(RecognizerIntent.EXTRA_LANGUAGE_PREFERENCE, lang)
            .putExtra(RecognizerIntent.EXTRA_MAX_RESULTS, 5).putExtra(RecognizerIntent.EXTRA_CALLING_PACKAGE, packageName))
    }
    private fun langOk(lang: String): Boolean {
        val r = tts?.isLanguageAvailable(Locale.forLanguageTag(lang)) ?: return false
        return r >= TextToSpeech.LANG_AVAILABLE
    }
    private fun speakNow(text: String, lang: String, id: String = "maata" + System.nanoTime()): String {
        val t = tts ?: return "error"
        if (!langOk(lang)) return "nolang"
        t.language = Locale.forLanguageTag(lang)
        t.setSpeechRate(0.95f)
        return if (t.speak(text, TextToSpeech.QUEUE_ADD, null, id) == TextToSpeech.SUCCESS) "ok" else "error"
    }

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val upf = android.content.IntentFilter(ACTION_INSTALL_STATUS)
        if (Build.VERSION.SDK_INT >= 33) registerReceiver(installStatus, upf, android.content.Context.RECEIVER_NOT_EXPORTED) else registerReceiver(installStatus, upf)
        Notifier.createChannels(this)
        startTts()
        FirebaseMessaging.getInstance().token.addOnSuccessListener { Notifier.saveToken(this, it) }

        web = WebView(this)
        setContentView(web)
        with(web.settings) {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
            mediaPlaybackRequiresUserGesture = false
            allowFileAccess = false
            setGeolocationEnabled(true)
            userAgentString = "$userAgentString MaataAndroid/${BuildConfig.VERSION_NAME}"
            // Follow the phone's font size like WhatsApp does, but within limits, so a very large
            // system font does not blow up the whole app (Maata's own Settings → Text size still works)
            textZoom = (this@MainActivity.resources.configuration.fontScale.coerceIn(0.9f, 1.15f) * 100).toInt()
        }
        CookieManager.getInstance().setAcceptCookie(true)
        web.addJavascriptInterface(Bridge(), "MaataAndroid")
        web.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, req: WebResourceRequest): Boolean {
                val u = req.url
                if ((u.scheme == "https" || u.scheme == "http") && u.host in OUR_HOSTS) return false
                openOutside(u); return true // WhatsApp invites, phone numbers, maps, websites
            }
            override fun onPageFinished(view: WebView, url: String) { pageReady = true }
            override fun onReceivedError(view: WebView, req: WebResourceRequest, err: WebResourceError) {
                if (req.isForMainFrame) view.loadData(OFFLINE_PAGE, "text/html", "utf-8")
            }
        }
        web.webChromeClient = object : WebChromeClient() {
            override fun onPermissionRequest(request: PermissionRequest) = runOnUiThread { handleWebPermission(request) }
            override fun onGeolocationPermissionsShowPrompt(origin: String, callback: GeolocationPermissions.Callback) {
                if (has(Manifest.permission.ACCESS_FINE_LOCATION)) callback.invoke(origin, true, false)
                else { pendingGeo = origin to callback; requestPermissions(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION), REQ_PERMS) }
            }
            override fun onShowFileChooser(view: WebView, cb: ValueCallback<Array<Uri>>, params: FileChooserParams): Boolean {
                fileCallback?.onReceiveValue(null); fileCallback = cb
                return try { startActivityForResult(params.createIntent().apply { if (params.mode == FileChooserParams.MODE_OPEN_MULTIPLE) putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true) }, REQ_FILE); true }
                catch (e: ActivityNotFoundException) { fileCallback = null; false }
            }
        }
        web.setDownloadListener { url, _, disposition, mime, _ ->
            try {
                val name = URLUtil.guessFileName(url, disposition, mime)
                val r = DownloadManager.Request(Uri.parse(url)).setMimeType(mime).setTitle(name)
                    .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
                    .setDestinationInExternalPublicDir(Environment.DIRECTORY_DOWNLOADS, name)
                getSystemService(DownloadManager::class.java).enqueue(r)
                Toast.makeText(this, "Downloading $name", Toast.LENGTH_SHORT).show()
            } catch (e: Exception) { openOutside(Uri.parse(url)) }
        }

        web.loadUrl(BuildConfig.APP_URL + (intent.getStringExtra(EXTRA_URL) ?: intent.data?.let { it.encodedPath + (it.encodedFragment?.let { f -> "#$f" } ?: "") } ?: "/"))
        if (intent.getBooleanExtra(EXTRA_STOP_RING, false)) Notifier.stopRinging(this)
        // Back: go back inside Maata; on the first screen keep Maata running in the background (Android 13+ way)
        if (Build.VERSION.SDK_INT >= 33) onBackInvokedDispatcher.registerOnBackInvokedCallback(android.window.OnBackInvokedDispatcher.PRIORITY_DEFAULT) {
            if (web.canGoBack()) web.goBack() else moveTaskToBack(true)
        }
        askFirstPermissions()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        if (intent.getBooleanExtra(EXTRA_STOP_RING, false)) Notifier.stopRinging(this)
        if (intent.getBooleanExtra(EXTRA_HANGUP, false)) { // "Hang up" from the ongoing-call notification
            web.evaluateJavascript("window.maataHangup && maataHangup()", null); CallService.stop(this); return
        }
        val path = intent.getStringExtra(EXTRA_URL) ?: intent.data?.let { it.encodedPath + (it.encodedFragment?.let { f -> "#$f" } ?: "") } ?: return
        val full = BuildConfig.APP_URL + path
        // the page is already open: tell it what to show (chat, or answer a call) without reloading
        if (pageReady) web.evaluateJavascript("window.handleAppLink ? handleAppLink(${org.json.JSONObject.quote(full)}) : location.assign(${org.json.JSONObject.quote(full)})", null)
        else web.loadUrl(full)
    }

    private fun askFirstPermissions() {
        if (Build.VERSION.SDK_INT >= 33 && !has(Manifest.permission.POST_NOTIFICATIONS))
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), REQ_PERMS)
        // Android 14+: calls can only show full screen if this is allowed
        if (Build.VERSION.SDK_INT >= 34) {
            val nm = getSystemService(NotificationManager::class.java)
            val asked = getSharedPreferences("maata", MODE_PRIVATE).getBoolean("askedFsi", false)
            if (!nm.canUseFullScreenIntent() && !asked) {
                getSharedPreferences("maata", MODE_PRIVATE).edit().putBoolean("askedFsi", true).apply()
                AlertDialog.Builder(this).setTitle("Show calls on full screen")
                    .setMessage("Allow Maata to show incoming calls on your full screen, like a normal phone call, even when your phone is locked.")
                    .setPositiveButton("Allow") { _, _ -> try { startActivity(Intent(Settings.ACTION_MANAGE_APP_USE_FULL_SCREEN_INTENT, Uri.parse("package:$packageName"))) } catch (_: Exception) {} }
                    .setNegativeButton("Later", null).show()
            }
        }
    }

    private fun has(p: String) = checkSelfPermission(p) == PackageManager.PERMISSION_GRANTED

    private fun handleWebPermission(request: PermissionRequest) {
        val need = mutableListOf<String>()
        if (request.resources.contains(PermissionRequest.RESOURCE_VIDEO_CAPTURE) && !has(Manifest.permission.CAMERA)) need += Manifest.permission.CAMERA
        if (request.resources.contains(PermissionRequest.RESOURCE_AUDIO_CAPTURE) && !has(Manifest.permission.RECORD_AUDIO)) need += Manifest.permission.RECORD_AUDIO
        if (need.isEmpty()) request.grant(request.resources)
        else { pendingWebPermission = request; requestPermissions(need.toTypedArray(), REQ_PERMS) }
    }

    override fun onRequestPermissionsResult(code: Int, perms: Array<out String>, results: IntArray) {
        super.onRequestPermissionsResult(code, perms, results)
        pendingWebPermission?.let { r ->
            val ok = r.resources.filter {
                (it == PermissionRequest.RESOURCE_VIDEO_CAPTURE && has(Manifest.permission.CAMERA)) || (it == PermissionRequest.RESOURCE_AUDIO_CAPTURE && has(Manifest.permission.RECORD_AUDIO))
            }
            if (ok.isNotEmpty()) r.grant(ok.toTypedArray()) else r.deny()
            pendingWebPermission = null
        }
        pendingListen?.let { (id, lang) -> pendingListen = null; if (has(Manifest.permission.RECORD_AUDIO)) startListening(id, lang) else listenReply(id, false, "perm") }
        pendingGeo?.let { (origin, cb) -> cb.invoke(origin, has(Manifest.permission.ACCESS_FINE_LOCATION) || has(Manifest.permission.ACCESS_COARSE_LOCATION), false); pendingGeo = null }
        if (pendingTrack) { pendingTrack = false; web.evaluateJavascript("window.maataTrackPerm && maataTrackPerm(" + org.json.JSONObject.quote(TeamTrackService.status(this)) + ")", null) }
        if (pendingBt) { pendingBt = false; val st = BtPrinter.status(this); web.evaluateJavascript("window.maataBtPerm && maataBtPerm(" + org.json.JSONObject.quote(st) + ")", null) }
    }
    private var pendingBt = false
    private var pendingTrack = false
    private var btOut = java.io.ByteArrayOutputStream()

    @Deprecated("Deprecated in Java")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == REQ_FILE) {
            val uris: Array<Uri>? = if (resultCode != RESULT_OK || data == null) null
                else data.clipData?.let { c -> Array(c.itemCount) { c.getItemAt(it).uri } } ?: data.data?.let { arrayOf(it) }
            fileCallback?.onReceiveValue(uris); fileCallback = null
        }
    }

    private fun openOutside(u: Uri) {
        try { startActivity(Intent(Intent.ACTION_VIEW, u).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) }
        catch (e: Exception) { Toast.makeText(this, "No app can open this link", Toast.LENGTH_SHORT).show() }
    }

    // ---------- incoming call while Maata is open: ring with the phone's own ringtone ----------
    @Volatile private var ringer: Ringtone? = null
    private fun vibrator(): Vibrator? = try {
        if (Build.VERSION.SDK_INT >= 31) getSystemService(VibratorManager::class.java)?.defaultVibrator
        else getSystemService(Vibrator::class.java)
    } catch (_: Exception) { null }
    fun startPhoneRing(vibrate: Boolean): Boolean = try {
        stopPhoneRing()
        val uri = RingtoneManager.getActualDefaultRingtoneUri(this, RingtoneManager.TYPE_RINGTONE)
            ?: RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE)
        ringer = RingtoneManager.getRingtone(this, uri)?.apply {
            // "ringtone" sound: follows the phone's silent / vibrate mode and ringtone volume, exactly like a normal call
            audioAttributes = AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE)
                .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION).build()
            if (Build.VERSION.SDK_INT >= 28) isLooping = true
            play()
        }
        val am = getSystemService(AudioManager::class.java)
        if (vibrate && am.ringerMode != AudioManager.RINGER_MODE_SILENT)
            vibrator()?.vibrate(VibrationEffect.createWaveform(longArrayOf(0, 900, 700), 0)) // repeat until answered
        ringer != null
    } catch (e: Exception) { false }
    fun stopPhoneRing() {
        try { ringer?.stop() } catch (_: Exception) { }
        ringer = null
        try { vibrator()?.cancel() } catch (_: Exception) { }
    }

    // ---------- call sound: loudspeaker or earpiece ----------
    @Volatile private var wantSpeaker: Boolean? = null   // what the person chose for this call (null = no call)
    private var routeGuard: Any? = null
    private var reapplies = 0
    private fun applyRoute(on: Boolean): Boolean = try {
        val am = getSystemService(AudioManager::class.java)
        if (am.mode != AudioManager.MODE_IN_COMMUNICATION) am.mode = AudioManager.MODE_IN_COMMUNICATION
        var ok = false
        if (Build.VERSION.SDK_INT >= 31) {
            val devs = am.availableCommunicationDevices
            val pick = if (on) devs.firstOrNull { it.type == AudioDeviceInfo.TYPE_BUILTIN_SPEAKER }
            else listOf(AudioDeviceInfo.TYPE_BLUETOOTH_SCO, AudioDeviceInfo.TYPE_BLE_HEADSET, AudioDeviceInfo.TYPE_WIRED_HEADSET,
                AudioDeviceInfo.TYPE_WIRED_HEADPHONES, AudioDeviceInfo.TYPE_USB_HEADSET, AudioDeviceInfo.TYPE_BUILTIN_EARPIECE)
                .firstNotNullOfOrNull { t -> devs.firstOrNull { it.type == t } }
            if (pick != null) ok = am.setCommunicationDevice(pick)
            am.isSpeakerphoneOn = on // also the older switch — some phones (Vivo, Oppo, Realme…) only listen to this one
            ok = true
        } else {
            am.isSpeakerphoneOn = on // older Android
            ok = true
        }
        val stream = AudioManager.STREAM_VOICE_CALL
        val max = am.getStreamMaxVolume(stream)
        try { if (am.getStreamVolume(stream) < max * 0.6) am.setStreamVolume(stream, (max * 0.8).toInt().coerceAtLeast(1), 0) } catch (_: Exception) { }
        runOnUiThread { volumeControlStream = stream }
        ok
    } catch (e: Exception) { false }

    /** The web engine inside the app switches to the loudspeaker by itself when call audio starts.
     *  While a call is on, watch for that and put back what the person chose. */
    private fun guardRoute() {
        if (Build.VERSION.SDK_INT < 31 || routeGuard != null) return
        val am = getSystemService(AudioManager::class.java)
        val l = AudioManager.OnCommunicationDeviceChangedListener { dev ->
            val want = wantSpeaker ?: return@OnCommunicationDeviceChangedListener
            val isSpeaker = dev?.type == AudioDeviceInfo.TYPE_BUILTIN_SPEAKER
            if (isSpeaker != want && reapplies < 30) { reapplies++; web.postDelayed({ wantSpeaker?.let { applyRoute(it) } }, 250) }
        }
        try { am.addOnCommunicationDeviceChangedListener(mainExecutor, l); routeGuard = l } catch (_: Exception) { }
    }
    private fun stopRouteGuard() {
        wantSpeaker = null; reapplies = 0
        if (Build.VERSION.SDK_INT >= 31) (routeGuard as? AudioManager.OnCommunicationDeviceChangedListener)?.let {
            try { getSystemService(AudioManager::class.java).removeOnCommunicationDeviceChangedListener(it) } catch (_: Exception) { }
        }
        routeGuard = null
    }

    // leaving Maata with an update downloaded: install it quietly (Android 12+), like Play Store apps
    override fun onStart() { super.onStart(); foreground = true }
    override fun onStop() {
        super.onStop(); foreground = false
        if (updFile != null && !CallService.running && canSilentUpdate()) try { installDownloaded() } catch (_: Exception) { }
    }

    override fun onDestroy() { try { unregisterReceiver(installStatus) } catch (_: Exception) { }; try { tts?.shutdown() } catch (_: Exception) { }; tts = null; try { recognizer?.destroy() } catch (_: Exception) { }; stopPhoneRing(); stopRouteGuard(); CallService.stop(this); super.onDestroy() }

    @Deprecated("Deprecated in Java")
    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else moveTaskToBack(true) // keep Maata running in the background
    }

    /** Functions the Maata web app can call: window.MaataAndroid.getFcmToken() and so on. */
    inner class Bridge {
        @JavascriptInterface fun getFcmToken(): String = Notifier.token(this@MainActivity)
        @JavascriptInterface fun deviceModel(): String = "${Build.MANUFACTURER} ${Build.MODEL}"
        @JavascriptInterface fun appVersion(): String = BuildConfig.VERSION_NAME
        /** The web app tells us a call started/ended, so the mic keeps working when the phone locks. */
        @JavascriptInterface fun callStarted(kind: String, name: String) { CallService.start(this@MainActivity, if (kind == "video") "video" else "voice", name.take(40)) }
        @JavascriptInterface fun callEnded() { runOnUiThread { stopRouteGuard(); volumeControlStream = AudioManager.USE_DEFAULT_STREAM_TYPE }; CallService.stop(this@MainActivity) }
        /** Loudspeaker on/off during a call. Off = phone earpiece (or a connected headset / Bluetooth), like a normal call. */
        /** Phone contacts for "New group" and the Contacts tab: JSON [{name, phones:[...]}], or "PERMISSION" while asking. */
        /** What the phone is really doing with call sound (shown when the speaker button is held). */
        @JavascriptInterface fun audioInfo(): String = try {
            val am = getSystemService(AudioManager::class.java)
            val modes = mapOf(0 to "NORMAL", 1 to "RINGTONE", 2 to "IN_CALL", 3 to "IN_COMMUNICATION")
            val types = mapOf(1 to "EARPIECE", 2 to "SPEAKER", 3 to "WIRED_HEADSET", 4 to "WIRED_HEADPHONES", 7 to "BLUETOOTH_SCO", 8 to "BLUETOOTH_A2DP", 22 to "USB_HEADSET", 26 to "BLE_HEADSET")
            val dev = if (Build.VERSION.SDK_INT >= 31) am.communicationDevice?.type?.let { types[it] ?: it.toString() } ?: "none" else "n/a"
            val outs = am.getDevices(AudioManager.GET_DEVICES_OUTPUTS).map { types[it.type] ?: it.type.toString() }.distinct().joinToString(",")
            org.json.JSONObject().put("android", Build.VERSION.SDK_INT).put("phone", Build.MANUFACTURER + " " + Build.MODEL)
                .put("mode", modes[am.mode] ?: am.mode.toString()).put("speakerphone", am.isSpeakerphoneOn).put("commDevice", dev)
                .put("want", wantSpeaker?.toString() ?: "none").put("callService", CallService.running).put("outputs", outs)
                .put("voiceVol", am.getStreamVolume(AudioManager.STREAM_VOICE_CALL).toString() + "/" + am.getStreamMaxVolume(AudioManager.STREAM_VOICE_CALL))
                .put("app", BuildConfig.VERSION_NAME).toString()
        } catch (e: Exception) { "{\"error\":\"" + (e.message ?: "?") + "\"}" }
        @JavascriptInterface fun getContacts(): String {
            if (checkSelfPermission(Manifest.permission.READ_CONTACTS) != PackageManager.PERMISSION_GRANTED) {
                runOnUiThread { requestPermissions(arrayOf(Manifest.permission.READ_CONTACTS), 77) }
                return "PERMISSION"
            }
            val byName = LinkedHashMap<String, MutableSet<String>>()
            try {
                contentResolver.query(android.provider.ContactsContract.CommonDataKinds.Phone.CONTENT_URI,
                    arrayOf(android.provider.ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME, android.provider.ContactsContract.CommonDataKinds.Phone.NUMBER),
                    null, null, android.provider.ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME + " ASC")?.use { c ->
                    while (c.moveToNext() && byName.size < 5000) {
                        val name = c.getString(0) ?: ""; val num = c.getString(1) ?: continue
                        byName.getOrPut(name.ifBlank { num }) { LinkedHashSet() }.add(num)
                    }
                }
            } catch (e: Exception) { return "[]" }
            val arr = org.json.JSONArray()
            for ((name, nums) in byName) arr.put(org.json.JSONObject().put("name", name).put("phones", org.json.JSONArray(nums.toList())))
            return arr.toString()
        }
        /** Save a file made by the web app (chat backup) and open Android's Share menu → Google Drive.
         *  Sent in pieces so big backups do not run out of memory. */
        private var shareOut: java.io.File? = null
        @JavascriptInterface fun shareFileStart(name: String): Boolean = try {
            val dir = java.io.File(cacheDir, "share").apply { mkdirs() }
            dir.listFiles()?.forEach { if (System.currentTimeMillis() - it.lastModified() > 3_600_000) it.delete() }
            shareOut = java.io.File(dir, name.replace(Regex("[^A-Za-z0-9._-]"), "_")).apply { writeBytes(ByteArray(0)) }
            true
        } catch (e: Exception) { false }
        @JavascriptInterface fun shareFileChunk(b64: String): Boolean = try {
            shareOut?.appendBytes(android.util.Base64.decode(b64, android.util.Base64.DEFAULT)); shareOut != null
        } catch (e: Exception) { false }
        @JavascriptInterface fun shareFileEnd(mime: String, title: String): Boolean = try {
            val f = shareOut ?: throw IllegalStateException("no file")
            val uri = androidx.core.content.FileProvider.getUriForFile(this@MainActivity, "$packageName.files", f)
            val send = Intent(Intent.ACTION_SEND).setType(mime).putExtra(Intent.EXTRA_STREAM, uri)
                .putExtra(Intent.EXTRA_TITLE, f.name).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            runOnUiThread { startActivity(Intent.createChooser(send, title)) }
            true
        } catch (e: Exception) { false }
        // 📍 Team tracking (on duty)
        @JavascriptInterface fun trackStatus(): String = TeamTrackService.status(this@MainActivity)
        @JavascriptInterface fun trackRunning(): Boolean = TeamTrackService.running
        @JavascriptInterface fun trackStart(json: String): Boolean = try { TeamTrackService.start(this@MainActivity, org.json.JSONObject(json)) } catch (e: Exception) { false }
        @JavascriptInterface fun trackStop() { TeamTrackService.stop(this@MainActivity) }
        @JavascriptInterface fun trackAskPermission() { runOnUiThread {
            if (!has(Manifest.permission.ACCESS_FINE_LOCATION)) { pendingTrack = true; requestPermissions(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION), REQ_PERMS) }
            else web.evaluateJavascript("window.maataTrackPerm && maataTrackPerm(" + org.json.JSONObject.quote(TeamTrackService.status(this@MainActivity)) + ")", null)
        } }
        /** 🔊 spoken order alert while Maata is open (same voice as when it is closed) */
        @JavascriptInterface fun speakAlert(text: String, lang: String, fallbackEn: String) { Thread { OrderVoice.speakBlocking(this@MainActivity, text, lang, fallbackEn, force = true) }.start() }
        @JavascriptInterface fun voiceAlertsOn(): Boolean = OrderVoice.enabled(this@MainActivity)
        @JavascriptInterface fun setVoiceAlerts(on: Boolean) { OrderVoice.setEnabled(this@MainActivity, on) }
        @JavascriptInterface fun trackBgStatus(): String = TeamTrackService.bgStatus(this@MainActivity).toString()
        /** "Allow all the time" location (Android 10+: Android shows its own settings page for this). */
        @JavascriptInterface fun trackAskBgLocation() { runOnUiThread {
            try {
                if (Build.VERSION.SDK_INT >= 29 && !has(Manifest.permission.ACCESS_BACKGROUND_LOCATION)) {
                    if (!has(Manifest.permission.ACCESS_FINE_LOCATION)) requestPermissions(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION), REQ_PERMS)
                    else requestPermissions(arrayOf(Manifest.permission.ACCESS_BACKGROUND_LOCATION), REQ_PERMS)
                }
            } catch (e: Exception) { openAppSettings() }
        } }
        /** Ask Android not to put Maata to sleep (battery optimisation off) so sharing keeps running when Maata is closed. */
        @JavascriptInterface fun trackAskBattery() { runOnUiThread {
            try { startActivity(Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, android.net.Uri.parse("package:" + packageName))) }
            catch (e: Exception) { try { startActivity(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS)) } catch (_: Exception) { } }
        } }
        @JavascriptInterface fun trackOpenAutostart() { runOnUiThread { TeamTrackService.openAutostart(this@MainActivity) } }
        @JavascriptInterface fun openLocationSettings() { runOnUiThread { try { startActivity(Intent(Settings.ACTION_LOCATION_SOURCE_SETTINGS)) } catch (e: Exception) { } } }
        // 🖨️ Bluetooth receipt printer
        @JavascriptInterface fun btStatus(): String = BtPrinter.status(this@MainActivity)
        @JavascriptInterface fun btPrinters(): String = BtPrinter.paired(this@MainActivity)
        @JavascriptInterface fun btAskPermission() { runOnUiThread {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && BtPrinter.needsPermission(this@MainActivity)) { pendingBt = true; requestPermissions(arrayOf(Manifest.permission.BLUETOOTH_CONNECT), REQ_PERMS) }
            else web.evaluateJavascript("window.maataBtPerm && maataBtPerm(" + org.json.JSONObject.quote(BtPrinter.status(this@MainActivity)) + ")", null)
        } }
        @JavascriptInterface fun btOpenSettings() { runOnUiThread { try { startActivity(Intent(Settings.ACTION_BLUETOOTH_SETTINGS)) } catch (e: Exception) { } } }
        @JavascriptInterface fun btScanStatus(): String = if (BtPrinter.needsScanPermission(this@MainActivity)) "perm" else BtPrinter.status(this@MainActivity)
        @JavascriptInterface fun btAskScanPermission() { runOnUiThread {
            val need = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) arrayOf(Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT) else arrayOf(Manifest.permission.ACCESS_FINE_LOCATION)
            if (BtPrinter.needsScanPermission(this@MainActivity)) { pendingBt = true; requestPermissions(need, REQ_PERMS) }
            else web.evaluateJavascript("window.maataBtPerm && maataBtPerm(" + org.json.JSONObject.quote(BtPrinter.status(this@MainActivity)) + ")", null)
        } }
        @JavascriptInterface fun btScan() { runOnUiThread { BtPrinter.scan(this@MainActivity,
            { j -> runOnUiThread { web.evaluateJavascript("window.maataBtFound && maataBtFound(" + j + ")", null) } },
            { st -> runOnUiThread { web.evaluateJavascript("window.maataBtScanEnd && maataBtScanEnd(" + org.json.JSONObject.quote(st) + ")", null) } }) } }
        /** Which Bluetooth permissions Maata has, and whether the app asks for them at all (for the printer check). */
        @JavascriptInterface fun btPermInfo(): String = try {
            val req = packageManager.getPackageInfo(packageName, PackageManager.GET_PERMISSIONS).requestedPermissions?.toList() ?: emptyList()
            val s31 = Build.VERSION.SDK_INT >= Build.VERSION_CODES.S
            org.json.JSONObject().put("sdk", Build.VERSION.SDK_INT)
                .put("connect", !s31 || has(Manifest.permission.BLUETOOTH_CONNECT)).put("scan", if (s31) has(Manifest.permission.BLUETOOTH_SCAN) else has(Manifest.permission.ACCESS_FINE_LOCATION))
                .put("declared", !s31 || (req.contains(Manifest.permission.BLUETOOTH_SCAN) && req.contains(Manifest.permission.BLUETOOTH_CONNECT))).toString()
        } catch (e: Exception) { "{}" }
        /** Opens Android's page for Maata (Permissions → Nearby devices). */
        @JavascriptInterface fun openAppSettings() { runOnUiThread { try { startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:" + packageName)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) } catch (e: Exception) { } } }
        @JavascriptInterface fun btStopScan() { runOnUiThread { BtPrinter.stopScan(this@MainActivity) } }
        @JavascriptInterface fun btPair(address: String) { runOnUiThread { BtPrinter.pair(this@MainActivity, address) { st -> runOnUiThread { web.evaluateJavascript("window.maataBtPaired && maataBtPaired(" + org.json.JSONObject.quote(address) + "," + org.json.JSONObject.quote(st) + ")", null) } } } }
        // 🔌 USB and 🌐 Wi-Fi / LAN receipt printers
        @JavascriptInterface fun usbPrinters(): String = try { UsbNetPrinter.list(this@MainActivity) } catch (e: Exception) { "[]" }
        @JavascriptInterface fun usbAskPermission(key: String) { runOnUiThread { UsbNetPrinter.askPermission(this@MainActivity, key) { ok -> runOnUiThread { web.evaluateJavascript("window.maataUsbPerm && maataUsbPerm(" + org.json.JSONObject.quote(key) + "," + ok + ")", null) } } } }
        @JavascriptInterface fun usbPrintEnd(key: String, id: String) {
            val bytes = btOut.toByteArray(); btOut = java.io.ByteArrayOutputStream()
            UsbNetPrinter.printUsb(this@MainActivity, key, bytes) { ok, msg -> runOnUiThread { web.evaluateJavascript("window.maataBtDone && maataBtDone(" + org.json.JSONObject.quote(id) + "," + ok + "," + org.json.JSONObject.quote(msg) + ")", null) } }
        }
        @JavascriptInterface fun netPrintEnd(host: String, port: Int, id: String) {
            val bytes = btOut.toByteArray(); btOut = java.io.ByteArrayOutputStream()
            UsbNetPrinter.printNet(host, port, bytes) { ok, msg -> runOnUiThread { web.evaluateJavascript("window.maataBtDone && maataBtDone(" + org.json.JSONObject.quote(id) + "," + ok + "," + org.json.JSONObject.quote(msg) + ")", null) } }
        }
        /** Save the file made by shareFileStart/Chunk into Downloads/Maata (bill PDFs). "ok", "share" (old Android: use the share menu) or "error". */
        private var savedUri: Uri? = null
        private var savedMime = "application/pdf"
        @JavascriptInterface fun saveFileEnd(mime: String): String = try {
            val f = shareOut ?: throw IllegalStateException("no file")
            if (Build.VERSION.SDK_INT < 29) "share" else {
                val cv = android.content.ContentValues().apply {
                    put(android.provider.MediaStore.MediaColumns.DISPLAY_NAME, f.name)
                    put(android.provider.MediaStore.MediaColumns.MIME_TYPE, mime)
                    put(android.provider.MediaStore.MediaColumns.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS + "/Maata")
                }
                val uri = contentResolver.insert(android.provider.MediaStore.Downloads.EXTERNAL_CONTENT_URI, cv) ?: throw IllegalStateException("insert")
                contentResolver.openOutputStream(uri)?.use { o -> f.inputStream().use { it.copyTo(o) } } ?: throw IllegalStateException("open")
                savedUri = uri; savedMime = mime; "ok"
            }
        } catch (e: Exception) { "error" }
        /** Open the file saved last (PDF viewer). */
        @JavascriptInterface fun openSavedFile(): Boolean = try {
            val u = savedUri ?: throw IllegalStateException("none")
            val v = Intent(Intent.ACTION_VIEW).setDataAndType(u, savedMime).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_ACTIVITY_NEW_TASK)
            runOnUiThread { try { startActivity(v) } catch (_: Exception) { web.evaluateJavascript("toast('No PDF app found. Install any PDF viewer, or tap Share.')", null) } }
            true
        } catch (e: Exception) { false }
        @JavascriptInterface fun printPace(n: Int) { BtPrinter.pace = n.coerceIn(1, 2) }
        @JavascriptInterface fun btPrintStart(): Boolean { btOut = java.io.ByteArrayOutputStream(); return true }
        @JavascriptInterface fun btPrintChunk(b64: String): Boolean = try { btOut.write(android.util.Base64.decode(b64, android.util.Base64.DEFAULT)); btOut.size() < 8_000_000 } catch (e: Exception) { false }
        @JavascriptInterface fun btPrintEnd(address: String, id: String) {
            val bytes = btOut.toByteArray(); btOut = java.io.ByteArrayOutputStream()
            BtPrinter.print(this@MainActivity, address, bytes) { ok, msg -> runOnUiThread { web.evaluateJavascript("window.maataBtDone && maataBtDone(" + org.json.JSONObject.quote(id) + "," + ok + "," + org.json.JSONObject.quote(msg) + ")", null) } }
        }
        /** Same as shareFileEnd, plus a caption that apps like WhatsApp show with the photo. */
        @JavascriptInterface fun shareFileEndText(mime: String, title: String, text: String): Boolean = try {
            val f = shareOut ?: throw IllegalStateException("no file")
            val uri = androidx.core.content.FileProvider.getUriForFile(this@MainActivity, "$packageName.files", f)
            val send = Intent(Intent.ACTION_SEND).setType(mime).putExtra(Intent.EXTRA_STREAM, uri)
                .putExtra(Intent.EXTRA_TITLE, f.name).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            if (text.isNotBlank()) send.putExtra(Intent.EXTRA_TEXT, text.take(4000))
            send.clipData = android.content.ClipData.newRawUri(f.name, uri)
            runOnUiThread { startActivity(Intent.createChooser(send, title)) }
            true
        } catch (e: Exception) { false }
        /** Share plain text (a message, a link) with Android's share menu. */
        @JavascriptInterface fun shareText(text: String, title: String): Boolean = try {
            val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, text.take(60000))
            runOnUiThread { startActivity(Intent.createChooser(send, title)) }
            true
        } catch (e: Exception) { false }
        /** In-app update: download the newest APK from maataapp.com and open Android's "Update" screen.
         *  Returns "OK", "PERMISSION" (the person must allow Maata to install apps first) or "ERROR". */
        @JavascriptInterface fun autoUpdate(url: String): String = when { updFile?.exists() == true -> "READY"; updDownloading -> "BUSY"; else -> { downloadUpdate(url); "STARTED" } }
        @JavascriptInterface fun installNow(): String = try { installDownloaded() } catch (e: Exception) { "ERROR" }
        @JavascriptInterface fun silentUpdate(): Boolean = canSilentUpdate()
        @JavascriptInterface fun installUpdate(url: String): String = try {
            if (Build.VERSION.SDK_INT >= 26 && !packageManager.canRequestPackageInstalls()) {
                runOnUiThread { try { startActivity(Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:$packageName"))) } catch (_: Exception) { } }
                "PERMISSION"
            } else {
                val dir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: throw IllegalStateException("no storage")
                val apk = java.io.File(dir, "maata-update.apk"); if (apk.exists()) apk.delete()
                val dm = getSystemService(DownloadManager::class.java)
                val id = dm.enqueue(DownloadManager.Request(Uri.parse(url)).setTitle("Maata update").setDescription("Downloading the new Maata app")
                    .setMimeType("application/vnd.android.package-archive").setDestinationUri(Uri.fromFile(apk))
                    .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE))
                val done = object : android.content.BroadcastReceiver() {
                    override fun onReceive(c: android.content.Context, i: Intent) {
                        if (i.getLongExtra(DownloadManager.EXTRA_DOWNLOAD_ID, -1L) != id) return
                        try { unregisterReceiver(this) } catch (_: Exception) { }
                        try {
                            val uri = androidx.core.content.FileProvider.getUriForFile(this@MainActivity, "$packageName.files", apk)
                            startActivity(Intent(Intent.ACTION_VIEW).setDataAndType(uri, "application/vnd.android.package-archive")
                                .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_ACTIVITY_NEW_TASK))
                        } catch (_: Exception) { Toast.makeText(this@MainActivity, "Open Downloads and tap the Maata update", Toast.LENGTH_LONG).show() }
                    }
                }
                val filter = android.content.IntentFilter(DownloadManager.ACTION_DOWNLOAD_COMPLETE)
                runOnUiThread { if (Build.VERSION.SDK_INT >= 33) registerReceiver(done, filter, android.content.Context.RECEIVER_EXPORTED) else registerReceiver(done, filter) }
                "OK"
            }
        } catch (e: Exception) { "ERROR" }
        /** App lock with the phone's fingerprint / face. "OK", "NONE_ENROLLED", "NO_HARDWARE" or "OLD" (Android 8). */
        @JavascriptInterface fun biometricStatus(): String = try {
            when {
                Build.VERSION.SDK_INT < 29 -> if (Build.VERSION.SDK_INT >= 28 && packageManager.hasSystemFeature(PackageManager.FEATURE_FINGERPRINT)) "OK" else "OLD"
                else -> {
                    val bm = getSystemService(android.hardware.biometrics.BiometricManager::class.java)
                    val r = if (Build.VERSION.SDK_INT >= 30) bm.canAuthenticate(android.hardware.biometrics.BiometricManager.Authenticators.BIOMETRIC_WEAK) else @Suppress("DEPRECATION") bm.canAuthenticate()
                    when (r) {
                        android.hardware.biometrics.BiometricManager.BIOMETRIC_SUCCESS -> "OK"
                        android.hardware.biometrics.BiometricManager.BIOMETRIC_ERROR_NONE_ENROLLED -> "NONE_ENROLLED"
                        else -> "NO_HARDWARE"
                    }
                }
            }
        } catch (e: Exception) { "NO_HARDWARE" }
        /** Shows the fingerprint sheet; answers through window.maataBioResult(id, ok, message). */
        @JavascriptInterface fun biometricAuth(id: String, title: String) {
            if (Build.VERSION.SDK_INT < 28) { bioReply(id, false, "OLD"); return }
            runOnUiThread {
                try {
                    val b = android.hardware.biometrics.BiometricPrompt.Builder(this@MainActivity)
                        .setTitle(title.ifBlank { "Unlock Maata" }).setSubtitle("Touch the fingerprint sensor")
                        .setNegativeButton("Use PIN", mainExecutor) { _, _ -> bioReply(id, false, "PIN") }
                    if (Build.VERSION.SDK_INT >= 30) b.setAllowedAuthenticators(android.hardware.biometrics.BiometricManager.Authenticators.BIOMETRIC_WEAK)
                    if (Build.VERSION.SDK_INT >= 29) b.setConfirmationRequired(false)
                    b.build().authenticate(android.os.CancellationSignal(), mainExecutor, object : android.hardware.biometrics.BiometricPrompt.AuthenticationCallback() {
                        override fun onAuthenticationSucceeded(result: android.hardware.biometrics.BiometricPrompt.AuthenticationResult) { bioReply(id, true, "") }
                        override fun onAuthenticationError(code: Int, msg: CharSequence) { bioReply(id, false, msg.toString()) }
                    })
                } catch (e: Exception) { bioReply(id, false, e.message ?: "error") }
            }
        }
        @JavascriptInterface fun speak(text: String, lang: String): String = try {
            when (ttsState) { 1 -> speakNow(text.take(400), lang); 0 -> { ttsPending = Triple(text.take(400), lang, "maata" + System.nanoTime()); startTts(); "wait" }; else -> "error" }
        } catch (_: Exception) { "error" }
        /** same as speak, with our own id: the page then gets start / word / done events for it (lip-sync) */
        @JavascriptInterface fun speakId(text: String, lang: String, id: String): String = try {
            when (ttsState) { 1 -> speakNow(text.take(400), lang, id); 0 -> { ttsPending = Triple(text.take(400), lang, id); startTts(); "wait" }; else -> "error" }
        } catch (_: Exception) { "error" }
        @JavascriptInterface fun ttsCheck(lang: String): String = when (ttsState) { 1 -> if (langOk(lang)) "ok" else "nolang"; 0 -> "wait"; else -> "error" }
        @JavascriptInterface fun listen(id: String, lang: String) { runOnUiThread { try { startListening(id, lang) } catch (e: Exception) { listenReply(id, false, "error") } } }
        @JavascriptInterface fun stopListening() { runOnUiThread { try { recognizer?.stopListening() } catch (_: Exception) { } } }
        @JavascriptInterface fun stopSpeaking() { try { tts?.stop() } catch (_: Exception) { } }
        @JavascriptInterface fun installTtsVoice() { runOnUiThread {
            try { startActivity(Intent(TextToSpeech.Engine.ACTION_INSTALL_TTS_DATA).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) }
            catch (_: Exception) { try { startActivity(Intent("com.android.settings.TTS_SETTINGS").addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) } catch (_: Exception) { Toast.makeText(this@MainActivity, "Open Settings → Text-to-speech to add the voice", Toast.LENGTH_LONG).show() } }
        } }
        @JavascriptInterface fun startPhoneRingtone(vibrate: Boolean): Boolean = startPhoneRing(vibrate)
        @JavascriptInterface fun stopPhoneRingtone() { stopPhoneRing() }
        @JavascriptInterface fun secureAlarm(on: Boolean): Boolean = if (on) SecureAlarm.start(this@MainActivity) else { SecureAlarm.stop(this@MainActivity); true }
        @JavascriptInterface fun setSpeaker(on: Boolean): Boolean { wantSpeaker = on; guardRoute(); return applyRoute(on) }
        @JavascriptInterface fun openNotificationSettings() {
            runOnUiThread { startActivity(Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, packageName)) }
        }
    }

    private val OFFLINE_PAGE = """<html><body style="margin:0;height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#0A3A47;color:#fff;font-family:sans-serif;text-align:center">
        <div style="font-size:64px">🦜</div><h2>No internet</h2><p style="opacity:.8">Check your connection and try again.</p>
        <button onclick="location.href='${BuildConfig.APP_URL}'" style="margin-top:12px;padding:14px 28px;border:0;border-radius:999px;background:#2E8B57;color:#fff;font-size:17px">Try again</button></body></html>"""
}
