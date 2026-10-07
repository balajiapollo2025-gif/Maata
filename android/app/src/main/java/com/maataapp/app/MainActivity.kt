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
import android.webkit.*
import android.widget.Toast
import com.google.firebase.messaging.FirebaseMessaging

/** Maata app: the Maata web app in a full-screen WebView, with native calls, notifications and permissions. */
class MainActivity : Activity() {
    companion object {
        const val EXTRA_URL = "maata_url"
        const val EXTRA_STOP_RING = "maata_stop_ring"
        const val EXTRA_HANGUP = "maata_hangup"
        private const val REQ_FILE = 11
        private const val REQ_PERMS = 12
        private val OUR_HOSTS = setOf("maataapp.com", "www.maataapp.com", "maata-siut.onrender.com")
    }
    private lateinit var web: WebView
    private var fileCallback: ValueCallback<Array<Uri>>? = null
    private var pendingWebPermission: PermissionRequest? = null
    private var pendingGeo: Pair<String, GeolocationPermissions.Callback>? = null
    private var pageReady = false

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Notifier.createChannels(this)
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
        pendingGeo?.let { (origin, cb) -> cb.invoke(origin, has(Manifest.permission.ACCESS_FINE_LOCATION) || has(Manifest.permission.ACCESS_COARSE_LOCATION), false); pendingGeo = null }
    }

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

    override fun onDestroy() { stopPhoneRing(); stopRouteGuard(); CallService.stop(this); super.onDestroy() }

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
        @JavascriptInterface fun startPhoneRingtone(vibrate: Boolean): Boolean = startPhoneRing(vibrate)
        @JavascriptInterface fun stopPhoneRingtone() { stopPhoneRing() }
        @JavascriptInterface fun setSpeaker(on: Boolean): Boolean { wantSpeaker = on; guardRoute(); return applyRoute(on) }
        @JavascriptInterface fun openNotificationSettings() {
            runOnUiThread { startActivity(Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, packageName)) }
        }
    }

    private val OFFLINE_PAGE = """<html><body style="margin:0;height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#0A3A47;color:#fff;font-family:sans-serif;text-align:center">
        <div style="font-size:64px">🦜</div><h2>No internet</h2><p style="opacity:.8">Check your connection and try again.</p>
        <button onclick="location.href='${BuildConfig.APP_URL}'" style="margin-top:12px;padding:14px 28px;border:0;border-radius:999px;background:#2E8B57;color:#fff;font-size:17px">Try again</button></body></html>"""
}
