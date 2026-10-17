package com.maataapp.app

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlarmManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.BatteryManager
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import android.os.SystemClock
import android.provider.Settings
import android.net.Uri
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/**
 * 📍 Team tracking: while a staff member is ON DUTY, sends the phone location to their team every 30 s – 5 min,
 * also when the screen is locked or Maata is in the background. It always shows a notification, it stops by itself
 * at the end of work hours, and it stops at once when the person ends duty or the team stops it.
 * Points are kept in a queue when there is no internet and sent later. A small "I am online" signal goes every interval
 * even when the person is not moving, so the admin never sees "offline" just because Maata is closed.
 */
class TeamTrackService : Service(), LocationListener {
    companion object {
        private const val CH = "maata_team_track"
        private const val NOTIF_ID = 4343
        private const val PREFS = "maata_track"
        @Volatile var running = false

        fun start(ctx: Context, cfg: JSONObject): Boolean {
            if (ctx.checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED &&
                ctx.checkSelfPermission(Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) return false
            ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putString("cfg", cfg.toString()).apply()
            val i = Intent(ctx, TeamTrackService::class.java)
            return try { if (Build.VERSION.SDK_INT >= 26) ctx.startForegroundService(i) else ctx.startService(i); true } catch (e: Exception) { false }
        }
        fun stop(ctx: Context) {
            ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().remove("cfg").apply()
            watchdog(ctx, false)
            try { ctx.stopService(Intent(ctx, TeamTrackService::class.java)) } catch (_: Exception) { }
        }
        fun onDuty(ctx: Context) = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString("cfg", null) != null
        /** Started again by the phone (after restart, app update, or the watchdog alarm) — only if the person is still on duty. */
        fun resume(ctx: Context) {
            if (!onDuty(ctx) || running) return
            val i = Intent(ctx, TeamTrackService::class.java)
            try { if (Build.VERSION.SDK_INT >= 26) ctx.startForegroundService(i) else ctx.startService(i) } catch (_: Exception) { }
        }
        /** Every ~10 min the phone checks that sharing is still running (OEM battery savers sometimes kill it when Maata is closed). */
        fun watchdog(ctx: Context, on: Boolean) {
            val am = ctx.getSystemService(AlarmManager::class.java) ?: return
            val pi = PendingIntent.getBroadcast(ctx, 77, Intent(ctx, TrackBootReceiver::class.java).setAction("com.maataapp.app.TRACK_WATCHDOG"), PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
            try {
                if (on) am.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime() + 10 * 60_000L, 10 * 60_000L, pi) else am.cancel(pi)
            } catch (_: Exception) { }
        }
        /** What is still needed so sharing keeps going while Maata is closed. */
        fun bgStatus(ctx: Context): JSONObject {
            val bgLoc = Build.VERSION.SDK_INT < 29 || ctx.checkSelfPermission(Manifest.permission.ACCESS_BACKGROUND_LOCATION) == PackageManager.PERMISSION_GRANTED
            val battery = try { ctx.getSystemService(PowerManager::class.java).isIgnoringBatteryOptimizations(ctx.packageName) } catch (_: Exception) { true }
            val notif = Build.VERSION.SDK_INT < 33 || ctx.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED
            val maker = Build.MANUFACTURER.lowercase()
            val autostart = listOf("xiaomi", "redmi", "poco", "oppo", "realme", "oneplus", "vivo", "iqoo", "huawei", "honor", "asus", "letv", "tecno", "infinix", "itel").any { maker.contains(it) }
            return JSONObject().put("bgLoc", bgLoc).put("battery", battery).put("notif", notif).put("maker", Build.MANUFACTURER).put("autostart", autostart).put("running", running)
        }
        /** Phone makers' own "Autostart / background" settings pages (Xiaomi, Oppo, Vivo, Realme…); falls back to Maata's app settings. */
        fun openAutostart(ctx: Context) {
            val tries = listOf(
                Intent().setClassName("com.miui.securitycenter", "com.miui.permcenter.autostart.AutoStartManagementActivity"),
                Intent().setClassName("com.coloros.safecenter", "com.coloros.safecenter.permission.startup.StartupAppListActivity"),
                Intent().setClassName("com.coloros.safecenter", "com.coloros.safecenter.startupapp.StartupAppListActivity"),
                Intent().setClassName("com.oppo.safe", "com.oppo.safe.permission.startup.StartupAppListActivity"),
                Intent().setClassName("com.vivo.permissionmanager", "com.vivo.permissionmanager.activity.BgStartUpManagerActivity"),
                Intent().setClassName("com.iqoo.secure", "com.iqoo.secure.ui.phoneoptimize.BgStartUpManager"),
                Intent().setClassName("com.huawei.systemmanager", "com.huawei.systemmanager.startupmgr.ui.StartupNormalAppListActivity"),
                Intent().setClassName("com.hihonor.systemmanager", "com.hihonor.systemmanager.startupmgr.ui.StartupNormalAppListActivity"),
                Intent().setClassName("com.asus.mobilemanager", "com.asus.mobilemanager.autostart.AutoStartActivity"),
                Intent().setClassName("com.transsion.phonemaster", "com.cyin.himgr.autostart.AutoStartActivity"))
            for (i in tries) try { ctx.startActivity(i.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)); return } catch (_: Exception) { }
            try { ctx.startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:" + ctx.packageName)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) } catch (_: Exception) { }
        }
        fun status(ctx: Context): String {
            if (ctx.checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) return "perm"
            val lm = ctx.getSystemService(Context.LOCATION_SERVICE) as LocationManager
            val on = try { lm.isProviderEnabled(LocationManager.GPS_PROVIDER) || lm.isProviderEnabled(LocationManager.NETWORK_PROVIDER) } catch (_: Exception) { false }
            return if (on) "ok" else "gps_off"
        }
    }

    private val h = Handler(Looper.getMainLooper())
    private val queue = JSONArray()
    private var cfg = JSONObject()
    private var lastQueued: Location? = null
    private var lastQueuedAt = 0L
    private var lastFixAt = 0L
    private var lastBeat = 0L
    private var gpsOff = false
    private var sending = false
    private var lock: PowerManager.WakeLock? = null
    private val tick = object : Runnable { override fun run() { freshFix(); flush(); h.postDelayed(this, intervalMs()) } }

    override fun onBind(intent: Intent?): IBinder? = null
    private fun intervalMs() = (cfg.optInt("interval", 60).coerceIn(30, 300)) * 1000L

    @SuppressLint("MissingPermission")
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val saved = getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString("cfg", null)
        if (saved == null) { stopSelf(); return START_NOT_STICKY }
        cfg = JSONObject(saved)
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26 && nm.getNotificationChannel(CH) == null)
            nm.createNotificationChannel(NotificationChannel(CH, "Team location (on duty)", NotificationManager.IMPORTANCE_LOW).apply { description = "Shown while you share your location with your team"; setShowBadge(false) })
        val open = PendingIntent.getActivity(this, 7, Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP), PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        val n = Notification.Builder(this, CH).setSmallIcon(R.drawable.ic_notification)
            .setContentTitle("📍 On duty · " + cfg.optString("team", "Team"))
            .setContentText("Your location is shared with the team admins until you end duty")
            .setOngoing(true).setContentIntent(open).setCategory(Notification.CATEGORY_SERVICE).build()
        try { if (Build.VERSION.SDK_INT >= 29) startForeground(NOTIF_ID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION) else startForeground(NOTIF_ID, n) }
        catch (e: Exception) { stopSelf(); return START_NOT_STICKY }
        if (!running) {
            running = true
            val lm = getSystemService(Context.LOCATION_SERVICE) as LocationManager
            val every = intervalMs() / 2
            try { if (lm.allProviders.contains(LocationManager.GPS_PROVIDER)) lm.requestLocationUpdates(LocationManager.GPS_PROVIDER, every, 0f, this, Looper.getMainLooper()) } catch (_: Exception) { }
            try { if (lm.allProviders.contains(LocationManager.NETWORK_PROVIDER)) lm.requestLocationUpdates(LocationManager.NETWORK_PROVIDER, every, 0f, this, Looper.getMainLooper()) } catch (_: Exception) { }
            try { if (lm.allProviders.contains(LocationManager.PASSIVE_PROVIDER)) lm.requestLocationUpdates(LocationManager.PASSIVE_PROVIDER, every, 0f, this, Looper.getMainLooper()) } catch (_: Exception) { }
            watchdog(this, true)
            val left = (cfg.optLong("endAt", 0) - System.currentTimeMillis()).coerceIn(60_000L, 14 * 3600_000L)
            lock = getSystemService(PowerManager::class.java).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "maata:team").apply { setReferenceCounted(false); acquire(left) }
            h.postDelayed(tick, 5000)
        } else { h.removeCallbacks(tick); h.postDelayed(tick, 3000) }
        return START_STICKY // Android restarts it if it is killed; the saved settings say where to continue
    }

    /** No location for a while (screen off / indoors)? Ask the phone for one fresh fix. */
    @SuppressLint("MissingPermission")
    private fun freshFix() {
        if (System.currentTimeMillis() - lastFixAt < intervalMs() * 3 / 2) return
        val lm = getSystemService(Context.LOCATION_SERVICE) as LocationManager
        for (pv in listOf(LocationManager.NETWORK_PROVIDER, LocationManager.GPS_PROVIDER)) {
            try {
                if (!lm.isProviderEnabled(pv)) continue
                if (Build.VERSION.SDK_INT >= 30) lm.getCurrentLocation(pv, null, mainExecutor) { l -> if (l != null) onLocationChanged(l) }
                else @Suppress("DEPRECATION") lm.requestSingleUpdate(pv, this, Looper.getMainLooper())
            } catch (_: Exception) { }
        }
    }
    private fun battery(): Int = try { getSystemService(BatteryManager::class.java).getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY) } catch (_: Exception) { -1 }
    private fun isMock(l: Location): Boolean = if (Build.VERSION.SDK_INT >= 31) l.isMock else @Suppress("DEPRECATION") l.isFromMockProvider

    override fun onLocationChanged(l: Location) {
        if (gpsOff) gpsOff = false
        if (System.currentTimeMillis() - l.time > 10 * 60_000L) return // an old cached fix: not a live location
        lastFixAt = System.currentTimeMillis()
        val prev = lastQueued
        val better = prev != null && l.time - lastQueuedAt < intervalMs() * 8 / 10 && l.accuracy < prev.accuracy - 10
        val due = prev == null || System.currentTimeMillis() - lastQueuedAt >= intervalMs() * 8 / 10 || l.distanceTo(prev) > 150
        if (!due && !better) return
        if (better && queue.length() > 0) queue.remove(queue.length() - 1)
        queue.put(JSONObject().put("t", System.currentTimeMillis()).put("lat", l.latitude).put("lng", l.longitude).put("acc", l.accuracy.toDouble())
            .put("spd", if (l.hasSpeed()) l.speed.toDouble() else 0.0).put("bat", battery()).put("mock", isMock(l)))
        while (queue.length() > 600) queue.remove(0)
        lastQueued = l; lastQueuedAt = System.currentTimeMillis()
    }
    override fun onProviderDisabled(provider: String) {
        val lm = getSystemService(Context.LOCATION_SERVICE) as LocationManager
        val any = try { lm.isProviderEnabled(LocationManager.GPS_PROVIDER) || lm.isProviderEnabled(LocationManager.NETWORK_PROVIDER) } catch (_: Exception) { false }
        if (!any) { gpsOff = true; flush() }
    }
    override fun onProviderEnabled(provider: String) { gpsOff = false }
    @Deprecated("Deprecated in Java") override fun onStatusChanged(provider: String?, status: Int, extras: Bundle?) { }

    private fun flush() {
        if (System.currentTimeMillis() > cfg.optLong("endAt", Long.MAX_VALUE) + 120_000) { stopAll(); return }
        val beat = queue.length() == 0 && !gpsOff
        if (sending || (beat && System.currentTimeMillis() - lastBeat < intervalMs() - 2000)) return
        lastBeat = System.currentTimeMillis()
        sending = true
        val batch = JSONArray(); for (k in 0 until queue.length()) batch.put(queue.get(k))
        val body = JSONObject().put("pts", batch).put("alive", true).put("bat", battery()).apply { if (gpsOff) put("gps", "off") }.toString()
        Thread {
            var keepGoing = true
            try {
                val c = URL(cfg.getString("base") + "/api/teams/" + cfg.getString("teamId") + "/loc").openConnection() as HttpURLConnection
                c.requestMethod = "POST"; c.connectTimeout = 15000; c.readTimeout = 20000; c.doOutput = true
                c.setRequestProperty("Content-Type", "application/json"); c.setRequestProperty("Authorization", "Bearer " + cfg.getString("token"))
                c.outputStream.use { it.write(body.toByteArray()) }
                val code = c.responseCode
                if (code == 401 || code == 403 || code == 404) keepGoing = false
                else if (code in 200..299) {
                    val r = JSONObject(c.inputStream.bufferedReader().readText())
                    h.post { for (k in 0 until batch.length()) if (queue.length() > 0) queue.remove(0) }
                    if (!r.optBoolean("on", false)) keepGoing = false
                    else { val iv = r.optInt("interval", cfg.optInt("interval", 60)); if (iv != cfg.optInt("interval")) cfg.put("interval", iv); if (r.has("endAt")) cfg.put("endAt", r.optLong("endAt")) }
                }
                c.disconnect()
            } catch (_: Exception) { /* no internet: keep the points and try again next time */ }
            h.post { sending = false; if (!keepGoing) stopAll() }
        }.start()
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        super.onTaskRemoved(rootIntent)
        if (!onDuty(this)) return
        val am = getSystemService(AlarmManager::class.java)
        val pi = PendingIntent.getBroadcast(this, 78, Intent(this, TrackBootReceiver::class.java).setAction("com.maataapp.app.TRACK_WATCHDOG"), PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        try { am?.set(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime() + 5000, pi) } catch (_: Exception) { }
    }

    private fun stopAll() {
        watchdog(this, false)
        getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().remove("cfg").apply()
        stopSelf()
    }

    override fun onDestroy() {
        running = false
        h.removeCallbacks(tick)
        try { (getSystemService(Context.LOCATION_SERVICE) as LocationManager).removeUpdates(this) } catch (_: Exception) { }
        try { lock?.release() } catch (_: Exception) { }
        super.onDestroy()
    }
}
