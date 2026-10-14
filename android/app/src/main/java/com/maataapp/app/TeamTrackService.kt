package com.maataapp.app

import android.Manifest
import android.annotation.SuppressLint
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
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/**
 * 📍 Team tracking: while a staff member is ON DUTY, sends the phone location to their team every 30 s – 5 min,
 * also when the screen is locked or Maata is in the background. It always shows a notification, it stops by itself
 * at the end of work hours, and it stops at once when the person ends duty or the team stops it.
 * Points are kept in a queue when there is no internet and sent later.
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
            try { ctx.stopService(Intent(ctx, TeamTrackService::class.java)) } catch (_: Exception) { }
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
    private var gpsOff = false
    private var sending = false
    private var lock: PowerManager.WakeLock? = null
    private val tick = object : Runnable { override fun run() { flush(); h.postDelayed(this, intervalMs()) } }

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
            val left = (cfg.optLong("endAt", 0) - System.currentTimeMillis()).coerceIn(60_000L, 14 * 3600_000L)
            lock = getSystemService(PowerManager::class.java).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "maata:team").apply { setReferenceCounted(false); acquire(left) }
            h.postDelayed(tick, 5000)
        }
        return START_STICKY // Android restarts it if it is killed; the saved settings say where to continue
    }

    private fun battery(): Int = try { getSystemService(BatteryManager::class.java).getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY) } catch (_: Exception) { -1 }
    private fun isMock(l: Location): Boolean = if (Build.VERSION.SDK_INT >= 31) l.isMock else @Suppress("DEPRECATION") l.isFromMockProvider

    override fun onLocationChanged(l: Location) {
        if (gpsOff) gpsOff = false
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
        if (sending || (queue.length() == 0 && !gpsOff)) return
        sending = true
        val batch = JSONArray(); for (k in 0 until queue.length()) batch.put(queue.get(k))
        val body = JSONObject().put("pts", batch).apply { if (gpsOff) put("gps", "off") }.toString()
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

    private fun stopAll() {
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
