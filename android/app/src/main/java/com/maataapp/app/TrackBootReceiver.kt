package com.maataapp.app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/** 📍 Keeps team location sharing going: after the phone restarts, after a Maata update, and every ~10 min (watchdog). */
class TrackBootReceiver : BroadcastReceiver() {
    override fun onReceive(ctx: Context, intent: Intent) {
        if (!TeamTrackService.onDuty(ctx)) return
        TeamTrackService.watchdog(ctx, true) // the next check in ~10 min (also in deep sleep)
        if (TeamTrackService.running) return
        // sharing was stopped by the phone: start it again, and send one place now
        val pr = goAsync()
        Thread { try { TeamTrackService.wakeNow(ctx, 8000) } finally { pr.finish() } }.start()
    }
}
