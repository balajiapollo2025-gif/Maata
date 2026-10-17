package com.maataapp.app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/** 📍 Keeps team location sharing going: after the phone restarts, after a Maata update, and every ~10 min (watchdog). */
class TrackBootReceiver : BroadcastReceiver() {
    override fun onReceive(ctx: Context, intent: Intent) {
        if (!TeamTrackService.onDuty(ctx)) return
        TeamTrackService.resume(ctx)
        if (intent.action != "com.maataapp.app.TRACK_WATCHDOG") TeamTrackService.watchdog(ctx, true)
    }
}
