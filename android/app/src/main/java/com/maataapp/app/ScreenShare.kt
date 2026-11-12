package com.maataapp.app

import android.content.Context
import android.content.Intent
import android.graphics.Bitmap
import android.graphics.PixelFormat
import android.hardware.display.DisplayManager
import android.hardware.display.VirtualDisplay
import android.media.ImageReader
import android.media.projection.MediaProjection
import android.media.projection.MediaProjectionManager
import android.os.Handler
import android.os.HandlerThread
import android.os.Looper
import android.util.Base64
import java.io.ByteArrayOutputStream

/**
 * 🖥️ Screen sharing in a Maata video call (like WhatsApp).
 * Android's WebView cannot share the screen by itself, so the app captures it (MediaProjection — Android shows its own
 * "start recording or casting?" question and a casting icon) and hands small JPEG pictures, ~8 per second, to the Maata
 * page. The page paints them on a canvas and sends that as the call's video. Nothing is saved anywhere.
 */
object ScreenShare {
    private const val FPS = 8
    private const val MAX_W = 720

    @Volatile var active = false
    private var projection: MediaProjection? = null
    private var display: VirtualDisplay? = null
    private var reader: ImageReader? = null
    private var thread: HandlerThread? = null
    private var lastAt = 0L
    @Volatile private var busy = false

    /** Starts capturing. [onFrame] gets "data:image/jpeg;base64,…"; it must call [frameDone] when the page has it. */
    fun begin(ctx: Context, resultCode: Int, data: Intent, onFrame: (String) -> Unit, onStop: () -> Unit): Boolean {
        stop(ctx, false)
        val mpm = ctx.getSystemService(MediaProjectionManager::class.java) ?: return false
        val mp = try { mpm.getMediaProjection(resultCode, data) } catch (_: Exception) { null } ?: return false
        projection = mp
        val ht = HandlerThread("maata-screen").apply { start() }; thread = ht
        val h = Handler(ht.looper)
        mp.registerCallback(object : MediaProjection.Callback() {
            override fun onStop() { Handler(Looper.getMainLooper()).post { if (active) { stop(ctx, false); onStop() } } }
        }, h)

        val dm = ctx.resources.displayMetrics
        val scale = if (dm.widthPixels > MAX_W) MAX_W.toFloat() / dm.widthPixels else 1f
        val w = ((dm.widthPixels * scale).toInt() / 2) * 2
        val hgt = ((dm.heightPixels * scale).toInt() / 2) * 2
        val ir = ImageReader.newInstance(w, hgt, PixelFormat.RGBA_8888, 2); reader = ir
        ir.setOnImageAvailableListener({ r ->
            val img = try { r.acquireLatestImage() } catch (_: Exception) { null } ?: return@setOnImageAvailableListener
            try {
                val now = System.currentTimeMillis()
                if (busy || now - lastAt < 1000L / FPS) return@setOnImageAvailableListener
                lastAt = now
                val plane = img.planes[0]
                val rowPad = plane.rowStride - plane.pixelStride * img.width
                val bmpW = img.width + rowPad / plane.pixelStride
                val full = Bitmap.createBitmap(bmpW, img.height, Bitmap.Config.ARGB_8888)
                full.copyPixelsFromBuffer(plane.buffer)
                val bmp = if (bmpW != img.width) Bitmap.createBitmap(full, 0, 0, img.width, img.height).also { full.recycle() } else full
                val out = ByteArrayOutputStream(64 * 1024)
                bmp.compress(Bitmap.CompressFormat.JPEG, 60, out); bmp.recycle()
                busy = true
                val url = "data:image/jpeg;base64," + Base64.encodeToString(out.toByteArray(), Base64.NO_WRAP)
                Handler(Looper.getMainLooper()).post { onFrame(url) }
            } catch (_: Exception) { busy = false } finally { img.close() }
        }, h)
        display = try {
            mp.createVirtualDisplay("maata-screen", w, hgt, dm.densityDpi, DisplayManager.VIRTUAL_DISPLAY_FLAG_AUTO_MIRROR, ir.surface, null, h)
        } catch (_: Exception) { null }
        if (display == null) { stop(ctx, false); return false }
        active = true; busy = false; lastAt = 0
        return true
    }

    fun frameDone() { busy = false }

    fun stop(ctx: Context, tellService: Boolean = true) {
        active = false; busy = false
        try { display?.release() } catch (_: Exception) { }
        try { reader?.close() } catch (_: Exception) { }
        try { projection?.stop() } catch (_: Exception) { }
        try { thread?.quitSafely() } catch (_: Exception) { }
        display = null; reader = null; projection = null; thread = null
        if (tellService) CallService.projection(ctx, false)
    }
}
