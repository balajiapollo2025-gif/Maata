package com.maataapp.app

import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.hardware.usb.UsbConstants
import android.hardware.usb.UsbDevice
import android.hardware.usb.UsbManager
import android.os.Build
import org.json.JSONArray
import org.json.JSONObject
import java.net.InetSocketAddress
import java.net.Socket

/** 🖨️ Receipt printers on a USB cable (phone OTG) or on the shop network (Wi-Fi / LAN, raw port 9100). */
object UsbNetPrinter {
    private const val ACTION_USB = "com.maataapp.app.USB_PRINTER_PERMISSION"
    @Volatile private var busy = false

    private fun mgr(ctx: Context) = ctx.getSystemService(Context.USB_SERVICE) as UsbManager
    private fun key(d: UsbDevice) = "${d.vendorId}:${d.productId}"
    private fun looksPrinter(d: UsbDevice): Boolean {
        if (d.deviceClass == UsbConstants.USB_CLASS_PRINTER) return true
        for (i in 0 until d.interfaceCount) if (d.getInterface(i).interfaceClass == UsbConstants.USB_CLASS_PRINTER) return true
        return false
    }
    private fun find(ctx: Context, k: String): UsbDevice? = mgr(ctx).deviceList.values.firstOrNull { key(it) == k || it.deviceName == k }

    /** [{"key","name","printer","allowed"}] for every USB device plugged into the phone */
    fun list(ctx: Context): String {
        val out = JSONArray()
        for (d in mgr(ctx).deviceList.values) {
            val name = (if (Build.VERSION.SDK_INT >= 21) listOfNotNull(d.manufacturerName, d.productName).joinToString(" ") else "").ifBlank { "USB device " + key(d) }
            out.put(JSONObject().put("key", key(d)).put("name", name.take(60)).put("printer", looksPrinter(d)).put("allowed", mgr(ctx).hasPermission(d)))
        }
        return out.toString()
    }

    fun askPermission(ctx: Context, k: String, done: (Boolean) -> Unit) {
        val d = find(ctx, k) ?: run { done(false); return }
        if (mgr(ctx).hasPermission(d)) { done(true); return }
        val rx = object : BroadcastReceiver() {
            override fun onReceive(c: Context, i: Intent) {
                try { ctx.unregisterReceiver(this) } catch (_: Exception) { }
                done(i.getBooleanExtra(UsbManager.EXTRA_PERMISSION_GRANTED, false) || mgr(ctx).hasPermission(d))
            }
        }
        val f = IntentFilter(ACTION_USB)
        if (Build.VERSION.SDK_INT >= 33) ctx.registerReceiver(rx, f, Context.RECEIVER_NOT_EXPORTED) else @Suppress("UnspecifiedRegisterReceiverFlag") ctx.registerReceiver(rx, f)
        val flags = if (Build.VERSION.SDK_INT >= 31) PendingIntent.FLAG_MUTABLE else 0
        val pi = PendingIntent.getBroadcast(ctx, 9, Intent(ACTION_USB).setPackage(ctx.packageName), flags)
        mgr(ctx).requestPermission(d, pi)
    }

    fun printUsb(ctx: Context, k: String, data: ByteArray, done: (Boolean, String) -> Unit) {
        if (busy) { done(false, "busy"); return }
        val d = find(ctx, k) ?: run { done(false, "usb_none"); return }
        if (!mgr(ctx).hasPermission(d)) { done(false, "usb_perm"); return }
        busy = true
        Thread {
            try {
                val conn = mgr(ctx).openDevice(d) ?: throw IllegalStateException("open")
                try {
                    var sent = false
                    loop@ for (i in 0 until d.interfaceCount) {
                        val itf = d.getInterface(i)
                        for (e in 0 until itf.endpointCount) {
                            val ep = itf.getEndpoint(e)
                            if (ep.type == UsbConstants.USB_ENDPOINT_XFER_BULK && ep.direction == UsbConstants.USB_DIR_OUT) {
                                if (!conn.claimInterface(itf, true)) continue
                                var off = 0
                                while (off < data.size) {
                                    val n = minOf(16384, data.size - off)
                                    val w = conn.bulkTransfer(ep, data, off, n, 5000)
                                    if (w <= 0) throw IllegalStateException("write")
                                    off += w
                                }
                                conn.releaseInterface(itf); sent = true; break@loop
                            }
                        }
                    }
                    if (!sent) throw IllegalStateException("noep")
                } finally { conn.close() }
                done(true, "ok")
            } catch (e: Exception) { done(false, "usb_fail") } finally { busy = false }
        }.start()
    }

    fun printNet(host: String, port: Int, data: ByteArray, done: (Boolean, String) -> Unit) {
        if (busy) { done(false, "busy"); return }
        busy = true
        Thread {
            try {
                Socket().use { s ->
                    s.connect(InetSocketAddress(host, if (port in 1..65535) port else 9100), 5000)
                    s.soTimeout = 10000
                    val o = s.getOutputStream()
                    var off = 0
                    while (off < data.size) { val n = minOf(8192, data.size - off); o.write(data, off, n); off += n }
                    o.flush(); Thread.sleep(400)
                }
                done(true, "ok")
            } catch (e: Exception) { done(false, "net_fail") } finally { busy = false }
        }.start()
    }
}
