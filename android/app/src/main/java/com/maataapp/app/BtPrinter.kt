package com.maataapp.app

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothClass
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothSocket
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID

/** 🖨️ Bluetooth receipt printers (58 / 80 mm thermal, ESC/POS over classic Bluetooth "SPP").
 *  The web page sends ready-made printer bytes; this file only connects to a paired printer and writes them. */
object BtPrinter {
    private val SPP: UUID = UUID.fromString("00001101-0000-1000-8000-00805F9B34FB")
    @Volatile private var busy = false

    private fun adapter(ctx: Context): BluetoothAdapter? = (ctx.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter

    fun needsPermission(ctx: Context): Boolean =
        Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && ctx.checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED

    /** "nobt" (no Bluetooth on this phone), "perm" (allow Nearby devices first), "off" (Bluetooth switched off) or "ok". */
    @SuppressLint("MissingPermission")
    fun status(ctx: Context): String {
        val a = adapter(ctx) ?: return "nobt"
        if (needsPermission(ctx)) return "perm"
        return try { if (a.isEnabled) "ok" else "off" } catch (e: SecurityException) { "perm" }
    }

    /** Paired devices, printers first: [{"name","address","printer"}] */
    @SuppressLint("MissingPermission")
    fun paired(ctx: Context): String {
        val out = JSONArray()
        val a = adapter(ctx) ?: return out.toString()
        if (needsPermission(ctx)) return out.toString()
        try {
            val list = a.bondedDevices?.toList() ?: emptyList()
            val rows = list.map { d ->
                val name = (d.name ?: d.address).take(60)
                val major = try { d.bluetoothClass?.majorDeviceClass } catch (e: Exception) { null }
                val looksPrinter = major == BluetoothClass.Device.Major.IMAGING ||
                    Regex("(?i)print|pos|rpp|mtp|xp-|tp-|bt-|thermal|receipt|pt-|zj|goojprt|epson|tvs|hoin|munbyn|sewoo|bixolon").containsMatchIn(name)
                Triple(name, d.address, looksPrinter)
            }.sortedByDescending { it.third }
            for ((name, addr, p) in rows) out.put(JSONObject().put("name", name).put("address", addr).put("printer", p))
        } catch (e: SecurityException) { }
        return out.toString()
    }

    /** Connect, write the bytes in small pieces (printers have small buffers), close. Runs off the main thread. */
    @SuppressLint("MissingPermission")
    fun print(ctx: Context, address: String, data: ByteArray, done: (Boolean, String) -> Unit) {
        if (busy) { done(false, "busy"); return }
        val a = adapter(ctx) ?: run { done(false, "nobt"); return }
        if (needsPermission(ctx)) { done(false, "perm"); return }
        busy = true
        Thread {
            var sock: BluetoothSocket? = null
            try {
                if (!a.isEnabled) throw IllegalStateException("off")
                val dev = a.getRemoteDevice(address)
                try { a.cancelDiscovery() } catch (_: Exception) { }
                sock = try { dev.createRfcommSocketToServiceRecord(SPP).also { it.connect() } }
                catch (e: Exception) {
                    try { sock?.close() } catch (_: Exception) { }
                    try { dev.createInsecureRfcommSocketToServiceRecord(SPP).also { it.connect() } }
                    catch (e2: Exception) { // some cheap printers only answer on channel 1
                        val m = dev.javaClass.getMethod("createRfcommSocket", Int::class.javaPrimitiveType)
                        (m.invoke(dev, 1) as BluetoothSocket).also { it.connect() }
                    }
                }
                val os = sock!!.outputStream
                var i = 0
                while (i < data.size) {
                    val n = minOf(1024, data.size - i)
                    os.write(data, i, n); os.flush(); i += n
                    Thread.sleep(25)
                }
                Thread.sleep(600) // let the printer finish before the link closes
                done(true, "ok")
            } catch (e: IllegalStateException) { done(false, e.message ?: "error")
            } catch (e: SecurityException) { done(false, "perm")
            } catch (e: Exception) { done(false, "connect")
            } finally { try { sock?.close() } catch (_: Exception) { }; busy = false }
        }.start()
    }
}
