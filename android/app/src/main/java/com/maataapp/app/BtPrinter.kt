package com.maataapp.app

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothClass
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothProfile
import android.bluetooth.BluetoothStatusCodes
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import android.bluetooth.BluetoothSocket
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
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

    private var scanRx: BroadcastReceiver? = null
    fun needsScanPermission(ctx: Context): Boolean =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) ctx.checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) != PackageManager.PERMISSION_GRANTED || needsPermission(ctx)
        else ctx.checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED
    /** Search for nearby Bluetooth devices (about 12 seconds). found(json) per device, end() when finished. */
    @SuppressLint("MissingPermission")
    fun scan(ctx: Context, found: (String) -> Unit, end: (String) -> Unit) {
        val a = adapter(ctx) ?: run { end("nobt"); return }
        if (needsScanPermission(ctx)) { end("perm"); return }
        try { if (!a.isEnabled) { end("off"); return } } catch (e: SecurityException) { end("perm"); return }
        stopScan(ctx)
        val rx = object : BroadcastReceiver() {
            override fun onReceive(c: Context, i: Intent) {
                when (i.action) {
                    BluetoothDevice.ACTION_FOUND -> {
                        val d: BluetoothDevice? = if (Build.VERSION.SDK_INT >= 33) i.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE, BluetoothDevice::class.java) else @Suppress("DEPRECATION") i.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE)
                        if (d != null) try {
                            val name = d.name ?: return
                            val major = try { d.bluetoothClass?.majorDeviceClass } catch (e: Exception) { null }
                            val p = major == BluetoothClass.Device.Major.IMAGING || Regex("(?i)print|pos|rpp|mtp|xp-|tp-|thermal|receipt|pt-|zj|zy|goojprt|tvs|hoin|munbyn|sewoo|bixolon").containsMatchIn(name)
                            found(JSONObject().put("name", name.take(60)).put("address", d.address).put("printer", p).put("paired", d.bondState == BluetoothDevice.BOND_BONDED).toString())
                        } catch (_: SecurityException) { }
                    }
                    BluetoothAdapter.ACTION_DISCOVERY_FINISHED -> { stopScan(ctx); end("done") }
                }
            }
        }
        scanRx = rx
        val f = IntentFilter(BluetoothDevice.ACTION_FOUND).apply { addAction(BluetoothAdapter.ACTION_DISCOVERY_FINISHED) }
        if (Build.VERSION.SDK_INT >= 33) ctx.registerReceiver(rx, f, Context.RECEIVER_EXPORTED) else @Suppress("UnspecifiedRegisterReceiverFlag") ctx.registerReceiver(rx, f)
        try { if (!a.startDiscovery()) { stopScan(ctx); end("fail") } } catch (e: SecurityException) { stopScan(ctx); end("perm") }
    }
    @SuppressLint("MissingPermission")
    fun stopScan(ctx: Context) {
        scanRx?.let { try { ctx.unregisterReceiver(it) } catch (_: Exception) { } }; scanRx = null
        try { adapter(ctx)?.cancelDiscovery() } catch (_: Exception) { }
    }
    /** Pair with a printer (Android shows its PIN box if the printer needs one: usually 0000 or 1234). */
    @SuppressLint("MissingPermission")
    fun pair(ctx: Context, address: String, done: (String) -> Unit) {
        val a = adapter(ctx) ?: run { done("nobt"); return }
        try {
            val d = a.getRemoteDevice(address)
            if (d.bondState == BluetoothDevice.BOND_BONDED) { done("paired"); return }
            try { a.cancelDiscovery() } catch (_: Exception) { }
            val rx = object : BroadcastReceiver() {
                override fun onReceive(c: Context, i: Intent) {
                    val dv: BluetoothDevice? = if (Build.VERSION.SDK_INT >= 33) i.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE, BluetoothDevice::class.java) else @Suppress("DEPRECATION") i.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE)
                    if (dv?.address != address) return
                    when (i.getIntExtra(BluetoothDevice.EXTRA_BOND_STATE, -1)) {
                        BluetoothDevice.BOND_BONDED -> { try { ctx.unregisterReceiver(this) } catch (_: Exception) { }; done("paired") }
                        BluetoothDevice.BOND_NONE -> { try { ctx.unregisterReceiver(this) } catch (_: Exception) { }; done("failed") }
                    }
                }
            }
            val f = IntentFilter(BluetoothDevice.ACTION_BOND_STATE_CHANGED)
            if (Build.VERSION.SDK_INT >= 33) ctx.registerReceiver(rx, f, Context.RECEIVER_EXPORTED) else @Suppress("UnspecifiedRegisterReceiverFlag") ctx.registerReceiver(rx, f)
            if (!d.createBond()) { try { ctx.unregisterReceiver(rx) } catch (_: Exception) { }; done("failed") }
        } catch (e: SecurityException) { done("perm") } catch (e: Exception) { done("failed") }
    }

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

    // printers' usual "write" characteristics on BLE
    private val BLE_WRITE = listOf("49535343-8841-43f4-a8d4-ecbe34729bb3", "0000ff02-0000-1000-8000-00805f9b34fb", "0000ae01-0000-1000-8000-00805f9b34fb", "0000fff2-0000-1000-8000-00805f9b34fb",
        "e7810a71-73ae-499d-8c15-faa9aef0c3f2", "0000ffe1-0000-1000-8000-00805f9b34fb", "bef8d6c9-9c21-4c9e-b632-bd58c1009f9f", "0000ff01-0000-1000-8000-00805f9b34fb")
    private fun canWrite(c: BluetoothGattCharacteristic) = c.properties and (BluetoothGattCharacteristic.PROPERTY_WRITE or BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE) != 0
    private fun pickChar(g: BluetoothGatt): BluetoothGattCharacteristic? {
        val all = g.services.filter { !it.uuid.toString().startsWith("00001800") && !it.uuid.toString().startsWith("00001801") }.flatMap { it.characteristics }.filter { canWrite(it) }
        for (u in BLE_WRITE) all.firstOrNull { it.uuid.toString().equals(u, true) }?.let { return it }
        return all.firstOrNull()
    }
    /** Send the bytes over Bluetooth Low Energy (GATT write), in small pieces. */
    @SuppressLint("MissingPermission")
    private fun bleWrite(ctx: Context, address: String, data: ByteArray): Boolean {
        val a = adapter(ctx) ?: return false
        val dev = a.getRemoteDevice(address)
        val ready = CountDownLatch(1)
        var ch: BluetoothGattCharacteristic? = null
        var mtu = 23
        var wrote = CountDownLatch(1)
        var lost = false
        val cb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) { if (!g.requestMtu(247)) g.discoverServices() }
                else if (newState == BluetoothProfile.STATE_DISCONNECTED) { lost = true; ready.countDown(); wrote.countDown() }
            }
            override fun onMtuChanged(g: BluetoothGatt, m: Int, status: Int) { if (status == BluetoothGatt.GATT_SUCCESS) mtu = m; g.discoverServices() }
            override fun onServicesDiscovered(g: BluetoothGatt, status: Int) { ch = pickChar(g); ready.countDown() }
            override fun onCharacteristicWrite(g: BluetoothGatt, c: BluetoothGattCharacteristic, status: Int) { wrote.countDown() }
        }
        val g = (if (Build.VERSION.SDK_INT >= 23) dev.connectGatt(ctx, false, cb, BluetoothDevice.TRANSPORT_LE) else dev.connectGatt(ctx, false, cb)) ?: return false
        try {
            if (!ready.await(15, TimeUnit.SECONDS) || lost) return false
            val c = ch ?: return false
            val noResp = c.properties and BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE != 0
            val type = if (noResp) BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE else BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
            c.writeType = type
            val size = (mtu - 3).coerceIn(20, 180)
            var off = 0
            while (off < data.size) {
                if (lost) return false
                val part = data.copyOfRange(off, minOf(data.size, off + size))
                wrote = CountDownLatch(1)
                var ok = false
                for (tries in 0 until 20) {
                    ok = if (Build.VERSION.SDK_INT >= 33) g.writeCharacteristic(c, part, type) == BluetoothStatusCodes.SUCCESS
                         else { @Suppress("DEPRECATION") run { c.value = part; g.writeCharacteristic(c) } }
                    if (ok) break; Thread.sleep(25)
                }
                if (!ok) return false
                wrote.await(2, TimeUnit.SECONDS)
                if (noResp) Thread.sleep(8) // let small printers keep up
                off += part.size
            }
            Thread.sleep(900)
            return true
        } catch (e: Exception) { return false }
        finally { try { g.disconnect() } catch (_: Exception) { }; try { g.close() } catch (_: Exception) { } }
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
                        try { val m = dev.javaClass.getMethod("createRfcommSocket", Int::class.javaPrimitiveType); (m.invoke(dev, 1) as BluetoothSocket).also { it.connect() } }
                        catch (e3: Exception) { null }
                    }
                }
                if (sock == null) { // not classic Bluetooth: many new printers use Bluetooth Low Energy (BLE)
                    if (bleWrite(ctx, address, data)) { done(true, "ok"); return@Thread }
                    throw java.io.IOException("connect")
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
