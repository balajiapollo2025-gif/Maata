package com.maataapp.app

import android.app.Activity
import android.app.KeyguardManager
import android.content.Intent
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.Gravity
import android.view.ViewGroup.LayoutParams.MATCH_PARENT
import android.view.ViewGroup.LayoutParams.WRAP_CONTENT
import android.view.WindowManager
import android.view.animation.AlphaAnimation
import android.view.animation.Animation
import android.widget.FrameLayout
import android.widget.LinearLayout
import android.widget.TextView

/** Full-screen incoming call, shown over the lock screen like WhatsApp. */
class IncomingCallActivity : Activity() {
    companion object { var current: IncomingCallActivity? = null }
    private var callId = ""
    private val handler = Handler(Looper.getMainLooper())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        current = this
        if (Build.VERSION.SDK_INT >= 27) { setShowWhenLocked(true); setTurnScreenOn(true) }
        else window.addFlags(WindowManager.LayoutParams.FLAG_SHOW_WHEN_LOCKED or WindowManager.LayoutParams.FLAG_TURN_SCREEN_ON or WindowManager.LayoutParams.FLAG_DISMISS_KEYGUARD)
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        callId = intent.getStringExtra("callId") ?: ""
        val caller = intent.getStringExtra("caller") ?: "Maata call"
        val video = intent.getStringExtra("kind") == "video"
        setContentView(buildUi(caller, video))
        handler.postDelayed({ Notifier.stopRinging(this) }, 45_000) // nobody answered
    }

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()

    private fun buildUi(caller: String, video: Boolean): FrameLayout {
        val root = FrameLayout(this).apply {
            background = GradientDrawable(GradientDrawable.Orientation.TOP_BOTTOM, intArrayOf(0xFF11606F.toInt(), 0xFF0A3A47.toInt(), 0xFF07232B.toInt()))
        }
        val col = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; gravity = Gravity.CENTER_HORIZONTAL; setPadding(dp(24), dp(72), dp(24), dp(48)) }
        col.addView(TextView(this).apply {
            text = (if (video) "🎥  Maata video call" else "📞  Maata voice call"); setTextColor(0xCCFFFFFF.toInt()); textSize = 16f; gravity = Gravity.CENTER
        })
        // avatar with initials and a soft pulsing ring
        val ring = FrameLayout(this).apply {
            background = GradientDrawable().apply { shape = GradientDrawable.OVAL; setColor(0x332E8B57) }
            startAnimation(AlphaAnimation(1f, 0.35f).apply { duration = 900; repeatMode = Animation.REVERSE; repeatCount = Animation.INFINITE })
        }
        val avatar = TextView(this).apply {
            text = caller.split(" ").filter { it.isNotBlank() }.take(2).joinToString("") { it.take(1) }.uppercase()
            setTextColor(Color.WHITE); textSize = 44f; typeface = Typeface.DEFAULT_BOLD; gravity = Gravity.CENTER
            background = GradientDrawable().apply { shape = GradientDrawable.OVAL; setColor(0xFFC8732E.toInt()) }
        }
        val holder = FrameLayout(this)
        holder.addView(ring, FrameLayout.LayoutParams(dp(170), dp(170), Gravity.CENTER))
        holder.addView(avatar, FrameLayout.LayoutParams(dp(130), dp(130), Gravity.CENTER))
        col.addView(holder, LinearLayout.LayoutParams(dp(170), dp(170)).apply { topMargin = dp(48) })
        col.addView(TextView(this).apply { text = caller; setTextColor(Color.WHITE); textSize = 30f; typeface = Typeface.DEFAULT_BOLD; gravity = Gravity.CENTER },
            LinearLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT).apply { topMargin = dp(24) })
        col.addView(TextView(this).apply { text = "Ringing…"; setTextColor(0xB3FFFFFF.toInt()); textSize = 16f; gravity = Gravity.CENTER },
            LinearLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT).apply { topMargin = dp(8) })
        root.addView(col, FrameLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT, Gravity.TOP))

        val buttons = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER; setPadding(dp(32), 0, dp(32), dp(72)) }
        buttons.addView(roundButton("✕", "Decline", 0xFFD93B3B.toInt()) { decline() }, LinearLayout.LayoutParams(0, WRAP_CONTENT, 1f))
        buttons.addView(roundButton(if (video) "🎥" else "📞", "Answer", 0xFF22B35E.toInt()) { answer() }, LinearLayout.LayoutParams(0, WRAP_CONTENT, 1f))
        root.addView(buttons, FrameLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT, Gravity.BOTTOM))
        return root
    }

    private fun roundButton(icon: String, label: String, color: Int, onClick: () -> Unit): LinearLayout {
        val box = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; gravity = Gravity.CENTER_HORIZONTAL }
        val b = TextView(this).apply {
            text = icon; textSize = 30f; setTextColor(Color.WHITE); gravity = Gravity.CENTER
            background = GradientDrawable().apply { shape = GradientDrawable.OVAL; setColor(color) }
            contentDescription = label; isClickable = true; isFocusable = true
            setOnClickListener { onClick() }
        }
        box.addView(b, LinearLayout.LayoutParams(dp(78), dp(78)))
        box.addView(TextView(this).apply { text = label; setTextColor(Color.WHITE); textSize = 15f; gravity = Gravity.CENTER },
            LinearLayout.LayoutParams(WRAP_CONTENT, WRAP_CONTENT).apply { topMargin = dp(10) })
        return box
    }

    private fun answer() {
        Notifier.stopRinging(this)
        val i = Intent(this, MainActivity::class.java).apply {
            putExtra(MainActivity.EXTRA_URL, "/#answer=$callId")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
        }
        // ask to unlock (if locked), then open the call in Maata
        val km = getSystemService(KeyguardManager::class.java)
        if (Build.VERSION.SDK_INT >= 26 && km.isKeyguardLocked) km.requestDismissKeyguard(this, null)
        startActivity(i); finish()
    }

    private fun decline() { CallActionReceiver.decline(this, callId); finish() }

    fun finishSoon() = runOnUiThread { if (!isFinishing) finish() }

    override fun onDestroy() { handler.removeCallbacksAndMessages(null); if (current === this) current = null; super.onDestroy() }
}
