"""
ui/overlay.py
PalmSync AR Holographic UI - With Back Navigation
"""

import tkinter as tk
from tkinter import font, Canvas
import math
import random
import threading
import time
from typing import Callable, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

class UIState(Enum):
    HIDDEN = "hidden"
    ACTIVATED = "activated"
    MODE_SELECTED = "mode_selected"
    READY_TO_SEND = "ready_to_send"
    TRANSFERRING = "transferring"
    INCOMING_REQUEST = "incoming_request"
    COMPLETE = "complete"
    ERROR = "error"
    EXPIRED = "expired"

class TransferMode(Enum):
    LAN = "LAN (Local Network)"
    WAN = "WAN (Internet)"

@dataclass
class UIData:
    state: UIState = UIState.HIDDEN
    current_file: str = ""
    file_size: str = ""
    sender_name: str = ""
    recipients: list = None
    selected_recipient: str = ""
    transfer_mode: Optional[TransferMode] = None
    progress: int = 0
    speed: str = ""
    message: str = ""
    error_message: str = ""

class Particle:
    def __init__(self, x, y, canvas):
        self.x = x
        self.y = y
        self.canvas = canvas
        self.size = random.randint(2, 6)
        self.speed_x = random.uniform(-0.5, 0.5)
        self.speed_y = random.uniform(-0.5, 0.5)
        self.life = random.randint(50, 100)
        self.max_life = self.life
        self.id = None
        
    def update(self):
        self.x += self.speed_x
        self.y += self.speed_y
        self.life -= 1
        w, h = self.canvas.winfo_width(), self.canvas.winfo_height()
        if self.x < 0 or self.x > w: self.speed_x *= -0.8
        if self.y < 0 or self.y > h: self.speed_y *= -0.8
        self.x = max(0, min(self.x, w))
        self.y = max(0, min(self.y, h))
        return self.life > 0
        
    def draw(self):
        if self.id: self.canvas.delete(self.id)
        alpha = self.life / self.max_life
        r, g, b = int(0), int(255 * alpha), int(255 * alpha)
        color = f"#{r:02x}{g:02x}{b:02x}"
        self.id = self.canvas.create_oval(
            self.x - self.size, self.y - self.size,
            self.x + self.size, self.y + self.size,
            fill=color, outline="", tags="particle"
        )

class HolographicButton:
    def __init__(self, canvas, x, y, width, height, text, color="#00ffff", command=None):
        self.canvas = canvas
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.text = text
        self.color = color
        self.command = command
        self.hover_progress = 0.0
        self.pulse_phase = random.uniform(0, math.pi * 2)
        self.float_offset = 0.0
        self.is_hovered = False
        self.particles = []
        self.items = {}
        
    def draw(self):
        for item in self.items.values():
            self.canvas.delete(item)
        self.items.clear()
        
        fy = self.y + self.float_offset
        alpha = 20 + int(self.hover_progress * 40)
        bg = f"#00{alpha:02x}{alpha:02x}"
        
        self.items['bg'] = self.canvas.create_rectangle(
            self.x, fy, self.x + self.width, fy + self.height,
            fill=bg, outline="", tags="button"
        )
        
        glow = 2 + int(self.hover_progress * 6)
        pulse = (math.sin(self.pulse_phase) + 1) * 0.3
        r, g, b = 0, int(150 + 105 * pulse), int(200 + 55 * pulse)
        glow_color = f"#{r:02x}{g:02x}{b:02x}"
        
        self.items['glow'] = self.canvas.create_rectangle(
            self.x - glow, fy - glow,
            self.x + self.width + glow, fy + self.height + glow,
            fill="", outline=glow_color, width=glow, tags="button"
        )
        
        self.items['border'] = self.canvas.create_rectangle(
            self.x, fy, self.x + self.width, fy + self.height,
            fill="", outline=self.color, width=2, tags="button"
        )
        
        tc = "#ffffff" if self.is_hovered else "#c0c0e0"
        self.items['text'] = self.canvas.create_text(
            self.x + self.width // 2, fy + self.height // 2,
            text=self.text, fill=tc, font=("Segoe UI", 12, "bold"), tags="button"
        )
        
        # Bind events
        for tag in ['bg', 'glow', 'border', 'text']:
            if tag in self.items:
                self.canvas.tag_bind(self.items[tag], "<Button-1>", lambda e: self._click())
                self.canvas.tag_bind(self.items[tag], "<Enter>", lambda e: self._enter())
                self.canvas.tag_bind(self.items[tag], "<Leave>", lambda e: self._leave())
        
    def _click(self):
        if self.command: self.command()
    def _enter(self):
        self.is_hovered = True
    def _leave(self):
        self.is_hovered = False
        
    def update(self, dt):
        self.pulse_phase += dt * 2
        self.float_offset = math.sin(self.pulse_phase * 0.5) * 3
        
        if self.is_hovered:
            self.hover_progress = min(1.0, self.hover_progress + dt * 4)
            if random.random() < 0.3:
                px = self.x + random.randint(0, self.width)
                py = self.y + int(self.float_offset) + random.randint(0, self.height)
                p = Particle(px, py, self.canvas)
                self.particles.append(p)
        else:
            self.hover_progress = max(0.0, self.hover_progress - dt * 3)
            
        for p in self.particles[:]:
            if not p.update(): self.particles.remove(p)
            else: p.draw()

class HolographicProgress:
    def __init__(self, canvas, x, y, width, height):
        self.canvas = canvas
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.progress = 0.0
        self.scan_pos = 0.0
        self.items = {}
        
    def set_progress(self, pct):
        self.progress = pct / 100.0
        
    def draw(self):
        for item in self.items.values():
            self.canvas.delete(item)
        self.items.clear()
        
        self.items['bg'] = self.canvas.create_rectangle(
            self.x, self.y, self.x + self.width, self.y + self.height,
            fill="#0a0a2a", outline="#00ffff", width=1
        )
        
        fw = int(self.width * self.progress)
        if fw > 0:
            self.items['fill'] = self.canvas.create_rectangle(
                self.x, self.y, self.x + fw, self.y + self.height,
                fill="#00ff88", outline=""
            )
            
        sx = self.x + int(self.width * self.scan_pos)
        self.items['scan'] = self.canvas.create_line(
            sx, self.y - 2, sx, self.y + self.height + 2,
            fill="#00ffff", width=2
        )
        
    def update(self, dt):
        self.scan_pos += dt * 0.5
        if self.scan_pos > 1.0: self.scan_pos = 0.0

class OverlayUI:
    """PalmSync Holographic UI with Back Navigation"""
    
    def __init__(self, on_mode_selected=None, on_recipient_selected=None, on_cancel=None):
        self.on_mode_selected = on_mode_selected
        self.on_recipient_selected = on_recipient_selected
        self.on_cancel = on_cancel
        
        self.root = None
        self.canvas = None
        self.is_showing = False
        self.current_data = UIData()
        self.pointer_position = (0.5, 0.5)
        
        self.progress_bar = None
        self.buttons = []
        self.bg_particles = []
        
        # Navigation history for back button
        self.state_history = []
        
        self.COLOR_BG = "#0a0a1a"
        self.COLOR_PRIMARY = "#00ffff"
        self.COLOR_SECONDARY = "#ff00ff"
        self.COLOR_SUCCESS = "#00ff88"
        self.COLOR_ERROR = "#ff3366"
        self.COLOR_WARNING = "#ffaa00"
        
        self.animation_running = True
        self.last_frame_time = time.time()
        self.drag_x = self.drag_y = 0
        
        self.ui_thread = threading.Thread(target=self._create_window, daemon=True)
        self.ui_thread.start()
        time.sleep(0.5)
        
    # ============================================
    # WINDOW SETUP
    # ============================================
    
    def _create_window(self):
        self.root = tk.Tk()
        self.root.title("PalmSync")
        
        w, h = 600, 420
        sw, sh = self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        x, y = (sw - w) // 2, sh - h - 50
        
        self.root.geometry(f"{w}x{h}+{x}+{y}")
        self.root.attributes('-topmost', True)
        self.root.attributes('-alpha', 0.95)
        self.root.overrideredirect(True)
        self.root.configure(bg=self.COLOR_BG)
        
        self.canvas = Canvas(self.root, width=w, height=h, bg=self.COLOR_BG, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)
        self.canvas.bind("<Button-1>", self._start_move)
        self.canvas.bind("<B1-Motion>", self._on_move)
        
        self._animation_loop()
        self.root.withdraw()
        self.root.mainloop()
        
    def _start_move(self, e):
        self.drag_x, self.drag_y = e.x, e.y
    def _on_move(self, e):
        x = self.root.winfo_x() + (e.x - self.drag_x)
        y = self.root.winfo_y() + (e.y - self.drag_y)
        self.root.geometry(f"+{x}+{y}")
        
    # ============================================
    # ANIMATION
    # ============================================
    
    def _animation_loop(self):
        if not self.root or not self.canvas: return
        
        dt = time.time() - self.last_frame_time
        self.last_frame_time = time.time()
        
        for btn in self.buttons:
            btn.update(dt)
            btn.draw()
            
        if self.progress_bar:
            self.progress_bar.update(dt)
            self.progress_bar.draw()
            
        if random.random() < 0.1:
            px = random.randint(0, self.canvas.winfo_width())
            py = random.randint(0, self.canvas.winfo_height())
            self.bg_particles.append(Particle(px, py, self.canvas))
            
        for p in self.bg_particles[:]:
            if not p.update(): self.bg_particles.remove(p)
            else: p.draw()
            
        if self.animation_running:
            self.root.after(16, self._animation_loop)
            
    # ============================================
    # PUBLIC METHODS
    # ============================================
    
    def show(self, data: UIData = None):
        if data:
            self.current_data = data
            self.state_history = []  # Reset history on new show
        self.root.after(0, self._update_ui)
        self.root.after(0, self.root.deiconify)
        self.is_showing = True
        
    def hide(self):
        self.root.after(0, self.root.withdraw)
        self.is_showing = False
        self.state_history = []
        
    def update_progress(self, progress: int, speed: str = ""):
        self.current_data.progress = progress
        self.current_data.speed = speed
        self.current_data.state = UIState.TRANSFERRING
        self.root.after(0, self._update_ui)
        
    def update_recipients(self, recipients: list):
        self.current_data.recipients = recipients
        self.root.after(0, self._update_ui)
        
    def set_state(self, state: UIState, message: str = ""):
        self.current_data.state = state
        if message: self.current_data.message = message
        self.root.after(0, self._update_ui)
        
    def set_error(self, msg: str):
        self.current_data.state = UIState.ERROR
        self.current_data.error_message = msg
        self.root.after(0, self._update_ui)
        
    def update_pointer_position(self, x: float, y: float):
        self.pointer_position = (x, y)
        
    # ============================================
    # BACK NAVIGATION
    # ============================================
    
    def go_back(self):
        """Navigate back to previous state"""
        if self.state_history:
            prev_data = self.state_history.pop()
            self.current_data = prev_data
            self._update_ui()
            
    def _push_state(self):
        """Save current state before navigating"""
        import copy
        self.state_history.append(copy.copy(self.current_data))
        
    # ============================================
    # UI RENDERING
    # ============================================
    
    def _update_ui(self):
        if not self.canvas: return
        
        self.canvas.delete("all")
        self.buttons.clear()
        self.progress_bar = None
        
        data = self.current_data
        
        self._draw_grid()
        
        # Title
        title = self._get_title()
        color = self._get_title_color()
        self.canvas.create_text(300, 40, text=title, fill=color, font=("Segoe UI", 18, "bold"))
        
        y = 80
        
        # File info
        if data.current_file:
            self.canvas.create_text(300, y, text=f"📁 {data.current_file}", fill="#a0a0c0", font=("Segoe UI", 11))
            y += 25
            if data.file_size:
                self.canvas.create_text(300, y, text=data.file_size, fill="#8080a0", font=("Segoe UI", 10))
                y += 30
                
        # State-specific content
        if data.state == UIState.ACTIVATED:
            self._draw_mode_buttons(y)
        elif data.state == UIState.MODE_SELECTED:
            self._draw_recipients(y)
        elif data.state == UIState.READY_TO_SEND:
            self._draw_ready(y)
        elif data.state == UIState.TRANSFERRING:
            self._draw_progress(y)
        elif data.state == UIState.INCOMING_REQUEST:
            self._draw_incoming(y)
        elif data.state == UIState.COMPLETE:
            self._draw_complete(y)
        elif data.state == UIState.ERROR:
            self._draw_error(y)
            
        # Back button (show when not on main screen)
        if data.state in [UIState.MODE_SELECTED, UIState.READY_TO_SEND] and self.state_history:
            self._draw_back_button()
            
    def _draw_grid(self):
        w, h = self.canvas.winfo_width(), self.canvas.winfo_height()
        for x in range(0, w, 40):
            self.canvas.create_line(x, 0, x, h, fill="#1a1a3a", width=1)
        for y in range(0, h, 40):
            self.canvas.create_line(0, y, w, y, fill="#1a1a3a", width=1)
            
        cs = 20
        self.canvas.create_line(0, cs, 0, 0, cs, 0, fill=self.COLOR_PRIMARY, width=2)
        self.canvas.create_line(w, cs, w, 0, w-cs, 0, fill=self.COLOR_PRIMARY, width=2)
        self.canvas.create_line(0, h-cs, 0, h, cs, h, fill=self.COLOR_PRIMARY, width=2)
        self.canvas.create_line(w, h-cs, w, h, w-cs, h, fill=self.COLOR_PRIMARY, width=2)
        
    # ============================================
    # BACK BUTTON
    # ============================================
    
    def _draw_back_button(self):
        """Draw a back button in the top-left corner"""
        btn = HolographicButton(
            self.canvas, 15, 55, 80, 30,
            "← BACK", "#ffaa00",
            command=self.go_back
        )
        btn.draw()
        self.buttons.append(btn)
        
    # ============================================
    # MODE SELECTION
    # ============================================
    
    def _draw_mode_buttons(self, y):
        self.canvas.create_text(300, y, text="Select Transfer Mode", fill="#ffffff", font=("Segoe UI", 12))
        y += 40
        
        lan_btn = HolographicButton(self.canvas, 150, y, 130, 60, "📡 LOCAL", self.COLOR_PRIMARY,
                                    command=lambda: self._select_mode(TransferMode.LAN))
        lan_btn.draw()
        self.buttons.append(lan_btn)
        
        wan_btn = HolographicButton(self.canvas, 320, y, 130, 60, "🌐 WIDE", self.COLOR_SECONDARY,
                                    command=lambda: self._select_mode(TransferMode.WAN))
        wan_btn.draw()
        self.buttons.append(wan_btn)
        
        y += 80
        cancel_btn = HolographicButton(self.canvas, 250, y, 100, 35, "Cancel", "#666666",
                                       command=self._on_cancel)
        cancel_btn.draw()
        self.buttons.append(cancel_btn)
        
    # ============================================
    # RECIPIENT SELECTION
    # ============================================
    
    def _draw_recipients(self, y):
        mode_name = self.current_data.transfer_mode.value if self.current_data.transfer_mode else ""
        self.canvas.create_text(300, y, text=f"{mode_name} - Select Recipient", fill="#ffffff", font=("Segoe UI", 12))
        y += 35
        
        if self.current_data.recipients and len(self.current_data.recipients) > 0:
            for i, r in enumerate(self.current_data.recipients[:4]):
                btn = HolographicButton(self.canvas, 150, y, 300, 40, f"👤 {r}", self.COLOR_PRIMARY,
                                        command=lambda name=r: self._select_recipient(name))
                btn.draw()
                self.buttons.append(btn)
                y += 50
        else:
            self.canvas.create_text(300, y, text="No devices found", fill="#666666", font=("Segoe UI", 11))
            
    # ============================================
    # READY TO SEND
    # ============================================
    
    def _draw_ready(self, y):
        self.canvas.create_text(300, y, text=f"Sending to: {self.current_data.selected_recipient}",
                               fill=self.COLOR_SUCCESS, font=("Segoe UI", 12))
        y += 35
        self.canvas.create_text(300, y, text="🖐️ OPEN PALM to send", fill="#ffffff", font=("Segoe UI", 11))
        y += 28
        self.canvas.create_text(300, y, text="🤚 CLOSE FIST to cancel", fill="#a0a0a0", font=("Segoe UI", 10))
        
    # ============================================
    # PROGRESS
    # ============================================
    
    def _draw_progress(self, y):
        self.canvas.create_text(300, y, text="Transferring...", fill="#ffffff", font=("Segoe UI", 12))
        y += 30
        
        self.progress_bar = HolographicProgress(self.canvas, 100, y, 400, 20)
        self.progress_bar.set_progress(self.current_data.progress)
        self.progress_bar.draw()
        y += 40
        
        speed = f" • {self.current_data.speed}" if self.current_data.speed else ""
        self.canvas.create_text(300, y, text=f"{self.current_data.progress}%{speed}",
                               fill="#a0a0c0", font=("Segoe UI", 10))
        
    # ============================================
    # INCOMING REQUEST
    # ============================================
    
    def _draw_incoming(self, y):
        self.canvas.create_text(300, y, text=f"📨 {self.current_data.sender_name} wants to send:",
                               fill=self.COLOR_WARNING, font=("Segoe UI", 12))
        y += 28
        self.canvas.create_text(300, y, text=self.current_data.current_file,
                               fill="#ffffff", font=("Segoe UI", 11))
        y += 45
        self.canvas.create_text(300, y, text="🤜 CLOSE FIST to accept", fill=self.COLOR_SUCCESS, font=("Segoe UI", 11))
        y += 28
        self.canvas.create_text(300, y, text="👎 THUMBS DOWN to reject", fill=self.COLOR_ERROR, font=("Segoe UI", 10))
        
    # ============================================
    # COMPLETE / ERROR
    # ============================================
    
    def _draw_complete(self, y):
        self.canvas.create_text(300, y + 20, text="✅ Transfer Complete!", fill=self.COLOR_SUCCESS, font=("Segoe UI", 14, "bold"))
        self.root.after(3000, self.hide)
        
    def _draw_error(self, y):
        self.canvas.create_text(300, y + 20, text=f"❌ {self.current_data.error_message}",
                               fill=self.COLOR_ERROR, font=("Segoe UI", 12))
        
    # ============================================
    # HELPERS
    # ============================================
    
    def _get_title(self):
        titles = {
            UIState.ACTIVATED: "📤 Share File",
            UIState.MODE_SELECTED: "👥 Select Recipient",
            UIState.READY_TO_SEND: "✅ Ready to Send",
            UIState.TRANSFERRING: "📤 Sending...",
            UIState.INCOMING_REQUEST: "📨 Incoming Transfer",
            UIState.COMPLETE: "🎉 Complete",
            UIState.ERROR: "❌ Error",
        }
        return titles.get(self.current_data.state, "PalmSync")
        
    def _get_title_color(self):
        colors = {
            UIState.READY_TO_SEND: self.COLOR_SUCCESS,
            UIState.INCOMING_REQUEST: self.COLOR_WARNING,
            UIState.ERROR: self.COLOR_ERROR,
        }
        return colors.get(self.current_data.state, self.COLOR_PRIMARY)
        
    # ============================================
    # ACTIONS
    # ============================================
    
    def _select_mode(self, mode: TransferMode):
        self._push_state()
        self.current_data.transfer_mode = mode
        self.current_data.state = UIState.MODE_SELECTED
        self._update_ui()
        if self.on_mode_selected: self.on_mode_selected(mode)
        
    def _select_recipient(self, recipient: str):
        self._push_state()
        self.current_data.selected_recipient = recipient
        self.current_data.state = UIState.READY_TO_SEND
        self._update_ui()
        if self.on_recipient_selected: self.on_recipient_selected(recipient)
        
    def _on_cancel(self):
        self.hide()
        if self.on_cancel: self.on_cancel()


# ============================================
# TEST
# ============================================

def test_overlay():
    import time
    
    def on_mode(mode):
        print(f"Mode: {mode.value}")
        ui.update_recipients(["Bob's Laptop", "Alice's PC", "Charlie's Mac"])
        
    def on_recipient(r):
        print(f"Recipient: {r}")
        
    def on_cancel():
        print("Cancelled")
        
    ui = OverlayUI(on_mode_selected=on_mode, on_recipient_selected=on_recipient, on_cancel=on_cancel)
    time.sleep(1)
    
    ui.show(UIData(state=UIState.ACTIVATED, current_file="movie.mp4", file_size="3.2 GB"))
    
    print("\n✨ UI Active! Click LAN/WAN, then use ← BACK button!")
    print("Press Ctrl+C to stop...")
    
    try:
        while True: time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nDone!")

if __name__ == "__main__":
    test_overlay()