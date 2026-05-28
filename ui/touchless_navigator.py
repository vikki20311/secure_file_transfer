"""
ui/touchless_navigator.py
Touchless UI navigation using finger pointing
Hover to select, dwell to click
"""

import cv2
import mediapipe as mp
import numpy as np
import time
import threading
from typing import Callable, Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum

class UIElement(Enum):
    MODE_LAN = "mode_lan"
    MODE_WAN = "mode_wan"
    RECIPIENT_1 = "recipient_1"
    RECIPIENT_2 = "recipient_2"
    RECIPIENT_3 = "recipient_3"
    SEND_BUTTON = "send_button"
    CANCEL_BUTTON = "cancel_button"
    ACCEPT_BUTTON = "accept_button"
    REJECT_BUTTON = "reject_button"

@dataclass
class UIButton:
    """Virtual button in the overlay"""
    element_id: UIElement
    x: int  # Screen coordinate
    y: int
    width: int
    height: int
    label: str
    is_hovered: bool = False
    hover_start_time: float = 0.0
    
    def contains_point(self, px: int, py: int) -> bool:
        """Check if point is inside button"""
        return (self.x <= px <= self.x + self.width and 
                self.y <= py <= self.y + self.height)

class TouchlessNavigator:
    """
    Navigate UI using finger pointing - No touch required!
    """
    
    DWELL_TIME = 1.0  # Hover for 1 second to "click"
    SMOOTHING_FRAMES = 5  # Smooth cursor movement
    
    def __init__(self, overlay_window):
        self.overlay = overlay_window
        self.screen_width = overlay_window.root.winfo_screenwidth()
        self.screen_height = overlay_window.root.winfo_screenheight()
        
        # Hand tracking
        self.mp_hands = mp.solutions.hands
        self.hands = self.mp_hands.Hands(
            static_image_mode=False,
            max_num_hands=1,
            min_detection_confidence=0.7,
            min_tracking_confidence=0.5
        )
        
        # Cursor state
        self.cursor_x = self.screen_width // 2
        self.cursor_y = self.screen_height // 2
        self.cursor_history: List[Tuple[int, int]] = []
        self.is_pointing = False
        
        # UI Buttons (will be updated by overlay)
        self.buttons: List[UIButton] = []
        self.hovered_button: Optional[UIButton] = None
        self.selected_button: Optional[UIButton] = None
        
        # Callbacks
        self.on_button_click = None
        self.on_cursor_move = None
        
        # Threading
        self.is_running = False
        self.tracking_thread = None
        
        # Visual feedback
        self.show_cursor = True
        self.cursor_color = (0, 255, 0)  # Green
        self.hover_color = (255, 165, 0)  # Orange
        self.progress_color = (0, 255, 255)  # Yellow
        
    def start(self):
        """Start touchless navigation"""
        self.is_running = True
        self.tracking_thread = threading.Thread(target=self._tracking_loop, daemon=True)
        self.tracking_thread.start()
        print("[TouchlessNavigator] ✅ Touchless navigation active")
        print("                    👆 Point to move cursor")
        print(f"                    ⏱️  Hover {self.DWELL_TIME}s to select")
        
    def stop(self):
        """Stop navigation"""
        self.is_running = False
        
    def update_buttons(self, buttons: List[UIButton]):
        """Update UI button positions from overlay"""
        self.buttons = buttons
        
    def _tracking_loop(self):
        """Main tracking loop"""
        cap = cv2.VideoCapture(0)
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
        
        while self.is_running:
            ret, frame = cap.read()
            if not ret:
                continue
                
            frame = cv2.flip(frame, 1)
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = self.hands.process(rgb_frame)
            
            if results.multi_hand_landmarks:
                for hand_landmarks in results.multi_hand_landmarks:
                    # Check if pointing gesture
                    if self._is_pointing(hand_landmarks):
                        # Get index finger tip position
                        index_tip = hand_landmarks.landmark[8]
                        
                        # Map to screen coordinates
                        screen_x = int(index_tip.x * self.screen_width)
                        screen_y = int(index_tip.y * self.screen_height)
                        
                        # Smooth cursor movement
                        self._update_cursor_position(screen_x, screen_y)
                        self.is_pointing = True
                        
                        # Check button hover
                        self._check_hover()
                        
                        # Draw cursor on frame (for debugging)
                        cv2.circle(frame, 
                                 (int(index_tip.x * 640), int(index_tip.y * 480)), 
                                 10, self.cursor_color, -1)
                    else:
                        self.is_pointing = False
                        self._clear_hover()
            else:
                self.is_pointing = False
                self._clear_hover()
                
            # Show frame with cursor
            if self.show_cursor:
                cv2.imshow("PalmSync - Touchless Navigation", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
                    
        cap.release()
        cv2.destroyAllWindows()
        
    def _is_pointing(self, landmarks) -> bool:
        """Check if hand is making pointing gesture"""
        # Index finger extended, others curled
        index_tip = landmarks.landmark[8]
        index_pip = landmarks.landmark[6]
        middle_tip = landmarks.landmark[12]
        middle_pip = landmarks.landmark[10]
        ring_tip = landmarks.landmark[16]
        ring_pip = landmarks.landmark[14]
        pinky_tip = landmarks.landmark[20]
        pinky_pip = landmarks.landmark[18]
        
        # Index extended (tip above PIP)
        index_extended = index_tip.y < index_pip.y
        
        # Others curled (tip below PIP)
        middle_curled = middle_tip.y > middle_pip.y
        ring_curled = ring_tip.y > ring_pip.y
        pinky_curled = pinky_tip.y > pinky_pip.y
        
        return index_extended and middle_curled and ring_curled and pinky_curled
        
    def _update_cursor_position(self, x: int, y: int):
        """Smooth cursor movement"""
        self.cursor_history.append((x, y))
        
        if len(self.cursor_history) > self.SMOOTHING_FRAMES:
            self.cursor_history.pop(0)
            
        # Average for smoothing
        avg_x = sum(p[0] for p in self.cursor_history) // len(self.cursor_history)
        avg_y = sum(p[1] for p in self.cursor_history) // len(self.cursor_history)
        
        self.cursor_x = avg_x
        self.cursor_y = avg_y
        
        if self.on_cursor_move:
            self.on_cursor_move(self.cursor_x, self.cursor_y)
            
    def _check_hover(self):
        """Check if cursor is hovering over any button"""
        for button in self.buttons:
            if button.contains_point(self.cursor_x, self.cursor_y):
                if self.hovered_button != button:
                    self._clear_hover()
                    self.hovered_button = button
                    button.is_hovered = True
                    button.hover_start_time = time.time()
                    self._on_hover_start(button)
                    
                # Check dwell time for click
                if button.hover_start_time > 0:
                    dwell_duration = time.time() - button.hover_start_time
                    
                    # Update progress
                    progress = min(dwell_duration / self.DWELL_TIME, 1.0)
                    self._update_hover_progress(button, progress)
                    
                    # Trigger click when dwell complete
                    if dwell_duration >= self.DWELL_TIME:
                        self._trigger_click(button)
                        button.hover_start_time = 0  # Reset
                        
                return
                
        # No button hovered
        self._clear_hover()
        
    def _clear_hover(self):
        """Clear hover state"""
        if self.hovered_button:
            self.hovered_button.is_hovered = False
            self.hovered_button.hover_start_time = 0
            self._on_hover_end(self.hovered_button)
            self.hovered_button = None
            
    def _on_hover_start(self, button: UIButton):
        """Called when hovering starts"""
        print(f"[Touchless] 👆 Hovering: {button.label}")
        
    def _on_hover_end(self, button: UIButton):
        """Called when hovering ends"""
        print(f"[Touchless] Hover ended: {button.label}")
        
    def _update_hover_progress(self, button: UIButton, progress: float):
        """Update hover progress visualization"""
        # This would draw a progress ring around the cursor
        pass
        
    def _trigger_click(self, button: UIButton):
        """Trigger button click"""
        print(f"[Touchless] ✅ SELECTED: {button.label}")
        
        if self.on_button_click:
            self.on_button_click(button.element_id, button)
            
    def get_cursor_position(self) -> Tuple[int, int]:
        """Get current cursor position"""
        return self.cursor_x, self.cursor_y
        
    def draw_cursor_overlay(self, frame):
        """Draw cursor and hover indicators on frame"""
        # Draw cursor
        cv2.circle(frame, (self.cursor_x, self.cursor_y), 15, self.cursor_color, 2)
        cv2.circle(frame, (self.cursor_x, self.cursor_y), 5, self.cursor_color, -1)
        
        # Draw hover progress if any
        if self.hovered_button and self.hovered_button.hover_start_time > 0:
            progress = min((time.time() - self.hovered_button.hover_start_time) / self.DWELL_TIME, 1.0)
            
            # Draw progress ring
            center = (self.cursor_x, self.cursor_y)
            radius = 20
            angle = int(360 * progress)
            
            cv2.ellipse(frame, center, (radius, radius), -90, 0, angle, self.progress_color, 3)