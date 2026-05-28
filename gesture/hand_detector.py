"""
gesture/hand_detector.py
Real-time hand gesture detection using MediaPipe
Detects: CLOSE FIST, OPEN PALM, POINTING
"""

import cv2
import mediapipe as mp
import numpy as np
import threading
import time
from collections import deque
from enum import Enum

class GestureType(Enum):
    CLOSE_FIST = "close_fist"      # Activation / Accept
    OPEN_PALM = "open_palm"        # Send
    POINTING = "pointing"          # Select
    THUMBS_UP = "thumbs_up"        # Alternative accept
    THUMBS_DOWN = "thumbs_down"    # Reject
    UNKNOWN = "unknown"

class HandDetector:
    def __init__(self, callback=None):
        """
        Initialize MediaPipe hand detector
        
        Args:
            callback: Function called when gesture detected
                     callback(gesture_type, hand_landmarks, frame)
        """
        self.mp_hands = mp.solutions.hands
        self.hands = self.mp_hands.Hands(
            static_image_mode=False,
            max_num_hands=1,
            min_detection_confidence=0.7,
            min_tracking_confidence=0.5
        )
        self.mp_draw = mp.solutions.drawing_utils
        
        self.callback = callback
        self.is_running = False
        self.camera_thread = None
        self.cap = None
        
        # Gesture confirmation (prevent false positives)
        self.gesture_history = deque(maxlen=5)
        self.last_gesture_time = 0
        self.gesture_cooldown = 1.0  # seconds
        
        # Landmark indices
        self.FINGER_TIPS = [4, 8, 12, 16, 20]  # Thumb, Index, Middle, Ring, Pinky
        self.FINGER_PIPS = [3, 6, 10, 14, 18]  # PIP joints
        
    def start(self):
        """Start camera capture in background thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.camera_thread = threading.Thread(target=self._camera_loop, daemon=True)
        self.camera_thread.start()
        print("[HandDetector] Started background detection")
        
    def stop(self):
        """Stop camera capture"""
        self.is_running = False
        if self.cap:
            self.cap.release()
        if self.camera_thread:
            self.camera_thread.join(timeout=2.0)
        print("[HandDetector] Stopped")
        
    def _camera_loop(self):
        """Main camera capture loop (runs in background)"""
        self.cap = cv2.VideoCapture(0)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, 320)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 240)
        self.cap.set(cv2.CAP_PROP_FPS, 30)
        
        while self.is_running:
            ret, frame = self.cap.read()
            if not ret:
                continue
                
            # Flip horizontally for natural interaction
            frame = cv2.flip(frame, 1)
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            
            # Detect hands
            results = self.hands.process(rgb_frame)
            
            if results.multi_hand_landmarks:
                for hand_landmarks in results.multi_hand_landmarks:
                    gesture = self._classify_gesture(hand_landmarks)
                    
                    # Draw landmarks (optional - can disable for performance)
                    self.mp_draw.draw_landmarks(
                        frame, hand_landmarks, self.mp_hands.HAND_CONNECTIONS
                    )
                    
                    # Handle gesture with cooldown
                    if gesture != GestureType.UNKNOWN:
                        self._handle_gesture(gesture, hand_landmarks, frame)
            
            # Small sleep to prevent CPU overuse
            time.sleep(0.01)
            
    def _classify_gesture(self, landmarks):
        """
        Classify hand gesture from landmarks
        
        Returns:
            GestureType enum
        """
        # Get finger states (extended = True, curled = False)
        fingers_extended = self._get_finger_states(landmarks)
        
        # CLOSE FIST: All fingers curled
        if not any(fingers_extended):
            return GestureType.CLOSE_FIST
            
        # OPEN PALM: All fingers extended
        if all(fingers_extended):
            return GestureType.OPEN_PALM
            
        # POINTING: Only index finger extended
        if fingers_extended[1] and not any(fingers_extended[2:]):
            return GestureType.POINTING
            
        # THUMBS UP: Only thumb extended upward
        if fingers_extended[0] and not any(fingers_extended[1:]):
            thumb_tip = landmarks.landmark[4]
            thumb_ip = landmarks.landmark[3]
            if thumb_tip.y < thumb_ip.y:  # Thumb pointing up
                return GestureType.THUMBS_UP
                
        # THUMBS DOWN: Thumb extended downward
        if fingers_extended[0] and not any(fingers_extended[1:]):
            thumb_tip = landmarks.landmark[4]
            thumb_ip = landmarks.landmark[3]
            if thumb_tip.y > thumb_ip.y:  # Thumb pointing down
                return GestureType.THUMBS_DOWN
                
        return GestureType.UNKNOWN
        
    def _get_finger_states(self, landmarks):
        """
        Determine which fingers are extended
        
        Returns:
            List of 5 booleans [thumb, index, middle, ring, pinky]
        """
        fingers = []
        
        # Thumb: Compare x-coordinate (horizontal extension)
        thumb_tip = landmarks.landmark[4]
        thumb_ip = landmarks.landmark[3]
        thumb_mcp = landmarks.landmark[2]
        thumb_extended = abs(thumb_tip.x - thumb_mcp.x) > 0.1
        fingers.append(thumb_extended)
        
        # Other 4 fingers: Compare y-coordinate (tip above PIP joint)
        for tip_idx, pip_idx in zip(self.FINGER_TIPS[1:], self.FINGER_PIPS[1:]):
            tip = landmarks.landmark[tip_idx]
            pip = landmarks.landmark[pip_idx]
            extended = tip.y < pip.y  # Tip is higher than PIP joint
            fingers.append(extended)
            
        return fingers
        
    def _handle_gesture(self, gesture, landmarks, frame):
        """Handle detected gesture with confirmation and cooldown"""
        current_time = time.time()
        
        # Add to history
        self.gesture_history.append(gesture)
        
        # Check cooldown
        if current_time - self.last_gesture_time < self.gesture_cooldown:
            return
            
        # Confirm gesture (must appear consistently)
        if self._is_gesture_confirmed(gesture):
            self.last_gesture_time = current_time
            
            if self.callback:
                # Get pointing coordinates if POINTING gesture
                pointer_pos = None
                if gesture == GestureType.POINTING:
                    index_tip = landmarks.landmark[8]
                    pointer_pos = (index_tip.x, index_tip.y)
                    
                self.callback(gesture, pointer_pos, frame)
                
    def _is_gesture_confirmed(self, gesture):
        """Check if gesture appears consistently in history"""
        if len(self.gesture_history) < 3:
            return False
            
        # Check last 3 frames
        last_three = list(self.gesture_history)[-3:]
        return all(g == gesture for g in last_three)
        
    def get_pointer_position(self, landmarks):
        """Get normalized (0-1) coordinates of index finger tip"""
        index_tip = landmarks.landmark[8]
        return (index_tip.x, index_tip.y)
        
    def set_callback(self, callback):
        """Set callback function for gesture events"""
        self.callback = callback
        
    def set_cooldown(self, seconds):
        """Set minimum time between gesture triggers"""
        self.gesture_cooldown = seconds


# ============================================
# TEST FUNCTION (Run this file directly)
# ============================================

def test_gesture_detector():
    """Test the hand detector with visual feedback"""
    
    def on_gesture(gesture_type, pointer_pos, frame):
        """Callback when gesture detected"""
        print(f"[GESTURE DETECTED] {gesture_type.value}")
        
        # Add text to frame
        cv2.putText(
            frame, 
            f"Gesture: {gesture_type.value}", 
            (10, 30), 
            cv2.FONT_HERSHEY_SIMPLEX, 
            0.7, 
            (0, 255, 0), 
            2
        )
        
    detector = HandDetector(callback=on_gesture)
    detector.start()
    
    print("Hand Detector Test Running...")
    print("Available gestures:")
    print("  - CLOSE FIST: Make a fist")
    print("  - OPEN PALM: Open hand fully")
    print("  - POINTING: Point with index finger")
    print("  - THUMBS UP/DOWN: Thumb gesture")
    print("\nPress Ctrl+C to stop...")
    
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nStopping...")
        detector.stop()
        

if __name__ == "__main__":
    test_gesture_detector()