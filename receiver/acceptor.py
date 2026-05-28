"""
receiver/acceptor.py
Handles gesture-based acceptance/rejection of incoming transfers
Integrates with hand gesture detection
"""

import threading
import time
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

# Import our modules
from .listener import IncomingTransfer, RequestStatus

# Import gesture types (will be available from main controller)
class GestureType:
    CLOSE_FIST = "close_fist"
    OPEN_PALM = "open_palm"
    POINTING = "pointing"
    THUMBS_UP = "thumbs_up"
    THUMBS_DOWN = "thumbs_down"
    UNKNOWN = "unknown"

class AcceptanceMethod(Enum):
    GESTURE = "gesture"
    BUTTON = "button"
    AUTO = "auto"
    TIMEOUT = "timeout"

@dataclass
class AcceptanceResult:
    """Result of acceptance decision"""
    transfer_id: str
    accepted: bool
    method: AcceptanceMethod
    timestamp: float
    gesture_type: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'transfer_id': self.transfer_id,
            'accepted': self.accepted,
            'method': self.method.value,
            'timestamp': self.timestamp,
            'gesture_type': self.gesture_type
        }

class GestureAcceptor:
    """
    Handles gesture-based acceptance of incoming transfers
    Maps gestures to accept/reject actions
    """
    
    # Gesture mapping
    ACCEPT_GESTURES = [GestureType.CLOSE_FIST, GestureType.THUMBS_UP, GestureType.OPEN_PALM]
    REJECT_GESTURES = [GestureType.THUMBS_DOWN]
    
    def __init__(self, gesture_detector=None):
        """
        Initialize gesture acceptor
        
        Args:
            gesture_detector: HandDetector instance from gesture module
        """
        self.gesture_detector = gesture_detector
        
        # Current pending transfer
        self.pending_transfer: Optional[IncomingTransfer] = None
        
        # Callbacks
        self.on_accept = None
        self.on_reject = None
        self.on_timeout = None
        
        # State
        self.is_waiting = False
        self.wait_start_time = 0
        self.gesture_timeout = 10  # seconds to wait for gesture
        self.confirmation_frames = 3  # frames to confirm gesture
        
        # Gesture tracking
        self.gesture_buffer = []
        self.last_gesture_time = 0
        self.gesture_cooldown = 1.0  # seconds between gestures
        
        # Threading
        self.timeout_thread = None
        self.is_running = False
        
    def start_waiting(self, transfer: IncomingTransfer,
                      on_accept: Callable = None,
                      on_reject: Callable = None,
                      on_timeout: Callable = None):
        """
        Start waiting for acceptance gesture
        
        Args:
            transfer: Incoming transfer request
            on_accept: Callback when accepted
            on_reject: Callback when rejected
            on_timeout: Callback on timeout
        """
        self.pending_transfer = transfer
        self.on_accept = on_accept
        self.on_reject = on_reject
        self.on_timeout = on_timeout
        
        self.is_waiting = True
        self.wait_start_time = time.time()
        self.gesture_buffer.clear()
        
        # Register gesture callback
        if self.gesture_detector:
            self.gesture_detector.set_callback(self._on_gesture_detected)
            
        # Start timeout thread
        self.timeout_thread = threading.Thread(target=self._timeout_loop, daemon=True)
        self.timeout_thread.start()
        
        print(f"[Acceptor] 🖐️ Waiting for gesture to accept/reject")
        print(f"            Transfer: {transfer.filename}")
        print(f"            Accept gestures: CLOSE_FIST, THUMBS_UP, OPEN_PALM")
        print(f"            Reject gestures: THUMBS_DOWN")
        print(f"            Timeout: {self.gesture_timeout}s")
        
    def _on_gesture_detected(self, gesture_type: str, pointer_pos, frame):
        """Handle detected gesture"""
        if not self.is_waiting or not self.pending_transfer:
            return
            
        current_time = time.time()
        
        # Check cooldown
        if current_time - self.last_gesture_time < self.gesture_cooldown:
            return
            
        # Add to buffer
        self.gesture_buffer.append(gesture_type)
        
        # Keep only recent frames
        if len(self.gesture_buffer) > self.confirmation_frames:
            self.gesture_buffer.pop(0)
            
        # Check if gesture confirmed
        if self._is_gesture_confirmed():
            confirmed_gesture = self.gesture_buffer[-1]
            self.last_gesture_time = current_time
            
            if confirmed_gesture in self.ACCEPT_GESTURES:
                self._accept(AcceptanceMethod.GESTURE, confirmed_gesture)
            elif confirmed_gesture in self.REJECT_GESTURES:
                self._reject(AcceptanceMethod.GESTURE, confirmed_gesture)
                
    def _is_gesture_confirmed(self) -> bool:
        """Check if gesture is consistently detected"""
        if len(self.gesture_buffer) < self.confirmation_frames:
            return False
            
        # All recent gestures must be the same
        return all(g == self.gesture_buffer[0] for g in self.gesture_buffer)
        
    def _timeout_loop(self):
        """Wait for timeout"""
        start_time = time.time()
        
        while self.is_waiting:
            elapsed = time.time() - start_time
            
            if elapsed >= self.gesture_timeout:
                self._on_timeout()
                break
                
            time.sleep(0.1)
            
    def _accept(self, method: AcceptanceMethod, gesture_type: str = None):
        """Accept the transfer"""
        if not self.is_waiting:
            return
            
        self.is_waiting = False
        
        result = AcceptanceResult(
            transfer_id=self.pending_transfer.transfer_id,
            accepted=True,
            method=method,
            timestamp=time.time(),
            gesture_type=gesture_type
        )
        
        print(f"[Acceptor] ✅ Transfer ACCEPTED via {method.value}")
        if gesture_type:
            print(f"            Gesture: {gesture_type}")
            
        if self.on_accept:
            self.on_accept(result)
            
        self._cleanup()
        
    def _reject(self, method: AcceptanceMethod, gesture_type: str = None):
        """Reject the transfer"""
        if not self.is_waiting:
            return
            
        self.is_waiting = False
        
        result = AcceptanceResult(
            transfer_id=self.pending_transfer.transfer_id,
            accepted=False,
            method=method,
            timestamp=time.time(),
            gesture_type=gesture_type
        )
        
        print(f"[Acceptor] ❌ Transfer REJECTED via {method.value}")
        if gesture_type:
            print(f"            Gesture: {gesture_type}")
            
        if self.on_reject:
            self.on_reject(result)
            
        self._cleanup()
        
    def _on_timeout(self):
        """Handle timeout"""
        if not self.is_waiting:
            return
            
        self.is_waiting = False
        
        result = AcceptanceResult(
            transfer_id=self.pending_transfer.transfer_id,
            accepted=False,
            method=AcceptanceMethod.TIMEOUT,
            timestamp=time.time()
        )
        
        print(f"[Acceptor] ⏰ Transfer TIMEOUT after {self.gesture_timeout}s")
        
        if self.on_timeout:
            self.on_timeout(result)
        elif self.on_reject:
            self.on_reject(result)  # Treat timeout as rejection
            
        self._cleanup()
        
    def accept_manually(self):
        """Manually accept (UI button fallback)"""
        if self.is_waiting:
            self._accept(AcceptanceMethod.BUTTON)
            
    def reject_manually(self):
        """Manually reject (UI button fallback)"""
        if self.is_waiting:
            self._reject(AcceptanceMethod.BUTTON)
            
    def cancel_waiting(self):
        """Cancel waiting (user dismissed)"""
        self.is_waiting = False
        self._cleanup()
        
    def _cleanup(self):
        """Clean up after acceptance/rejection"""
        # Unregister gesture callback
        if self.gesture_detector:
            self.gesture_detector.set_callback(None)
            
        self.pending_transfer = None
        self.gesture_buffer.clear()
        
    def is_waiting_for(self, transfer_id: str) -> bool:
        """Check if waiting for specific transfer"""
        return (self.is_waiting and 
                self.pending_transfer and 
                self.pending_transfer.transfer_id == transfer_id)
                
    def get_remaining_time(self) -> int:
        """Get remaining wait time in seconds"""
        if not self.is_waiting:
            return 0
        elapsed = time.time() - self.wait_start_time
        return max(0, int(self.gesture_timeout - elapsed))


class TransferAcceptor:
    """
    Main acceptor controller
    Manages the complete acceptance flow
    """
    
    def __init__(self, listener, gesture_detector=None, ui_overlay=None):
        """
        Initialize transfer acceptor
        
        Args:
            listener: ReceiverListener instance
            gesture_detector: HandDetector instance
            ui_overlay: OverlayUI instance
        """
        self.listener = listener
        self.gesture_detector = gesture_detector
        self.ui_overlay = ui_overlay
        
        # Gesture acceptor
        self.gesture_acceptor = GestureAcceptor(gesture_detector)
        
        # Current transfer being processed
        self.current_transfer: Optional[IncomingTransfer] = None
        
        # State
        self.is_processing = False
        
        # Callbacks
        self.on_accepted = None
        self.on_rejected = None
        
    def start(self):
        """Start listening for transfers"""
        # Register callback with listener
        self.listener.on_transfer_request = self._on_transfer_received
        
        print("[TransferAcceptor] ✅ Started")
        
    def _on_transfer_received(self, transfer: IncomingTransfer):
        """Handle incoming transfer request"""
        if self.is_processing:
            print("[TransferAcceptor] Already processing a transfer")
            # Could queue or auto-reject
            return
            
        self.current_transfer = transfer
        self.is_processing = True
        
        print(f"\n[TransferAcceptor] {'='*50}")
        print(f"📨 NEW TRANSFER REQUEST")
        print(f"   From: {transfer.sender_name}")
        print(f"   File: {transfer.filename}")
        print(f"   Size: {transfer.format_size()}")
        print(f"   Type: {transfer.transfer_type.value}")
        print(f"{'='*50}\n")
        
        # Show UI notification
        if self.ui_overlay:
            from ui.overlay import UIData, UIState
            ui_data = UIData(
                state=UIState.INCOMING_REQUEST,
                current_file=transfer.filename,
                file_size=transfer.format_size(),
                sender_name=transfer.sender_name,
                message=f"{transfer.sender_name} wants to send:\n{transfer.filename}"
            )
            self.ui_overlay.show(ui_data)
            
        # Start waiting for gesture
        self.gesture_acceptor.start_waiting(
            transfer,
            on_accept=self._on_accepted,
            on_reject=self._on_rejected,
            on_timeout=self._on_timeout
        )
        
    def _on_accepted(self, result: AcceptanceResult):
        """Handle acceptance"""
        print(f"\n[TransferAcceptor] ✅ Transfer accepted!")
        
        # Accept with listener
        self.listener.accept_transfer(result.transfer_id)
        
        # Update UI
        if self.ui_overlay:
            from ui.overlay import UIState
            self.ui_overlay.set_state(
                UIState.TRANSFERRING,
                "Transfer accepted! Preparing to receive..."
            )
            
        # Notify callback
        if self.on_accepted:
            self.on_accepted(self.current_transfer, result)
            
        self.is_processing = False
        
    def _on_rejected(self, result: AcceptanceResult):
        """Handle rejection"""
        print(f"\n[TransferAcceptor] ❌ Transfer rejected")
        
        # Reject with listener
        self.listener.reject_transfer(result.transfer_id)
        
        # Update UI
        if self.ui_overlay:
            self.ui_overlay.hide()
            
        # Notify callback
        if self.on_rejected:
            self.on_rejected(self.current_transfer, result)
            
        self.current_transfer = None
        self.is_processing = False
        
    def _on_timeout(self, result: AcceptanceResult):
        """Handle timeout"""
        print(f"\n[TransferAcceptor] ⏰ Transfer timed out")
        
        # Reject with listener
        self.listener.reject_transfer(result.transfer_id)
        
        # Update UI
        if self.ui_overlay:
            from ui.overlay import UIState, UIData
            ui_data = UIData(
                state=UIState.EXPIRED,
                message="Transfer request expired"
            )
            self.ui_overlay.show(ui_data)
            
        # Notify callback
        if self.on_rejected:
            self.on_rejected(self.current_transfer, result)
            
        self.current_transfer = None
        self.is_processing = False
        
    def manual_accept(self):
        """Manually accept current transfer"""
        if self.is_processing:
            self.gesture_acceptor.accept_manually()
            
    def manual_reject(self):
        """Manually reject current transfer"""
        if self.is_processing:
            self.gesture_acceptor.reject_manually()
            
    def get_current_transfer(self) -> Optional[IncomingTransfer]:
        """Get currently pending transfer"""
        return self.current_transfer
        
    def get_remaining_time(self) -> int:
        """Get remaining time for current transfer"""
        return self.gesture_acceptor.get_remaining_time()
        
    def cancel_current(self):
        """Cancel current transfer processing"""
        if self.is_processing:
            self.gesture_acceptor.cancel_waiting()
            self.current_transfer = None
            self.is_processing = False


# ============================================
# TEST FUNCTION
# ============================================

def test_acceptor():
    """Test acceptor functionality"""
    print("🖐️ Transfer Acceptor Test")
    print("=" * 50)
    
    # Mock listener
    class MockListener:
        def __init__(self):
            self.on_transfer_request = None
            
    # Mock gesture detector
    class MockGestureDetector:
        def __init__(self):
            self.callback = None
            
        def set_callback(self, callback):
            self.callback = callback
            
        def simulate_gesture(self, gesture_type):
            if self.callback:
                self.callback(gesture_type, None, None)
                
    # Create components
    listener = MockListener()
    gesture_detector = MockGestureDetector()
    
    acceptor = TransferAcceptor(listener, gesture_detector)
    
    def on_accepted(transfer, result):
        print(f"\n✅ ACCEPTED: {transfer.filename}")
        print(f"   Method: {result.method.value}")
        
    def on_rejected(transfer, result):
        print(f"\n❌ REJECTED: {transfer.filename}")
        print(f"   Method: {result.method.value}")
        
    acceptor.on_accepted = on_accepted
    acceptor.on_rejected = on_rejected
    acceptor.start()
    
    # Simulate incoming transfer
    print("\n1️⃣ Simulating incoming transfer...")
    transfer = IncomingTransfer(
        transfer_id="test-123",
        sender_id="sender-456",
        sender_name="Alice",
        sender_ip="192.168.1.10",
        transfer_type="lan",
        filename="vacation.mp4",
        file_size=50 * 1024 * 1024,
        file_type="video/mp4",
        total_chunks=50
    )
    
    # Trigger transfer
    listener.on_transfer_request(transfer)
    
    # Simulate gesture detection
    print("\n2️⃣ Simulating CLOSE_FIST gesture...")
    time.sleep(1)
    gesture_detector.simulate_gesture(GestureType.CLOSE_FIST)
    gesture_detector.simulate_gesture(GestureType.CLOSE_FIST)
    gesture_detector.simulate_gesture(GestureType.CLOSE_FIST)
    
    time.sleep(2)
    print("\n✅ Test complete!")


if __name__ == "__main__":
    test_acceptor()